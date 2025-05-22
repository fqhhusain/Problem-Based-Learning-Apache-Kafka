#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# Inisialisasi Spark Session
spark = SparkSession \
    .builder \
    .appName("KafkaSensorMonitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# Nonaktifkan logging info untuk mengurangi output
spark.sparkContext.setLogLevel("WARN")

# Schema untuk data sensor suhu
schema_suhu = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Schema untuk data sensor kelembaban
schema_kelembaban = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Fungsi untuk memproses data suhu
def process_temperature_data():
    # Membaca data dari Kafka topic
    df_suhu = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-suhu-gudang") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse data JSON dari Kafka
    df_suhu = df_suhu.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema_suhu).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Filter untuk suhu tinggi
    suhu_tinggi = df_suhu.filter(col("suhu") > 80)
    
    # Menampilkan peringatan suhu tinggi
    query_suhu = suhu_tinggi \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return df_suhu

# Fungsi untuk memproses data kelembaban
def process_humidity_data():
    # Membaca data dari Kafka topic
    df_kelembaban = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-kelembaban-gudang") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse data JSON dari Kafka
    df_kelembaban = df_kelembaban.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema_kelembaban).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Filter untuk kelembaban tinggi
    kelembaban_tinggi = df_kelembaban.filter(col("kelembaban") > 70)
    
    # Menampilkan peringatan kelembaban tinggi
    query_kelembaban = kelembaban_tinggi \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return df_kelembaban

# Fungsi untuk menggabungkan dan memproses data dari kedua sensor
def join_and_process():
    # Dapatkan dataframe untuk kedua sensor
    df_suhu = process_temperature_data()
    df_kelembaban = process_humidity_data()
    
    # Menggunakan window pada kedua stream untuk join
    df_suhu_windowed = df_suhu \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds"), col("gudang_id")) \
        .max("suhu") \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("gudang_id"),
            col("max(suhu)").alias("suhu")
        )
    
    df_kelembaban_windowed = df_kelembaban \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds"), col("gudang_id")) \
        .max("kelembaban") \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("gudang_id"),
            col("max(kelembaban)").alias("kelembaban")
        )
    
    # Join kedua stream berdasarkan window dan gudang_id
    joined_df = df_suhu_windowed.join(
        df_kelembaban_windowed,
        (df_suhu_windowed.window_start == df_kelembaban_windowed.window_start) & 
        (df_suhu_windowed.window_end == df_kelembaban_windowed.window_end) & 
        (df_suhu_windowed.gudang_id == df_kelembaban_windowed.gudang_id),
        "inner"
    ).select(
        df_suhu_windowed.gudang_id,
        df_suhu_windowed.suhu,
        df_kelembaban_windowed.kelembaban
    )
    
    # Menambahkan status berdasarkan kondisi
    joined_df = joined_df.withColumn(
        "status",
        when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
        .when(col("suhu") > 80, "Suhu tinggi, kelembaban normal")
        .when(col("kelembaban") > 70, "Kelembaban tinggi, suhu aman")
        .otherwise("Aman")
    )
    
    # Menampilkan hasil join dengan format yang sesuai
    query_joined = joined_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return query_joined

if __name__ == "__main__":
    print("PySpark Kafka Consumer dimulai...")
    try:
        # Jalankan proses penggabungan
        query = join_and_process()
        
        # Tunggu hingga proses streaming selesai
        query.awaitTermination()
    except KeyboardInterrupt:
        print("PySpark Kafka Consumer dihentikan.")
        spark.stop()
