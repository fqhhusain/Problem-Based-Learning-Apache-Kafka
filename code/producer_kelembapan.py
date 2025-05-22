#!/usr/bin/env python3
from kafka import KafkaProducer
import json
import time
import random

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Sesuaikan dengan port yang diexpose oleh container Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List ID gudang
gudang_ids = ["G1", "G2", "G3", "G4"]

# Fungsi untuk mengirim data kelembaban
def send_humidity_data():
    while True:
        for gudang_id in gudang_ids:
            # Generate data kelembaban acak: 65-80%
            kelembaban = round(random.uniform(65, 80), 1)
            
            # Membuat payload data
            data = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
                "timestamp": int(time.time())
            }
            
            # Mengirim data ke topik Kafka
            producer.send('sensor-kelembaban-gudang', data)
            print(f"Mengirim data kelembaban: {data}")
        
        # Flush untuk memastikan semua pesan terkirim
        producer.flush()
        # Tunggu 1 detik sebelum pengiriman berikutnya
        time.sleep(1)

if __name__ == "__main__":
    print("Producer Sensor Kelembaban dimulai...")
    try:
        send_humidity_data()
    except KeyboardInterrupt:
        print("Producer Sensor Kelembaban dihentikan.")
