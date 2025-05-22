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

# Fungsi untuk mengirim data suhu
def send_temperature_data():
    while True:
        for gudang_id in gudang_ids:
            # Generate data suhu acak: 75-90°C
            suhu = round(random.uniform(75, 90), 1)
            
            # Membuat payload data
            data = {
                "gudang_id": gudang_id,
                "suhu": suhu,
                "timestamp": int(time.time())
            }
            
            # Mengirim data ke topik Kafka
            producer.send('sensor-suhu-gudang', data)
            print(f"Mengirim data suhu: {data}")
        
        # Flush untuk memastikan semua pesan terkirim
        producer.flush()
        # Tunggu 1 detik sebelum pengiriman berikutnya
        time.sleep(1)

if __name__ == "__main__":
    print("Producer Sensor Suhu dimulai...")
    try:
        send_temperature_data()
    except KeyboardInterrupt:
        print("Producer Sensor Suhu dihentikan.")
