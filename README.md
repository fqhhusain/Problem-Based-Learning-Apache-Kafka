# Problem-Based-Learning-Apache-Kafka

## 🎯 Latar Belakang Masalah
Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor: <br/>
- Sensor Suhu
- Sensor kelembapan

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembapan berlebih.

## 📋 Tugas Mahasiswa
1. Buat Topik Kafka
Buat dua topik di Apache Kafka:

- sensor-suhu-gudang
- sensor-kelembapan-gudang

Topik ini akan digunakan untuk menerima data dari masing-masing sensor secara real-time.

2. Simulasikan Data Sensor (Producer Kafka)
Buat dua Kafka producer terpisah:

a. Producer Suhu
Kirim data setiap detik

Format:

{"gudang_id": "G1", "suhu": 82}
b. Producer kelembapan
Kirim data setiap detik

Format:

{"gudang_id": "G1", "kelembapan": 75}
Gunakan minimal 3 gudang: G1, G2, G3.

3. Konsumsi dan Olah Data dengan PySpark
a. Buat PySpark Consumer
Konsumsi data dari kedua topik Kafka.

b. Lakukan Filtering:
Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi

kelembapan > 70% → tampilkan sebagai peringatan kelembapan tinggi

Contoh Output:
[Peringatan Suhu Tinggi]
Gudang G2: Suhu 85°C

[Peringatan kelembapan Tinggi]
Gudang G3: kelembapan 74%
4. Gabungkan Stream dari Dua Sensor
Lakukan join antar dua stream berdasarkan gudang_id dan window waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

c. Buat Peringatan Gabungan:
Jika ditemukan suhu > 80°C dan kelembapan > 70% pada gudang yang sama, tampilkan peringatan kritis.

✅ Contoh Output Gabungan:
[PERINGATAN KRITIS]
Gudang G1:
- Suhu: 84°C
- kelembapan: 73%
- Status: Bahaya tinggi! Barang berisiko rusak

Gudang G2:
- Suhu: 78°C
- kelembapan: 68%
- Status: Aman

Gudang G3:
- Suhu: 85°C
- kelembapan: 65%
- Status: Suhu tinggi, kelembapan normal

Gudang G4:
- Suhu: 79°C
- kelembapan: 75%
- Status: kelembapan tinggi, suhu aman

## Prerequisites
- docker
- wsl

## Step by step penyelesaian
1. Installing Kafka Zookeeper
```
nano docker-compose.yml
```
```
docker-compose up -d
```
![image](https://github.com/user-attachments/assets/258b9f1f-4df2-4405-a639-b5b9ec8c16ec)

```
docker ps
```
![image](https://github.com/user-attachments/assets/463baf6b-857f-4d50-83f3-29194bcaceb2)

simpan container id

2. Buat topik suhu dan kelembapan
topik suhu
```
docker exec -it <container id> kafka-topics.sh --create --topic sensor-suhu-gudang \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3
```
topik kelembapan
```
docker exec -it <containerId> kafka-topics.sh --create --topic sensor-kelembapan-gudang \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3
```
3. Buat file python untuk create producer data, pyspark consumer
Install lib
```
pip install kafka-python
pip install pyspark findspark
```
Buat file
```
nano producer_suhu.py
nano producer_kelembapan.py
nano pyspark_consumer.py
```
4. Run file dan tampilkan output
```
python3 producer_suhu.py
```
![image](https://github.com/user-attachments/assets/f4309688-a2f3-4c92-9323-30d239f2a4b2)


```
python3 producer_kelembapan.py
```
![image](https://github.com/user-attachments/assets/5758757b-f136-46d1-af90-fe1888918d9b)

```
python3 pyspark_consumer.py
```

![image](https://github.com/user-attachments/assets/5dd45a3d-7902-42ce-88e1-d18b2dca9fd9)

![image](https://github.com/user-attachments/assets/dff321c4-7bdf-4489-a6b1-b0bb58cf00f5)









