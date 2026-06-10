# DUSTINIADELIXIA GROCERIA - FINANCE ANALYSIS

| Nama | NRP |
|----------|----------|
| Dilbina Windi Azahra    | 5025241180    |

Project ini adalah pipeline data analytics berbasis Apache Airflow yang digunakan untuk mengekstrak, memvalidasi, lalu memproses dataset e-commerce ke dalam database analitik ClickHouse. Selain itu, project ini juga terdapat visualisasi data lewat Metabase, sehingga hasil analisis bisa langsung dipakai untuk dashboard atau eksplorasi data.

## 1. Tujuan project

Project ini dibuat untuk menangani alur data dari file CSV hingga ke visualisasi metabase. Secara umum, project ini bertujuan untuk:
- memvalidasi keberadaan semua file dataset yang dibutuhkan,
- menjalankan proses ETL (Extract, Transform, Load) secara otomatis,
- menyimpan data ke dalam tabel analitik di ClickHouse,
- menyiapkan tabel hasil analisis seperti customer_rfm untuk segmentasi pelanggan,
- menyediakan dashboard dan query analitik melalui Metabase.

## 2. Gambaran besar sistem

Arsitektur project ini terdiri dari beberapa komponen:

1. Airflow sebagai penggerak utama.
   - Airflow mengatur urutan eksekusi task.
   - DAG utama didefinisikan di `dags/pipeline.py`.

2. Script Python sebagai worker ETL.
   - `extract_dataset.py` memeriksa data mentah.
   - `process_dataset.py` membersihkan tipe data, lalu mengisi ClickHouse.

3. ClickHouse sebagai tempat data analitik.
   - Semua tabel hasil olahan disimpan di database `analytics`.
   - Data ini nantinya bisa dipakai untuk query analitik, dashboard, atau laporan.

4. Metabase sebagai tempat visualisasi.
   - Metabase menghubungkan data dari ClickHouse dan membuat analisis secara visual untuk dashboard.

## 3. Struktur folder dan fungsi tiap file

## Struktur folder

```text
DustiniaDelixia-pipeline/
│
├── dags/
│   ├── pipeline.py
│   └── scripts/
│       ├── extract_dataset.py
│       └── process_dataset.py
│
├── data/
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── metabase.sql
```

### Description

* **dags/** : Berisi workflow Airflow dan script ETL yang digunakan dalam pipeline.
* **pipeline.py** : Mendefinisikan DAG utama yang menjalankan proses validasi dataset dan pemrosesan data.
* **scripts/** : Berisi script ETL yang digunakan oleh Airflow.
* **extract_dataset.py** : Memvalidasi keberadaan dan keterbacaan dataset sebelum proses ETL dijalankan.
* **process_dataset.py** : Melakukan transformasi data, pembuatan tabel ClickHouse, dan proses loading data ke database analytics.
* **data/** : Berisi dataset mentah dalam format CSV.
* **docker-compose.yml** : Konfigurasi layanan Docker yang digunakan dalam project.
* **Dockerfile** : Konfigurasi image Airflow beserta dependency tambahan.
* **requirements.txt** : Daftar library Python yang digunakan.
* **metabase.sql** : Kumpulan query analitik untuk eksplorasi data dan dashboard.

### DAG Airflow
<img width="1916" height="1037" alt="Screenshot 2026-05-30 230006" src="https://github.com/user-attachments/assets/b57aedd9-4acb-4dfd-a4ed-b77e5fc61634" />

### Metabase
<img width="1920" height="780" alt="image" src="https://github.com/user-attachments/assets/faf30754-e72c-405b-be61-5b700b4bc7e2" />



## 4. Alur kerja pipeline secara detail

### Langkah 1: Ekstraksi dan validasi dataset

Saat DAG dijalankan, task pertama yang aktif adalah `extract_dataset`.

Fungsi yang berjalan:
- membaca daftar file yang wajib ada
- memeriksa apakah file tersebut ada di folder `data/`
- membaca isi file menggunakan `pandas`
- menghitung jumlah baris, jumlah kolom, dan ukuran file
- menampilkan log status untuk setiap file

Jika ada file yang hilang atau gagal dibaca, pipeline akan langsung gagal.

### Langkah 2: Proses transform dan load ke ClickHouse

Jika validasi berhasil, task kedua `process_dataset` akan berjalan.

Proses yang dilakukan:

1. Membuat database `analytics` jika belum ada.
2. Membuat tabel-tabel utama seperti:
   - `category_translation`
   - `customers`
   - `geolocation`
   - `order_items`
   - `order_payments`
   - `orders`
   - `products`
3. Mengubah kolom tertentu menjadi tipe data yang lebih sesuai:
   - kolom angka menjadi numeric
   - kolom tanggal menjadi `datetime`
   - nilai kosong diubah menjadi `None`
4. Menghapus isi tabel lama (`TRUNCATE`) lalu mengisi data baru dari CSV ke ClickHouse.
5. Membuat tabel tambahan `customer_rfm` untuk analisis pelanggan.

### Langkah 3: Analisis pelanggan dengan RFM

Tabel `customer_rfm` dihitung berdasarkan kombinasi:

- Recency: seberapa baru pelanggan melakukan pesanan terakhir
- Frequency: seberapa sering pelanggan membeli
- Monetary: total nilai transaksi pelanggan

Setiap pelanggan diberikan skor 1–4 untuk masing-masing komponen RFM berdasarkan distribusi data (quartile). Skor RFM kemudian dijumlahkan dan digunakan untuk mengelompokkan pelanggan ke dalam beberapa segmen:
- Champions
- Loyal
- Potential
- At Risk
- Hibernating

<img width="1916" height="913" alt="image" src="https://github.com/user-attachments/assets/3e8b380f-0766-4ca3-9d39-c2695cf30ae9" />


## 5. Teknologi yang digunakan

- Apache Airflow untuk orkestrasi workflow.
- Python untuk logika ETL.
- Pandas untuk pembacaan dan manipulasi data CSV.
- PySpark untuk kebutuhan analitik berbasis Spark.
- ClickHouse sebagai database analitik.
- Metabase untuk visualisasi dan query dashboard.
- Docker Compose untuk menjalankan seluruh stack secara terintegrasi.

## 6. Cara menjalankan project

### Prasyarat

Pastikan environment sudah memiliki:
- Docker
- Docker Compose

### Langkah menjalankan

1. Masuk ke folder project:

   ```bash
   cd DustiniaDelixia-pipeline-fpmci
   ```

2. Jalankan seluruh layanan:

   ```bash
   docker-compose build
   docker-compose up airflow init
   docker-compose up -d
   ```

3. Tunggu hingga container siap. Setelah itu, buka layanan berikut:
   - Airflow UI: http://localhost:8080
   - Metabase: http://localhost:3000
   - ClickHouse HTTP: http://localhost:8123

4. Buka Airflow, lalu jalankan DAG `DustiniaDelixia_pipeline`.

5. Jika pipeline berhasil dijalankan, data akan masuk ke tabel-tabel di ClickHouse untuk dianalisis lebih lanjut.

## 7. Dashboard 
<img width="1345" height="811" alt="Screenshot 2026-06-10 191133" src="https://github.com/user-attachments/assets/1882c9c6-d32d-4f9c-9f43-63b7059ac203" />
<img width="1346" height="770" alt="Screenshot 2026-06-10 191300" src="https://github.com/user-attachments/assets/edcbf926-8773-42b7-b203-32ec85ecc927" />
<img width="1349" height="759" alt="Screenshot 2026-06-10 191326" src="https://github.com/user-attachments/assets/f6ffaffc-6929-49a8-a5e2-49f96f40bb57" />
<img width="1348" height="927" alt="Screenshot 2026-06-10 190455" src="https://github.com/user-attachments/assets/a3c61260-852c-4d31-a33d-c524a4e81bf4" />
<img width="1348" height="511" alt="Screenshot 2026-06-10 190523" src="https://github.com/user-attachments/assets/fc4fefa5-f4d7-4b11-b5ed-861c034ad920" />
<img width="1351" height="987" alt="Screenshot 2026-06-10 190551" src="https://github.com/user-attachments/assets/cad8cf0c-d64c-4131-bc40-808154d90d6a" />
<img width="1348" height="210" alt="Screenshot 2026-06-10 190623" src="https://github.com/user-attachments/assets/addc5b7f-041c-41a5-9806-8478a8614f98" />
<img width="1345" height="1058" alt="Screenshot 2026-06-10 190641" src="https://github.com/user-attachments/assets/c33c9de3-ed05-4ffd-b277-eba59e41dc2e" />
<img width="1347" height="514" alt="Screenshot 2026-06-10 190700" src="https://github.com/user-attachments/assets/d9bfa0bc-4d86-47a6-bc17-8cdaebabb14f" />





*Dibuat untuk keperluan Final Project Seleksi Camin MCI - Dilbina Windi A.*
