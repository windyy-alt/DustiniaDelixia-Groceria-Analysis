# DustiniaDelixia Pipeline

Project ini adalah pipeline data analytics berbasis Apache Airflow yang digunakan untuk mengekstrak, memvalidasi, lalu memproses dataset e-commerce ke dalam database analitik ClickHouse. Selain itu, project ini juga menyiapkan antarmuka visualisasi data lewat Metabase, sehingga hasil analisis bisa langsung dipakai untuk dashboard atau eksplorasi data.

## 1. Tujuan project

Project ini dibuat untuk menangani alur data dari file CSV ke warehouse analitik secara otomatis. Secara umum, project ini bertujuan untuk:

- memvalidasi keberadaan semua file dataset yang dibutuhkan,
- menjalankan proses ETL (Extract, Transform, Load) secara otomatis,
- menyimpan data ke dalam tabel analitik di ClickHouse,
- menyiapkan tabel hasil analisis seperti customer_rfm untuk segmentasi pelanggan,
- menyediakan dashboard dan query analitik melalui Metabase.

## 2. Gambaran besar sistem

Arsitektur project ini terdiri dari beberapa komponen:

1. Airflow sebagai orchestrator utama.
   - Airflow mengatur urutan eksekusi task.
   - DAG utama didefinisikan di `dags/pipeline.py`.

2. Script Python sebagai worker ETL.
   - `extract_dataset.py` memeriksa data mentah.
   - `process_dataset.py` membaca CSV, membersihkan tipe data, lalu mengisi ClickHouse.

3. ClickHouse sebagai tempat data analitik.
   - Semua tabel hasil olahan disimpan di database `analytics`.
   - Data ini nantinya bisa dipakai untuk query analitik, dashboard, atau laporan.

4. Metabase sebagai layer visualisasi.
   - Metabase menghubungkan data dari ClickHouse dan memudahkan analisis visual.

## 3. Struktur folder dan fungsi tiap file

### Folder utama

- `dags/`
  - Berisi definisi workflow Airflow.
  - Di dalamnya terdapat file DAG dan script ETL pendukung.

- `dags/pipeline.py`
  - File ini mendefinisikan DAG bernama `DustiniaDelixia_pipeline`.
  - DAG ini menjalankan dua task secara berurutan:
    1. `extract_dataset`
    2. `process_dataset`
  - Artinya, proses akan berhenti di task pertama jika data tidak valid.

- `dags/scripts/`
  - Folder ini menyimpan logika ETL yang dipanggil oleh Airflow.

- `dags/scripts/extract_dataset.py`
  - Script ini berfungsi sebagai pemeriksa data awal.
  - Ia memeriksa semua file CSV yang diharapkan ada di folder `data/`.
  - Jika ada file yang hilang atau gagal dibaca, script akan mengangkat error.
  - Tujuan utamanya adalah memastikan pipeline tidak berjalan jika dataset tidak lengkap.

- `dags/scripts/process_dataset.py`
  - Script ini adalah inti pipeline processing.
  - Ia melakukan beberapa hal:
    - membuat database `analytics` jika belum ada,
    - membuat tabel-tabel analitik di ClickHouse,
    - membaca file CSV dari folder `data/`,
    - mengubah kolom tertentu menjadi tipe numerik atau tanggal,
    - membersihkan nilai kosong menjadi `None`,
    - mengisi data ke tabel ClickHouse,
    - membuat tabel tambahan `customer_rfm` untuk analisis segmentasi pelanggan.

- `data/`
  - Folder ini berisi dataset mentah dalam bentuk CSV.
  - Dataset yang digunakan biasanya mencakup:
    - `customers.csv`
    - `orders.csv`
    - `order_items.csv`
    - `order_payments.csv`
    - `products.csv`
    - `sellers.csv`
    - dan file pendukung lain seperti `mql.csv`, `closed_deals.csv`, `geolocation.csv`, `reviews`, dan `category_translation.csv`.

- `docker-compose.yml`
  - Menentukan seluruh layanan yang berjalan di project ini.
  - Layanan yang tersedia:
    - Airflow Web Server
    - Airflow Scheduler
    - PostgreSQL (untuk metadata Airflow)
    - ClickHouse Server
    - Metabase

- `Dockerfile`
  - Digunakan untuk membangun image Airflow.
  - Image ini menginstal Java Runtime Environment (JRE) karena project ini juga memakai Spark/PySpark.

- `requirements.txt`
  - Daftar dependency Python yang dipasang di environment Airflow.
  - Contohnya: `pandas`, `pyspark`, `clickhouse-driver`, `requests`, dan `pyarrow`.

- `metabase.sql`
  - Berisi contoh query analitik yang bisa dipakai untuk eksplorasi data di Metabase.
  - File ini berguna untuk analisis awal, misalnya menghitung jumlah baris tiap tabel atau memeriksa missing value.

## 4. Alur kerja pipeline secara detail

### Langkah 1: Ekstraksi dan validasi dataset

Saat DAG dijalankan, task pertama yang aktif adalah `extract_dataset`.

Fungsi yang berjalan:
- membaca daftar file yang wajib ada,
- memeriksa apakah file tersebut ada di folder `data/`,
- membaca isi file menggunakan `pandas`,
- menghitung jumlah baris, jumlah kolom, dan ukuran file,
- menampilkan log status untuk setiap file.

Jika ada file yang hilang atau gagal dibaca, pipeline akan langsung gagal. Ini penting agar proses tidak lanjut ke tahap loading dengan data yang tidak lengkap.

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
   - kolom angka menjadi numeric,
   - kolom tanggal menjadi `datetime`,
   - nilai kosong diubah menjadi `None`.
4. Menghapus isi tabel lama (`TRUNCATE`) lalu mengisi data baru dari CSV ke ClickHouse.
5. Membuat tabel tambahan `customer_rfm` untuk analisis pelanggan.

### Langkah 3: Analisis pelanggan dengan RFM

Tabel `customer_rfm` dihitung berdasarkan kombinasi:

- Recency: seberapa baru pelanggan melakukan pesanan terakhir,
- Frequency: seberapa sering pelanggan membeli,
- Monetary: total nilai transaksi pelanggan.

Hasil analisis ini kemudian diberi skor dan segmentasi seperti:
- Champions
- Loyal
- Potential
- At Risk
- Hibernating

Segmentasi ini sangat berguna untuk memahami perilaku pelanggan secara bisnis.

## 5. Teknologi yang digunakan

- Apache Airflow untuk orkestrasi workflow.
- Python untuk logika ETL.
- Pandas untuk pembacaan dan manipulasi data CSV.
- PySpark untuk kebutuhan analitik berbasis Spark.
- ClickHouse sebagai database analitik.
- Metabase untuk visualisasi dan query dashboard.
- Docker Compose untuk menjalankan seluruh stack secara terintegrasi.

## 6. Dependency utama

Package yang digunakan dalam project ini antara lain:

- `pyspark==3.5.1`
- `clickhouse-driver==0.2.7`
- `pandas==2.2.1`
- `requests==2.31.0`
- `pyarrow==15.0.2`

## 7. Cara menjalankan project

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
   docker-compose up -d
   ```

3. Tunggu hingga container siap. Setelah itu, buka layanan berikut:
   - Airflow UI: http://localhost:8080
   - Metabase: http://localhost:3000
   - ClickHouse HTTP: http://localhost:8123

4. Buka Airflow, lalu jalankan DAG `DustiniaDelixia_pipeline`.

5. Jika pipeline berhasil dijalankan, data akan masuk ke tabel-tabel di ClickHouse dan siap untuk dianalisis lebih lanjut.

## 8. Catatan penting

- Dataset CSV harus tersedia di folder `data/` sebelum pipeline dijalankan.
- Airflow menggunakan PostgreSQL sebagai database metadata.
- ClickHouse digunakan sebagai storage analitik untuk data hasil ETL.
- Jika terjadi error, cek log di Airflow atau log container Docker untuk melihat file mana yang gagal.
- File `metabase.sql` bisa dipakai sebagai referensi query untuk eksplorasi awal.

## 9. Ringkasan singkat

Project ini adalah contoh pipeline data end-to-end yang menggabungkan:

- Airflow untuk orkestrasi,
- Python untuk ETL,
- ClickHouse untuk penyimpanan analitik,
- Metabase untuk visualisasi.

Dengan struktur ini, data mentah dari CSV bisa diproses secara otomatis menjadi data siap analisis yang bisa ditampilkan di dashboard atau dipakai untuk laporan bisnis.
