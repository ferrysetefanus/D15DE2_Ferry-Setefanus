# Assignment Day 15 Dibimbing Data Engineering Batch 2

## Bakcground Story
Sebuah perusahaan retail multinasional XYZ telah mengumpulkan data penjualan dari berbagai negara selama beberapa tahun terakhir. Data ini mencakup berbagai detail, termasuk produk yang terjual, kuantitasnya, dan negara tempat penjualan tersebut tercatat. Perusahaan tersebut ingin mengidentifikasi produk-produk terlaris di berbagai negara untuk memahami preferensi pelanggan dan mengoptimalkan stok serta strategi pemasaran. Dengan menggunakan alat pemrosesan data canggih seperti Apache Spark, perusahaan dapat dengan mudah menghitung total volume penjualan untuk setiap produk di berbagai negara. 
Melalui kode di bawah ini, perusahaan tersebut telah menjalankan analisis ini dengan mengelompokkan data berdasarkan negara dan deskripsi produk. Setelah menghitung total volume penjualan, perusahaan tersebut memberikan peringkat produk dengan total volume penjualan tertinggi di setiap negara.
Hasil dari analisis ini ditampilkan dalam dataframe total_volume_top3, memberikan insight yang berharga. 
Perusahaan tersebut sekarang dapat melihat produk-produk yang paling laris di setiap negara, yang dapat membantu dalam mengambil keputusan strategis seperti pengaturan stok, perencanaan promosi, atau peningkatan pasokan produk yang paling populer.
Dengan pemahaman yang lebih baik tentang preferensi pelanggan di berbagai negara, perusahaan tersebut selangkah lebih maju untuk menghadapi persaingan global.

## Apa saja yang dilakukan
1. Menambahkan file jar untuk koneksi spark ke postgresql secara manual
   ![jar](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/jar.jpg)
2. Melakukan konfigurasi pada operator sparksubmitoperator
   ![sparksubmitoperator](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/airflow_spark.jpg)
3.Melakukan konfigurasi pada skrip spark
   ![spark skrip](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/spark_skrip.jpg)
4. Menjalankan DAG menggunakan airflow
   ![airflow](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/bukti_airflow.jpg)
5. Cek apakah terhubung dengan spark master
   ![spark master](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/bukti_spark.jpg)
6. Cek hasil pengolahan menggunakan spark pada postgres, hasil pengolahan menggunakan spark akan dikembalikan dalam bentuk tabel pada postgres dengan nama top_3_volume
   ![postgres](https://github.com/ferrysetefanus/D15DE2_Ferry-Setefanus/blob/main/images/bukti_postgres.jpg)

## Cara menjalankan
make postgres -> make spark -> make airflow -> jalankan dag -> finish

