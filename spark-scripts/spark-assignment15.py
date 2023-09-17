# import semua library yang dibutuhkan
import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# membaca isi file .env
dotenv_path = Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

# mendefinisikan variabel untuk connect ke postgres
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# inisialisasi dan melakukan konfigurasi terhadap lingkungan cluster spark master
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('spark://dataeng-spark-master:7077')
        .set("spark.jars", "/spark-scripts/postgresql-42.6.0.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# melakukan koneksi ke postgresql
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# membaca tabel retail menggunakan spark berdasarkan koneksi ke postgresql di atas
retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

retail_df.show(5)
'''
Sebuah perusahaan retail multinasional XYZ telah mengumpulkan data penjualan dari berbagai negara selama beberapa tahun terakhir. 
Data ini mencakup berbagai detail, termasuk produk yang terjual, kuantitasnya, dan negara tempat penjualan tersebut tercatat. 
Perusahaan tersebut ingin mengidentifikasi produk-produk terlaris di berbagai negara untuk memahami preferensi pelanggan dan mengoptimalkan stok serta strategi pemasaran.
Dengan menggunakan alat pemrosesan data canggih seperti Apache Spark, perusahaan dapat dengan mudah menghitung total volume penjualan untuk setiap produk di berbagai negara. 
Melalui kode di bawah ini, perusahaan tersebut telah menjalankan analisis ini dengan mengelompokkan data berdasarkan negara dan deskripsi produk. 
Setelah menghitung total volume penjualan, perusahaan tersebut memberikan peringkat produk dengan total volume penjualan tertinggi di setiap negara.
Hasil dari analisis ini ditampilkan dalam dataframe total_volume_top3, memberikan insight yang berharga. 
Perusahaan tersebut sekarang dapat melihat produk-produk yang paling laris di setiap negara, yang dapat membantu dalam mengambil keputusan strategis seperti pengaturan stok, perencanaan promosi, atau peningkatan pasokan produk yang paling populer.
Dengan pemahaman yang lebih baik tentang preferensi pelanggan di berbagai negara, perusahaan tersebut selangkah lebih maju untuk menghadapi persaingan global.
'''

total_volume = retail_df.groupBy('country', 'description').agg(F.sum('quantity').alias('total_volume')).orderBy('total_volume', ascending=False)

# Membuat window function berdasarkan kolom 'country' dan mengurutkannya berdasarkan 'total_volume' secara descending
window_spec = Window.partitionBy('country').orderBy(F.desc('total_volume'))

# Menambahkan kolom rank ke dataframe
total_volume_with_rank = total_volume.withColumn('rank', rank().over(window_spec))

# Memfilter hanya peringkat 1 hingga 3 per country
total_volume_top3 = total_volume_with_rank.filter(total_volume_with_rank['rank'] <= 3)

total_volume_top3.show()

# buat tabel baru dari dataframe hasil analisis
total_volume_top3.write.mode("overwrite").jdbc(url=jdbc_url, table="top_3_volume", mode="overwrite", properties=jdbc_properties)
