from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("First_try").getOrCreate()
df = spark.read.csv("C:/Users/moumn/big_data_git/src/big_data_git/job/data.csv",header=True, inferSchema = True)

# Afficher le schéma et les données
print("Schéma du DataFrame :")
df.printSchema()

print("Données du DataFrame :")
df.show()

# Effectuer une transformation : filtrer les personnes de plus de 30 ans
df_filtered = df.filter(col("age") > 30)

# Afficher les résultats filtrés
print("Personnes de plus de 30 ans :")
df_filtered.show()

# Arrêter SparkSession
spark.stop()