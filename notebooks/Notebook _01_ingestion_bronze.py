#Notebook : 01_ingestion_bronze.py
# Objectif : Ingestion des fichiers CSV dans la zone Bronze du Lakehouse

from pyspark.sql import SparkSession

# Initialisation Spark
spark = SparkSession.builder.appName("Ingestion Bronze - Banque").getOrCreate()

# Chemin d'accès brut (source CSV)
path_clients = "Files/raw/clients.csv"
path_accounts = "Files/raw/accounts.csv"
path_transactions = "Files/raw/transactions.csv"

# Lecture des CSV brut
clients_df = spark.read.option("header", True).csv(path_clients)
accounts_df = spark.read.option("header", True).csv(path_accounts)
transactions_df = spark.read.option("header", True).csv(path_transactions)

# Sauvegarde en Delta format dans zone Bronze
clients_df.write.format("delta").mode("overwrite").save("Files/bronze/clients")
accounts_df.write.format("delta").mode("overwrite").save("Files/bronze/accounts")
transactions_df.write.format("delta").mode("overwrite").save("Files/bronze/transactions")


# Publication comme tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_clients
    USING DELTA
    LOCATION 'Files/bronze/clients'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_accounts
    USING DELTA
    LOCATION 'Files/bronze/accounts'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_transactions
    USING DELTA
    LOCATION 'Files/bronze/transactions'
""")

print("Ingestion terminée avec succès dans la zone Bronze.")
