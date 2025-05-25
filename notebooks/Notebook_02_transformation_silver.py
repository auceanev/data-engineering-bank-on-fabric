# Notebook : 02_transformation_silver
# Objectif : Nettoyage et enrichissement des données Bronze vers la zone Silver

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, year, month, dayofweek,
    upper, trim, expr, avg, count, lag, regexp_replace, lower
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Transformation Silver - Banque").getOrCreate()

# Chargement des données Bronze
clients_df = spark.read.format("delta").load("Files/bronze/clients")
accounts_df = spark.read.format("delta").load("Files/bronze/accounts")
transactions_df = spark.read.format("delta").load("Files/bronze/transactions")

# Nettoyage des clients
clients_clean = clients_df.dropDuplicates(["client_id"]).dropna(subset=["first_name", "last_name"])
clients_clean = clients_clean.withColumn("full_name", upper(trim(col("first_name"))) + lit(" ") + upper(trim(col("last_name"))))

# Nettoyage des emails : suppression des espaces et mise en minuscule
clients_clean = clients_clean.withColumn("email_clean", regexp_replace(lower(col("email")), "\\s+", ""))

# Nettoyage des comptes
accounts_clean = accounts_df.dropDuplicates(["account_id"])

# Nettoyage des transactions
transactions_clean = transactions_df \
    .withColumn("transaction_date", to_date("transaction_date")) \
    .filter((col("deposit_amount").isNotNull()) | (col("withdrawal_amount").isNotNull())) \
    .filter(~((col("deposit_amount") == 0) & (col("withdrawal_amount") == 0)))

# Ajout de colonnes calculées enrichies
transactions_enriched = transactions_clean \
    .withColumn("transaction_type", when(col("withdrawal_amount") > 0, lit("withdrawal"))
                                     .when(col("deposit_amount") > 0, lit("deposit"))
                                     .otherwise(lit("unknown"))) \
    .withColumn("net_transaction", (col("deposit_amount") - col("withdrawal_amount"))) \
    .withColumn("year", year("transaction_date")) \
    .withColumn("month", month("transaction_date")) \
    .withColumn("weekday", dayofweek("transaction_date")) \
    .withColumn("is_large_deposit", when(col("deposit_amount") > 10000, lit(True)).otherwise(lit(False))) \
    .withColumn("is_unusual_withdrawal", when(col("withdrawal_amount") > 5000, lit(True)).otherwise(lit(False)))

# Enrichissement par jointure client (pays, type)
transactions_joined = transactions_enriched \
    .join(clients_clean.select("client_id", "country", "account_type"), on="client_id", how="left")

# Sauvegarde des données Silver
clients_clean.write.format("delta").mode("overwrite").save("Files/silver/clients")
accounts_clean.write.format("delta").mode("overwrite").save("Files/silver/accounts")
transactions_joined.write.format("delta").mode("overwrite").save("Files/silver/transactions")

print("Transformation enrichie vers Silver terminée.")

# Publication comme tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_clients
    USING DELTA
    LOCATION 'Files/silver/clients'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_accounts
    USING DELTA
    LOCATION 'Files/silver/accounts'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_transactions
    USING DELTA
    LOCATION 'Files/silver/transactions'
""")

print("Transformation Silver terminée.")
