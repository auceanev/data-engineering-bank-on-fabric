# Notebook : 03_modelisation_gold.py
# Objectif : Modélisation en étoile

from pyspark.sql.functions import monotonically_increasing_id

clients = spark.read.format("delta").load("Files/silver/clients")
accounts = spark.read.format("delta").load("Files/silver/accounts")
transactions = spark.read.format("delta").load("Files/silver/transactions")

# DIMENSIONS
dim_client = clients.withColumn("client_sk", monotonically_increasing_id())
dim_account = accounts.withColumn("account_sk", monotonically_increasing_id())
dim_date = transactions.select("transaction_date").dropna().dropDuplicates().withColumn("date_sk", monotonically_increasing_id())

dim_client.write.format("delta").mode("overwrite").save("Files/gold/dim_client")
dim_account.write.format("delta").mode("overwrite").save("Files/gold/dim_account")
dim_date.write.format("delta").mode("overwrite").save("Files/gold/dim_date")

# Publication comme tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_client
    USING DELTA
    LOCATION 'Files/gold/dim_client'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_account
    USING DELTA
    LOCATION 'Files/gold/dim_account'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date
    USING DELTA
    LOCATION 'Files/gold/dim_date'
""")

# TABLE DE FAITS
fact_transaction = transactions \
    .join(dim_client, on="client_id", how="left") \
    .join(dim_account, on="account_id", how="left") \
    .join(dim_date, on="transaction_date", how="left") \
    .select("client_sk", "account_sk", "date_sk", "withdrawal_amount", "deposit_amount", "net_transaction", "transaction_type")

fact_transaction.write.format("delta").mode("overwrite").save("Files/gold/fact_transaction")

spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_transaction
    USING DELTA
    LOCATION 'Files/gold/fact_transaction'
""")

print("Modèle en étoile enregistré avec succès.")

