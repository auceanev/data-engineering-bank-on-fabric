# Notebook : 05_powerbi_extraction.py
# Objectif : Vues KPI pour Power BI

from pyspark.sql.functions import sum, avg, countDistinct, year, month

fact_transaction = spark.read.format("delta").load("Files/gold/fact_transaction")
dim_client = spark.read.format("delta").load("Files/gold/dim_client")
dim_account = spark.read.format("delta").load("Files/gold/dim_account")
dim_date = spark.read.format("delta").load("Files/gold/dim_date")

dim_date = dim_date.withColumn("year", year("transaction_date")) \
                   .withColumn("month", month("transaction_date"))

view = fact_transaction \
    .join(dim_client, "client_sk", "left") \
    .join(dim_account, "account_sk", "left") \
    .join(dim_date.select("date_sk", "year", "month"), "date_sk", "left")

kpi_clients = view.groupBy("client_sk").agg(
    sum("deposit_amount").alias("total_deposits"),
    sum("withdrawal_amount").alias("total_withdrawals"),
    avg("net_transaction").alias("avg_net"),
    countDistinct("account_sk").alias("nb_accounts")
)

kpi_clients.write.format("delta").mode("overwrite").saveAsTable("kpi_clients")
spark.sql("""
    CREATE TABLE IF NOT EXISTS kpi_clients
    USING DELTA
    LOCATION 'Files/pbi/kpi_clients'
""")

kpi_monthly = view.groupBy("year", "month").agg(
    sum("deposit_amount").alias("monthly_deposits"),
    sum("withdrawal_amount").alias("monthly_withdrawals"),
    avg("net_transaction").alias("avg_net_monthly")
)
kpi_monthly.write.format("delta").mode("overwrite").saveAsTable("kpi_monthly")
spark.sql("""
    CREATE TABLE IF NOT EXISTS kpi_monthly
    USING DELTA
    LOCATION 'Files/pbi/kpi_monthly'
""")

print("Vues KPI Power BI prêtes.")