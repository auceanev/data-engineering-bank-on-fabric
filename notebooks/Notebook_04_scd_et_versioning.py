# Notebook : 04_scd_et_versioning.py
# Objectif : SCD1 et SCD2

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit

clients_df = spark.read.format("delta").load("Files/silver/clients")
accounts_df = spark.read.format("delta").load("Files/silver/accounts")

# SCD1 : client (overwrite)
clients_df.write.format("delta").mode("overwrite").save("Files/gold/dim_client")

# SCD2 : account
accounts_scd2 = accounts_df \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True))

if DeltaTable.isDeltaTable(spark, "Files/gold/dim_account"):
    target = DeltaTable.forPath(spark, "Files/gold/dim_account")
    source = accounts_scd2.alias("source")

    target.alias("target").merge(
        source,
        "target.account_id = source.account_id AND target.is_current = true"
    ).whenMatchedUpdate(condition="target.balance_amount != source.balance_amount",
        set={
            "end_date": "current_date()",
            "is_current": "false"
        }).whenNotMatchedInsertAll()
else:
    accounts_scd2.write.format("delta").mode("overwrite").save("Files/gold/dim_account")

print("SCD1 et SCD2 appliqués.")
