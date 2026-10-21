from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("lakeflow-time-travel")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Display the current table
print("\n=== CURRENT ORDERS ===")
spark.sql("""
    SELECT order_id, status, amount, payment_method
    FROM local.demo.orders
    ORDER BY order_id
""").show(truncate=False)

# Display Iceberg snapshot history
print("\n=== SNAPSHOT HISTORY ===")
snapshots = spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM local.demo.orders.snapshots
    ORDER BY committed_at
""")
snapshots.show(truncate=False)

# Get the earliest available snapshot
first_snapshot = snapshots.first()

if first_snapshot:
    snapshot_id = first_snapshot["snapshot_id"]

    print(f"\n=== TIME TRAVEL TO SNAPSHOT {snapshot_id} ===")

    spark.sql(f"""
        SELECT order_id, status, amount
        FROM local.demo.orders
        VERSION AS OF {snapshot_id}
        ORDER BY order_id
    """).show(truncate=False)
else:
    print("No snapshots found.")

spark.stop()