from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys
import time
import os
import glob
import shutil

if len(sys.argv) < 5:
    print("Usage: spark-submit filter_count_no_pandas.py <input_csv> <output_dir> <pattern_A> <column_index>")
    sys.exit(1)

input_csv = sys.argv[1]
output_dir = sys.argv[2]
pattern_A = sys.argv[3]
col_index = int(sys.argv[4])  # 0-based index for Nth column
batch_size = int(sys.argv[5])

spark = SparkSession.builder.appName("FilterAndCountNoPandas").getOrCreate()
print("Spark session started.")

start_time_total = time.time()  # start total timer

# Read CSV into DataFrame
df = spark.read.option("header", "false").csv(input_csv)
total_rows = df.count()
print(f"Total rows in CSV: {total_rows}")

# Add line number column to help batch processing
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy(lit(1))
df = df.withColumn("line_no", row_number().over(window_spec))
remaining_rows = total_rows

# Dictionary to hold counts
key_counts = {}

batch_num = 0
while remaining_rows > 0:
    batch_num += 1
    start_row = (batch_num - 1) * batch_size + 1
    end_row = min(batch_num * batch_size, total_rows)

    batch_start_time = time.time()

    batch_df = df.filter((col("line_no") >= start_row) & (col("line_no") <= end_row))
    batch_rows = batch_df.collect()
    print(f"\nProcessing batch {batch_num}: lines {start_row}-{end_row}, {len(batch_rows)} rows")

    for row in batch_rows:
        line_cols = row[:-1]  # all columns except line_no
        line_str = ",".join([str(x) for x in line_cols])
        if pattern_A in line_str:
            # get Nth column as key
            if col_index < len(line_cols):
                key = str(line_cols[col_index])
            else:
                key = ""  # missing column
            key_counts[key] = key_counts.get(key, 0) + 1

    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    print(f"Batch {batch_num} processed in {batch_duration:.3f} seconds")
    print("Current counts for all keys so far:")
    for k, v in key_counts.items():
        print(f'"{k}": {v}')

    remaining_rows -= len(batch_rows)
    # time.sleep(max(0, 1-batch_duration))  # simulate 100 lines/sec
    wait_time = max(0, 1 - batch_duration)

    print(f"Batch {batch_num} timing:")
    print(f"  Actual batch processing time : {batch_duration:.3f} sec")
    print(f"  Waiting (sleep) time         : {wait_time:.3f} sec")

    time.sleep(wait_time)

end_time_total = time.time()
total_duration = end_time_total - start_time_total

print("\nAll batches processed.")
print(f"Total runtime for entire job: {total_duration:.3f} seconds")
print("Final counts for all keys:")
for k, v in key_counts.items():
    print(f'"{k}": {v}')

# Convert dictionary to DataFrame for saving
count_rows = [(k, v) for k, v in key_counts.items()]
if len(count_rows) > 0:
    df_counts = spark.createDataFrame(count_rows, ["key", "count"])
    df_counts.show(truncate=False)

    # Write output normally (one file per partition)
    # Each executor writes its part files to local filesystem
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Writing without coalesce(1) → preserves multiple partitions
    df_counts.write.mode("overwrite").option("header", True).csv(output_dir)

    print(f"\nFinal CSV(s) saved locally at: {output_dir}")
    print("Note: in cluster mode, each part file is written by an executor to its local disk.")

else:
    print("No matching rows found. No output CSV created.")

spark.stop()
print("Spark session stopped.")






# -------------------------------------------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType
import sys
import time

# ----------------------------
# ARGUMENT PARSING
# ----------------------------
if len(sys.argv) < 6:
    print("Usage: spark-submit filter_count_streaming.py <input_csv> <output_dir> <pattern_A> <column_index> <rows_per_sec>")
    sys.exit(1)

input_csv = sys.argv[1]
output_dir = sys.argv[2]
pattern_A = sys.argv[3]
col_index = int(sys.argv[4])      # 0-based index
rows_per_sec = int(sys.argv[5])  #  TRUE TUPLES PER SECOND CONTROL

# ----------------------------
# SPARK SESSION
# ----------------------------
spark = SparkSession.builder.appName("FilterAndCountStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark streaming session started.")


# ----------------------------
# READ CSV AS STATIC DATA
# ----------------------------
df_static = spark.read.option("header", "false").csv(input_csv)

total_rows = df_static.count()
print(f"Total rows in CSV: {total_rows}")

# ✅ SAFE ID creation using RDD (NOT monotonically_increasing_id)
df_static_rdd = df_static.rdd.zipWithIndex()

df_static = df_static_rdd.map(
    lambda x: tuple(list(x[0]) + [x[1]])
).toDF(df_static.columns + ["rid"])


# ----------------------------
# RATE STREAM (THROTTLE SOURCE)
# ----------------------------
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", rows_per_sec) \
    .load()

# ----------------------------
# JOIN RATE WITH CSV (ROW-BY-ROW RELEASE)
# ----------------------------
stream_df = rate_df.join(
    df_static,
    rate_df.value == df_static.rid,
    "inner"
).drop("value", "timestamp", "rid")

# ----------------------------
# FILTER PATTERN + EXTRACT KEY
# ----------------------------
all_cols = df_static.columns[:-1]  # exclude rid

df_with_str = stream_df.withColumn(
    "line_str",
    expr("concat_ws(',', " + ",".join(all_cols) + ")")
)

filtered_df = df_with_str.filter(col("line_str").contains(pattern_A))

key_df = filtered_df.withColumn("key", col(all_cols[col_index]))

# ----------------------------
# STREAMING AGGREGATION
# ----------------------------
counts_df = key_df.groupBy("key").count()

# ----------------------------
# TIMING + DEBUG OUTPUT
# ----------------------------
def log_batch(df, batch_id):
    start_time = time.time()

    rows = df.collect()

    end_time = time.time()
    duration = end_time - start_time

    print(f"\n========= MICRO-BATCH {batch_id} =========")
    print(f"Batch processing time: {duration:.3f} sec")

    for r in rows:
        print(f'"{r["key"]}": {r["count"]}')

# ----------------------------
# STREAM OUTPUT
# ----------------------------
query = counts_df.writeStream \
    .foreachBatch(log_batch) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/filter_count_checkpoint") \
    .start()

query.awaitTermination()