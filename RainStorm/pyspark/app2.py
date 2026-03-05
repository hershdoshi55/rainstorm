from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys
import time
import os

if len(sys.argv) < 5:
    print("Usage: spark-submit filter_transform.py <input_csv> <output_dir> <pattern_B> <batch_size>")
    sys.exit(1)

input_csv = sys.argv[1]
output_dir = sys.argv[2]
pattern_B = sys.argv[3]
batch_size = int(sys.argv[4])

spark = SparkSession.builder.appName("FilterAndTransform").getOrCreate()
print("Spark session started.")

start_time_total = time.time()  # total runtime timer

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

# List to hold output rows
output_rows = []

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
        line_no = row[-1]  # line_no column
        if pattern_B in line_str:
            # pick first 3 columns (or less if missing)
            first_three = [str(line_cols[i]) if i < len(line_cols) else "" for i in range(3)]
            output_rows.append((line_no, ",".join(first_three)))

    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    print(f"Batch {batch_num} processed in {batch_duration:.3f} seconds")
    print(f"Total matching lines so far: {len(output_rows)}")
    for row in output_rows:
        print(f"Line {row[0]}: {row[1]}")

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
print(f"Total matching lines: {len(output_rows)}")

# Convert to DataFrame for saving
if len(output_rows) > 0:
    df_out = spark.createDataFrame(output_rows, ["line_no", "first_3_cols"])
    df_out.show(truncate=False)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write normally: each partition writes its own CSV
    df_out.write.mode("overwrite").option("header", True).csv(output_dir)
    print(f"\nFinal CSV(s) saved locally at: {output_dir}")
    print("Note: in cluster mode, each part file is written by an executor to its local disk.")
else:
    print("No matching lines found. No output CSV created.")

spark.stop()
print("Spark session stopped.")
