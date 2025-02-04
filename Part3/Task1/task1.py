# Importing the necessary libraries
import time
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lit, sum as spark_sum

# The function to compute rank contributions of a page to its linked neighbors
def compute_probs(pages, rank):
    num_pages = len(pages)
    for page in pages:
        yield (page, rank / num_pages)

# The function to parse a line into (page, neighbor) tuples
def parse_neighbors(pages):
    neighbors = re.split(r'\s+', pages)
    return neighbors[0], neighbors[1]

# The main function to execute the PageRank algorithm
def run_pagerank(input_path, output_path, num_partitions=8, num_iters=10):
    init_start = time.time()
    spark = (SparkSession.
            builder.
            appName("PageRank").
            getOrCreate())

    init_end = time.time()
    # Loading input files
    lines_df = spark.read.text(input_path)

    # Split the input text into source and target columns
    edges_df = lines_df.withColumn("split", split(col("value"), "\\s+")) \
            .select(col("split")[0].alias("source"), col("split")[1].alias("target"))

    # Initialize ranks: each page starts with rank 1.0
    ranks_df = edges_df.select("source").distinct().withColumn("rank", lit(1.0))
    read_end = time.time()

    # Iteratively update page ranks
    for iteration in range(num_iters):
        # Compute contributions to target pages
        contributions_df = edges_df.join(ranks_df, "source", "left") \
                                   .groupBy("target") \
                                   .agg(spark_sum(col("rank") / lit(1)).alias("contrib"))

        # Apply the damping factor
        ranks_df = contributions_df.withColumn("rank", col("contrib") * 0.85 + 0.15) \
                                   .select(col("target").alias("source"), "rank")

    compute_end = time.time()
        
    # Writing the output
    ranks_df.write.csv(output_path, header=True)

    write_end = time.time()
    spark.stop()
    end = time.time()
    print("----------------------------------")
    print("Initialization time: " + str(init_end - init_start) + " sec")
    print("Read time: " + str(read_end - init_end) + " sec")
    print("Compute time: " + str(compute_end - read_end) + " sec")
    print("Write time: " + str(write_end - compute_end) + " sec")
    print("Resource time for cleanup: " + str(end - write_end) + " sec")
    print("Total time elapsed: " + str(end - init_start) + " sec")
    print("----------------------------------")

if _name_ == "_main_":
    if len(sys.argv) < 3:
        print("Usage: part3_task1.py <input> <output>")
        sys.exit(-1)

    run_pagerank(sys.argv[1], sys.argv[2])
