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
    spark = (SparkSession.
            builder.
            appName("PageRank").
            getOrCreate())

    # Loading input files
    lines_df = spark.read.text(input_path)

    # Split the input text into source and target columns
    edges_df = lines_df.withColumn("split", split(col("value"), "\\s+")) \
                       .select(col("split")[0].alias("source"), col("split")[1].alias("target")).cache()

    # Repartitioning the links
    edges_df = edges_df.repartition(num_partitions)

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

     # Writing the output
    ranks_df.write.csv(output_path, header=True)

     spark.stop()

if _name_ == "_main_":
    if len(sys.argv) < 4:
        print("Usage: pagerank_partition.py <input> <output> <num_partition>")
        sys.exit(-1)

    num_partitions = sys.argv[3]
    start = time.time()
    run_pagerank(sys.argv[1], sys.argv[2], num_partitions=int(num_partitions))
    end = time.time()
    print("------------------------------")
    print("Elapsed time: " + str(end - start) + " sec")
    print("------------------------------")
