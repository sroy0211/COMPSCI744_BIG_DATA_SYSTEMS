# Importing the necessary libraries
import time
import re
import sys
from operator import add
from pyspark.sql import SparkSession

# The function to compute rank contributions of a page to its linked neighbors
def prob_compute(pages, rank):
    num_pages = len(pages)
    for page in pages:
        yield (page, rank / num_pages)

# The function to parse a line into (page, neighbor) tuples
def neighbors_parse(pages):
    neighbors = re.split(r'\s+', pages) 
    return neighbors[0], neighbors[1]

# The main function to execute the PageRank algorithm
def pagerank(input_path, output_path, num_iters=10):
    init_start = time.time()
    spark = (SparkSession.
            builder.
            appName("PageRank").
            getOrCreate())
    init_end = time.time()

    # Loading input files
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

    # Reading the pages in input files and initializing their neighbors
    links = lines.map(lambda pages: neighbors_parse(pages)).distinct().groupByKey()
    
    # Initializing the ranks
    ranks = links.map(lambda page_neighbors: (page_neighbors[0], 1.0))
    read_end = time.time()

    # Calculating and updating page ranks up to number of iterations
    for iteration in range(num_iters):
        # Calculating page contributions to the rank of other pages
        probs = links.join(ranks).flatMap(
                lambda page_pages_rank: prob_compute(page_pages_rank[1][0], page_pages_rank[1][1]))

        # Re-calculates page ranks based on neighbor contributions
        ranks = probs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    compute_end = time.time()
        
    # Writing the output
    ranks.saveAsTextFile(output_path)
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

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: part3_task1.py <input> <output>")
        sys.exit(-1)

    pagerank(sys.argv[1], sys.argv[2])
