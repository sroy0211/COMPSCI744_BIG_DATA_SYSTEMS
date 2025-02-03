import time
import re
import sys
from operator import add
from pyspark.sql import SparkSession

# Function to distribute the rank of each page evenly among its neighbors
def compute_probs(pages, rank):
    num_pages = len(pages)
    for page in pages:
        yield (page, rank / num_pages)

# Function to parse the input data and extract the page and its neighbor
def parse_neighbors(pages):
    neighbors = re.split(r'\s+', pages) # Split the line into the page and its neighbors
    return neighbors[0], neighbors[1]

# Function to run the PageRank algorithm
def run_pagerank(input_path, output_path, num_partitions=8, num_iters=10):
    spark = (SparkSession.
            builder.
            appName("PageRank").
            getOrCreate())
    
    # Load the input data and convert it to RDD format
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

    
    #gatting all input files,reading it ,initializing the neighbors and set it to persist the dataframes
    links = lines.map(lambda pages: parse_neighbors(pages)).distinct().groupByKey().cache()
    
    # Parse the input data and group the pages by their neighbors
    links = links.repartition(num_partitions)

    # Initialize the ranks of all pages to 1.0
    ranks = links.map(lambda page_neighbors: (page_neighbors[0], 1.0)).repartition(num_partitions)

    # Run the PageRank algorithm for the specified number of iterations
    for iteration in range(num_iters):
        # Calculate the contribution of each page's rank to its neighbors
        probs = links.join(ranks).flatMap(
                lambda page_pages_rank: compute_probs(page_pages_rank[1][0], page_pages_rank[1][1]))

        # Recompute the rank of each page by aggregating the contributions from its neighbors
        ranks = probs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15).repartition(num_partitions)
        
    # Parse the number of partitions from the command-line arguments
    ranks.saveAsTextFile(output_path)

    spark.stop()

# Main execution of the script, handling command-line arguments and timing
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: pagerank_cache.py <input> <output> <num_partitions>")
        sys.exit(-1)

    num_partitions = sys.argv[3]
    start = time.time()
    # Run the PageRank algorithm
    run_pagerank(sys.argv[1], sys.argv[2], num_partitions=int(num_partitions))
    end = time.time()
    print("------------------------------")
    print("Elapsed time: " + str(end - start) + " sec")
    print("------------------------------")
