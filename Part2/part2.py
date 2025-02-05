# Importing the necessory libraries
from pyspark.sql import SparkSession
import sys

# Function for sorting the given data based on country code and timestamp
def spark_application(input_path, output_path):
    # Initialize spark sql instance
    spark = (SparkSession
            .builder
            .appName("part2")
            .getOrCreate())

    # Loading data
    df = spark.read.option("header", True).csv(input_path)

    # Sorting data firstly by country code and then by timetstamp
    df_sorted = df.sort(['cca2', 'timestamp'], ascending=[True, True])

    # Writing output in csv file
    df_sorted.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)


if _name_ == "_main_":
    print("Arguments", sys.argv)
    spark_application(sys.argv[1], sys.argv[2])
