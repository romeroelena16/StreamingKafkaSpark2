import sys
from operator import add

from pyspark.sql import SparkSession


def basic_df_example(spark: SparkSession) -> None:
    dataRdd = spark.sparkContext.parallelize([("Bryan", 30),("Julia",39),("Valeria", 23),("Bryan", 18)])
    dataDf = dataRdd.toDF(["nombre","saldo"])
    dataDf.show()



if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python") \
        .getOrCreate()
    # $example off:init_session$

    basic_df_example(spark)

    spark.stop()