from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F


def promediarValores(df):
   df.createOrReplaceTempView("vResultado")
   promedios = spark.sql("""SELECT nombre, apellido, pais FROM vResultado""")
   return promedios


if __name__ == "__main__":
    spark = SparkSession\
            .builder \
            .appName("KafkaIntegration")\
            .master("local[3]")\
            .config("spark.sql.shuffle.partitions", 3)\
            .getOrCreate()
    
            #.config("spark.streaming.stopGracefullyOnShutdown", "true")\
    
    tiposStreamingDF = (spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "kafka1:9092, kafka2:9092, kafka3:9092")\
                        .option("subscribe", "personas")\
                        .option("startingOffsets","earliest") #from-beggining\
                        .load())
    
    """ Options:
    .option("startingOffsets","latest")   # last message
    .option("startingOffsets","earliest") #from-beggining\
    """
    
    esquema = StructType([\
        StructField("nombre", StringType()),\
        StructField("apellido", StringType()),\
        StructField("pais", StringType())\
        ])
    
    parsedDF = tiposStreamingDF\
        .select("value")\
        .withColumn("value", F.col("value").cast(StringType()))\
        .withColumn("input", F.from_json(F.col("value"), esquema))\
        .withColumn("nombre", F.col("input.nombre"))\
        .withColumn("apellido", F.col("input.apellido"))\
        .withColumn("pais", F.col("input.pais"))

    promediosStreamingDF = promediarValores(parsedDF)

    groupByPromediosDf = promediosStreamingDF.groupBy("pais").count()
    
    salida = groupByPromediosDf\
        .writeStream\
        .queryName("query_paises_kafka")\
        .outputMode("complete")\
        .format("memory")\
        .start()
    
    """
    transf:         append = memory   #select, where, selectExpr
    agg(groupBy):   complete = memory #groupBy avg
    """
    
    from time import sleep
    for x in range(50):
        spark.sql("select * from query_paises_kafka").show(1000, False)
        sleep(1)

    
    print('llego hasta aqui')
    
    salida.awaitTermination()
