from pyspark.sql import SparkSession, Row, Window
from pyspark.sql import functions as pf
import config
from CreateDir import CreateDirectory


def map_fields(line):

    """Function parse line of tab separated value file and returns instance of Row class"""

    fields = line.split('\t')
    return Row(ID=int(fields[0]), URL=str(fields[1]), title=str(fields[2]), readNumber=int(fields[3]), likeNumber=int(fields[4]), timestamp=str(fields[6]))


def analyze():

    """The main function creates SparkSession and SparkContext.
    Based on that creates DF that is ingested with mapped Rows.
    In order to retrieved max clicks and likes it uses partioning
    function over URL to find max values per URL - creates
    temporary column called maxReadNumber and then it comapers
    each record within certain URL. Finally only records with max values
    remains and temporary column is deleted."""

    # Create a SparkSession
    spark = SparkSession.builder.config(config.WH_DIR, config.TEMP_DIR).appName("GetMaxReadLike").getOrCreate()

    lines = spark.sparkContext.textFile(config.CLICKS_FILE_TO_PROC)
    clicks = lines.map(map_fields)

    # Infer the schema and register the DataFrame as a table.
    clicks_DF = spark.createDataFrame(clicks).cache()
    clicks_DF.createOrReplaceTempView("clicks")

    # Set Window
    w = Window.partitionBy('URL')

    # Calculate max read per article
    clicks_DF.withColumn('maxReadNumber', pf.max('readNumber').over(w))\
            .where(pf.col('readNumber') == pf.col('maxReadNumber'))\
            .drop('maxReadNumber')\
            .coalesce(1)\
            .write \
            .option("delimiter", "\t") \
            .csv(config.CLICK_ANALYSIS_DIR + '\ReadCnt')

    # Calculate max likes per article
    clicks_DF.withColumn('maxLikeNumber', pf.max('likeNumber').over(w)) \
            .where(pf.col('likeNumber') == pf.col('maxLikeNumber')) \
            .drop('maxLikeNumber')\
            .coalesce(1) \
            .write \
            .option("delimiter", "\t")\
            .csv(config.CLICK_ANALYSIS_DIR + '\LikeCnt')


if __name__ == '__main__':
    analyze()