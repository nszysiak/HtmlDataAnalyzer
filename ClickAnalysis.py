from pyspark.sql import SparkSession, Row, Window
from pyspark.sql import functions as pf
import config
import os


def create_dir():
    path = config.CLICK_ANALYSIS_DIR
    try:
        os.mkdir(path)
    except OSError:
        print("Directory %s has not been created" % path)
    else:
        print("Successfully created the dir %s " % path)

# Create a function in order to map proper fields as a Row class
def mapFields(line):
    fields = line.split('\t')
    return Row(ID=int(fields[0]), URL=str(fields[1].encode("utf-8")), title=str(fields[2]), readNumber=int(fields[3]), likeNumber=int(fields[4]), timestamp=str(fields[6]))

# Create a function to analyze read number of articles and like number of articles
def runClicksAnalysis():
    # Create a SparkSession
    spark = SparkSession.builder.config(config.WH_DIR, config.TEMP_DIR).appName("GetMaxReadLike").getOrCreate()

    lines = spark.sparkContext.textFile("clicks")
    clicks = lines.map(mapFields)

    # Infer the schema and register the DataFrame as a table.
    ClicksDF = spark.createDataFrame(clicks).cache()
    ClicksDF.createOrReplaceTempView("clicks")

    # Create dir
    create_dir()

    # Set Window
    w = Window.partitionBy('URL')

    # Calculate max read per article
    read_rows = ClicksDF.withColumn('maxReadNumber', pf.max('readNumber').over(w))\
                .where(pf.col('readNumber') == pf.col('maxReadNumber'))\
                .drop('maxReadNumber')\
                .coalesce(1)\
                .write\
                .csv(config.CLICK_ANALYSIS_DIR + '\ReadCnt', header=True)

    # Calculate max likes per article
    like_rows = ClicksDF.withColumn('maxLikeNumber', pf.max('likeNumber').over(w)) \
        .where(pf.col('likeNumber') == pf.col('maxLikeNumber')) \
        .drop('maxLikeNumber')\
        .coalesce(1) \
        .write \
        .csv(config.CLICK_ANALYSIS_DIR + '\LikeCnt', header=True)


if __name__ == '__main__':
    runClicksAnalysis()