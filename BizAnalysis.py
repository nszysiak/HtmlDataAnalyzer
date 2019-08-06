from pyspark import SparkConf, SparkContext
import csv, io
import config


def list_to_csv_str(x):
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline


def runBizAnalysis():
    conf = SparkConf().setMaster("local[*]").setAppName("PopularMovies")
    sc = SparkContext(conf = conf)

    def parseLine(line):
        fields = line.split('\t')
        ID = str(fields[1])
        desc = str(fields[4])
        name = str(fields[2])
        return ID, desc, name

    lines = sc.textFile("biz")
    parsedLines = lines.map(parseLine)
    movies = parsedLines.map(list_to_csv_str)
    movies.saveAsTextFile(config.BIZ_ANALYSIS_DIR)


if __name__ == '__main__':
    runBizAnalysis()