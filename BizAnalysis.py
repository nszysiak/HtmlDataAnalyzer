from pyspark import SparkConf, SparkContext
import csv, io
import config


def parse_line(line):

    """Function parse line of text file and returns determined fields"""

    fields = line.split('\t')
    ID = str(fields[1])
    desc = str(fields[4])
    name = str(fields[2])
    return ID, desc, name


def csv_converter(line):

    """From list of str, function returns a csv string."""

    output = io.StringIO("")
    csv.writer(output, delimiter='\t').writerow(line)
    return output.getvalue().strip()


def main():

    """The main function creates sc, reads biz file, creates
    RDD on that, simultaneously parsing each line. Finally
    it converts an outcome of RDD to proper-csv-formatted file."""

    conf = SparkConf().setMaster("local[*]").setAppName("RunBizAnalysis")
    sc = SparkContext(conf = conf)

    lines = sc.textFile(config.BIZ_FILE_TO_PROC)
    parsed_lines = lines.map(parse_line)
    csv_line = parsed_lines.map(csv_converter)
    csv_line.saveAsTextFile(config.BIZ_ANALYSIS_DIR)


if __name__ == '__main__':
    main()