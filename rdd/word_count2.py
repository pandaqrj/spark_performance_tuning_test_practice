from pyspark import SparkContext


def exe_spark():
    sc = SparkContext.getOrCreate()

    rdd1 = sc.textFile(name="../data4.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" ")).map(lambda x: x.replace(".", ""))
    rdd3 = rdd2.groupBy(lambda x: x)
    rdd4 = rdd3.map(lambda x: (x[0], len(x[1])))
    rdd4.foreach(print)


if __name__ == '__main__':
    exe_spark()
