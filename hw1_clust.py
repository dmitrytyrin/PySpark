from pyspark.sql import SparkSession


def pp(s):
    decorator = "=" * 50
    print("\n\n{} {} {}".format(decorator, s, decorator))


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    sc = spark.sparkContext

    tf = sc.textFile('all_stocks_5yr.csv')
    header = tf.first()
    tf = tf.filter(lambda x: x != header)

    pp("2 a")
    pairs = tf.map(lambda x: (x.split(',')[6], x.split(',')[4])) # name + close
    rdd_max = pairs.reduceByKey(max)
    rdd_min = pairs.reduceByKey(min)
    rdd_diff = rdd_max.subtract(rdd_min)
    rdd_sort = rdd_diff.sortBy(lambda x: x[1], ascending=False)
    print(f'наибольшее колебание: {rdd_sort.take(3)}')
    rdd_sort = rdd_sort\
        .zipWithIndex()\
        .filter(lambda x: x[1] < 3)\
        .map(lambda x: ('a', x[0][0]))\
        .groupByKey()\
        .map(lambda x: '2a - ' + ','.join(x[1]))\

    #with open('2a.txt', 'w') as f:
    #    f.write(','.join([i[0] for i in rdd_sort.take(3)]))


    pp("2 b")
    # name + date + close
    pairs = tf\
        .map(lambda x: (x.split(',')[6], x.split(',')[0], x.split(',')[4]))\
        .sortBy(lambda x: (x[0], x[1]), ascending=True)
    rdd1 = pairs.zipWithIndex().map(lambda x: ((x[1], x[0][0]), (x[0][0], x[0][1], x[0][2])))
    rdd2 = pairs.zipWithIndex().map(lambda x: ((x[1]+1, x[0][0]), (x[0][0], x[0][1], x[0][2])))
    #print(rdd1.take(3))
    #print(rdd2.take(3))

    rdd3 = rdd1\
        .join(rdd2)\
        .map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][1], x[1][1][2], float(x[1][0][2])/float(x[1][1][2]) - 1))\
        .sortBy(lambda x: x[5], ascending=False)\
        .map(lambda x: (x[0], x[5]))
    print(f'наибольший рост за день: {rdd3.take(3)}')

    #with open('2b.txt', 'w') as f:
    #    f.write(','.join([i[0] for i in rdd3.take(3)]))

    rdd3 = rdd3\
        .zipWithIndex()\
        .filter(lambda x: x[1] < 3)\
        .map(lambda x: ('a', x[0][0]))\
        .groupByKey()\
        .map(lambda x: '2b - ' + ','.join(x[1]))\

    rdd_sort\
        .union(rdd3)\
        .repartition(1) \
        .saveAsTextFile('hw1')

    spark.stop()
