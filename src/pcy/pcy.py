from pyspark import SparkContext
from operator import add 
import itertools
sc = SparkContext("local", "PCY")


filepath = "input/totaldata"


lines = sc.textFile(filepath)

MIN_SUPPORT = 1500

charts = lines.map(lambda line: sorted(set(line.split()))).cache()

support_item = charts.flatMap(lambda line: line)        \
                .map(lambda item: (item, 1))            \
                .reduceByKey(add)                       \
                .filter(lambda x: x[1] > MIN_SUPPORT)   \
                .map(lambda a: a[0])

support_set = set(support_item.collect())

# ((a, b), count)
tuple2 = charts.flatMap(lambda line: itertools.combinations([x for x in line if x in support_set and len(x) > 0], 2)) \
                .map(lambda x: (x, 1))      \
                .reduceByKey(add)           \
                .filter(lambda (k, v): v > MIN_SUPPORT).cache()

def hash1(param):
    tup, count = param
    return (tup[0] + tup[1], count)

def hash2(param):
    tup, count = param
    return (int(tup[0]) + int(tup[1]), count)

hash_buckets1 = tuple2.map(hash1).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k).collect()
hash_buckets1 = set(hash_buckets1)

hash_buckets2 = tuple2.map(hash2).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k).collect()
hash_buckets2 = set(hash_buckets2)

def generate3tuple(chart):
    if len(chart) >= 3:
        for x in xrange(len(chart) - 2):
            xx = chart[x]
            for y in xrange(x+1, len(chart)-1):
                yy = chart[y]
                if (hash1(((xx, yy), 0)))[0] in hash_buckets1 and (hash2(((xx, yy), 0)))[0] in hash_buckets2:
                    for z in xrange(y+1, len(chart)):
                        zz = chart[z]
                        if (hash1(((xx, zz), 0)))[0] in hash_buckets1 and (hash2(((xx, zz), 0)))[0] in hash_buckets2 and \
                            (hash1(((yy, zz), 0)))[0] in hash_buckets1 and (hash2(((yy, zz), 0)))[0] in hash_buckets2:
                            yield ((xx,yy,zz), 1)


tuple3s = charts.repartition(12).flatMap(generate3tuple).reduceByKey(add).collect()


