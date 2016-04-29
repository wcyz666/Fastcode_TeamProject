from pyspark import SparkContext
sc = SparkContext("local", "PCY")

from operator import add 
import itertools

filepath = "input/T40I10D100K.dat"


lines = sc.textFile(filepath)

MIN_SUPPORT = 150

charts = lines.map(lambda line: sorted(map(int,set(line.split())))).cache()

support_item = charts.flatMap(lambda line: line)        \
                .map(lambda item: (item, 1))            \
                .reduceByKey(add)                       \
                .filter(lambda x: x[1] > MIN_SUPPORT)

support_set = set(support_item.map(lambda (k, v): k).collect())


def hash1(param):
    item, count = param
    return (item % 400, count)

def hash2(param):
    item, count = param
    return (item % 600, count)

hash_buckets1_rdd = support_item.map(hash1).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k)
hash_buckets1 = set(hash_buckets1_rdd.collect()) 

hash_buckets2_rdd = support_item.map(hash2).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k)
hash_buckets2 = set(hash_buckets2_rdd.collect())

def generate2tuple(chart):
    if len(chart) >= 2:
        for x in xrange(len(chart) - 1):
            xx = chart[x]
            if xx in support_set and hash1((xx, 0))[0] in hash_buckets1 and hash2((xx, 0))[0] in hash_buckets2:
                for y in xrange(x+1, len(chart)):
                    yy = chart[y]
                    if yy in support_set and hash1((yy, 0))[0] in hash_buckets1 and hash2((yy, 0))[0] in hash_buckets2:
                        yield ((xx, yy), 1)


# ((a, b), count)
tuple2_rdd = charts.flatMap(generate2tuple).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k)
tuple2 = set(tuple2_rdd.collect())

def generate3tuple(chart):
    if len(chart) >= 3:
        for x in xrange(len(chart) - 2):
            xx = chart[x]
            if xx in support_set and hash1((xx, 0))[0] in hash_buckets1 and hash2((xx, 0))[0] in hash_buckets2:
                for y in xrange(x+1, len(chart)-1):
                    yy = chart[y]
                    if yy in support_set and hash1((yy, 0))[0] in hash_buckets1 and hash2((yy, 0))[0] in hash_buckets2 and (xx, yy) in tuple2:
                            for z in xrange(y+1, len(chart)):
                                zz = chart[z]
                                if zz in support_set and hash1((zz, 0))[0] in hash_buckets1 and hash2((zz, 0))[0] in hash_buckets2 and \
                                    (xx, zz) in tuple2 and (yy, zz) in tuple2:
                                    yield ((xx,yy,zz), 1)


tuple3s = charts.repartition(36).flatMap(generate3tuple).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).collect()

top100 = sorted(map(lambda (k,v): (v, k), tuple3s))[:100]
for t in top100:
    print t


