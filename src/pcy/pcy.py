from pyspark import SparkContext
sc = SparkContext("local", "PCY")

from operator import add 
import itertools

filepath = "input/T40I10D100K.dat"


lines = sc.textFile(filepath)

MIN_SUPPORT = 150

charts = lines.map(lambda line: sorted(map(int,set(line.split())))).cache()
#charts = sc.parallelize([[3,4,5,6],[2,3,4,5],[2, 3,4,6]])

def hash1(tup):
    return tup[0] * 100 + tup[1]

def hash2(tup):
    return (tup[0] * 100 - tup[1])


support_item = charts.flatMap(lambda line: line)        \
                .map(lambda item: (item, 1))            \
                .reduceByKey(add)                       \
                .filter(lambda x: x[1] > MIN_SUPPORT)   \
                .map(lambda a: a[0])

support_set = set(support_item.collect())

def generate2tuple(chart):
    if len(chart) >= 2:
        for x in xrange(len(chart) - 1):
            xx = chart[x]
            if xx in support_set:
                for y in xrange(x+1, len(chart)):
                    yy = chart[y]
                    if yy in support_set:
                        yield ((xx, yy), 1)

# ((a, b), count)
tuple2 = charts.repartition(36).flatMap(generate2tuple)      \
                .reduceByKey(add)                            \
                .filter(lambda (k, v): v > MIN_SUPPORT)

hash_buckets1_rdd = tuple2.map(lambda (k, v): (hash1(k), v)).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k)
hash_buckets1 = set(hash_buckets1_rdd.collect()) 

hash_buckets2_rdd = tuple2.map(lambda (k, v): (hash2(k), v)).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).map(lambda (k, v): k)
hash_buckets2 = set(hash_buckets2_rdd.collect())

tuple2 = tuple2.filter(lambda (k, v): hash1(k) in hash_buckets1 and hash2(k) in hash_buckets2).map(lambda (k, v): k)
tuple2_set = set(tuple2.collect())


# def generate3tuple(chart):
#     if len(chart) >= 3:
#         for x in xrange(len(chart) - 2):
#             xx = chart[x]
#             if xx in support_set:
#                 for y in xrange(x+1, len(chart)-1):
#                     yy = chart[y]
#                     if yy in support_set and (xx, yy) in tuple2_set:
#                         for z in xrange(y+1, len(chart)):
#                             zz = chart[z]
#                             if zz in support_set and (xx, zz) in tuple2_set and (yy, zz) in tuple2_set:
#                                 yield ((xx,yy,zz), 1)

def generate3tuple(chart):
    if len(chart) >= 3:
        for x in xrange(len(chart) - 2):
            xx = chart[x]
            for y in xrange(x+1, len(chart)-1):
                yy = chart[y]
                if (xx, yy) in tuple2_set:
                    for z in xrange(y+1, len(chart)):
                        zz = chart[z]
                        if (xx, zz) in tuple2_set and (yy, zz) in tuple2_set:
                            yield ((xx,yy,zz), 1)


tuple3s = charts.repartition(36).flatMap(generate3tuple).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).collect()

top100 = sorted(map(lambda (k,v): (v, k), tuple3s), reverse=True)[:100]

for t in top100:
    print t

