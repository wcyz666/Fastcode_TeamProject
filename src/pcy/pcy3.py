from pyspark import SparkContext
sc = SparkContext("local", "PCY")

from operator import add 
import itertools

filepath = "input/T40I10D100K.dat"


lines = sc.textFile(filepath)

MIN_SUPPORT = 150

charts = lines.map(lambda line: sorted(map(int,set(line.split())))).cache()
hash_buckets1 = {}
hash_buckets2 = {}

def hash1(tup):
    return tup[0] + tup[1]

def hash2(tup):
    return (tup[0] - tup[1]) % 900

def firstPassAndHash(chart):
    if len(chart) >= 2:
        for tup in itertools.combinations(chart, 2):
            hash_buckets1[hash1(tup)] = hash_buckets1.get(hash1(tup), 0) + 1
            hash_buckets2[hash2(tup)] = hash_buckets2.get(hash2(tup), 0) + 1
    for x in chart:
        yield (x, 1)

support_item = charts.flatMap(firstPassAndHash)         \
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
                    if yy in support_set and hash1((xx, yy)) in hash_buckets1 and hash2((xx, yy)) in hash_buckets2:
                        yield ((xx, yy), 1)

# ((a, b), count)
tuple2 = charts.repartition(36).flatMap(generate2tuple)     \
                .reduceByKey(add)                           \
                .filter(lambda (k, v): v > MIN_SUPPORT)

tuple2_set = set(tuple2.map(lambda (k, v): k).collect())


def generate3tuple(chart):
    if len(chart) >= 3:
        for x in xrange(len(chart) - 2):
            xx = chart[x]
            if xx in support_set:
                for y in xrange(x+1, len(chart)-1):
                    yy = chart[y]
                    if yy in support_set and (xx, yy) in tuple2_set:
                        for z in xrange(y+1, len(chart)):
                            zz = chart[z]
                            if zz in support_set and (xx, zz) in tuple2_set and (yy, zz) in tuple2_set:
                                yield ((xx,yy,zz), 1)


tuple3s = charts.repartition(36).flatMap(generate3tuple).reduceByKey(add).filter(lambda (k, v): v > MIN_SUPPORT).collect()

top100 = sorted(map(lambda (k,v): (v, k), tuple3s))[:100]

for t in top100:
    print t

