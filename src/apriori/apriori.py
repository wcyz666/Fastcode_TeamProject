from __future__ import print_function

from operator import add

from pyspark import SparkContext

import time

if __name__ == "__main__":
    def transacToInt(line):
        nums = line.strip().split(" ")
        for i in range(len(nums)):
            nums[i] = int(nums[i])
        return nums


    def conbination1(nums):
        for i in nums:
            yield i,1

    def conbination2(nums):
        nums.sort()
        for i in range(len(nums)):
            if nums[i] in setOfFrequentItem1:
                for j in range(i + 1,len(nums)):
                    if nums[j] in setOfFrequentItem1:
                        yield (nums[i],nums[j]),1

    def conbination3(nums):
        nums.sort()
        for i in range(len(nums)):
            for j in range(i + 1,len(nums)):
                if (nums[i],nums[j]) in setOfFrequentItem2:
                    for k in range(j + 1, len(nums)):
                        if (nums[j],nums[k]) in setOfFrequentItem2 and (nums[i],nums[k]) in setOfFrequentItem2:
                            yield (nums[i],nums[j], nums[k]),1

    t = time.time()

    MIN = 1500
    input = "input/data1.dat"
    sc = SparkContext(appName="apriori")

    lines = sc.textFile(input, 12)
    transaction = lines.map(transacToInt).persist()

    #list of [(id, count),()]
    frequentItem1 = transaction            \
                    .flatMap(conbination1) \
                    .reduceByKey(add, 36)  \
                    .filter(lambda (x,y): y > MIN)

    setOfFrequentItem1 = set(frequentItem1.keys().collect())



    #list : [ ((1, 4), 7), ((2, 3), 3)]
    frequentItem2 = transaction            \
                    .flatMap(conbination2) \
                    .reduceByKey(add, 36)  \
                    .filter(lambda (x,y): y > MIN)

    setOfFrequentItem2 = set(frequentItem2.keys().collect())


    #rdd : [ ((1, 4, 5), 7),...]
    frequentItem3 = transaction        \
                .flatMap(conbination3) \
                .reduceByKey(add, 36)  \
                .filter(lambda (x,y): y > MIN)



    frequentItem3.saveAsTextFile("final")

    a = time.time() -t
    print ("time======= " + str(a))

    sc.stop()