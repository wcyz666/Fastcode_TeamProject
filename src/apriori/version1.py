from operator import add

from pyspark import SparkContext

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
        #nums = lines.split(" ")
        nums.sort()
        for i in range(len(nums)):
            if nums[i] in find_one:
                for j in range(i + 1,len(nums)):
                    if nums[j] in find_one:
                        yield (nums[i],nums[j]),1

    def conbination3(nums):
        nums.sort()
        for i in range(len(nums)):
            for j in range(i + 1,len(nums)):
                if (nums[i],nums[j]) in find_two:
                    for k in range(j + 1, len(nums)):
                        if (nums[j],nums[k]) in find_two and (nums[i],nums[k]) in find_two:
                            yield (nums[i],nums[j], nums[k]),1


    MIN = 1500
    input = "input/data.dat"
    sc = SparkContext(appName="apriori")
    print ("context created============")
    lines = sc.textFile(input)
    transaction = lines.map(transacToInt).cache()

    #list of [(id, count),()]
    one_data_list = transaction.flatMap(conbination1).reduceByKey(add).filter(lambda (x,y): y > MIN).collect()

    find_one = set()
    for key, val in one_data_list:
        find_one.add(key)


    #list : [((1, 2), 1), ((1, 3), 1), ((1, 4), 1), ((2, 3), 1), ((2, 4), 1), ((3, 4), 1)]
    two_data_list = transaction.flatMap(conbination2).reduceByKey(add).filter(lambda (x,y): y > MIN).collect()

    find_two = set()
    for key,v in two_data_list:
        find_two.add(key)

    #list:
    three_list = transaction.flatMap(conbination3).reduceByKey(add).filter(lambda (x,y): y > MIN)

    three_list.saveAsTextFile("final")
    #rank.collect().foreach(print)
    sc.stop()





