Frequent Itemset Algorithms
===========================

### Apriori Spark
* path: src/apriori/apriori.py
* run in spark: spark-submit --master-client apriori.py
### Apriori Mapreduce
* path: src/aprioriMapreduce
* run: hadoop jar name.jar Apriori -input [input] -output [output]
### PCY Spark
* path: src/pcy/pcy.py
* run: spark-submit --master-client src/pcy/pcy.py
### SON Mapreduce
* path: src/itemcountSON
* run: hadoop jar name.jar SON -input [input] -output [output] -pass [pass1file]
