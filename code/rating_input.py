from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

inputs = sys.argv[1]
output = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local rating_input.py review_post.json rating_del

conf = SparkConf().setAppName('rating input')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
review = text.map(lambda line: json.loads(line)).cache()

# Review Spring:
#'business_id', 'date'

review_data = review.map(lambda data: (data["business_id"], data["user_id"], data["stars"],data["date"]))\
    .map(lambda (business_id, user_id, stars, date): u"%s::%s::%s::%s" % (user_id, business_id, stars, date))\
    .coalesce(1)\
    .cache()

review_data.saveAsTextFile(output)


