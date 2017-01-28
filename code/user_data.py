from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

inputs = sys.argv[1]
output = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('user data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
user = text.map(lambda line: json.loads(line)).cache()

#User:
#'user_id', 'review count', 'average_stars', 'yelping since'

user_rdd = user.map(lambda data: (data["user_id"], data["review_count"], data["average_stars"], data["yelping_since"]))\
    .map(lambda (user_id, review_count, average_stars, yelping_since): (user_id, review_count, average_stars, datetime.datetime.strptime(yelping_since, '%Y-%m')))

df_user = sqlContext.createDataFrame(user_rdd, ['user_id', 'review_count', 'average_stars', 'yelping_since']).coalesce(1)

df_user.write.save(output, format='json', mode='overwrite')


