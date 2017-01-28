from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

review_db = sys.argv[1]
output = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('reivew')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text_review = sc.textFile(review_db)

# save the commentdata RDD
review = text_review.map(lambda line: json.loads(line)).cache()


# Review Spring:
#'business_id', 'date'

review_data = review.map(lambda data: (data["business_id"], data["user_id"], data["stars"],data["date"]))\
    .map(lambda (business_id, user_id, stars, date): (business_id, user_id, stars, datetime.datetime.strptime(date, '%Y-%m-%d').date()))\
    .cache()

df_review = sqlContext.createDataFrame(review_data, ['business_id', 'user_id', 'stars', 'date'])\
    .coalesce(1)

df_review.write.save(output, format='json', mode='overwrite')

