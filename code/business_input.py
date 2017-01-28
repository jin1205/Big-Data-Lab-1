from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

inputs = sys.argv[1]
openTrue = sys.argv[2]
openFalse = sys.argv[3]

#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local business_input.py yelp_business.json bus_del_op bus_del_cl

conf = SparkConf().setAppName('business data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
business = text.map(lambda line: json.loads(line)).cache()

#Business:
#'business_id', 'name', 'city', 'latitude', 'longitude', 'stars', 'review_count', 'categories', 'attributes'

business_openTrue = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"], data["categories"], data["attributes"]))\
    .map(lambda (business_id, name, city, state, latitude, longitude, stars, review_count, categories, attributes): (business_id, name, city, state))\
    .map(lambda (business_id, name, city, state): u"%s::%s::%s::%s" % (business_id, name, city, state))\
    .coalesce(1)\
    .cache()

business_openFalse = business.filter(lambda data: data["open"] != True)\
    .map(lambda data: (data["business_id"], data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"], data["categories"], data["attributes"]))\
    .map(lambda (business_id, name, city, state, latitude, longitude, stars, review_count, categories, attributes): (business_id, name, city, state))\
    .map(lambda (business_id, name, city, state): u"%s::%s::%s::%s" % (business_id, name, city, state))\
    .coalesce(1)\
    .cache()

business_openTrue.saveAsTextFile(openTrue)
business_openFalse.saveAsTextFile(openFalse)



