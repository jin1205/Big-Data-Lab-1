from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

inputs = sys.argv[1]
openTrue = sys.argv[2]
openFalse = sys.argv[3]

#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('business data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
business = text.map(lambda line: json.loads(line)).cache()

#Business:
#'business_id', 'name', 'city', 'latitude', 'longitude', 'stars', 'review_count', 'categories', 'attributes'

business_openTrue = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"], data["categories"], data["attributes"]))

business_openFalse = business.filter(lambda data: data["open"] != True)\
    .map(lambda data: (data["business_id"], data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"], data["categories"], data["attributes"]))


df_openTrue = sqlContext.createDataFrame(business_openTrue, ['business_id', 'name', 'city', 'state', 'latitude', 'longitude',"stars", "review_count", "categories", "attributes"])\
    .coalesce(1)

df_openFalse = sqlContext.createDataFrame(business_openFalse, ['business_id', 'name', 'city', 'state', 'latitude', 'longitude',"stars", "review_count", "categories", "attributes"])\
    .coalesce(1)

df_openTrue.write.save(openTrue, format='json', mode='overwrite')
df_openFalse.write.save(openFalse, format='json', mode='overwrite')

