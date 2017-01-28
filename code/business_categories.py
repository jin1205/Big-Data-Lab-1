from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

inputs = sys.argv[1]
OpenRestaurant = sys.argv[2]
CloseRestaurant = sys.argv[3]

#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local business_categories.py yelp_business.json OpenNightlife CloseNightlife

conf = SparkConf().setAppName('business data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)


# save the commentdata RDD
business = text.map(lambda line: json.loads(line)).cache()

# *********************************************************************************************************************************************************************
# "open" = True, categories = "Restaurants"
business_open = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], (data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"])))\
    .cache()

business_open_catlist = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], data["categories"]))\
    .cache()

business_open_unpack = business_open_catlist.flatMap(lambda data: [(data[0], x) for x in data[1]]).cache()

business_open_restaurant = business_open_unpack.filter(lambda (business_id, categories): categories == 'Nightlife')\
    .join(business_open)\
    .map(lambda (business_id, (categories, (data))): (business_id, categories, data[0], data[1], data[2], data[3], data[4], data[5], data[6]))\
    .cache()

df_openRestaurants = sqlContext.createDataFrame(business_open_restaurant, ['business_id', 'categories', 'name', 'city', 'state', 'latitude', 'longitude','stars', 'review_count'])\
    .coalesce(1)

# *********************************************************************************************************************************************************************
'''
print(business_open_restaurant.take(1))
print("************************* (^_^) *************************")

'''


# *********************************************************************************************************************************************************************
# "open" = False, categories = "Restaurants"
business_close = business.filter(lambda data: data["open"] != True)\
    .map(lambda data: (data["business_id"], (data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"])))\
    .cache()

business_close_catlist = business.filter(lambda data: data["open"] != True)\
    .map(lambda data: (data["business_id"], data["categories"]))\
    .cache()

business_close_unpack = business_close_catlist.flatMap(lambda data: [(data[0], x) for x in data[1]]).cache()

business_close_restaurant = business_close_unpack.filter(lambda (business_id, categories): categories == 'Nightlife')\
    .join(business_close)\
    .map(lambda (business_id, (categories, (data))): (business_id, categories, data[0], data[1], data[2], data[3], data[4], data[5], data[6]))\
    .cache()

df_closeRestaurants = sqlContext.createDataFrame(business_close_restaurant, ['business_id', 'categories', 'name', 'city', 'state', 'latitude', 'longitude','stars', 'review_count'])\
    .coalesce(1)

# *********************************************************************************************************************************************************************

'''
business_close = business.filter(lambda data: data["open"] != True)\
    .map(lambda data: (data["business_id"], data["name"], data["city"], data["state"], data["latitude"], data["longitude"], data["stars"], data["review_count"], data["categories"], data["attributes"]))\
    .cache()
'''

df_openRestaurants.write.save(OpenRestaurant, format='json', mode='overwrite')
df_closeRestaurants.write.save(CloseRestaurant, format='json', mode='overwrite')

