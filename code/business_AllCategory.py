from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

inputs = sys.argv[1]
categories_reviewcount = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local business_AllCategory.py yelp_business.json business_categories

conf = SparkConf().setAppName('business data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
business = text.map(lambda line: json.loads(line)).cache()

# *********************************************************************************************************************************************************************
# "open" = True, categories = "Restaurants"
business_list = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], (data["full_address"], data["name"], data["city"], data["state"], data["stars"], data["review_count"])))\
    .cache()

business_catlist = business.map(lambda data: (data["business_id"], data["categories"]))\
    .cache()

business_unpack = business_catlist.flatMap(lambda data: [(data[0], x) for x in data[1]]).cache()

business_cat_count = business_unpack.filter(lambda (business_id, categories): (((categories == 'Food') or categories == 'Restaurants') or (categories == 'Nightlife')
                                                                                     or (categories == 'Shopping') or (categories == 'Active Life') or (categories == 'Automotive')
                                                                                     or (categories == 'Beauty & Spas') or (categories == 'Health & Medical') or (categories == 'Hotels & Travel')
                                                                                     or (categories == 'Pets')))\
    .join(business_list)\
    .map(lambda (business_id, (categories, (data))): (business_id, categories, data[0], data[1], data[2], data[3], data[4], data[5]))\
    .coalesce(1)\
    .cache()

df_cat_count = sqlContext.createDataFrame(business_cat_count, ['business_id', 'categories', 'full_address', 'name', 'city', 'state','stars', 'review_count'])

# *********************************************************************************************************************************************************************
'''
print(business_open_restaurant.take(1))
print("************************* (^_^) *************************")

'''

df_cat_count.write.save(categories_reviewcount, format='json', mode='overwrite')


