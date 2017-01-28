from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

def top(line):
    key = line[0]
    value = list(line[1])
    sorted_count = sorted(value, key = lambda tup: tup[0], reverse = True)
    top = [(i[1], i[2]) for i in sorted_count][0:50]
    outstr = "|".join("%s,%s" % tup for tup in top)
    return (key, outstr)


review_db = sys.argv[1]
business_db = sys.argv[2]
output = sys.argv[3]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('reivew')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text_review = sc.textFile(review_db)
text_business = sc.textFile(business_db)

# save the commentdata RDD
review = text_review.map(lambda line: json.loads(line)).cache()
business = text_business.map(lambda line: json.loads(line)).cache()


review_data = review.map(lambda data: (data["business_id"], data["date"]))\
    .map(lambda (business_id, date): (business_id, datetime.datetime.strptime(date[0:10], '%Y-%m-%d')))\
    .cache()

review_moth_data = review_data.map(lambda (business_id, date): (business_id, date, date.month)).cache()


review_spring = review_moth_data.filter(lambda (business_id, date, month): month in [3, 4, 5]).map(lambda (business_id, date, month): ((business_id, "spring"),1))
review_summer = review_moth_data.filter(lambda (business_id, date, month): month in [6, 7, 8]).map(lambda (business_id, date, month): ((business_id, "summer"),1))
review_autumn = review_moth_data.filter(lambda (business_id, date, month): month in [9, 10, 11]).map(lambda (business_id, date, month): ((business_id, "autumn"),1))
review_winter = review_moth_data.filter(lambda (business_id, date, month): month in [12, 1, 2]).map(lambda (business_id, date, month): ((business_id, "winter"),1))

spring_count = review_spring.reduceByKey(operator.add)
summer_count = review_summer.reduceByKey(operator.add)
autumn_count = review_autumn.reduceByKey(operator.add)
winter_count = review_winter.reduceByKey(operator.add)

review_season = sc.union([spring_count, summer_count, autumn_count, winter_count])

season_count = review_season.map(lambda ((business_id, season), count): (business_id, (season, count)))\
    .filter(lambda (business_id, (season, count)): count > 10)\
    .cache()

business_data = business.filter(lambda data: data["open"] == True)\
    .map(lambda data: (data["business_id"], data["name"], data["state"]))\
    .map(lambda (business_id, name, state): (business_id, (name, state)))\
    .cache()

review_business = season_count.join(business_data).cache()
top_stores = review_business.map(lambda (business_id, \
						((season, review_count), (name, state))): \
						((state, season), (review_count, business_id, name))) \
                        .groupByKey().map(top).cache()

outdata = top_stores.map(lambda ((state, season), stores): "%s::%s::%s" % (state, season, stores))

outdata.coalesce(1).saveAsTextFile(output)

