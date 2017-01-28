from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

inputs = sys.argv[1]
#output = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('tip data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
tip = text.map(lambda line: json.loads(line)).cache()

#Tip:
# business_id, date

tip_rdd = tip.map(lambda data: (data["business_id"], data["date"])).map(lambda (business_id, date): (business_id, datetime.datetime.strptime(date, '%Y-%m-%d')))
month = tip_rdd.map(lambda (business_id, date): (business_id, date, date.month))

spring = month.filter(lambda (business_id, date, month): month in [3, 4, 5])

print(spring.take(10))
print("\n")
print("******************************* (^_^)")
'''
df_tip = sqlContext.createDataFrame(tip_rdd, ['business_id', 'date']).coalesce(1)

df_tip.write.save(output, format='json', mode='overwrite')
'''
