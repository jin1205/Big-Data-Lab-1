from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
import datetime

inputs = sys.argv[1]
output = sys.argv[2]


#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local test.py yelp_business.json output-test

conf = SparkConf().setAppName('check in data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

# save the commentdata RDD
checkin = text.map(lambda line: json.loads(line)).cache()

#Tip:
# business_id, checkin_info

checkin_rdd = checkin.map(lambda data: (data["business_id"], data["checkin_info"])).map(lambda (business_id, checkin_info): (business_id, checkin_info))

df_checkin = sqlContext.createDataFrame(checkin_rdd, ['business_id', 'checkin_info']).coalesce(1)

df_checkin.write.save(output, format='json', mode='overwrite')

