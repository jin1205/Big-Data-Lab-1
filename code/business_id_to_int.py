from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

business_inputs = sys.argv[1]
bus_id_map = sys.argv[2]
openTrue_newid_with_address = sys.argv[3]
# review_inputs = sys.argv[2]
# user_inputs =  sys.argv[3]
# openTrue_newid = sys.argv[4]
# rating_newid = sys.argv[5]
# bus_id_map = sys.argv[6]
# user_id_map = sys.argv[7]

#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local business_input.py yelp_business.json bus_del_op bus_del_cl

conf = SparkConf().setAppName('business data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(business_inputs)
text4 = sc.textFile(bus_id_map)

# text2 = sc.textFile(review_inputs)

# text3 = sc.textFile(user_inputs)

# save the business RDD
business = text.map(lambda line: json.loads(line))

# since we only need to recommend open stores, only parse open business
business_openTrue = business.map(lambda data: (data["business_id"], (data["name"], data["city"], data["state"], data["full_address"])))

# # save the review RDD
# review = text2.map(lambda line: json.loads(line))

# # parse review RDD
# review_data = review.map(lambda data: (data["business_id"], (data["user_id"], data["stars"],data["date"]))) 

# new business_id RDD to map original business id to new integer id
# business_id_rdd = business.map(lambda data: (data["business_id"]))

# business_id_rdd = business_id_rdd.zipWithIndex()

business_id_rdd = text4.map(lambda line: json.loads(line))
business_id_rdd = business_id_rdd.map(lambda data: (data["business_id"], data["bus_new_id"]))
# # save a json file mapping from original business_id to bus_new_id
# df_bus_id_map = sqlContext.createDataFrame(business_id_rdd, ['business_id', 'bus_new_id'])\
#     .coalesce(1)
# df_bus_id_map.write.save(bus_id_map, format='json', mode='overwrite')

# join new RDD to original business to obtain new RDD for business
joined_bus = business_id_rdd.join(business_openTrue).map(lambda (business_id, (new_id, (name, city, state, address))): (new_id, name, city, state, address))

business_openTrue_newid = joined_bus.map(lambda (business_id, name, city, state, address): u"%s::%s::%s::%s::%s" % (business_id, name, city, state, address.replace("\n", ", ")))\
    .coalesce(1)\
    

# join new RDD to original review data to obtain new RDD for review
# joined_review = business_id_rdd.join(review_data).map(lambda (business_id, (new_busid, (user_id, stars, date))): (user_id, (new_busid, stars, date)))

# review_newid = joined_review.map(lambda (business_id, user_id, stars, date): u"%s::%s::%s::%s" % (user_id, business_id, stars, date))\
#     .coalesce(1)\

# new user_id RDD to map original user id to new integer id
# user = text3.map(lambda line: json.loads(line))
# user_id_rdd = user.map(lambda data: (data["user_id"])).zipWithIndex()

# # save a json file mapping from original user_id to new_user_id
# df_user_id_map = sqlContext.createDataFrame(user_id_rdd, ['user_id', 'user_new_id'])\
#     .coalesce(1)
# df_user_id_map.write.save(user_id_map, format='json', mode='overwrite')

# # join new user_id RDD to joined_review
# joined_review = user_id_rdd.join(joined_review).map(lambda (user_id, (new_userid, (bus_id, stars, date))): (new_userid, bus_id, stars, date))
# review_newid = joined_review.map(lambda (user_id, bus_id, stars, date): u"%s::%s::%s::%s" % (user_id, bus_id, stars, date))\
#     .coalesce(1)\

# Business output: business_id::name::city::state
business_openTrue_newid.saveAsTextFile(openTrue_newid_with_address)
# Rating output: user_id::business_id::rating:date
# review_newid.saveAsTextFile(rating_newid)



