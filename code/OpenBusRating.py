from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext

business_input = sys.argv[1]
spring_input = sys.argv[2]
summer_input = sys.argv[3]
autumn_input = sys.argv[4]
winter_input = sys.argv[5]
output = sys.argv[6]

#setenv SPARK_HOME /Volumes/projects/big-data/spark-1.5.1-bin-hadoop2.6/
#${SPARK_HOME}/bin/spark-submit --master local OpenBusRating.py bus_del_open_new_id rating_del_new_id_spring rating_del_new_id_summer rating_del_new_id_autumn rating_del_new_id_winter BusRating

conf = SparkConf().setAppName('open business rating')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

business = sc.textFile(business_input)
spring = sc.textFile(spring_input)
summer = sc.textFile(summer_input)
autumn = sc.textFile(autumn_input)
winter = sc.textFile(winter_input)

businessRDD = business.map(lambda list: list.split('::'))\
    .map(lambda (business_id, name, city, state): ((business_id), (state)))\
    .cache()

springRDD = spring.map(lambda list: list.split('::'))\
    .map(lambda (user_id, business_id, rating, date): ((business_id), (user_id, rating, date)))\
    .cache()

summerRDD = summer.map(lambda list: list.split('::'))\
    .map(lambda (user_id, business_id, rating, date): ((business_id), (user_id, rating, date)))\
    .cache()

autumnRDD = autumn.map(lambda list: list.split('::'))\
    .map(lambda (user_id, business_id, rating, date): ((business_id), (user_id, rating, date)))\
    .cache()

winterRDD = winter.map(lambda list: list.split('::'))\
    .map(lambda (user_id, business_id, rating, date): ((business_id), (user_id, rating, date)))\
    .cache()

BR_spring = springRDD.join(businessRDD)\
    .map(lambda (business_id, (rating_data, state)): (business_id, rating_data[0], rating_data[1], rating_data[2], state))\
    .cache()

BR_summer = summerRDD.join(businessRDD)\
    .map(lambda (business_id, (rating_data, state)): (business_id, rating_data[0], rating_data[1], rating_data[2], state))\
    .cache()

BR_autumn = autumnRDD.join(businessRDD)\
    .map(lambda (business_id, (rating_data, state)): (business_id, rating_data[0], rating_data[1], rating_data[2], state))\
    .cache()

BR_winter = winterRDD.join(businessRDD)\
    .map(lambda (business_id, (rating_data, state)): (business_id, rating_data[0], rating_data[1], rating_data[2], state))\
    .cache()


# 10 cities in Spring
EDH_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "EDH")
BW_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "BW")
QC_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "QC")
ON_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "ON")
PA_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "PA")
NC_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "NC")
IC_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "IL")
AZ_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "AZ")
NV_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "NV")
WI_spring = BR_spring.filter(lambda (business_id, user_id, rating, date, state): state == "WI")


# 10 cities in Summer
EDH_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "EDH")
BW_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "BW")
QC_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "QC")
ON_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "ON")
PA_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "PA")
NC_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "NC")
IC_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "IL")
AZ_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "AZ")
NV_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "NV")
WI_summer = BR_summer.filter(lambda (business_id, user_id, rating, date, state): state == "WI")


# 10 cities in Autumn
EDH_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "EDH")
BW_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "BW")
QC_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "QC")
ON_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "ON")
PA_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "PA")
NC_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "NC")
IC_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "IL")
AZ_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "AZ")
NV_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "NV")
WI_autumn = BR_autumn.filter(lambda (business_id, user_id, rating, date, state): state == "WI")


# 10 cities in Winter
EDH_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "EDH")
BW_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "BW")
QC_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "QC")
ON_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "ON")
PA_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "PA")
NC_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "NC")
IC_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "IL")
AZ_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "AZ")
NV_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "NV")
WI_winter = BR_winter.filter(lambda (business_id, user_id, rating, date, state): state == "WI")


# ****************************************************************************************************************************

EDH_spring_output = EDH_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
BW_spring_output = BW_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
QC_spring_output = QC_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
ON_spring_output = ON_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
PA_spring_output = PA_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NC_spring_output = NC_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
IC_spring_output = IC_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
AZ_spring_output = AZ_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NV_spring_output = NV_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
WI_spring_output = WI_spring.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()

# ****************************************************************************************************************************

EDH_summer_output = EDH_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
BW_summer_output = BW_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
QC_summer_output = QC_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
ON_summer_output = ON_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
PA_summer_output = PA_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NC_summer_output = NC_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
IC_summer_output = IC_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
AZ_summer_output = AZ_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NV_summer_output = NV_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
WI_summer_output = WI_summer.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()

# ****************************************************************************************************************************

EDH_autumn_output = EDH_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
BW_autumn_output = BW_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
QC_autumn_output = QC_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
ON_autumn_output = ON_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
PA_autumn_output = PA_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NC_autumn_output = NC_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
IC_autumn_output = IC_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
AZ_autumn_output = AZ_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NV_autumn_output = NV_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
WI_autumn_output = WI_autumn.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()

# ****************************************************************************************************************************

EDH_winter_output = EDH_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
BW_winter_output = BW_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
QC_winter_output = QC_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
ON_winter_output = ON_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
PA_winter_output = PA_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NC_winter_output = NC_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
IC_winter_output = IC_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
AZ_winter_output = AZ_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
NV_winter_output = NV_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()
WI_winter_output = WI_winter.map(lambda (business_id, user_id, rating, date, state): u"%s::%s::%s::%s" % (user_id, business_id, rating, date))\
    .coalesce(1)\
    .cache()

# ****************************************************************************************************************************


EDH_spring_output.saveAsTextFile(output + '/EDH_spring')
BW_spring_output.saveAsTextFile(output + '/BW_spring')
QC_spring_output.saveAsTextFile(output + '/QC_spring')
ON_spring_output.saveAsTextFile(output + '/ON_spring')
PA_spring_output.saveAsTextFile(output + '/PA_spring')
NC_spring_output.saveAsTextFile(output + '/NC_spring')
IC_spring_output.saveAsTextFile(output + '/IL_spring')
AZ_spring_output.saveAsTextFile(output + '/AZ_spring')
NV_spring_output.saveAsTextFile(output + '/NV_spring')
WI_spring_output.saveAsTextFile(output + '/WI_spring')


EDH_summer_output.saveAsTextFile(output + '/EDH_summer')
BW_summer_output.saveAsTextFile(output + '/BW_summer')
QC_summer_output.saveAsTextFile(output + '/QC_summer')
ON_summer_output.saveAsTextFile(output + '/ON_summer')
PA_summer_output.saveAsTextFile(output + '/PA_summer')
NC_summer_output.saveAsTextFile(output + '/NC_summer')
IC_summer_output.saveAsTextFile(output + '/IL_summer')
AZ_summer_output.saveAsTextFile(output + '/AZ_summer')
NV_summer_output.saveAsTextFile(output + '/NV_summer')
WI_summer_output.saveAsTextFile(output + '/WI_summer')


EDH_autumn_output.saveAsTextFile(output + '/EDH_autumn')
BW_autumn_output.saveAsTextFile(output + '/BW_autumn')
QC_autumn_output.saveAsTextFile(output + '/QC_autumn')
ON_autumn_output.saveAsTextFile(output + '/ON_autumn')
PA_autumn_output.saveAsTextFile(output + '/PA_autumn')
NC_autumn_output.saveAsTextFile(output + '/NC_autumn')
IC_autumn_output.saveAsTextFile(output + '/IL_autumn')
AZ_autumn_output.saveAsTextFile(output + '/AZ_autumn')
NV_autumn_output.saveAsTextFile(output + '/NV_autumn')
WI_autumn_output.saveAsTextFile(output + '/WI_autumn')


EDH_winter_output.saveAsTextFile(output + '/EDH_winter')
BW_winter_output.saveAsTextFile(output + '/BW_winter')
QC_winter_output.saveAsTextFile(output + '/QC_winter')
ON_winter_output.saveAsTextFile(output + '/ON_winter')
PA_winter_output.saveAsTextFile(output + '/PA_winter')
NC_winter_output.saveAsTextFile(output + '/NC_winter')
IC_winter_output.saveAsTextFile(output + '/IL_winter')
AZ_winter_output.saveAsTextFile(output + '/AZ_winter')
NV_winter_output.saveAsTextFile(output + '/NV_winter')
WI_winter_output.saveAsTextFile(output + '/WI_winter')
