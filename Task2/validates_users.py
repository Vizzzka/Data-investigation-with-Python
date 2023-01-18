import pyspark
from pyspark import SparkContext
from pyspark.sql.functions import *
import re
from pyspark.sql import SparkSession
import pandas as pd


# for validating an Email
regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
# all country codes
countries = "AD,AE,AF,AG,AI,AL,AM,AO,AQ,AR,AS,AT,AU,AW,AX,AZ,BA,BB,BD,BE,BF,BG,B,BI,BJ,BL,BM,BN,BO,BQ,BR,BS," \
            "BT,BV,BW,BY,BZ,CA,CC,CD,CF,CG,CH,CI,CK,CL,CM,CN,CO,CR,CU,CV,CW,CX,CY,CZ,DE,DJ,DK,DM,DO,DZ,EC,EE," \
            "EG,EH,ER,ES,ET,FI,FJ,FK,FM,FO,FR,GA,GB,GD,GE,GF,GG,GH,GI,GL,GM,GN,GP,GQ,GR,GS,GT,GU,GW,GY,HK,HM," \
            "HN,HR,HT,HU,ID,IE,IL,IM,IN,IO,IQ,IR,IS,IT,JE,JM,JO,JP,KE,KG,KH,KI,KM,KN,KP,KR,KW,KY,KZ,LA,LB,LC," \
            "LI,LK,LR,LS,LT,LU,LV,LY,MA,MC,MD,ME,MF,MG,MH,MK,ML,MM,MN,MO,MP,MQ,MR,MS,MT,MU,MV,MW,MX,MY,MZ,NA," \
            "NC,NE,NF,NG,NI,NL,NO,NP,NR,NU,NZ,OM,PA,PE,PF,PG,PH,PK,PL,PM,PN,PR,PS,PT,PW,PY,QA,RE,RO,RS,RU,RW," \
            "SA,SB,SC,SD,SE,SG,SH,SI,SJ,SK,SL,SM,SN,SO,SR,SS,ST,SV,SX,SY,SZ,TC,TD,TF,TG,TH,TJ,TK,TL,TM,TN,TO," \
            "TR,TT,TV,TW,TZ,UA,UG,UM,US,UY,UZ,VA,VC,VE,VG,VI,VN,VU,WF,WS,YE,YT,ZA,ZM,ZW,UK".split(",")


def is_sub_valid(x):
    return x == "0" or x == "1"


def is_email_valid(x):
    return re.fullmatch(regex, x) is not None


def is_country_valid(x):
    return x in countries


def is_name_valid(x):
    return x != ""


def validate_single_user_spark_rdd(user_rdd):
    return user_rdd.filter(lambda x: is_name_valid(x[1]) and is_name_valid(x[2])) \
                   .filter(lambda x: is_email_valid(x[3])) \
                   .filter(lambda x: is_country_valid(x[4])) \
                   .filter(lambda x: is_sub_valid(x[5]))


def validate_single_user_spark_df(users_df):
    return users_df.filter((users_df.fname != "") & (users_df.lname != ""))\
                   .filter(users_df.email.rlike(regex))\
                   .filter(users_df.country.isin(countries))\
                   .filter((users_df.subscription == "0") | (users_df.subscription == "1"))


def validate_with_df(data_path, input_users_file, input_new_users_file):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # This is required if you are using service account and set true,
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "../fit-legacy-350921-25f33c4f998e.json")

    all_users = spark.read.csv(data_path + input_users_file, header="true")
    new_users = spark.read.csv(data_path + input_new_users_file, header="true")

    valid_new_users = validate_single_user_spark_df(new_users)
    valid_users = validate_single_user_spark_df(all_users)

    invalid_users = all_users.subtract(valid_users).union(new_users.subtract(valid_new_users))
    shared_users = valid_users.alias('a').join(valid_new_users.alias('b'), on=["id"], how='inner').select('a.*')
    valid_users = valid_users.subtract(shared_users).union(valid_new_users)

    valid_users.write.parquet("gs://edu_q4_task2/valid_users.parquet")
    invalid_users.write.parquet("gs://edu_q4_task2/invalid_users.parquet")

    #store_to_gc("edu_q4_task2", "valid_users.parquet", valid_users_df)
    #store_to_gc("edu_q4_task2", "invalid_users.parquet", invalid_users_df)


def validate_with_rdd(data_path, input_users_file, input_new_users_file):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(data_path + input_users_file)
    all_users = rdd.map(lambda x: x.split(",")).filter(lambda x: x[0] != "id")

    valid_users = validate_single_user_spark_rdd(all_users)
    invalid_users = all_users.map(tuple).subtract(valid_users.map(tuple)).map(list)

    rdd2 = spark.sparkContext.textFile(data_path + input_new_users_file)
    new_users = rdd2.map(lambda x: x.split(",")).filter(lambda x: x[0] != "id")
    valid_new_users = validate_single_user_spark_rdd(new_users)
    invalid_new_users = new_users.map(tuple).subtract(valid_new_users.map(tuple)).map(list)

    valid_users = valid_users.map(lambda x: (x[0], x[1:]))
    valid_new_users = valid_new_users.map(lambda x: (x[0], x[1:]))

    invalid_users = invalid_users.map(lambda x: (x[0], x[1:]))
    invalid_new_users = invalid_new_users.map(lambda x: (x[0], x[1:]))

    valid_users = valid_users.fullOuterJoin(valid_new_users)\
                             .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else x[1][0]))\
                             .map(lambda x: [x[0]] + x[1])
    invalid_users = invalid_users.fullOuterJoin(invalid_new_users)\
                                 .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else x[1][0]))\
                                 .map(lambda x: [x[0]] + x[1])

    valid_users = valid_users.map(lambda data: ','.join(str(d) for d in data))
    #valid_users.saveAsTextFile("valid_users.parquet")

    #store_to_gc("edu_q4_task2", "valid_users.parquet", valid_users_df)
    #store_to_gc("edu_q4_task2", "invalid_users.parquet", invalid_users_df)


def store_to_gc(bucket_name, blob_name, df):
    df.columns = df.columns.astype(str)
    df.to_parquet("gs://{}/{}".format(bucket_name, blob_name), storage_options={"token": "../fit-legacy-350921-25f33c4f998e.json"})


if __name__ == "__main__":
    validate_with_rdd("../Training Project Data v0.1/", "users.csv", "users-011.csv")