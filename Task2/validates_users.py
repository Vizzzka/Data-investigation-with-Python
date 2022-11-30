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
            "TR,TT,TV,TW,TZ,UA,UG,UM,US,UY,UZ,VA,VC,VE,VG,VI,VN,VU,WF,WS,YE,YT,ZA,ZM,ZW,UK"


def is_sub_valid(x):
    return x == "0" or x == "1"


def is_email_valid(x):
    return re.fullmatch(regex, x) is not None


def is_country_valid(x):
    return x in countries


def is_name_valid(x):
    return x != ""


def validate_with_rdd(data_path, input_users_file, input_new_users_file):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()
    rdd = spark.sparkContext.textFile(data_path + input_users_file)
    all_users = rdd.map(lambda x: x.split(",")).filter(lambda x: x[0] != "id")

    valid_users = all_users.filter(lambda x: is_name_valid(x[1]) and is_name_valid(x[2]))\
                           .filter(lambda x: is_email_valid(x[3]))\
                           .filter(lambda x: is_country_valid(x[4]))\
                           .filter(lambda x: is_sub_valid(x[5]))

    invalid_users = all_users.map(tuple).subtract(valid_users.map(tuple)).map(list)

    rdd2 = spark.sparkContext.textFile(data_path + input_new_users_file)
    new_users = rdd2.map(lambda x: x.split(",")).filter(lambda x: x[0] != "id")

    valid_users = valid_users.map(lambda x: (x[0], x[1:]))
    new_users = new_users.map(lambda x: (x[0], x[1:]))

    valid_users = valid_users.fullOuterJoin(new_users)\
                             .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else x[1][0]))\
                             .map(lambda x: [x[0]] + x[1])

    valid_users_df = pd.DataFrame(valid_users.collect())
    invalid_users_df = pd.DataFrame(invalid_users.collect())

    store_to_gc("edu_q4_task2", "valid_users.parquet", valid_users_df)
    store_to_gc("edu_q4_task2", "invalid_users.parquet", invalid_users_df)


def validate_with_df(data_path, input_users_file, input_new_users_file):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()

    all_users = spark.read.csv(data_path + input_users_file, header="true")
    new_users = spark.read.csv(data_path + input_new_users_file, header="true")

    valid_users = all_users.filter((all_users.fname != "") & (all_users.lname != ""))\
                           .filter(all_users.email.rlike(regex))\
                           .filter(all_users.country.isin(countries.split(",")))\
                           .filter((all_users.subscription == "0") | (all_users.subscription == "1"))

    invalid_users = all_users.subtract(valid_users)
    shared_users = valid_users.alias('a').join(new_users.alias('b'), on=["id"], how='inner').select('a.*')
    valid_users = valid_users.subtract(shared_users).union(new_users)

    valid_users_df = valid_users.toPandas()
    invalid_users_df = invalid_users.toPandas()

    store_to_gc("edu_q4_task2", "valid_users.parquet", valid_users_df)
    store_to_gc("edu_q4_task2", "invalid_users.parquet", invalid_users_df)


def store_to_gc(bucket_name, blob_name, df):
    df.columns = df.columns.astype(str)
    df.to_parquet("gs://{}/{}".format(bucket_name, blob_name), storage_options={"token": "../fit-legacy-350921-25f33c4f998e.json"})


if __name__ == "__main__":
    validate_with_df("../Training Project Data v0.1/", "users.csv", "users-011.csv")
