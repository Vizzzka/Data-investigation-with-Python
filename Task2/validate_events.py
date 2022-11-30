import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, when, array_contains, lit
from pyspark.sql.types import StringType
import pandas as pd


def validate_events_rdd(data_path, input_events):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()
    rdd = spark.sparkContext.textFile(data_path + input_events)
    all_events = rdd.map(lambda x: json.loads(x)).flatMap(lambda x: [x] if "events" not in x else x["events"])
    valid_events = all_events.filter(lambda x: type(x["user_id"]) == int)\
                             .filter(lambda x: type(x["video_id"]) == int)\
                             .filter(lambda x: x["event"] in "created,like,commented,add_tags,remove_tags")\
                             .filter(lambda x: type(x["timestamp"]) == int)

    #invalid_events = all_events.map(lambda x: tuple(x.items())).subtract(valid_events.map(lambda x: tuple(x.items())))
    #print(invalid_events.collect())

    valid_events_df = pd.DataFrame(valid_events.collect())
    #invalid_events_df = pd.DataFrame(invalid_events.collect())

    store_to_gc("edu_q4_task2", "valid_events.parquet", valid_events_df)
    #store_to_gc("edu_q4_task2", "invalid_events.parquet", invalid_events_df)


def validate_events_df(data_path, input_events):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("validate_users") \
        .getOrCreate()

    all_events = spark.read.json(data_path + input_events)

    nested_events = all_events.filter(all_events.events.isNotNull()).select(explode(all_events.events))
    nested_events = nested_events.withColumnRenamed("col", "events")
    nested_events = nested_events.withColumn("comment", nested_events.events["comment"]) \
                                 .withColumn("event", nested_events.events["event"]) \
                                 .withColumn("tags", lit(None)) \
                                 .withColumn("timestamp", nested_events.events["timestamp"]) \
                                 .withColumn("user_id", nested_events.events["user_id"]) \
                                 .withColumn("video_id", nested_events.events["video_id"]) \
                                 .drop("events")
    all_events = all_events.drop("events").dropna("all")
    all_events = all_events.union(nested_events)
    valid_events = all_events.filter(all_events.event.isin(["created", "like", "commented", "add_tags", "remove_tags"]))

    valid_events_df = valid_events.toPandas()

    store_to_gc("edu_q4_task2", "valid_events.parquet", valid_events_df)


def store_to_gc(bucket_name, blob_name, df):
    df.columns = df.columns.astype(str)
    df.to_parquet("gs://{}/{}".format(bucket_name, blob_name), storage_options={"token": "../fit-legacy-350921-25f33c4f998e.json"})


if __name__ == "__main__":
    validate_events_df("../Training Project Data v0.1/", "events.jsonl")