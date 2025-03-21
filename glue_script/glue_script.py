import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, mean, max, countDistinct, sum as _sum, row_number, dense_rank, concat_ws, collect_list, round
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DYNAMODB_TABLE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data from the intermediate S3 bucket
streams_df = spark.read.csv(f"{args['S3_SOURCE_PATH']}/streams/combined_streams.csv", header=True, inferSchema=True)
users_df = spark.read.csv(f"{args['S3_SOURCE_PATH']}/users/users.csv", header=True, inferSchema=True)
songs_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("mode", "PERMISSIVE") \
    .csv(f"{args['S3_SOURCE_PATH']}/songs/songs.csv")

# Show the schema and some sample data
streams_df.printSchema()
streams_df.show(5)

users_df.printSchema()
users_df.show(5)

songs_df.printSchema()
songs_df.show(5)

# Checking for missing values
print("Streams missing values:")
streams_df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in streams_df.columns]).show()

print("Users missing values:")
users_df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in users_df.columns]).show()

print("Songs missing values:")
songs_df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in songs_df.columns]).show()

# Replace the missing values in songs with "Unknown"
songs_df = songs_df.na.fill("Unknown")

# Remove duplicates and drop null values in all dataframes
streams_df = streams_df.dropDuplicates().dropna()
users_df = users_df.dropDuplicates().dropna()
songs_df = songs_df.dropDuplicates().dropna()

print("Songs missing values after filling:")
songs_df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in songs_df.columns]).show()

# Merging songs and streams data and converting listen_time to timestamp to extract the date
merged_df = streams_df.join(songs_df, on="track_id", how="left")
merged_df = merged_df.withColumn("listen_time", F.to_timestamp(col("listen_time")))
merged_df = merged_df.withColumn("date", F.to_date(col("listen_time")))
merged_df.show(5)
merged_df.printSchema()

# Compute daily genre-level KPIs
genre_kpis = merged_df.groupBy("date", "track_genre").agg(
    count("track_id").alias("listen_count"),
    countDistinct("user_id").alias("unique_listeners"),
    _sum("duration_ms").alias("total_listening_time_ms"),
    (F.sum("duration_ms") / countDistinct("user_id")).alias("avg_listening_time_per_user_ms")
)

# Convert total_listening_time and avg_listening_time_per_user from ms to minutes
genre_kpis = genre_kpis.withColumn(
    "total_listening_time", round(col("total_listening_time_ms") / 60000, 2)
).withColumn(
    "avg_listening_time_per_user", round(col("avg_listening_time_per_user_ms") / 60000, 2)
).drop("total_listening_time_ms", "avg_listening_time_per_user_ms")

# Compute the most popular songs per genre per day
song_per_genre_rank_window = Window.partitionBy("date", "track_genre").orderBy(F.desc("track_listen_count"))

song_plays = merged_df.groupBy("date", "track_genre", "track_name").agg(
    count("track_id").alias("track_listen_count")
)

song_per_genre_rank = song_plays.withColumn("rank", dense_rank().over(song_per_genre_rank_window))
top_3_songs_per_genre_per_day = song_per_genre_rank.filter(col("rank") <= 3).orderBy("date", "track_genre", "rank")

# Aggregate the top 3 songs per genre per day into a single column
top_3_songs_aggregated = top_3_songs_per_genre_per_day.groupBy("date", "track_genre").agg(
    concat_ws("|", collect_list("track_name")).alias("top_3_songs_per_genre")
)

# Join the aggregated top 3 songs with genre_kpis
genre_kpis_with_top_songs = genre_kpis.join(
    top_3_songs_aggregated,
    on=["date", "track_genre"],
    how="left"
)

# Compute the top 5 genres per day based on listen count
top_5_genre = Window.partitionBy("date").orderBy(F.desc("listen_count"))

top_5_genres_per_day = genre_kpis.withColumn("rank", row_number().over(top_5_genre)) \
    .filter(col("rank") <= 5).orderBy("date", "rank")

# Aggregating the top 5 genres per day into a single column
top_5_genres_aggregated = top_5_genres_per_day.groupBy("date").agg(
    concat_ws("|", collect_list("track_genre")).alias("top_5_genres_per_day")
)

# Join the aggregated top 5 genres with genre_kpis
complete_genre_kpis = genre_kpis_with_top_songs.join(
    top_5_genres_aggregated,
    on="date",
    how="left"
)

# Order by date asc and listen_count desc
complete_genre_kpis = complete_genre_kpis.orderBy("date", F.desc("listen_count"))

# Show the final genre_kpis with top 3 songs
complete_genre_kpis.show()

# Convert the final KPIs DataFrame to a DynamicFrame
dynamic_frame = DynamicFrame.fromDF(complete_genre_kpis, glueContext, "dynamic_frame")

# Write the DynamicFrame to DynamoDB
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": args["DYNAMODB_TABLE"],
        "dynamodb.throughput.write.percent": "1.0"
    }
)

# Commit the Glue job
job.commit()