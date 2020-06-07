import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as ST, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as LT, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates a spark session or update the current one"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Imports and process data from song dataset and then write data to
    parquet files on Amazon S3

    Parameters:
        spark: spark session
        input_data: S3 bucket path to input data from.
        output_data: another S3 bucket path to write data to it.
    """

    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"

    SongSchema= ST([Fld("song_id",Str()),
                    Fld("artist_id",Str()),
                    Fld("artist_latitude",Dbl()),
                    Fld("artist_location",Str()),
                    Fld("artist_longitude",Dbl()),
                    Fld("artist_name",Str()),
                    Fld("duration",Dbl()),
                    Fld("num_songs",Int()),
                    Fld("title",Str()),
                    Fld("year",Int()),

    ])



    # read song data file
    df = spark.read.json(song_data, schema=SongSchema).dropDuplicates(['song_id', 'artist_id'])

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Imports and process data from log dataset and then write data to
    parquet files on Amazon S3

    Parameters:
        spark: spark session
        input_data: S3 bucket path to input data from.
        output_data: another S3 bucket path to write data to it.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    LogSchema = ST([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", LT()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId",  LT()),
        Fld("song", Str()),
        Fld("status", LT()),
        Fld("ts",  LT()),
        Fld("userAgent", Str()),
        Fld("userId", Str()),
    ])

    # read log data file
    df = spark.read.json(log_data, schema = LogSchema).dropDuplicates(['userId'])

    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table
    users_table = df.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),"gender","level").dropDuplicates(['user_id'])

    # write users table to parquet files
    users_table.write.parquet(output_data + "users")


    time_format = "yyyy-MM-dd HH:MM:ss z"

    get_timestamp = df.withColumn('ts',
                               to_timestamp(date_format((df.ts
                                                         /1000).cast(dataType=TimestampType()), time_format),time_format))
    # extract columns to create time table
    time_table = get_timestamp.select(col("ts").alias("start_time"),
                                   hour(col("ts")).alias("hour"),
                                   dayofmonth(col("ts")).alias("day"),
                                   weekofyear(col("ts")).alias("week"),
                                   month(col("ts")).alias("month"),
                                   year(col("ts")).alias("year"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_data = input_data + "song-data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.join(df, song_df.artist_name==df.artist)\
    .withColumn("songplay_id",monotonically_increasing_id())\
    .withColumn('start_time', to_timestamp(date_format((col("ts") /1000).cast(dataType=TimestampType()), time_format),time_format))\
    .select("songplay_id", "start_time",
           col("userId").alias("user_id"),
           "level","song_id","artist_id",
           col("sessionId").alias("session_id"),
           col("artist_location").alias("location"), "userAgent",
           month(col("start_time")).alias("month"),
           year(col("start_time")).alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-tables/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
