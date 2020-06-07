# Summary of the Project
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Tasks
1. Create EMR cluster on AWS and connecting to it using SSH protocol.
2. Creating Schema-On-Read for sparkify streaming data using Apache Spark.
2. Build an ETL pipeline in python to automate the process.

# Input data
All data resides in Amazon S3 bucket in two folders (song-data and log-data).

1. Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.

2. Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

# Output data
Using the song and log datasets, we create a star schema optimized for queries on song play analysis. This includes the following tables.

1. Fact Table
songplays
- Records: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.
- Primary Key: songplay_id.
- Foreign Keys: start_time, user_id, song_id, artist_id.

2. Dimension Tables
users
* Records: first_name, last_name, gender, level.
* Primary key: user_id.

songs
* Records: title, artist_id, year, duration.
* Primary key: song_id.

artists
* Records: artist_name, artist_location, artist_latitude, artist_longitude.
* Primary key: artist_id.

time
* Records: hour, day, week, month, year, weekday.
* Primary key: start_time. 

# Data Explanation
* etl.py: reads and processes data from S3 buckets using spark and creates a star schema on-the-fly then writes them back to another pre-created S3 bucket in parquet format.
* dwh.cfg: configuration file that has amazon credentials to create and access the AWS EMR cluster.
* README.md: provides discussion on the process and decisions.

# How to run:
1. Create an EMR cluster using dwh.cfg and connect to it using SSH.
2. Run the following command on EMR terminal. 
    "spark-submit etl.py". 
