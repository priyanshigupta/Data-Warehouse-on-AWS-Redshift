In this project, I am working with a music streaming startup Sparkify which places its data 
on AWS S3 bucket. I have created an ETL pipeline which makes use of a AWS Redshift cluster
 to connect to postgresql database and model the data in star schema.
 
Steps followed in the project:
1) Created one IAM user called "dwhRole" with "AdministeratorAccess"  policy . I used its 
key and secret key to build an AWS Redshift cluster and configured it.
Included the configurations in 'dwh.cfg' file.

2)Define all table dropping, creation and insertion queries in 'sql_queries.py' file.

3) Run create_tables.py on terminal to first drop then create the following tables :

-staging_events == STAGING TABLE
-staging_songs == STAGING TABLE
-users== DIMENSION TABLE
-song== DIMENSION TABLE
-artist== DIMENSION TABLE 
-time== DIMENSION TABLE
-songplay == FACT TABLE
 
4) Run etl.py on terminal to insert data into these tables tables :
- copied data from the s3 url ('s3://udacity-dend/song_data','s3://udacity-dend/log_data',
  's3://udacity-dend/log_json_path.json'(menifest)) to the target staging tables.
- insert data into dimension table using staging tables.
- insert data into fact table using staging and dimension tables.
 
