## Date created
Date : 03-07-2019

## Project Title
Data Warehouse

## Description
In this project, we will use the knowledge of data warehouses and AWS to build an *ETL pipeline for a database hosted on* **Redshift**.

A music streaming startup, **Sparkify**, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an **ETL pipeline** that extracts their data from **S3**, stages them in **Redshift**, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to

## Files used
### Python Files:
* create_tables.py
* etl.py
* sql_queries.py

### Config File:
* dwh.cfg

### Project Steps
* Create table schemas 
* Implement the logic in etl.py to load data from S3 to staging tables on Redshift
* Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift
* Implement the create tables logic and loading data login in sql_queries.py


## Credits
http://millionsongdataset.com/

