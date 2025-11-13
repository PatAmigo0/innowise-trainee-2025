# Этот репозиторий используется для хранения и будущей проверки ментором решенных заданий из roadmap PyTraineeIntroduction

The main purpose of this task is to gain knowledge of Snowflake as a columnar database and its main features.

Task Description

Creation of small DWH (5-10 tables) 5 with several layer, which will be based on input data and creatine ETL pipeline of processing data into target layer using AIrflow

Acceptance Criteria

Data is processed into DWH through several stages of storage:

1 Stage - raw data, which is being proceeded from the source into Snowflake

2 Stage - transformed and cleaned data, which are distributed by tables data from 1 stage with table constraints.

3 Stage  datamart  layer with data, which will be prepared for dashboards

Data should be loaded in several ways using Airflow:

Initial Load - the first load of all data from the source

Incremental Load - next load, which is based on Cdc [Change Data Capture, Use Cases and real-world examples using Debezium | by Eresh Gorantla | Geek Culture | Medium|https://medium.com/geekculture/change-data-capture-use-cases-and-real-world-example-using-debezium-fe4098579d49]

Additional required tasks from the Task Steps section are done

Task Steps

Creation of free-trial account (working solution):

Go to Snowflake Site Snowflake Trial and fill registration form

Choose Country  - Poland

Choose AWS provider and Frankfurt region (choosing AWS, you can load data from GCP and Azure)

Create a procedure and run with Airflow, which loads 1-1 data into DWH 1 stage from the stage. You can choose the following ways :

Upload data into the Internal Snowflake Stage and then load into 1 Stage of Snowflake DWH using COPY. (You also should create a File format for CSV file)

Load data from External Stage into 1 Stage of Snowflake DWH using COPY (You also should create a File format for CSV file, Integration, etc.)

(Optional) If you use External Stage, create Snowpipe, which will be triggered by message service or by Rest to load data into your DWH

Create a main pipeline (1 Stage -> 2 Stage ->  3 Stage), which should be based on procedures, streams and orchestrated by Airflow

Create a logging process, which will log the number of affected rows(insert, update) into a separate audit table.

Write 2 DDL and 2 DML queries using time-travel feature of Snowflake

Create Secure View for any fact table and attach Row Level Security policy

(Optional) Connect any BI tool with Snowflake and create 1 simple report

Potential Challenges

Design of DWH,  design of ELT/ETL pipeline

Resources and Training

[Snowflake Guides | Snowflake Documentation|https://docs.snowflake.com/en/guides-overview]
