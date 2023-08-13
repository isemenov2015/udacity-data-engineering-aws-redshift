# Sparkify DWH project

## Overview

The goal of Sparkify DWH project is to create a data warehouse for analytical purposes for Sparkify app startup.
The DWH uses raw data on songs and artists in JSON format and log files on app usage in JSON format also. All raw data provided by Udacity.
DWH utilises Amazon Redshift service as a backend.

## DWH purpose

Key goals of Sparkify DWH:

* store preprocessed analytics data in a systematic way using star schema for fact and dimension tables
* provide scalable solution for analytics data storage
* improve analytical queries performance in terms of access speed
* provide backend for various analytical tools such as dashboard systems etc.

## DWH schema

Sparkify DWH utilises one fact table SONGPLAYS and four dimensional tables: TIME, ARTISTS, USERS and SONGS.

## ETL pipeline

Sparkify DWH ETL pipeline consists of three basic steps:

* DWH tables creation
* raw data load into two stage tables SONGS_STAGE and PLAYS_STAGE; paths to JSON files for raw data import described in [S3] section of DWH.CFG configuration file
* transform pipeline that processes raw data into final star schema tables

## DWH cluster DB credentials

DB credentials for Redshift cluster should be placed into [CLUSTER] section of DWH.CFG configuration file.

## Project run sequence

In order to run the project from scratch use the following pipeline:

* deploy Redshift cluster (with at least 4 nodes in order to ensure the ETL pipeline will perform fast enough) with DB credentials from [CLUSTER] *dwh.cfg* section, assign a separate cluster role for running queries, check if the cluster has public access for queries
* run *python create_tables.py* in a project directory
* run *python etl.py* in a project directory

## Demo queries on Sparkify DWH

In order to give a flavour on intended DWH usage I added demo_queries.py script that has two sample queries for:

* count number of users that listened for a specific song in a specific year
* provide Top 10 app users that has the greatest number of song listening events

The queries themselves could be found in "DEMO QUERIES" section of sql_queries.py.
In order to run demo, run the following in the project directory

*python demo_queries.py*

## Possible improvements

1. For simplicity purposes (it's easier to drop dimension tables this way, since the drop order is not important) I omitted REFERENCES constraints on foreign keys in SONGPLAYS facts table. 
2. artists$name, artists$location and songs$title fields has VARCHAR(4096) length that may be way too big. The field length may be lowered after more thorough exploratory data analysis. From the other hand it's almost OK taking into account "cheap data storage" concept instructor mentioned during the lectures.

**Thanks for reading up to that point :)**
