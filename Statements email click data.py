#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import os as os
import numpy as np
from google.cloud import storage # api for extracting files from gcs
import time # convert datetime to timestamp and vice versa
import pytz # for checnging date into a specific time zone
import fnmatch

pd.set_option('display.max_columns' , 150)
pd.set_option('display.max_rows' , 150)

tz = pytz.timezone('America/New_York') # specify the timezone you are looking for
local = pytz.timezone('Asia/Muscat') # to specify the local timezone when converting date

#import gcsfs
print(storage.__version__)

import datetime



import pysftp # to transfer files from the sftp server
import paramiko
from datetime import timedelta

from io import StringIO

# to connect to snowflake

import snowflake.connector

# write into snowflake tables

from snowflake.connector.pandas_tools import write_pandas


#from IPython.core.display import display, HTML
#display(HTML("<style>.container { width:100% !important; }</style>"))




# ## Actual Code - GCS Data

# In[4]:


# validate the current working directory

print(os.getcwd())

os.chdir('C:\\Users\\s456781\\OneDrive - Emirates Group\\Barclays Project\\Data for Stitching\\LIVE Data')


# In[5]:


# connect to Google client Storage using json file with credentials


storage_client = storage.Client.from_service_account_json('./Barclays_data_share_key.json')
bucket = storage_client.bucket('skywards-statement-data-share')


# ### Block Summary data: create tables

# In[50]:


len(list(bucket.list_blobs(prefix= 'statement_offer_block_summary_data')))


# In[51]:


# create the tables that the records need to be inserted into

# connect to snowflake

con = snowflake.connector.connect(
user = 'ruchir.varma@emirates.com',
authenticator='externalbrowser',
account='emirates.west-europe.azure',
schema='WS_SKYWARDS_PROD',
warehouse='SKYWARDS_WH_PROD',
role='ROLE_S456781',
autocommit=True
)


# run the first set of loops for creating table for block summary

for i in range (1 ,  len(list(bucket.list_blobs(prefix= 'statement_offer_block_summary_data'))) + 1):
    cs = con.cursor()
    sql = 'USE DATABASE EDWH_PROD;'
    cs.execute(sql).fetchone()

    sql = 'USE schema WS_SKYWARDS_PROD;'
    cs.execute(sql).fetchone()


#     sql = 'drop table if exists "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_block_summ_data_{};'.format(i)
#     print(cs.execute(sql).fetchone())



create blank tables for the data to be read in

    sql = 'CREATE or replace TABLE "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_block_summ_data_{}     ( person_id STRING,     send_date STRING,     campaign STRING,     offer STRING,     offer_type STRING,     offer_priority INTEGER,     offer_position INTEGER,     total_clicks INTEGER,     unique_clicks INTEGER,     first_click_date DATE,     last_click_date DATE     );'.format(i)


    print(cs.execute(sql).fetchone())
    
    


# #### Push data into tables on snowflake

# In[43]:



# loop through each of the files and fetch contents


y = 0
for file in bucket.list_blobs(prefix= 'statement_offer_block_summary_data' ):
    
    if fnmatch.fnmatch( file.name ,  "*.csv"  ):
        y = y + 1
        
        # create a blob object to read the contents into memory
        
        Blob = bucket.blob(file.name)
        print (Blob)
        print (y)
        
        # source the contents
        
        contents = Blob.download_as_text()
        for_df = StringIO(contents)
        df_to_insert = pd.read_csv(for_df, sep=",", low_memory=False)
        

        # establish the connection with snowflake to push data into tables
     
        con = snowflake.connector.connect(
        user = 'ruchir.varma@emirates.com',
        authenticator='externalbrowser',
        account='emirates.west-europe.azure',
        schema='WS_SKYWARDS_PROD',
        warehouse='SKYWARDS_WH_PROD',
        role='ROLE_S456781',
        autocommit=True
        )
        
        cs = con.cursor()
        sql = 'USE DATABASE EDWH_PROD;'
        cs.execute(sql).fetchone()

        sql = 'USE schema WS_SKYWARDS_PROD;'
        cs.execute(sql).fetchone()
        
        # use write python to load each record in the tables created
        
        with con as db_conn_sf, db_conn_sf.cursor() as db_cursor_sf:
            inserted_rows = write_pandas(conn = db_conn_sf, 
                                         df = df_to_insert, 
                                         table_name = 'skw_antcs_block_summ_data_{}'.format(y),
                                         quote_identifiers = False)
            
        
        print ('data loaded in into skw_antcs_block_summ_data_{}'.format(y))
        
# print final completion statement
        
print ('Completed and created {} Tables'.format(y))
        
   


# In[46]:


# union all tables into one master table

con = snowflake.connector.connect(
user = 'ruchir.varma@emirates.com',
authenticator='externalbrowser',
account='emirates.west-europe.azure',
schema='WS_SKYWARDS_PROD',
warehouse='SKYWARDS_WH_PROD',
role='ROLE_S456781',
autocommit=True
) 


sql = 'CREATE or replace TABLE "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_block_summ_data_master table as ( select *  from  (select * from skw_antcs_block_summ_data_1 union all select * from skw_antcs_block_summ_data_2  union all select * from skw_antcs_block_summ_data_3 union all select * from skw_antcs_block_summ_data_4 union all select * from skw_antcs_block_summ_data_5 union all select * from skw_antcs_block_summ_data_6 union all select * from skw_antcs_block_summ_data_7 union all select * from skw_antcs_block_summ_data_8 union all select * from skw_antcs_block_summ_data_9 union all select * from skw_antcs_block_summ_data_10 union all select * from skw_antcs_block_summ_data_11 union all select * from skw_antcs_block_summ_data_12 union all select * from skw_antcs_block_summ_data_13 union all select * from skw_antcs_block_summ_data_14 union all select * from skw_antcs_block_summ_data_15 union all select * from skw_antcs_block_summ_data_16 union all select * from skw_antcs_block_summ_data_17 union all select * from skw_antcs_block_summ_data_18 union all select * from skw_antcs_block_summ_data_19 union all select * from skw_antcs_block_summ_data_20 union all select * from skw_antcs_block_summ_data_21 union all select * from skw_antcs_block_summ_data_22 union all select * from skw_antcs_block_summ_data_23 union all select * from skw_antcs_block_summ_data_24 union all select * from skw_antcs_block_summ_data_25 union all select * from skw_antcs_block_summ_data_26 union all select * from skw_antcs_block_summ_data_27 union all select * from skw_antcs_block_summ_data_28 union all select * from skw_antcs_block_summ_data_29 union all select * from skw_antcs_block_summ_data_30 union all select * from skw_antcs_block_summ_data_31 union all select * from skw_antcs_block_summ_data_32 union all select * from skw_antcs_block_summ_data_33 union all select * from skw_antcs_block_summ_data_34 union all select * from skw_antcs_block_summ_data_35 union all select * from skw_antcs_block_summ_data_36 union all select * from skw_antcs_block_summ_data_37 union all select * from skw_antcs_block_summ_data_38 union all select * from skw_antcs_block_summ_data_39 union all select * from skw_antcs_block_summ_data_40 union all select * from skw_antcs_block_summ_data_41 union all select * from skw_antcs_block_summ_data_42 union all select * from skw_antcs_block_summ_data_43 union all select * from skw_antcs_block_summ_data_44 union all select * from skw_antcs_block_summ_data_45 union all select * from skw_antcs_block_summ_data_46 union all select * from skw_antcs_block_summ_data_47 union all select * from skw_antcs_block_summ_data_48 union all select * from skw_antcs_block_summ_data_49 union all select * from skw_antcs_block_summ_data_50 union all select * from skw_antcs_block_summ_data_51 union all select * from skw_antcs_block_summ_data_52 union all select * from skw_antcs_block_summ_data_53 union all select * from skw_antcs_block_summ_data_54 union all select * from skw_antcs_block_summ_data_55 union all select * from skw_antcs_block_summ_data_56 union all select * from skw_antcs_block_summ_data_57 union all select * from skw_antcs_block_summ_data_58 union all select * from skw_antcs_block_summ_data_59 ) as ss1 );'


print(cs.execute(sql).fetchone())


# #### Summary data : create multiple tables

# In[49]:


len(list(bucket.list_blobs(prefix= 'statement_summary_data')))


# In[42]:


# create the tables that the records need to be inserted into

# connect to snowflake

con = snowflake.connector.connect(
user = 'ruchir.varma@emirates.com',
authenticator='externalbrowser',
account='emirates.west-europe.azure',
schema='WS_SKYWARDS_PROD',
warehouse='SKYWARDS_WH_PROD',
role='ROLE_S456781',
autocommit=True
)


# run the first set of loops for creating table for summary data

for i in range (1 ,  len(list(bucket.list_blobs(prefix= 'statement_summary_data'))) + 1):
    cs = con.cursor()
    sql = 'USE DATABASE EDWH_PROD;'
    cs.execute(sql).fetchone()

    sql = 'USE schema WS_SKYWARDS_PROD;'
    cs.execute(sql).fetchone()


#     sql = 'drop table if exists "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_click_data_{};'.format(i)
#     print(cs.execute(sql).fetchone())



# create blank tables for the data to be read in

    sql = 'CREATE or replace TABLE "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_stmnt_summ_data_{}     ( person_id STRING,     send_date STRING,     campaign STRING,     offer STRING,     offer_type STRING,     offer_priority INTEGER,     offer_position INTEGER,     total_clicks INTEGER,     unique_clicks INTEGER,     first_click_date DATE,     last_click_date DATE     );'.format(i)


    print(cs.execute(sql).fetchone())
    
    


# #### Load the data into the tables create

# In[43]:



# loop through each of the files and fetch contents only for summary data


y = 0
for file in bucket.list_blobs(prefix= 'statement_summary_data' ):
    
    if fnmatch.fnmatch( file.name ,  "*.csv"  ):
        y = y + 1
        
        # create a blob object to read the contents into memory
        
        Blob = bucket.blob(file.name)
        print (Blob)
        print (y)
        
        # source the contents
        
        contents = Blob.download_as_text()
        for_df = StringIO(contents)
        df_to_insert = pd.read_csv(for_df, sep=",", low_memory=False)
        

        # establish the connection with snowflake to push data into tables
     
        con = snowflake.connector.connect(
        user = 'ruchir.varma@emirates.com',
        authenticator='externalbrowser',
        account='emirates.west-europe.azure',
        schema='WS_SKYWARDS_PROD',
        warehouse='SKYWARDS_WH_PROD',
        role='ROLE_S456781',
        autocommit=True
        )
        
        cs = con.cursor()
        sql = 'USE DATABASE EDWH_PROD;'
        cs.execute(sql).fetchone()

        sql = 'USE schema WS_SKYWARDS_PROD;'
        cs.execute(sql).fetchone()
        
        # use write python to load each record in the tables created
        
        with con as db_conn_sf, db_conn_sf.cursor() as db_cursor_sf:
            inserted_rows = write_pandas(conn = db_conn_sf, 
                                         df = df_to_insert, 
                                         table_name = 'skw_antcs_stmnt_summ_data_{}'.format(y),
                                         quote_identifiers = False)
            
        
        print ('data loaded in into skw_antcs_stmnt_summ_data_{}'.format(y))
        
# print final completion statement
        
print ('Completed and created {} Tables'.format(y))
        
    


# In[46]:


# union all tables into one master table

con = snowflake.connector.connect(
user = 'ruchir.varma@emirates.com',
authenticator='externalbrowser',
account='emirates.west-europe.azure',
schema='WS_SKYWARDS_PROD',
warehouse='SKYWARDS_WH_PROD',
role='ROLE_S456781',
autocommit=True
)    

    sql = 'CREATE or replace TABLE "EDWH_PROD"."WS_SKYWARDS_PROD".skw_antcs_stmnt_summ_data__master table as \
    ( select *  \
    from  \
    (select * from skw_antcs_stmnt_summ_data_1 \
    union all \
    select * from skw_antcs_stmnt_summ_data_2  \
    union all \
    select * from skw_antcs_stmnt_summ_data_3 \
    union all \
    select * from skw_antcs_stmnt_summ_data_4 \
    union all \
    select * from skw_antcs_stmnt_summ_data_5 \
    union all \
    select * from skw_antcs_stmnt_summ_data_6 \
    ) as ss1 \
    );'


print(cs.execute(sql).fetchone())






