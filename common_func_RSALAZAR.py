########################################################################
# PURPOSE: Generic Functions                                           #
########################################################################
import sys
import os
sys.path.append('../')
sys.path.append('/home/hadoop/')
import time
import logging
import datetime
import os.path
import traceback
from pyspark import StorageLevel
import common.config as config

def registerRedshiftTable(hive_context, db_table_name, table_name):
    dupeCheck(hive_context, table_name, 'Regestering Redshift Table')
    temp_table_name="rs_{}_mstr".format(table_name)
    
    redshift_data_frame = hive_context.read.format(config.SparkRedshiftLib) \
    .option("url",config.url) \
    .option("dbtable", db_table_name) \
    .option("tempdir", config.tempdir) \
    .option("temporary_aws_access_key_id", config.aws_access_key_id) \
    .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
    .option("temporary_aws_session_token", config.aws_session_token).load().na.fill("").na.fill(0)
    
    redshift_data_frame.registerTempTable(temp_table_name)
    return redshift_data_frame


def registerRedshiftQuery(hive_context, db_query_name, table_name):
    #PENDING-skipping duplicate check dupeCheck(hive_context, table_name, 'Regestering Redshift Table')
    temp_table_name = "rs_{}_mstr".format(table_name)

    redshift_data_frame = hive_context.read.format(config.SparkRedshiftLib) \
        .option("url", config.url) \
        .option("query", db_query_name) \
        .option("tempdir", config.tempdir) \
        .option("temporary_aws_access_key_id", config.aws_access_key_id) \
        .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
        .option("temporary_aws_session_token", config.aws_session_token).load().na.fill("").na.fill(0)

    redshift_data_frame.registerTempTable(temp_table_name)
    return redshift_data_frame


################################################  Function - registerRedshiftTable ################################################
# To register Redshift Tables using JDBC connection
# The function accepts table_name and temp_table_name as input arguments and returns the data frame
#   db_table_name   - Redshift Data mart Table or Redshift Select query to be registered as Data Frame
#   table_name - Table name, with which the Temp Table as to be registered on the Data Frame

def registerRedshiftTable_original(hive_context, db_table_name, table_name):
    
    temp_table_name="rs_{}_mstr".format(table_name)
    
    redshift_data_frame = hive_context.read.format(config.SparkRedshiftLib) \
    .option("url",config.url) \
    .option("dbtable", db_table_name) \
    .option("tempdir", config.tempdir) \
    .option("temporary_aws_access_key_id", config.aws_access_key_id) \
    .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
    .option("temporary_aws_session_token", config.aws_session_token).load().na.fill("").na.fill(0)
    
    redshift_data_frame.registerTempTable(temp_table_name)
    return redshift_data_frame
#####################################################################################################################################

  

################################################  Function - insertDataFrameToS3 ################################################  
# To write the RI failed Dimension/Master records into S3 bucket for Re-Processing
# The function accepts dataframe_name and table_name as input arguments
#   dataframe_name  - Data frame to be inserted into S3 location
#   table_name      - Directory into which the records needs to be inserted

def insertDataFrameToS3(dataframe_name, path):
    
    dataframe_name.write \
    .mode("append") \
    .save(path=path, format="csv", sep='|', header="false", compression="gzip")
    
    #dataframe_name.coalesce(config.coalesce_value).write \
    #.mode("append") \
    #.option("sep","|") \
    #.option("header","false") \
    #.option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
    #.csv(path)
    
    logging.info(path)

    
#####################################################################################################################################



################################################  Function - registerSourceHiveTable ################################################  
# To Register hive external table as data frame
# The function accepts schema, table_name, col_name, co_nbr_list as input arguments and returns the data frame

def registerSourceHiveTable(hive_context, schema, table_name, col_name, co_nbr_list, key_cols='', orderby_cols='', filter='', isEmptyCheck_Flag='YES',removeDuplicate='YES'):
    
    filter_condition = ''
    if not (co_nbr_list == "'000'" or co_nbr_list == "''") :
        filter_condition = "where trim(co_nbr) in ({}) ".format(co_nbr_list)
    
    if not (filter is None or filter == '' ):
        if filter_condition == '' :
            filter_condition = 'where ' + filter
        else:
            filter_condition = filter_condition + ' and ' + filter


    if key_cols is None or key_cols == '':
        
        if removeDuplicate=='YES' :
            hive_select_query="Select distinct {} from {}.{} {}".format(col_name, schema, table_name, filter_condition)
        else:
            hive_select_query="Select  {} from {}.{} {}".format(col_name, schema, table_name, filter_condition)
    else:
        if orderby_cols is None or orderby_cols == '':
            orderby_cols = key_cols
        
        hive_select_query="select {} from (Select {}, row_number() over (partition by {}  order by {}) row_number from {}.{} {}) a where row_number=1".format(col_name, col_name, key_cols, orderby_cols, schema, table_name, filter_condition)
    
    temp_table_name="hive_{}_src".format(table_name)
    print hive_select_query
    hive_data_frame=hive_context.sql(hive_select_query).na.fill("").na.fill(0)

    if isEmptyCheck_Flag.upper() == 'YES':
        if len(hive_data_frame.head(1)) == 0:
            logging.info('There is no data in the source file to process, - %s for OpCo - %s', table_name,co_nbr_list)
            exit(0) 
    
    hive_data_frame.registerTempTable(temp_table_name)
    return hive_data_frame
#####################################################################################################################################



################################################  Function - riCheckProcess ################################################  
# To execute the ri_query and check if there are any RI failed records.
# If available call insertDataFrameToS3 function to insert RI failed records into S3 bucket

def riCheckProcess(logging, hive_context, ri_query, table_name, current_date):
    
    table_name = table_name.upper()
    timestmp = int(round(time.time()*10000000000))  
    ri_data_frame=hive_context.sql(ri_query)
    ri_data_frame.registerTempTable('ri_table')
    
    if len(ri_data_frame.head(1)) == 0:
        logging.info('No RI Records for %s \n', table_name)
    else:
        if table_name in config.infaRiCols:
            logging.info('Infa Object')
            ri_cols = config.infaRiCols[table_name]
        else:
            logging.info('EMR Object')
            cols = hive_context.sql("desc {}.{}".format(config.riHiveDatabase,table_name)).collect()
            ri_cols = ', '.join(i[0] for i in cols)
        
        new_ri_data_frame = hive_context.sql("select {} from ri_table".format(ri_cols))
        
        if table_name in config.s3FolderDetail:
            folder_name = config.s3FolderDetail[table_name]
        else:
            folder_name = 'CorpData/Dimension'
        
        if table_name[-4:] == '_SYG':
            table_name = table_name[:-4]
        
        if table_name in config.infaDimObj:
            path="{}/{}/{}/RI-Data/RIC/{}/".format(config.s3_bucket,folder_name,table_name,current_date+str(timestmp))
        else:
            path="{}/{}/{}/RI-Data/{}/".format(config.s3_bucket,folder_name,table_name,current_date+str(timestmp))
        
        insertDataFrameToS3(new_ri_data_frame,path)
        logging.info('RI Records for %s got inserted to S3 bucket. \n', table_name)



#####################################################################################################################################


################################################  Function - loadDataIntoRedshift ################################################  
# To insert data into Redshift table
#

def loadDataIntoRedshift(logging, load_type, schema_name, table_name, data_frame, opco_list, key_columns=None, filter=None, preaction_query=None, postaction_query=None):
    
    if table_name.upper() == 'LOAD_HIST_FACT':
        table_name = table_name.upper()
        timestmp = int(round(time.time()*10000000000))
        current_date=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        folder_name = 'OpcoData/Shared-Dimension'
        path="{}/{}/{}/{}/".format(config.s3_bucket,folder_name,table_name,current_date+str(timestmp))
        insertDataFrameToS3(data_frame,path)
        logging.info('Data Loaded into LOAD_HIST_FACT table. \n')
    
    else:
        if load_type.upper() == 'INSERT':
        
            logging.info('Load Type - Insert')
            table = "{}.{}".format(schema_name,table_name)
            
            if len(data_frame.head(1)) == 0:
                logging.info('No Records to load table %s \n', table_name)
            else:
                logging.info('Start Time - %s', datetime.datetime.now())
            
                data_frame.write.format(config.SparkRedshiftLib) \
                .option("url", config.url) \
                .option("dbtable", table) \
                .option("extracopyoptions", "ACCEPTINVCHARS AS '?' TRUNCATECOLUMNS MAXERROR 50") \
                .option("tempdir", config.tempdir) \
                .option("temporary_aws_access_key_id", config.aws_access_key_id) \
                .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
                .option("temporary_aws_session_token", config.aws_session_token).mode("append").save()
                
                logging.info('End Time - %s', datetime.datetime.now())      
                logging.info('Data loaded into table %s \n', table_name)
            
        elif load_type.upper() == 'UPSERT':
            
            logging.info('Load Type - UpdateElseInsert or DeleteInsert')
            
            co_nbr_suffix='_'.join("{0}".format(co_nbr[1:-1]) for co_nbr in opco_list.split(', '))
            temp_table_name = "{}.{}_temp_{}".format(config.stageSchema, table_name,co_nbr_suffix)
            
            if len(data_frame.head(1)) == 0:
                logging.info('No Records to load table %s \n', table_name)
            else:
                preaction_query = """
                begin;
                drop table if exists {};
                create table {} (like {}.{});
                end;""".format(temp_table_name, temp_table_name, schema_name, table_name)
                
                if filter is None or filter == '':
                    filter=''
                else:
                    filter = ' and '+ filter
                
                postaction_query = """
                begin;
                delete from {}.{} where ({}) in (select {} from {}) {};
                insert into {}.{} select * from {};
                drop table if exists {};
                end;""".format(schema_name, table_name, key_columns, key_columns, temp_table_name, filter, schema_name, table_name, temp_table_name, temp_table_name)

                logging.info('Start Time - %s', datetime.datetime.now())
                
                data_frame.write.format(config.SparkRedshiftLib) \
                .option("url", config.url) \
                .option("preactions",preaction_query) \
                .option("dbtable", temp_table_name) \
                .option("extracopyoptions", "ACCEPTINVCHARS AS '?' TRUNCATECOLUMNS MAXERROR 50") \
                .option("postactions",postaction_query) \
                .option("tempdir", config.tempdir) \
                .option("temporary_aws_access_key_id", config.aws_access_key_id) \
                .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
                .option("temporary_aws_session_token", config.aws_session_token) .mode("append").save()
                
                logging.info('End Time - %s', datetime.datetime.now())
                logging.info('Data loaded into table %s \n', table_name)
        
        elif load_type.upper() == 'CUSTOM':
            if len(data_frame.head(1)) == 0:
                logging.info('No Records to load table %s \n', table_name)
            else:
                if preaction_query is None:
                    preaction=''
                else:
                    preaction='.option("preactions",preaction_query)'
                
                if postaction_query is None:
                    postaction=''
                else:
                    postaction='.option("postactions",postaction_query)'
                
                table = schema_name+'.'+table_name
                logging.info('Start Time - %s', datetime.datetime.now())
                
                cmd ="""data_frame.write.format(config.SparkRedshiftLib) .option("url", config.url) {} .option("dbtable", table) {} .option("extracopyoptions", "ACCEPTINVCHARS AS '?' TRUNCATECOLUMNS MAXERROR 50") .option("tempdir", config.tempdir) .option("temporary_aws_access_key_id", config.aws_access_key_id) .option("temporary_aws_secret_access_key", config.aws_secret_access_key) .option("temporary_aws_session_token", config.aws_session_token) .mode("append").save()""".format(preaction,postaction)
                exec(cmd)
                
                logging.info('End Time - %s', datetime.datetime.now())
                logging.info('Data loaded into table %s \n', table_name)
        
        else:
            logging.info('Enter valid load type - INSERT/UPSERT')

#####################################################################################################################################


################################################  Function - initializeSparkHiveContext ################################################ 
# To initialize Spark Context and Hive Context
# Accepts Application Name as input and retruns hive_context

def initializeSparkHiveContext(application_name):
    from pyspark.sql import HiveContext
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext

    spark = SparkSession.builder.master("yarn").appName(application_name).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.kryoserializer.buffer.max","126mb").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    hive_context = HiveContext(sc)
    print 'spark/hive context'
    return hive_context
    
#####################################################################################################################################


################################################  Function - duplicateCheck ################################################ 
# To check duplicates
# Accepts table name and exists if there are duplicates.

def dupeCheck(hive_context, table_name, func_call):
    current_date=datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    table_name = table_name.upper()
    temp_table_name='{}_bkp_dupe_{}'.format(table_name,current_date)
    if table_name in config.dupeMaster:
        
        details = config.dupeMaster[table_name]
        schema = details[0]
        key_cols = details[1]
        all_cols = config.allColsList[table_name]
        
        filter_condition = f = f_cond = ''
        if len(details) == 3 :
            filter_condition = 'WHERE ' + details[2]
            f = details[2] + ' and'
            f_cond = "and {0}.{1} and tmp.{1}".format(table_name,details[2])
        
        join_cond = ' and ' .join("{0}.{1}=tmp.{1}".format(table_name,i)  for i in key_cols.split(','))
        
        dupe_query = "(SELECT COUNT(1) FROM (SELECT {} FROM {}.{} {} GROUP BY {} HAVING COUNT(1)>1)A)".format(key_cols,schema,table_name,filter_condition,key_cols)
        print dupe_query
        
        dupe_count = hive_context.read.format(config.SparkRedshiftLib) \
        .option("url",config.url) \
        .option("dbtable", dupe_query) \
        .option("tempdir", config.tempdir) \
        .option("temporary_aws_access_key_id", config.aws_access_key_id) \
        .option("temporary_aws_secret_access_key", config.aws_secret_access_key) \
        .option("temporary_aws_session_token", config.aws_session_token).load().na.fill("").na.fill(0).collect()[0][0]
        print dupe_count
        if dupe_count <> 0 :
            msg = func_call + " : There are " + str(dupe_count) + " duplicates in " + table_name + " table."
            
            remove_dupe_query = """create table {0}.{1} (like {2}.{3});
            
insert into {0}.{1} select * from {2}.{3} where {7} ({5}) in (select {5} from {2}.{3} {6} group by {5} having count(1)>1);          

delete from {2}.{3} using {0}.{1} tmp
where {9} 
{8};            

insert into {2}.{3}
SELECT {4} from (
select *, row_number() over(partition by {5} order by sys_cd_1 desc, insrt_dttm desc, updt_dttm desc) as rn from
(select *,case when trim(src_sys_cd) in ('ERR','',NULL) THEN 'A' ELSE src_sys_cd END AS sys_cd_1
from {0}.{1} {6}) a
) b where rn =1 """.format('temp',temp_table_name,'edwp',table_name,all_cols,key_cols,filter_condition,f,f_cond,join_cond)
            
            logging.info("Dupe Removal Query (This is a sample query. Please validate the duplicates in the table and the query before using it)")
            logging.info(remove_dupe_query)
            sys.exit(msg)
        else:
            print "There are no duplicates in " + table_name
    
    else:
        print 'No duplicate check'

#####################################################################################################################################
