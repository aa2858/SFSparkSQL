########################################################################
# PURPOSE: TO PERFORM THE AGGREGATION FOR THE PO_CO_ITM_VNDR_DAY_AGGR   #
########################################################################

import sys
import time
import logging
import datetime
import os.path
import traceback
from pyspark import StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import next_day,to_date

sys.path.append('../')
sys.path.append('/home/hadoop/')
import common.config as config

import string

#PENDING RESTORE TO ORIGINAL CODE import common.common_func_RSALAZAR as common_func
import common_func_RSALAZAR as common_func

import RevManEarnedIncomeQueries as sqlfile

#ON HOLD USING QUERY FILE - PENDING import po_co_itm_vndr_day_aggr_sql as query_file

def main():

    #PENDING Try Sending output to screen

    # To fetch the current date and script name from sys.argv[] and generate log file path.
    current_date = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    file_name = sys.argv[0].split('/')[-1].split('.')[0]

    log_file_path = "{}/{}_{}.log".format(config.log_file_directory, file_name, current_date)


    # To initialize logging
    #cHANGED FROM INFO to WARNING.
    logging.basicConfig(filename=log_file_path, filemode='w', level=logging.ERROR)

    logging.info('\n945##################  Mapping Logic Started at %s ##################', datetime.datetime.now())

    if len(sys.argv) > 1:
        co_nbrs = sys.argv[1].split(',')
        co_nbr_list = ', '.join("'{0}'".format(co_nbr.zfill(3)) for co_nbr in co_nbrs)
        logging.info('Company Number - %s', co_nbr_list)

    else:
        co_nbr_list = "'000'"
        logging.info("Company Number is not passed as argument. Script will process data for all OpCo's")

    # calling initializeSparkHiveContext() function from common_func.py to initialize spark session, register spark and hive context.
    #pending replace later hive_context = common_func.initializeSparkHiveContext('VendorAgreements')

    #---------------------------------------------------------------------------------------------------
    from pyspark.sql import HiveContext
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext

    spark = SparkSession.builder.master("yarn").appName("Purchase Order").config("spark.serializer",
                                                                                 "org.apache.spark.serializer.KryoSerializer").config(
        "spark.kryoserializer.buffer.max", "126mb").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    hive_context = HiveContext(sc)

    # Control the logs to the stdout (console)
    # Other     options     for Level include: all, debug, error, fatal, info, off, trace, trace_int, warn
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    # ---------------------------------------------------------------------------------------------------

    logging.info('\n##################  Mapping Logic Started at %s ##################', datetime.datetime.now())


    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Register Temporary Tables for Sources  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    #
    #   _____                _         _                              _   _        _     _
    #  / ____|              | |       | |                            | | | |      | |   | |
    # | |     _ __ ___  __ _| |_ ___  | |_ _ __ ___  _ __   ___  __ _| | | |_ __ _| |__ | | ___  ___
    # | |    | '__/ _ \/ _` | __/ _ \ | __| '_ ` _ \| '_ \ / __|/ _` | | | __/ _` | '_ \| |/ _ \/ __|
    # | |____| | |  __/ (_| | ||  __/ | |_| | | | | | |_) |\__ \ (_| | | | || (_| | |_) | |  __/\__ \
    #  \_____|_|  \___|\__,_|\__\___|  \__|_| |_| |_| .__/ |___/\__, |_|  \__\__,_|_.__/|_|\___||___/
    #                                               | |______      | |
    #                                               |_|______|     |_|
    #

    dfsrc1 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_oblig_dtl + "  ", "DUMMY_TMP_TABLE_MISSING_WEEK_ENDING_SALE")
    dfsrc1 = dfsrc1.withColumn('week_ending_sale', next_day(dfsrc1.oblig_dt, 'Sun'))
    dfsrc1.createOrReplaceTempView("rs_TMP_SQL_src_sale_oblig_dtl_fact_mstr")


    dfsrc2 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_oblig_head + "  ", "TMP_SQL_src_sale_oblig_head_fact")
    dfsrc3 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_itm + "  ", "TMP_SQL_src_itm_dim")
    dfsrc4 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_itm_co_itm + "  ", "TMP_SQL_src_itm_co_itm_rel")
    dfsrc5 = common_func.registerRedshiftQuery(hive_context, sqlfile.SqlVendorAgreement + "  ", "TMP_SQL_src_agr_vndr_agr_trans_fact")

    dfsrc6 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_cust_ship_to + "  ", "TMP_SQL_src_cust_ship_to_dim")
    dfsrc7 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_calendar + "  ",  "TMP_SQL_src_calendar")
    dfsrc8 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_cal_day_dim  + "  ",  "TMP_SQL_src_cal_day_dim")

    dfsrc9 = common_func.registerRedshiftQuery(hive_context, 'select * from intp.ei_sap_go_live_dates', "TMP_SQL_src_ei_sap_go_live_dates")


    #REFERENCE FOR oblig_dtl.week_ending_sale = sus_weekly.week_ending

    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%READ ETL STAGE INTERMEDIATE  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    dfstg1 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_stg_agr_sum + "  ", "TMP_SQL_stg_ei_agr_vndr_agr_trans_reseg")
    dfstg2 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_stg_sus_weekly + "  ", "TMP_SQL_stg_ei_sus_weekly")

    print("***************************ORIGINAL QUERY direct reference using x.var *************************")
    sqlmain = sqlfile.sql_ei_main_full
    print(sqlfile.sql_ei_main_full)
    print("***************************ORIGINAL QUERY *************************")
    print(sqlmain)

    sqlmain = sqlmain.replace("edwp.sale_oblig_dtl_fact", "rs_TMP_SQL_src_sale_oblig_dtl_fact_mstr")
    sqlmain = sqlmain.replace("edwp.sale_oblig_head_fact", "rs_TMP_SQL_src_sale_oblig_head_fact_mstr")
    sqlmain = sqlmain.replace("edwp.itm_dim", "rs_TMP_SQL_src_itm_dim_mstr")
    sqlmain = sqlmain.replace("edwp.itm_co_itm_rel", "rs_TMP_SQL_src_itm_co_itm_rel_mstr")

    sqlmain = sqlmain.replace("edwp.agr_vndr_agr_trans_fact vndragr", "rs_TMP_SQL_src_agr_vndr_agr_trans_fact_mstr")
    sqlmain = sqlmain.replace("edwp.cust_ship_to_dim", "rs_TMP_SQL_src_cust_ship_to_dim_mstr")
    sqlmain = sqlmain.replace("edwp.cal_day_dim day_dim", "rs_TMP_SQL_src_cal_day_dim_mstr")
    sqlmain = sqlmain.replace("intp.ei_sap_go_live_dates", "rs_TMP_SQL_src_ei_sap_go_live_dates_mstr")

    #STAGING
    sqlmain = sqlmain.replace("intp.ei_agr_vndr_agr_trans_reseg", "rs_TMP_SQL_stg_ei_agr_vndr_agr_trans_reseg_mstr")
    sqlmain = sqlmain.replace("intp.ei_sus_weekly", "rs_TMP_SQL_stg_ei_sus_weekly_mstr")


    print("************************** REPLACE_QUERY  **************************")
    print(sqlmain)

    #spark.stop()
    # print(dfsrc1.count())
    # print(dfsrc2.count())
    # print(dfsrc3.count())
    # print(dfsrc4.count())
    # print(dfsrc5.count())
    # print(dfsrc6.count())
    #
    # print(dfstg1.count())
    # print(dfstg2.count())

    #The collect proves that is reading well from s3 buckets in the source AWS
    # dfsrc1.collect()
    # dfsrc2.collect()
    # dfsrc3.collect()
    # dfsrc4.collect()
    # dfsrc5.collect()
    # dfsrc6.collect()

    # dflogic1.createOrReplaceTempView("rs_TMP_SQL_sql_ei_main_part_a_mstr")
    # dflogic2.createOrReplaceTempView("rs_TMP_SQL_ei_purchase_order_item_level_mstr")
    # dflogic3.createOrReplaceTempView("rs_TMP_SQL_ei_purchase_order_item_level_mstr")

    # PROCESS BUSINESS LOGIC

    print('*logic3')
    dflogic1=spark.sql(sqlmain)

    #PENDING STORE RESULTS OF dflogic3

    #LOAD DATA
    # print("*** Read table ei_purchase_order_item_level and create a temporary SQL table")
    # df1 = common_func.registerRedshiftQuery(hive_context, 'SELECT * FROM intp.ei_purchase_order_item_level', "TMP_NOT_USED")

    # df2.createOrReplaceTempView("rs_TMP_SQL_ei_purchase_order_item_level_mstr")
    #
    # print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  MAIN QUERY%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")
    #
    # print(sqlfile.sqlPurchaseOrdersWeekly)
    # df3 = spark.sql(sqlfile.sqlPurchaseOrdersWeekly)
    #
    # print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%SCHEMA%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")
    # # Count using Select statement
    # # TEMPLATE EXAMPLE countDistinctDF_sql = sqlContext.sql("SELECT firstName, lastName, count(distinct firstName) as distinct_first_names FROM databricks_df_example GROUP BY firstName, lastName")
    #
    #
    # ##The tempdir values is tempdir="s3://sysco-nonprod-seed-spark-redshift-temp/
    # print("%%*step 1 before writing%%*")
    # # need to call function insertDataFrameToS3(dataframe_name, path)
    # # sample call common_func.loadDataIntoRedshift(logging, 'CUSTOM', config.dataMartSchema, 'PO_UNIQUE', PO_UNIQUE_INSERT_DATA_FRAME,    co_nbr_list, preaction_query=preaction_query)
    #
    # #
    # # param1=logging
    # # param2='INSERT','UPSERT'
    # # param3=schema (intp value for stageSchema)
    # # param4=table_name (final destination)
    # # param5=dataframe
    #
    # print("%%*Prepare Company List%%*")
    # #PENGIND START USING FUNCTIONALITY FOR NBR LIST
    # co_nbr_list = "'000'"
    #
    # print("%%*Number of Records calculated:")
    # print(df3.count())
    #
    print("%%*Insert Statements %%*")
    common_func.loadDataIntoRedshift(logging, 'INSERT', 'intp', 'ei_sus_ei', dflogic1, opco_list=co_nbr_list)

    print("%%*Program finished%%*")

    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%THE END  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    logging.info('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%', datetime.datetime.now())
    logging.info('Script read01_afr_vendor_enterprise completed %s', datetime.datetime.now())

#Pending persist

if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        logging.error(traceback.format_exc())
        raise
