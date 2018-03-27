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

    filter_type=1

    if filter_type==1:
        type1_filter_VendorAgreement = " WHERE co_skey in (7,56) and incm_ern_dt>='2018-01-01' and vndragr.itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "
        type1_filter_oblig_dtl = " WHERE  oblig_dt>='01/01/2018' "
        type1_filter_oblig_head = " WHERE  oblig_dt>='01/01/2018' "
        type1_filter_itm_ = " WHERE itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "
        type1_filter_itm_co_itm = " WHERE itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "
        type1_filter_calendar = " WHERE day_dt>'01/01/2018' "
        type1_filter_cal_day_dim = " WHERE day_dt>'01/01/2018' "
        type1_filter_cust_ship_to = " WHERE co_skey in (7,56) "

        var_sql_VendorAgreement = sqlfile.sql_src_VendorAgreement
        var_sql_oblig_dtl =sqlfile.sql_src_oblig_dtl
        var_sql_oblig_head =sqlfile.sql_src_oblig_head
        var_sql_itm =sqlfile.sql_src_itm
        var_sql_itm_co_itm =sqlfile.sql_src_itm_co_itm
        var_sql_calendar =sqlfile.sql_src_calendar
        var_sql_cal_day_dim =sqlfile.sql_src_cal_day_dim
        var_sql_cust_ship_to =sqlfile.sql_src_cust_ship_to

        var_sql_VendorAgreement = var_sql_VendorAgreement.replace("where 1=1", type1_filter_VendorAgreement)
        var_sql_oblig_dtl = var_sql_oblig_dtl.replace("where 1=1", type1_filter_oblig_dtl)
        var_sql_oblig_head = var_sql_oblig_head.replace("where 1=1", type1_filter_oblig_head)
        var_sql_itm_ = var_sql_itm.replace("where 1=1", type1_filter_itm_)
        var_sql_itm_co_itm = var_sql_itm_co_itm.replace("where 1=1", type1_filter_itm_co_itm)
        var_sql_calendar = var_sql_calendar.replace("where 1=1", type1_filter_calendar)
        var_sql_cal_day_dim = var_sql_cal_day_dim.replace("where 1=1", type1_filter_cal_day_dim)
        var_sql_cust_ship_to = var_sql_cust_ship_to.replace("where 1=1", type1_filter_cust_ship_to)
    else:
        print("Do nothing-will process the whole entire set with no filters set")


    dfsrc1 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_oblig_dtl + "  ", "DUMMY_TMP_TABLE_MISSING_WEEK_ENDING_SALE")
    dfsrc1 = dfsrc1.withColumn('week_ending_sale', next_day(dfsrc1.oblig_dt, 'Sun'))
    dfsrc1.createOrReplaceTempView("rs_TMP_SQL_src_sale_oblig_dtl_fact_mstr")


    dfsrc2 = common_func.registerRedshiftQuery(hive_context, var_sql_oblig_head, "TMP_SQL_src_sale_oblig_head_fact")
    dfsrc3 = common_func.registerRedshiftQuery(hive_context, var_sql_itm_, "TMP_SQL_src_itm_dim")
    dfsrc4 = common_func.registerRedshiftQuery(hive_context, var_sql_itm_co_itm, "TMP_SQL_src_itm_co_itm_rel")
    dfsrc5 = common_func.registerRedshiftQuery(hive_context, var_sql_VendorAgreement , "TMP_SQL_src_agr_vndr_agr_trans_fact")

    dfsrc6 = common_func.registerRedshiftQuery(hive_context, var_sql_cust_ship_to, "TMP_SQL_src_cust_ship_to_dim")
    dfsrc7 = common_func.registerRedshiftQuery(hive_context, var_sql_calendar ,  "TMP_SQL_src_calendar")
    dfsrc8 = common_func.registerRedshiftQuery(hive_context, var_sql_cal_day_dim,  "TMP_SQL_src_cal_day_dim")

    dfsrc9 = common_func.registerRedshiftQuery(hive_context, 'select * from intp.ei_sap_go_live_dates', "TMP_SQL_src_ei_sap_go_live_dates")


    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%READ ETL STAGE INTERMEDIATE  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    dfstg1 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_stg_agr_sum , "TMP_SQL_stg_ei_agr_vndr_agr_trans_reseg")
    dfstg2 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_stg_sus_weekly , "TMP_SQL_stg_ei_sus_weekly")

    print("***************************CACHE TABLES *************************")

    spark.cacheTable("rs_TMP_SQL_src_sale_oblig_dtl_fact_mstr")
    spark.cacheTable("rs_TMP_SQL_src_sale_oblig_head_fact_mstr")
    spark.cacheTable("rs_TMP_SQL_src_itm_dim_mstr")
    spark.cacheTable("rs_TMP_SQL_src_itm_co_itm_rel_mstr")
    spark.cacheTable("rs_TMP_SQL_src_agr_vndr_agr_trans_fact_mstr")
    spark.cacheTable("rs_TMP_SQL_src_cust_ship_to_dim_mstr")
    spark.cacheTable("rs_TMP_SQL_src_cal_day_dim_mstr")
    spark.cacheTable("rs_TMP_SQL_src_ei_sap_go_live_dates_mstr")

    #Two staging tables
    spark.cacheTable("rs_TMP_SQL_stg_ei_agr_vndr_agr_trans_reseg_mstr")
    spark.cacheTable("rs_TMP_SQL_stg_ei_sus_weekly_mstr")

    print("***************************ORIGINAL QUERY direct reference using x.var *************************")
    sqlmain = sqlfile.sql_final_main_full

    print("***************************REPLACING EDWP TABLES by LOADED HADOOP TABLES *************************")

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
