########################################################################
# PURPOSE: TO PERFORM THE AGGREGATION FOR THE PO_CO_ITM_VNDR_DAY_AGGR   #
########################################################################
#1144

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

sys.path.append('../')
sys.path.append('/home/hadoop/')
import common.config as config

#PENDING RESTORE TO ORIGINAL CODE import common.common_func_RSALAZAR as common_func
import common_func_RSALAZAR as common_func

import LogisticEarnedIncomeSQL  as sqlfile

#ON HOLD USING QUERY FILE - PENDING import po_co_itm_vndr_day_aggr_sql as query_file

def main():

    #PENDING Try Sending output to screen

    # To fetch the current date and script name from sys.argv[] and generate log file path.
    current_date = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    file_name = sys.argv[0].split('/')[-1].split('.')[0]

    log_file_path = "{}/{}_{}.log".format(config.log_file_directory, file_name, current_date)


    # To initialize logging
    #cHANGED FROM INFO to WARNING.
    logging.basicConfig(filename=log_file_path, filemode='w', level=logging.INFO)

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

    logging.info('Assigning values Started at  %s', datetime.datetime.now())

    # sqlstatement1_po_detail
    # sqlstatement2_po_header
    # sqlstatement3_join

    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%READ SOURCES PURCHASE ORDER HEADER AND DETAIL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    print("%%*sqlfile. read source po_dtl_fact")
    print(sqlfile.sql_src_po_dtl_fact)
    df1 = common_func.registerRedshiftQuery(hive_context, sqlfile.sql_src_po_dtl_fact,"TMP_SQL_src_po_dtl_fact")
    #this converts into rs_TMP_SQL_src_po_dtl_fact_mstr

    print("%%*sqlfile. read source po_head_fact")
    print(sqlfile.PurchaseOrderHeader)
    df2 = common_func.registerRedshiftQuery(hive_context, sqlfile.PurchaseOrderHeader,"TMP_SQL_src_PurchaseOrderHeader")
    #this converts into rs_TMP_SQL_src_po_dtl_fact_mstr


    print("%%*sqlfile.TransactionsOrder1")
    print(sqlfile.TransactionsOrder1)
    df3 = common_func.registerRedshiftQuery(hive_context, sqlfile.TransactionsOrder1, "TMP_SQL_src_TransactionsOrder1")
    # this converts into rs_TMP_SQL_src_po_dtl_fact_mstr

#Apply filters to dataframes for testing
    #     sql_list_po = """ SELECT co_po_nbr where co_po_typ_cd = 'DRP' and co_po_nbr > '00476040' and co_po_nbr < '00790830' """

    #Header and Detail tables have the same column names thus the same filter can be applied
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%aADDING FILTERS TO QUERIES USING METHOD %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    #df1 = df1.filter("co_po_typ_cd = 'DRP' and co_po_nbr > '00476040' and co_po_nbr < '00790830' ")
    #df2 = df2.filter("co_po_typ_cd = 'DRP' and co_po_nbr > '00476040' and co_po_nbr < '00790830' ")
    #For transactions the field is ordr_po_nbr= '00758850'
    #df3 = df3.filter("ordr_po_nbr  > '00476040' and ordr_po_nbr < '00790830'")

    print(        "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%READ SOURCES TRANSACTION ORDER TABLES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    # RETURNED/REGISTERED NAME IS rs_TMP_SQL_agr_vndr_agr_trans_fact_mstr

    #template    df1_count_all = hive_context.sql("SELECT count(*) COUNT_records_po_detail_mstr FROM rs_TMP_SQL_po_detail_mstr")

    df1_count_all = hive_context.sql("SELECT count(*) COUNT_TMP_SQL_src_PurchaseOrderHeader_mstr FROM rs_TMP_SQL_src_PurchaseOrderHeader_mstr")
    # df1_count_all.printSchema()
    df1_count_all.show()

    df2_count_all = hive_context.sql("SELECT count(*) COUNT_TMP_SQL_src_po_dtl_fact_mstr FROM rs_TMP_SQL_src_po_dtl_fact_mstr")
    #df2_count_all.printSchema()
    df2_count_all.show()

    df3_count_all = hive_context.sql("SELECT count(*) COUNT_TMP_SQL_src_TransactionsOrder1_mstr FROM rs_TMP_SQL_src_TransactionsOrder1_mstr")
    df3_count_all.printSchema()
    df3_count_all.show()


    print("%%%%%%%%%%%%%%%%%%%%%%%%%% Defining PURCHASE ORDER data frames DEPENDING ON DETAIL TABLE%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    # pending review conversion do decimal(19,2)
    # This data frams are all using the same source, so the data is read once, and the dataframes only hold
    # the definition of the different queries .

    print("%%PurchaseOrder1_rdc")
    print(sqlfile.PurchaseOrder1_rdc)
    dfPO1 = spark.sql(sqlfile.PurchaseOrder1_rdc)
    dfPO1.createOrReplaceTempView("TMP_SQL_PurchaseOrder1_rdc")
    #print("Count Records:")
    #print(dfTransformation1.count())

    print("%%PurchaseOrder2_non_rdc")
    print(sqlfile.PurchaseOrder2_non_rdc)
    dfPO2 = spark.sql(sqlfile.PurchaseOrder2_non_rdc)
    dfPO2.createOrReplaceTempView("TMP_SQL_PurchaseOrder2_non_rdc")
    #print("Count Records:")
    #print(dfTransformation1.count())

    print("%%PurchaseOrder4")
    print(sqlfile.PurchaseOrder4)
    dfPO3 = spark.sql(sqlfile.PurchaseOrder4)
    dfPO3.createOrReplaceTempView("TMP_SQL_PurchaseOrder4")
    # print("Count Records:")
    # print(dfTransformation1.count())


    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%JOIN STATEMENT MAIN QUERY%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*")

    #pending review conversion do decimal(19,2)

    print("%%Transformation1 - TMP_SQL_Join4Pieces")
    print(sqlfile.Join4Pieces)
    dfTransformation1 = spark.sql(sqlfile.Join4Pieces)
    dfTransformation1.createOrReplaceTempView("TMP_SQL_Join4Pieces")
    dfTransformation1.printSchema()

    #print("Count Records:")
    #print(dfTransformation1.count())


    print("%%*MAIN_QUERY")

    varAllCols=1

    if varAllCols:
        print("Printing all columns used in the program for debugging purposes")
        dfmain = spark.sql(sqlfile.LogisticEarnedIncomeMainQryAllCols)
        print(sqlfile.LogisticEarnedIncomeMainQryAllCols)
        common_func.loadDataIntoRedshift(logging, 'INSERT', 'intp', 'ei_sus_lei_all_cols2', dfmain, opco_list=co_nbr_list)
    else:
        print("0 - Storing only columns originally defined in 1010")
        dfmain = spark.sql(sqlfile.LogisticEarnedIncomeMainQry1)
        print(sqlfile.LogisticEarnedIncomeMainQry1)
        common_func.loadDataIntoRedshift(logging, 'INSERT', 'intp', 'ei_sus_lei', dfmain, opco_list=co_nbr_list)


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
