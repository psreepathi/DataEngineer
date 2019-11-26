# Created on Oct 17, 2019
# @author: AG22157
# This script is using for History file load validation.

# importing Required classes and methods.
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
import time
import sys
import logging
import traceback
import yaml
import argparse
import pyspark
from datetime import datetime
from pyspark import SparkFiles
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.utils import *
from subprocess import call
import csv
import random

# Creating the Class
class HistFileLoadValidation(object):

    def __init__(self, spark, param_dict, argv):
        self.params = {}
        self.spark = spark
        self.param_dict = param_dict
        self.argv = argv
           
        
    # write_hist_hbase_llk method
    def write_hist_hbase_llk(self):
        try:
            spark = self.spark
            params = self.params
            param_dict = self.param_dict
            
            print(" ")
            print("####################################")
            print("write_hist_hbase_llk method execution started")
            print(" ")
            
            subject = sys.argv[2]
            source = sys.argv[3]
            start_dt = sys.argv[4]
            
            subject_name = subject.strip().upper()
            source_name = source.strip().upper()
            
            print("subject area is: {}".format(subject_name))
            print("source system is: {}".format(source_name))
            
            spark.sql(""" insert overwrite table {0}.hist_hbase_llk
            select
            sc.subject_area ,
            sc.source_system, 
            log.load_ingstn_id, 
            sc.tablename,
            log.load_log_key ,
            log.work_flow_nm, 
            'FILE' as ingestion_type,
            'H' as load_type,
            ' ' as initial_offset, 
            ' ' as final_offset,
            current_timestamp() as load_end_dtm, 
            case when upper(log.pblsh_ind)='Y' then 'GREEN' else 'RED' end as  load_status, 
            ' ' as load_reason 
            from {0}.audit_bdf_load_schedule sc 
            left join {0}.audit_bdf_load_log log on log.work_flow_nm like concat_ws(sc.work_flow_nm,'%','%') 
            where trim(upper(sc.subject_area)) = '{1}' and trim(upper(sc.source_system)) = '{2}' 
            and log.load_ingstn_id >= '{3}'
            and trim(upper(sc.Ingestion_type)) = 'FILE'  """.format(param_dict['qc_database'],subject_name,source_name,start_dt))
            
            print("hist_hbase_llk table has been loaded.")
           
            # Tablename,total_hbase_llk_count details.
            print(" ")
            print("hist_hbase_llk data")
            spark.sql(""" select 
            tablename, 
            count(*) as total_hbase_llk_count 
            from {0}.hist_hbase_llk 
            group by tablename """.format(param_dict['qc_database'])).show(100, False)
            
            current_time=datetime.now()
            print(" ")
            print("Current Time is: {}".format(current_time))
            
            print("write_hist_hbase_llk method execution completed")
            print("####################################")

        except Exception as error:
            print("write_hist_hbase_llk method execution failed. Error Message is: {0}".format(error))
            raise
            

    # write_hist_hive_llk method
    def write_hist_hive_llk(self):
        try:
            spark = self.spark
            params = self.params
            param_dict = self.param_dict
            
            print(" ")
            print("####################################")
            print("write_hist_hive_llk method execution started")
            print(" ")
            
            tablenames_df=spark.sql("select tablename from {}.hist_hbase_llk group by tablename".format(param_dict['qc_database']))
            tablenames_df.cache().show(100,False)
            tables_count=tablenames_df.count()
            print("total number of tables: {0}".format(tables_count))
            print(" ")
            
            # Adding tablenames into list.
            tables_list=[]
            tablenames=tablenames_df.rdd.collect()
            tables_list.append(tablenames)
            
            # Running BDF HIVE Queries with each tablename.
            cnt = 0
            hive_llks = []
            while cnt < tables_count:
                tablename=tables_list[0][cnt]
                tablename = ', '.join(tablename)
                print("executing query for tablename is: {}".format(tablename))
                
                start_dt = sys.argv[4]
                end_dt = sys.argv[5]
                
                # Preparing Query to get hive_llk_result            
                query = "select '{1}' as tablename, concat_ws('|',collect_set(bdf_load_log_key)) as load_log_key from {0}.{1} where load_ingstn_id >= '{2}' and load_ingstn_id <= '{3}' group by '{1}'".format(param_dict['hive_bdf_rawz_db'],tablename,start_dt,end_dt)
                                
                try:
                    hive_llk_result=spark.sql(query)
                    llk = hive_llk_result.collect()[0]["load_log_key"]
                    hive_llk_rows = Row(tablename=str(tablename), load_log_key=str(llk))
                    hive_llks.append(hive_llk_rows)
                    print("{} query executed and table's LLKs are captured.".format(tablename))
                    cnt += 1
                except:
                    print("error occurred while executing {} query. So,please check the table...".format(tablename))
                    cnt += 1
            
            print("All tables queries are executed.")
            
            hive_llk_rdd = spark.sparkContext.parallelize(hive_llks)
            hive_llk_df = hive_llk_rdd.toDF()
            hive_llk_exp_df = hive_llk_df.select("tablename", explode(split(col("load_log_key"), "\\|")).alias("load_log_key"))
            
            print(" ")
            print("Schema of hive_llk_df ")
            hive_llk_exp_df.printSchema()
            
            hive_llk_exp_df.createOrReplaceTempView("hive_llk_exp_view")
            
            spark.sql(""" insert overwrite table {0}.hist_hive_llk
            select
            tablename as tablename,
            load_log_key as load_log_key
            from hive_llk_exp_view """.format(param_dict['qc_database']))
            
            print("hist_hive_llk table has been loaded.")

            # Tablename,total_hive_llk_count details.
            print("hist_hive_llk data")
            spark.sql(""" select
            tablename,
            count(*) as total_hive_llk_count
            from {0}.hist_hive_llk
            group by tablename """.format(param_dict['qc_database'])).show(100, False)
            
            current_time=datetime.now()
            print(" ")
            print("Current Time is: {}".format(current_time))

            print("write_hist_hive_llk method execution completed")
            print("####################################")

        except Exception as error:
            print("write_hist_hive_llk method execution failed. Error Message is: {0}".format(error))
            raise


    # write_hist_missing_llk method
    def write_hist_missing_llk(self):
        try:
            spark = self.spark
            params = self.params
            param_dict = self.param_dict

            start_dt = sys.argv[4]
            end_dt = sys.argv[5]
            
            print(" ")
            print("####################################")
            print("write_hist_missing_llk method execution started")
            print(" ")
            
            spark.sql(""" insert overwrite table {0}.hist_missing_llk
            select
            hbase.tablename as tablenmae,
            hbase.load_log_key as load_log_key
            from {0}.hist_hbase_llk hbase left join {0}.hist_hive_llk hive 
            on upper(hbase.tablename)=upper(hive.tablename) and hbase.load_log_key=hive.load_log_key
            and hbase.load_ingstn_id between '{1}' and '{2}'
            where hive.load_log_key is null """.format(param_dict['qc_database'],start_dt,end_dt))
            
            print("hist_missing_llk table has been loaded.")

            # Tablename,total_missing_llk_count details.
            print("hist_missing_llk data")
            spark.sql(""" select tablename,
            count(*) as total_missing_llk_count
            from {0}.hist_missing_llk
            group by tablename """.format(param_dict['qc_database'])).show(100, False)
            
            current_time=datetime.now()
            print(" ")
            print("Current Time is: {}".format(current_time))

            print("write_hist_missing_llk method execution completed")
            print("####################################")

        except Exception as error:
            print("write_hist_missing_llk method execution failed. Error Message is: {0}".format(error))
            raise


    # write_hist_file_load_validation_results method
    def write_hist_file_load_validation_results(self):
        try:
            spark = self.spark
            params = self.params
            param_dict = self.param_dict

            start_dt = sys.argv[4]
            end_dt = sys.argv[5]
            
            print(" ")
            print("####################################")
            print("write_hist_file_load_validation_results method execution started")
            print(" ")
            
            spark.sql(""" select 
            missing.tablename as tablename,
            missing.load_log_key as load_log_key,
            hbase.work_flow_nm as work_flow_nm 
            from {0}.hist_missing_llk missing inner join {0}.hist_hbase_llk hbase 
            on trim(upper(missing.tablename))=trim(upper(hbase.tablename)) and trim(missing.load_log_key)=trim(hbase.load_log_key) 
            """.format(param_dict['qc_database'])).createOrReplaceTempView("missing_llk_wf_name_view")

            spark.sql(""" select 
            tablename,
            load_log_key,
            work_flow_nm,
            case when extension_length=7 then substr(work_flow_nm,1,(length(work_flow_nm)-7)) 
            when extension_length=4 then substr(work_flow_nm,1,(length(work_flow_nm)-7)) end as work_flow_pattern
            from (select 
            tablename, 
            load_log_key, 
            work_flow_nm, 
            case when upper(substr(trim(work_flow_nm),-7)) in ('.DAT.GZ','.TZT.GZ') then 7 
            when upper(substr(trim(work_flow_nm),-4)) in ('.DAT','.OUT','.PST','.TXT') then 4 end as extension_length 
            from missing_llk_wf_name_view) A 
            """).createOrReplaceTempView("missing_llk_wf_pattern_view")  # Query needs to updated with more file extension types  

            spark.sql("""  select 
            tablename, 
            load_log_key, 
            work_flow_nm,
            work_flow_pattern,
            Next_workflow, 
            case when Next_workflow like concat_ws(work_flow_pattern,'%','%')  then 'Y' else 'N' end as max_file_flag 
            from 
            (select tablename,
            load_log_key,
            work_flow_nm, 
            work_flow_pattern, 
            lead(work_flow_pattern) over(partition by tablename order by work_flow_nm) as Next_workflow 
            from missing_llk_wf_pattern_view) A """).createOrReplaceTempView("missing_llk_wf_pattern_flag_view")
            
            spark.sql(""" select
            tablename,
            load_log_key,
            work_flow_nm,
            work_flow_pattern
            from missing_llk_wf_pattern_flag_view 
            where trim(upper(max_file_flag)) = 'N' """).createOrReplaceTempView("missing_llk_max_wf_pattern_flag_view")

            spark.sql(""" select 
            tablename,
            load_log_key ,
            work_flow_nm
            from {0}.hist_hbase_llk hbase 
            where load_status = 'GREEN' 
            and hbase.load_log_key not in (select distinct load_log_key from {0}.hist_missing_llk)
            """.format(param_dict['qc_database'])).createOrReplaceTempView("hbase_data_green_view")

            spark.sql(""" select 
            missing.load_log_key 
            from missing_llk_max_wf_pattern_flag_view missing inner join hbase_data_green_view hbase  
            on trim(upper(missing.tablename)) =trim(upper(hbase.tablename))  
            and hbase.work_flow_nm like concat_ws(missing.work_flow_pattern,'%','%') 
            group by missing.load_log_key """).createOrReplaceTempView("reprocessed_missing_llk_view")

            spark.sql("""  select 
            load_log_key 
            from  missing_llk_max_wf_pattern_flag_view 
            where load_log_key not in (select load_log_key from reprocessed_missing_llk_view)  
            """).createOrReplaceTempView("not_reprocessed_missing_llk_view")

            spark.sql(""" select 
            hbase.subject_area, 
            hbase.source_system,
            hbase.tablename,
            hbase.load_log_key, 
            hbase.work_flow_nm,
            hbase.ingestion_type,
            hbase.load_type, 
            hbase.initial_offset, 
            hbase.final_offset,
            hbase.load_end_dtm, 
            'RED' as load_status, 
            hbase.load_reason, 
            hbase.load_ingstn_id
            from  {0}.hist_hbase_llk hbase  
            where load_log_key in (select load_log_key from not_reprocessed_missing_llk_view)
            and load_ingstn_id between '{1}' and '{2}'
            """.format(param_dict['qc_database'],start_dt,end_dt)).createOrReplaceTempView("red_status_records")

            spark.sql(""" select 
            hbase.subject_area, 
            hbase.source_system,
            hbase.tablename,
            hbase.load_log_key, 
            hbase.work_flow_nm,
            hbase.ingestion_type,
            hbase.load_type, 
            hbase.initial_offset, 
            hbase.final_offset,
            hbase.load_end_dtm, 
            'GREEN' as load_status, 
            hbase.load_reason,
            hbase.load_ingstn_id
            from  {0}.hist_hbase_llk hbase  
            where load_log_key in (select load_log_key from hbase_data_green_view
            where load_ingstn_id between '{1}' and '{2}') 
            """.format(param_dict['qc_database'],start_dt,end_dt)).createOrReplaceTempView("green_status_records")

            spark.sql(""" insert into table {0}.AUDIT_BDF_LOAD_VALIDATION partition(load_ingstn_id)
            select * from green_status_records
            union 
            select * from red_status_records    
            """.format(param_dict['qc_database']))
            
            print("audit_bdf_load_validation table has been loaded.")
        
            # To print GREEN and RED status record count of tables.
            spark.sql(""" select 
            tablename,
            case when collect_set(GREEN_STATUS_RECORDS_1)[0] is null then 0 else collect_set(GREEN_STATUS_RECORDS_1)[0] end as GREEN_STATUS_RECORDS,
            case when collect_set(RED_STATUS_RECORDS_1)[0] is null then 0 else collect_set(RED_STATUS_RECORDS_1)[0] end as RED_STATUS_RECORDS
            from(
            select 
            tablename,
            case when upper(load_status)='GREEN' then count END as GREEN_STATUS_RECORDS_1,
            case when upper(load_status)='RED' then count END as RED_STATUS_RECORDS_1
            from (
            select tablename,load_status,count(*) as count from {0}.audit_bdf_load_validation 
            where tablename in (select distinct tablename from {0}.hist_hbase_llk)
            and upper(load_type) = 'H' and cast(load_end_dtm as date) >= cast(date_sub(current_date(),3) as date)
            group by tablename,load_status  order by tablename,load_status
            ) A)B
            group by tablename """.format(param_dict['qc_database'])).show(100,False)
            
            print(" ")
            print("write_hist_file_load_validation_results method execution completed")
            print("####################################")

        except Exception as error:
            print("write_hist_file_load_validation_results method execution failed. Error Message is: {0}".format(error))
            raise


# main method
def main(argv):
    print("######  HISTORY FILE LOAD VALIDATION JOB STARTED  ######")
    
    job_start_time=datetime.now()
    print("Job started at: {}".format(job_start_time))
    
    # Creating SPARK Session
    spark = SparkSession.builder.appName('HIST_FILE_LOAD_VALIDATION').enableHiveSupport().getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    # printing all command line arguments.
    print(" ")
    print("####################################")
    print("Printing All command line arguments")
    print(" ")
    print("environment is ==> " + argv[1])
    print("Subject Area is ==> " + argv[2])
    print("Source System is ==> " + argv[3])
    print("Load_ingstn_id start date is ==> " + argv[4])
    print("Load_ingstn_id end date is ==> " + argv[5])
    print("####################################")
    print(" ")

    # Reading YAML file
    try:
        with open("yaml_file", 'r') as f:
            parser = argparse.ArgumentParser()
            try:
                with open("yaml_file", 'r') as f:
                    parser = argparse.ArgumentParser()
                    try:
                        doc = yaml.load(f)

                        # Reading & printing hive_settings path  from YAML file
                        hive_settings_params = doc[sys.argv[1]]["hive_settings"]
                        hive_bdf_rawz_db = hive_settings_params["hive_bdf_rawz_db"]
                        qc_database = hive_settings_params["qc_database"]

                        print("####################################")
                        print("Printing YAML contents")
                        print(" ")
                        print("hive_bdf_rawz_db is ==> " + hive_bdf_rawz_db)
                        print("qc_database is ==> " + qc_database)
                        print("####################################")

                    except:
                        raise Exception("Please check for the incorrect arguments passed")

            except:
                raise Exception("Please check for the valid configuration keys in the YAML environment list ")

    except:
        raise Exception(" YAML file is not loaded ")

    # Creating the parm_dict with YAML file values
    param_dict = {'hive_bdf_rawz_db': hive_bdf_rawz_db, 'qc_database': qc_database}

    # HistFileLoadValidation methods execution start from here..
    Hist_file_load_validation = HistFileLoadValidation(spark, param_dict, argv)
    Hist_file_load_validation.write_hist_hbase_llk()
    Hist_file_load_validation.write_hist_hive_llk()
    Hist_file_load_validation.write_hist_missing_llk()
    Hist_file_load_validation.write_hist_file_load_validation_results()
    
    job_end_time=datetime.now()
    print(" ")
    print("Job ended at: {}".format(job_end_time))
    
    job_run_time=job_end_time-job_start_time
    job_run_time_in_sec=job_run_time.total_seconds()
    job_run_time_in_hours = divmod(job_run_time_in_sec, 60)[0]
    
    print(" ")
    print("Job Run Time in Minutes: {}".format(job_run_time_in_hours))
    print(" ")
    print("######  HISTORY FILE LOAD VALIDATION JOB COMPLETED  ######")
    
    # Exiting from process.
    exit(0)
     
    
# Program main function
if __name__ == '__main__':
    main(sys.argv)