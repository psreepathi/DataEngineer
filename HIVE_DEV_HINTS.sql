###############
#    HIVE     #    
###############


FOR HIVE DATABASE
====================
CREATE DATABASE IF NOT EXISTS DATABASE_NAME;  [or]
CREATE DATABASE DATABASE_NAME comment 'my_commnet' Location 'path';
Example: create database stgcps_dv1 LOCATION 'maprfs:/app/HadoopCPS/hive/warehouse/stgcps_dv1.db'

USE DATABASE_NAME;
show tables like 'pattern*'; -- to check tables in database with pattern
show tables in Database_name;  -- to list tables in another DB.
DROP DATABASE IF EXISTS DATABASE_NAME;


To Create Table
===============
EXTERNAL:
********
CREATE EXTERNAL TABLE IF NOT EXISTS Database_name.TABLE_NAME (SCHEMA)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '(delimiter)'
LINES TERMINATED BY '\n'
STORED AS fileformat
Location 'hdfs_file_path';

MANAGED or INTERNAL:
*******************
CREATE TABLE IF NOT EXISTS Database_name.TABLE_NAME (SCHEMA)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '(delimiter)'
LINES TERMINATED BY '\n'
STORED AS fileformat;

PARTITIONED TABLE:
*******************
CREATE EXTERNAL TABLE IF NOT EXISTS Database_name.TABLE_NAME (col1,col2,....)
PARTITIONED BY (partition_col datatype)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '(delimiter)'
LINES TERMINATED BY '\n'
STORED AS fileformat;

BUCKETING TABLE:
*******************
CREATE EXTERNAL TABLE IF NOT EXISTS Database_name.TABLE_NAME (col1,col2,....)
PARTITIONED BY (partition_col datatype)
CLUSTERED BY(col1) INTO 256 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '(delimiter)'
LINES TERMINATED BY '\n'
STORED AS fileformat;

NOTE: we can modify bucket number based on data volume and our requirement.

Skewed table creation
*******************************************************
create table table_name(c1,c2....) skewed by(col_name) on ('value')
create table hdpbartwa.skewed_data(id int, name string) skewed by(id) on ('-999')

To drop TABLE
==============
DROP TABLE IF EXISTS Database_name.TABLE_NAME;


LOADING DATA INTO TABLE:
======================
From HDFS location
*******************
LOAD DATA INPATH 'file_path' [OVERWRITE] INTO TABLE Database_name.TABLE_NAME;

From localfile system (unix)
****************************
LOAD DATA LOCAL INPATH 'file_path' [OVERWRITE] INTO TABLE Database_name.TABLE_NAME;

insert Records Manually
***********************
INSERT INTO Database_name.TABLE_NAME (column_list) values (column_values);  --- to insert records manually.

INSERT DATA FROM ONE TABLE TO ONOTHER TABLE
********************************************
INSERT [OVERWRITE | INTO] TABLE Database_name.target_TABLE_NAME
SELECT CLOUMN NAMES FROM Database_name.source_TABLE_NAME;          (or)

FROM Database_name.source_TABLE_NAME
INSERT [OVERWRITE | INTO] TABLE Database_name.target_TABLE_NAME
SELECT COLUMN NAMES;

Creating tables using other tables
**********************************
Create table Database_name.TARGET_TABLE_NAME stored as fileformat as
select columns_list from Database_name.SOURCE_TABLE_NAME;

Note: we cannot create EXTERNAL tables using above Query.


HIVE shortcuts
===============
Ctrl+A goes to the beginning of the line

Ctrl+E goes to the end of the line

hive -e 'QUERY'  or hive -e "Query" - to run thehive query out side hive shell [e-evaluate]

hive -S -e 'QUERY' - to run in silent mode [S -silent mode, no progress in shell]

hive -f 'hive script file'  - to run hive script file outside from hive shell [f - file]

source scriptfile - to run hive script file in hive shell

!command - execute any command from hive shell [ex: !cat \users\username\file1.txt -- display file1.txt content in hive shell]



To describe table information
==============================
SHOW CREATE TABLE Database_name.TABLE_NAME  -  to display create table statement
DESCRIBE  [or desc] Database_name.TABLE_NAME; - to display SCHEMA of table
DESCRIBE EXTENDED Database_name.TABLE_NAME;	  - to display schema with table info also.
DESCRIBE FORMATTED Database_name.TABLE_NAME;  - to display schema with table info also.


ALTER Table properties
=======================
Note: dont use DBname in alter table statement. please run use db; before running any alter statement.

ALTER TABLE old_tablename RENAME TO new_table_name - To rename the table

ALTER TABLE table_name ADD COLUMNS (column_name INT); -  To add a column in a existing table

ALTER TABLE table_name REPLACE COLUMNS(columns_list [we have to give the column names which we want to keep in the table]); - to delete column in table

ALTER TABLE table_name CHANGE column old_column_name new_column_name data_type - to change the column name

ALTER TABLE table_name CHANGE column old_column_name new_column_name new_data_type  -  need To change the both column_name and datatype of a column.

ALTER TABLE tablename SET TBLPROPERTIES ('serialization.null.format'='');

ALTER TABLE tablename SET LOCATION 'path';

ALTER TABLE tablename RENAME to new_DB_name.tablename;

ALTER TABLE update_test DROP PARTITION ( colname = '__HIVE_DEFAULT_PARTITION__');  -- to drop the partition
[NOTE: we cannot remove the integer datatype partitions. So, we have to change datatype to string then drop partition and revert back to int again]

ALTER TABLE $tablename SET TBLPROPERTIES('EXTERNAL'='False'); -- To make the table as internal

show tables in  database like "tablename*"


JOINS
=======
SELECT /*+ MAPJOIN(c) */ * FROM tablename1 join tablename2;  -  to enable mapjoin in query  or
SET hive.auto.convert.join = true  (less than 25 MB tables)

note: we cannot use "OR" operator in ON clause while joining tables.


for hive  variables configuration
=================================
Script name: my_script.hql  (we can run .hql and.sql files from unix shell using hive -f filename)

QUERY inside the script:  select ${hiveconf:column1},${hiveconf:column2} from tablename1.

To execute in unix shell:  hive -hiveconf column1='name' -hiveconf column2=salary -f my_script.hql


To store hive query results into local directory in the delimiter seperated file
=================================================================================
INSERT OVERWRITE LOCAL DIRECTORY '/home/test/result/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY 'delimiter'
SELECT * from table;										(or)

select * from table >> 'path'  (local system path)

hive -e  " Query" > Path


HIVE PARAMETERS:
================
set HIVE.<TAB>  -  TAB auto completion

set hive.cli.print.current.db=true;   - To Print Current DB in use
SET hive.cli.print.header=true;  -  Print Column Headers


COST BASED QUERY OPTIMIZATION
============================================
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.stats.dbclass=fs;
SET hive.optimize.ppd=true;


batch of rows processing -  - by performing them in batches of 1024 rows at once instead of single row each time.
**************************
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


Enable Hive to use Tez DAG APIs
*********************************
set hive.execution.engine=tez;


To enable auto join, so that no need use map join hints in hive query
***************************************************************************************************
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;  - size configuration enables the user to control what size table can fit in memory


Sort Merge Bucket Map(SMB Map) Join
******************************************************************
set hive.enforce.bucketing=true;
set hive.auto.convert.sortmerge.join=true
set hive.optimize.bucketmapjoin = true
set hive.optimize.bucketmapjoin.sortedmerge = true
set hive.auto.convert.sortmerge.join.noconditionaltask=true

SKEW JOINS
********************************************
set hive.optimize.skewjoin = true;
set hive.groupby.skewindata=true;

set hive.skewjoin.key = 100000;
set hive.skewjoin.mapjoin.map.tasks = 1000;
set hive.skewjoin.mapjoin.min.split = 33554432;

SET hive.mapred.supports.subdirectories=true;


Dynamic partition
*********************************
set hive.exec.dynamic.partition = true - Needs to be set to true to enable dynamic partition inserts
set hive.exec.dynamic.partition.mode = nonstrict -  in nonstrict mode all partitions are allowed to be dynamic
set hive.exec.max.dynamic.partitions.pernode = 100 - Maximum number of dynamic partitions allowed to be created in each mapper/reducer node
set hive.exec.max.dynamic.partitions = 1000 - Maximum number of dynamic partitions allowed to be created in total
set hive.exec.max.created.files = 10000 - Maximum number of HDFS files created by all mappers/reducers in a MapReduce job
set hive.error.on.empty.partition = false - Whether to throw an exception if dynamic partition insert generates empty results

set mapreduce.reduce.memory.mb= 12288;
set mapreduce.map.memory.mb= 12288;
set mapreduce.reduce.child.java.opts=-Xmx9830m;
set mapreduce.map.child.java.opts=-Xmx9830m;
SET hive.parquet.timestamp.skip.conversion = false;
set mapreduce.map.cpu.vcores=0;
set mapred.max.split.size=1073741824;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.auto.convert.join=false;
set hive.exec.parallel=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set mapreduce.map.output.compress=false;
set mapreduce.map.sort.spill.percent=0.99;
set mapreduce.reduce.shuffle.parallelcopies=20;
set mapreduce.reduce.merge.inmem.threshold=0;
set hive.optimize.insert.dest.volume=false;

memory settings
******************************************************************
set mapreduce.map.memory.mb=8192;  - RAM per container
set mapreduce.reduce.memory.mb=8192; - RAM per container

SET mapred.reduce.child.java.opts= -Xmx8000m; -  0.8 * RAM per container
SET mapred.map.child.java.opts= -Xmx8000m;  -  0.8 * RAM per container

set hive.exec.parallel=true;  - if job has independent queries, it will execute parallel

set hive.exec.reducers.bytes.per.reducer=209715200;  -- each reducer will process 2GB data, so that depends on data reducers will run.
(data size is 10 gb then 5 reducers)

HIVE SETTINGS:
**************************************
hive.merge.mapfiles -- Merge small files at the end of a map-only job.
hive.merge.mapredfiles -- Merge small files at the end of a map-reduce job.
hive.merge.size.per.task -- Size of merged files at the end of the job.
hive.merge.smallfiles.avgsize -- When the average output file size of a job is less than this number, Hive will start an additional map-reduce job to merge the output files into bigger files. This is only done for map-only jobs if hive.merge.mapfiles is true, and for map-reduce jobs if hive.merge.mapredfiles is true.

set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;



ACID settings for HIVE UPDATE
******************************************************************
set hive.support.concurrency=true;

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=2;


SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec
SET hive.exec.compress.output=true
SET mapreduce.output.fileoutputformat.compress=true


MSCK REPAIR  (to add  new partitions information to metastore of the table)
***************************************************************************************************
msck repair table tablename  - to repair partitions in hive table.
set hive.msck.path.validation=ignore; or set hive.msck.path.validation=skip;
Note: use db; before running above statement.

DATE convert from hive non compatible type to hive compatible type:
***************************************************************************************************
select CAST(from_unixtime(unix_timestamp(column_name, 'MM/dd/yyyy HH:mm:ss'),'yyyy-MM-dd') as date);  --- to date

select CAST(from_unixtime(unix_timestamp(column_name,'MM/dd/yyyy HH:mm:ss'))as timestamp);   --- to timestamp

to_date(from_utc_timestamp(createddate,'PST8UDT')) as createddate_pst  --- to convert from utc to PST zone.

select from_unixtime(unix_timestamp('column_date_part','yy-MM-dd'),'u') as dow; --- to get day of week

to_date(createddate) -- it return only date value

CAST(column as date) -- it convert to date and returns only date_part, previously column has timestamp value.

from_unixtime(unix_timestamp())  or current_timestamp()--- to print current_datetime.

current_date() -- it retrns current date value

unix_timestamp() - it returns current time unix epoch seconds value.

from_unixtime(cast(COLNAME/1000 as bigint)) -- to return datetime from epoch time

from_unixtime(unix_timestamp(substr(log_source_time,0,11),'dd-MMM-yyyy'))



HIVE TABLE STATISTICS:
******************************************************************
Table-level statistics:
ANALYZE TABLE <table_name> COMPUTE STATISTICS;

Column-level statistics (critical):
ANALYZE TABLE <table_name> COMPUTE STATISTICS for COLUMNS;

As new partitions are added to the table, if the table is partitioned on "col1" and the new partition has the key "x" then you must also use the following command:
ANALYZE TABLE <table_name> partition (coll="x") COMPUTE STATISTICS for COLUMNS;

ANALYZE TABLE db_name.table_name partition (partition_column) COMPUTE STATISTICS ; -- for full partitioned table

ANALYZE TABLE db_name.table_name partition (partition_column) COMPUTE STATISTICS for COLUMNS; -- for all columns of partitioned table


If you have hive query and its timing out , you can set below configurations in following way:
set mapred.tasktracker.expiry.interval=1800000;
set mapred.task.timeout= 1800000;


Hive Query logs:
*******************************************************
hadoop fs -get /var/mapr/cluster/yarn/rm/staging/hdpdfs/.staging/job_1492285682883_610692/job.xml

hadoop fs -get /var/mapr/cluster/yarn/rm/staging/hdpbartwa/.staging/job_1492285682883_614745/job.xml


OUTPUT FROM BEELINE:
=========================
beeline -u 'BEELINE_URL' --showHeader=false --outputformat=csv2 -e "query" > <PATH>


to kill hadoop job
*******************
/opt/mapr/hadoop/hadoop-2.7.0/bin/hadoop job  -kill job_1489149673263_567973
