##############################
#   SPARK for PYTHON/SCALA   #
##############################

KILL YARN APPLICATION:
======================
yarn application -kill application_1498328801805_1403890

YARN LOGS:
==========
yarn logs -applicationId application_1504913348944_11004

spark-sql:
----------
/opt/mapr/spark/spark-2.2.1/bin/spark-sql --master yarn --deploy-mode client --driver-memory 12G --num-executors 60 --executor-cores 4 --executor-memory 7G --conf spark.driver.maxResultSize=8G  

set mapreduce.input.fileinputformat.input.dir.recursive = true;
set spark.sql.hive.convertMetastoreParquet = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.rpc.askTimeout=600s
spark.sql.shuffle.partitions=2


spark-shell
******************
spark-shell  --master yarn --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.sql.crossJoin.enabled=true  --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.yarn.executor.memoryOverhead=2048 --executor-memory 12g --driver-memory 12g --num-executors 60 --executor-cores 4 --conf spark.driver.maxResultSize=8G  

spark2-shell  --master yarn --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.sql.crossJoin.enabled=true  --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.yarn.executor.memoryOverhead=2048 --executor-memory 12g --driver-memory 12g --num-executors 60 --executor-cores 4 --conf spark.driver.maxResultSize=8G 

SPARK CONFIGURATION PROPERTIES:
***********************************
spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions","32")
spark.conf.set("conf spark.driver.maxResultSize","8G")
spark.conf.set("conf spark.debug.maxToStringFields","true")
spark.conf.set("conf spark.executor.heartbeatInterval","600s")
spark.conf.set("conf spark.rpc.askTimeout","600s")
spark.conf.set("conf spark.network.timeout","600s")
spark.conf.set("hive.exec.dynamic.partition","true")
spark.conf.set("hive.exec.max.dynamic.partitions","1000")
spark.conf.set("hive.exec.max.dynamic.partitions.pernode","1000") 


--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.initialExecutors=5 
--conf spark.dynamicAllocation.maxExecutors=30 
--conf spark.dynamicAllocation.minExecutors=6
--conf spark.dynamicAllocation.executorIdleTimeout=60s 
--conf spark.dynamicAllocation.cachedExecutorIdleTimeout=infinity (bcoz if we  remove this executor data may lose)
--conf spark.dynamicAllocation.schedulerBacklogTimeout=1s (If dynamic allocation is enabled and there have been pending tasks backlogged for more than this duration, new executors will be requested)

--executor-cores also 5

--conf spark.shuffle.spill=true 
--conf spark.shuffle.service.enabled=true
--conf spark.executor.heartbeatInterval=600s
--conf spark.rpc.askTimeout=600s
--conf spark.network.timeout=600s
--conf spark.yarn.executor.memoryOverhead=2560
--executor-memory 13g 
--driver-memory 10g 
--conf spark.driver.maxResultSize=4g 
--num-executors 80 
--executor-cores 2 
--conf spark.sql.autoBroadcastJoinThreshold=-1
--conf spark.sql.cbo.enabled=true
--conf spark.sql.broadcastTimeout=1200s ( to resolve Caused by: org.apache.spark.SparkException: Exception thrown in awaitResult: error)

:help (to check in spark-shell/scala)

:paste --> enter multiple spark commands and then (ctrl-D to finish/ or to evaluate)

Extract output into file in spark 
***********************************
spark.sql(""" Query """).coalesce(1).write.format("csv").option("header", "true").save("path") -- path should be HDFS


scala> :help
All commands can be abbreviated, e.g., :he instead of :help.
:edit <id>|<line>        edit history
:help [command]          print this summary or command-specific help
:history [num]           show the history (optional num is commands to show)
:h? <string>             search the history
:imports [name name ...] show import history, identifying sources of names
:implicits [-v]          show the implicits in scope
:javap <path|class>      disassemble a file or class name
:line <id>|<line>        place line(s) at the end of history
:load <path>             interpret lines in a file
:paste [-raw] [path]     enter paste mode or paste a file
:power                   enable power user mode
:quit                    exit the interpreter
:replay [options]        reset the repl and replay all previous commands
:require <path>          add a jar to the classpath
:reset [options]         reset the repl to its initial state, forgetting all session entries
:save <path>             save replayable session to a file
:sh <command line>       run a shell command (result is implicitly => List[String])
:settings <options>      update compiler options, if possible; see reset
:silent                  disable/enable automatic printing of results
:type [-v] <expr>        display the type of an expression without evaluating it
:kind [-v] <expr>        display the kind of expression's type
:warnings                show the suppressed warnings from the most recent line which had any'


To see the methods of any class from package 
***************************************************
ex: import org.json4s.jackson.JsonMethods.<press tab>
asJValue   asJsonNode   compact   fromJValue   fromJsonNode   mapper   parse   parseOpt   pretty   render


Loading data from dataframe to hive tables
=======================================
df.write().mode("overwrite").saveAsTable("schemaName.tableName");  [OR]
df.select(df.col("col1"),df.col("col2"), df.col("col3")) .write().mode("overwrite").saveAsTable("schemaName.tableName");  [OR]
df.write().mode(SaveMode.Overwrite).saveAsTable("dbName.tableName");

//writing final dataset in temp table
df_hist_load_new.createOrReplaceTempView("W_PROJECT_TASKS_INCR")
val tbl_Path_load = s"${as_proj_parent_hive_f}/W_PROJECT_TASKS_INCR"
val option_load = Map("path" -> tbl_Path_load)
df_hist_load_new.write.format("parquet").options(option_load).mode(SaveMode.Overwrite).saveAsTable(s"${adv_work_db}.W_PROJECT_TASKS_INCR")

println("loading data in to final table")
//copying from temp table to target tables    
val final_dataset = hql.sql(s"""select * from ${adv_work_db}.W_PROJECT_TASKS_INCR""").cache()
val  tbl_Path_Update = s"${as_proj_parent_hive_f}/AS_PROJECT_TASKS"
val    option_Update = Map("path" -> tbl_Path_Update)
final_dataset.write.format("parquet").options(option_Update).mode(SaveMode.Overwrite).saveAsTable(s"${adv_service_db}.AS_PROJECT_TASKS")
	
for Partitioned table
***************************
we have to enable the following properties to make it work.
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

df.write().mode(SaveMode.Append).partitionBy("colname").saveAsTable("Table")

TO RUN SCALA FROM UNIX SHELL:
*********************************
spark-shell -i --master yarn --deploy-mode client --files /opt/mapr/spark/spark-2.1.0/conf/hive-site.xml --conf spark.local.dir=/opt/mapr/tmp/ --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.yarn.executor.memoryOverhead=2560 --executor-memory 13g --driver-memory 10g --conf spark.driver.maxResultSize=8g --num-executors 60 --executor-cores 4 /users/hdpasbi/bookings_extraction_data/childsku_executive_dashboard_fnl.scala

df.createOrReplaceTempView("people") 
df.createGlobalTempView("people")
spark.sql("SELECT * FROM global_temp.people").show()

Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, 

you can create a global temporary view. Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.

spark.sql(""" select * from advance_services_db.as_quote limit 10 """).createOrReplaceGlobalTempView("AS_QUOTE_TEMP")


partitions count
*******************
val as_quote = spark.sql(""" select * from advance_services_db.as_quote """)
as_quote.rdd.partitions.size

TO CREATE SPARKSESSION:
**************************
val spark = SparkSession
   .builder()
   .appName("SparkSessionZipsExample")
   .config("spark.sql.warehouse.dir", warehouseLocation)
   .enableHiveSupport()
   .getOrCreate()
   
SPARK SUBMIT SAMPLE:
********************** 
/opt/mapr/spark/spark-2.2.1/bin/spark-submit --master yarn --deploy-mode cluster --files /opt/mapr/spark/spark-2.1.0/conf/hive-site.xml  --conf spark.local.dir=/opt/mapr/tmp/ --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.yarn.executor.memoryOverhead=600 --executor-memory 7g --driver-memory 6g --num-executors 40 --conf ""spark.driver.extraClassPath=/opt/mapr/hive/hive-2.1/lib/hive-hbase-handler-2.1.1-mapr-1703-r1.jar"" --conf ""spark.executor.extraClassPath=/opt/mapr/hive/hive-2.1/lib/hive-hbase-handler-2.1.1-mapr-1703-r1.jar"" --class <CLASSNAME> /hdfs/app/ASBI/scripts/jars/asbi_financial.jar /app/ASBI/param/asbi.properties as_project_tasks 2018-10-24 10:00:37


SPARK DATAFRAME JOINS: (SCALA)
=====================================
val joinDF = peopleDF.join(dqDF, peopleDF("id") === dqDF("dq_id"))

val df_project = df_Project_Info_OP.join(df_Customer_OP, Seq("PROJECT_ID"), "left_outer").drop(df_Customer_OP("project_id"))
.join( df_Resource_OP, Seq("PROJECT_ID"), "left_outer").drop(df_Resource_OP("project_id")).select(
df_Project_Info_OP("PROJECT_ID").cast(LongType).alias("SK_AS_PROJECT_ID"),

final_incr_result.write.format("parquet").options(option).mode(SaveMode.Overwrite).saveAsTable(s"${adv_work_db}.W_PROJECT_TASKS_INCREMENT")

   


CREATING DATAFRAMES:
*****************************
1. USE DATAFRAME API

val df = sc.read.format("csv").option("header","true").option("infraSchema","true").load("path")
df.createOrReplaceTempView("tablename")
spark.sql(""" select * from tablename """).show()

val df = sqlContext.read.parquet("src/main/resources/peopleTwo.parquet")

2.  CREATE DATAFRAME IN PROGRAMATICALLY SPECIFYING SCHEMA

1. Import the necessary libraries

import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spark.implicits._

2. Create an RDD

val peopleRDD = spark.sparkContext.textFile("/tmp/people.txt")

3. Encode the Schema in a string

val schemaString = "name age"

4. Generate the schema based on the string of schema

val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))

val schema = StructType(fields)

5. Convert records of the RDD (people) to Rows

val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))

6. Apply the schema to the RDD

val peopleDF = spark.createDataFrame(rowRDD, schema)

6. Creates a temporary view using the DataFrame

peopleDF.createOrReplaceTempView("people")

7. SQL can be run over a temporary view created using DataFrames

val results = spark.sql("SELECT name FROM people")

8.The results of SQL queries are DataFrames and support all the normal RDD operations. The columns of a row in the result can be accessed by field index or by field name

results.map(attributes => "Name: " + attributes(0)).show()

This will produce an output similar to the following:

...
+-------------+
|        value|
+-------------+
|Name: Michael|
|   Name: Andy|
| Name: Justin|
+-------------+

3.  CASE CLASSES



SPARK CBO
==
spark.sql.cbo.joinReorder.enabled
spark.sql.cbo.enabled

ANALYZE TABLE  <tablename> COMPUTE STATISTICS
ANALYZE TABLE  <tablename> PARTITION (p1, p2) COMPUTE STATISTICS
ANALYZE TABLE <tableName> COMPUTE STATISTICS FOR COLUMNS columnNames

use DESCRIBE FORMATTED [db_name.]table_name  [PARTITION (partition_spec)].

DESCRIBE extended TABLE PARTITION (PARTITION_COL=20190305)  (or )
DESCRIBE formatted TABLE PARTITION (PARTITION_COL=20190305) 

  
  
Spark dataframe API
*******************
val bookings = spark.sql(""" select * from advance_services_db.as_bookings_executive_dashboard  """)

val arch = bookings.filter("trim(upper(ato_arch_name)) = 'COLLABORATION' ")    

val gr = arch.groupBy("ato_arch_name").agg(sum("total_bookings")) -- single column in groupBy

val gr = arch.groupBy("ato_arch_name", "BK_AS_QUOTE_NUM").agg(sum("total_bookings"), max("total_bookings"),min("total_bookings") ) -- two column in groupBy

val gr = arch.groupBy("ato_arch_name", "BK_AS_QUOTE_NUM").agg(sum("total_bookings").alias("TOT_BKGS"), max("total_bookings").alias("MAX_BKG"),min("total_bookings").alias("MIN_BKG") ) -- 2 cols with aliases

gr.show()

gr.withColumn("id",monotonicallyIncreasingId).show(false) -- with rownum as id


to change datatype : val df2 = df.withColumn("yearTmp", df.year.cast(IntegerType))
.drop("year")
.withColumnRenamed("yearTmp", "year")


Window functions examples
==========================
import org.apache.spark.sql.expressions.Window

val df = spark.sql(" select * from dv_bdfrawzph_nogbd_r000_wh.CLM_ACES_INSTAL_AUDIT_TRAIL ")
val df1= df.cache()
val partitionWindow = Window.partitionBy($"CLAIM_NBR",$"CLAIM_SQNC_NBR").orderBy($"LOAD_DTM".desc)
val ranking = rank().over(partitionWindow)
val rownumber = row_number().over(partitionWindow)
df1.select("CLAIM_NBR","CLAIM_SQNC_NBR","LOAD_DTM").withColumn("rankingNum",ranking).withColumn("row_number",rownumber).show(100,false)


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Create Spark Session
val sparkSession = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
import sparkSession.implicits._

// Create Sample Dataframe
val empDF = sparkSession.createDataFrame(Seq(
(7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
(7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
(7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
(7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
(7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
(7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
(7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
(7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
(7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
(7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
(7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
)).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")


First of all we will need to define the window we will be working on i.e. we will partition by department (deptno) and order by salary (sal). 
Below is the code to do it via Spark Dataframe API.

val partitionWindow = Window.partitionBy($"deptno").orderBy($"sal".desc)

Rank salary within each department
-------------------------------------------
//SQL 
SELECT empno,deptno,sal,RANK() OVER (partition by deptno ORDER BY sal desc) as rank FROM emp;

//DF API 
val rankTest = rank().over(partitionWindow)
empDF.select($"*", rankTest as "rank").show


RUN UNIX Commands in Scala
=========================
scala> import sys.process._
import sys.process._

scala> "ls".!
300}
bdf_bdfz_etl
hive
input1.txt
input2.txt
inputfile.txt
logs
lookup_test.awk
process_tail_records.sh
shell_test.sh
workspace
res2: Int = 0

scala> "pwd".!
/home/ag22157
res3: Int = 0

scala>

! returns exit code of the command after executing

!! returns the output of the command after executing  (here we can store data into variables)



spark-shell --help
**********************
Usage: ./bin/spark-shell [options]

Options:
--master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
--deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
						  on one of the worker machines inside the cluster ("cluster")
						  (Default: client).
--class CLASS_NAME          Your application's main class (for Java / Scala apps).
--name NAME                 A name of your application.
--jars JARS                 Comma-separated list of local jars to include on the driver
						  and executor classpaths.
--packages                  Comma-separated list of maven coordinates of jars to include
						  on the driver and executor classpaths. Will search the local
						  maven repo, then maven central and any additional remote
						  repositories given by --repositories. The format for the
						  coordinates should be groupId:artifactId:version.
--exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
						  resolving the dependencies provided in --packages to avoid
						  dependency conflicts.
--repositories              Comma-separated list of additional remote repositories to
						  search for the maven coordinates given with --packages.
--py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
						  on the PYTHONPATH for Python apps.
--files FILES               Comma-separated list of files to be placed in the working
						  directory of each executor.

--conf PROP=VALUE           Arbitrary Spark configuration property.
--properties-file FILE      Path to a file from which to load extra properties. If not
						  specified, this will look for conf/spark-defaults.conf.

--driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
--driver-java-options       Extra Java options to pass to the driver.
--driver-library-path       Extra library path entries to pass to the driver.
--driver-class-path         Extra class path entries to pass to the driver. Note that
						  jars added with --jars are automatically included in the
						  classpath.

--executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).

--proxy-user NAME           User to impersonate when submitting the application.

--help, -h                  Show this help message and exit
--verbose, -v               Print additional debug output
--version,                  Print the version of current Spark

Spark standalone with cluster deploy mode only:
--driver-cores NUM          Cores for driver (Default: 1).

Spark standalone or Mesos with cluster deploy mode only:
--supervise                 If given, restarts the driver on failure.
--kill SUBMISSION_ID        If given, kills the driver specified.
--status SUBMISSION_ID      If given, requests the status of the driver specified.

Spark standalone and Mesos only:
--total-executor-cores NUM  Total cores for all executors.

Spark standalone and YARN only:
--executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
						  or all available cores on the worker in standalone mode)

YARN-only:
--driver-cores NUM          Number of cores used by the driver, only in cluster mode
						  (Default: 1).
--queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
--num-executors NUM         Number of executors to launch (Default: 2).
--archives ARCHIVES         Comma separated list of archives to be extracted into the
						  working directory of each executor.
--principal PRINCIPAL       Principal to be used to login to KDC, while running on
						  secure HDFS.
--keytab KEYTAB             The full path to the file that contains the keytab for the
						  principal specified above. This keytab will be copied to
						  the node running the Application Master via the Secure
						  Distributed Cache, for renewing the login tickets and the
						  delegation tokens periodically.


System.exit(0) --> to comeout from scala shell


SPARK SQL QUERIES WITH UNIX VARIABLES
********************************************
export p="select FD827905_IMG_CD from dv_bdfrawz_r000_wh_nohaphi_xm.clm_aces_rd050015 limit 10"

spark2-shell << EOF
import sys.process._
spark.sql("${p}").show(100, false)
:quit
EOF




PYTHON:
***********
val df = spark.read.format("csv").option("header","true").option("infraSchema","true").load("PATH") -- for csv file
val df = spark.read.parquet("PATH") -- for parquet
	
spark-submit --master yarn --jars example.jar --conf spark.executor.instances=10 --name example_job example.py arg1 arg2

NOTE: getDynamicAllocationInitialExecutors first makes sure that spark.dynamicAllocation.initialExecutors is equal or greater than spark.dynamicAllocation.minExecutors.

new_file_hbase =("/dv/hdfsdata/ve2/bdf/rawz/phi/no_gbd/r000/qltycntrl/stage/hbase_details.txt")
hbase_data_df = spark.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load(new_file_hbase)
hbase_data_df.printSchema()

new_file_llk=("/dv/hdfsdata/ve2/bdf/rawz/phi/no_gbd/r000/qltycntrl/stage/missingLLK.out")
LLK_fields = [StructField("Table_name", StringType(), True),StructField("LLK", StringType(), True),StructField("Date_time", StringType(), True)]
LLK_fields_schema = StructType(LLK_fields)
missing_llk_df=spark.read.format('com.databricks.spark.csv').options(header='false', sep=',', inferschema='true').schema(LLK_fields_schema).load(new_file_llk)

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("hive.exec.dynamic.partition","true")
spark.conf.set("hive.exec.max.dynamic.partitions","1000")
spark.conf.set("hive.exec.max.dynamic.partitions.pernode","1000")  

