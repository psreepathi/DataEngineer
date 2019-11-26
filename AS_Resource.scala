package com.cisco.services.asbi.financial

/**
 * @author Chandra Sekhar Marisetti (cmariset@cisco.com).
 */
//spark-submit --master yarn  --driver-memory 5G --executor-memory 7G --executor-cores 16 --class com.cisco.asbi.services.projectprofitability.As_Project_Budget mt_asbi_sc.jar /app/ASBI/param/asbi.properties "AS Approved Cost" "cost" 

//import statements

import org.apache.spark.sql.functions.{ coalesce, lit }
import org.apache.log4j.LogManager
import sys.process._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.expressions._
import com.cisco.services.asbi.common.SparkUtil
import java.util.Properties
import com.cisco.services.asbi.common.LoadProperties
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.commons.beanutils.converters.DateConverter
import sun.java2d.pipe.SpanShapeRenderer.Simple
import java.util.Calendar
import java.text.SimpleDateFormat

/**
 *  This is the main object for the AS_Resource_TV.
 *  This program enables the dimension information of AS resources
 */

object AS_Resource {

  val spark: SparkUtil = new SparkUtil()
  val hql: SparkSession = spark.getSparkSession("AS_Resource")

  var advservice_pa: String = null
  var advservice_hr: String = null
  var advservice_xxcas_o: String = null
  var advservice_app: String = null
  var advservice_applsys: String = null
  var advservice_jtf: String = null
  var adv_service_db: String = null
  var adv_work_db: String = null
  var reference_tddev: String = null
  var reference_tddev3: String = null
  var advservice_osd: String = null
  var as_proj_parent_hive_f: String = null
  var x="success"
  var tableSep: String = null
  //Setting dynamic partitions to hiveContext
  hql.conf.set("hive.exec.dynamic.partition", "true")
  hql.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

  /* Declaring a common class */

  /**
   * This is the main method for the application
   * @param args the arguments to the application.
   */
  def main(args: Array[String]): Unit = {
    val propLocation = args(0)
    val propLoc = s"maprfs://${propLocation}"
    val lprops: LoadProperties = new LoadProperties()
    val props: Properties = lprops.load(hql, propLoc)

    //schema properties
    advservice_pa = props.getProperty("as_db_pa")
    advservice_hr = props.getProperty("as_db_hr")
    advservice_xxcas_o = props.getProperty("as_db_xxcas")
    advservice_app = props.getProperty("as_db_apps")
    advservice_applsys = props.getProperty("as_db_applsys")
    advservice_jtf = props.getProperty("as_db_jtf")
    adv_service_db = props.getProperty("asbi_service_db")
    adv_work_db = props.getProperty("asbi_work_db")
    reference_tddev = props.getProperty("reference_datalake_db")
    reference_tddev3 = props.getProperty("reference_datalake_db3")
    as_proj_parent_hive_f = props.getProperty("as_proj_parent_hive_f")

    println("**************************")
    println("AS Resource TV Population Started")
    try {
      val t1 = System.currentTimeMillis()

      //******************************************** getting required tables from hive to spark *******************************************************

      var as_Query = s"""select
                           cast(person_id as bigint) as person_id
                          ,cast(person_type_id as int) as person_type_id
                          ,cast(employee_number as bigint) as employee_number
                          ,cast(effective_start_date as date)
                          ,cast(effective_end_date as date)
                          ,creation_date
                          from $advservice_hr.per_all_people_f"""

      val getppf = runQuery(as_Query)

      /* get the CISCO valid organizations */

      as_Query = s"""select
                        distinct
                        cast(ou.organization_id as bigint) as organization_id
                        from $advservice_hr.hr_all_organization_units  ou
                        inner join $advservice_hr.hr_organization_information oi  
                        on ou.organization_id = oi.organization_id
                        and oi.org_information1 = 'PA_EXPENDITURE_ORG'"""

      val getCiscoOrganizations = runQuery(as_Query)
	  
	  println("getpaf  Population Started")

      as_Query = s"""select
                           cast(paf.person_id as bigint) as person_id
                          ,cast(paf.assignment_id as bigint) as assignment_id
                          ,paf.primary_flag
                          ,cast(paf.effective_start_date as date)
                          ,cast(paf.effective_end_date as date)
                          ,paf.employment_category
                          ,cast(paf.organization_id as bigint) as organization_id 
                          ,paf.normal_hours
                          ,paf.job_id
						              ,paf.last_update_date as last_update_date
                          from $advservice_hr.per_all_assignments_f paf"""

      val getpaf = runQuery(as_Query)
	  
	  println("pv_cisco_worker_party population started")

      as_Query = s"""select distinct cisco_worker_party_key, bk_employee_id as employee_number from $reference_tddev3.pv_cisco_worker_party"""

      val getcwp = runQuery(as_Query)

//code changes for new pafmax logic		  
val getpafmax_intermediate = getpaf.join(getppf, Seq("person_id"), "inner").
join(getCiscoOrganizations, Seq("organization_id"), "inner"). //checking with Organization
join(getcwp, Seq("employee_number"), "inner").
filter((getppf("person_type_id") === 6) && (getpaf("effective_start_date").between(getppf("effective_start_date"), getppf("effective_end_date")))).select(getcwp("cisco_worker_party_key").alias("cisco_worker_party_key"), getpaf("person_id").alias("person_id"),getpaf("assignment_id").alias("assignment_id"),getpaf("effective_start_date").alias("effective_start_date"),getpaf("effective_end_date").alias("effective_end_date"),
getpaf("last_update_date").alias("last_update_date"))

 
 val getpafmax=getpafmax_intermediate.withColumn("row_num",row_number().over(Window.partitionBy(col("person_id")).
 orderBy(col("effective_end_date").desc,col("last_update_date").desc))).filter(col("row_num") === 1)

      //******************************** Populating latest data from the target table  **********************************************

      as_Query = s"""select * from advance_services_db.AS_RESOURCE_TV"""   

      val gettgttbldatatmp = runQuery(as_Query)

      val gettgttbldata = gettgttbldatatmp.withColumn("row_num", row_number().over(Window.partitionBy("as_rsrc_cisco_wrkr_prty_key").
        orderBy(col("end_tv_dt").desc))).filter(col("row_num") === 1)

      println("\n Tables reading completed")

      // *********************** discarding the existing profiles & identifying Active & In-Active Profiles which are not in Target table (CDC) **************************

      val getresourceProfiles = getResourceProfilesCDC(getppf, getpaf, getpafmax, gettgttbldata)

      println("\n Incremental Resource identified")

      //********************** Processing all attributes for the incremental data ***********************************

      val getCdcASResourcetmp1 = (getresourceProfiles.join(getOrgUnits(), Seq("organization_id"), "inner"). //organizations
        join(getResponsibleDetails(getresourceProfiles), Seq("person_id"), "left"). //Respname
        join(getNormalHrs(), Seq("organization_id"), "left"). //NormalHrs
        join(getPerJobs(), Seq("job_id"), "left"). //jobid
        join(getMsngTimCrdNtfcsnFlg(), Seq("person_id"), "left"). //Missing Timecard Notification Flag
        select(
          getresourceProfiles("cisco_worker_party_key"),
          when((col("hours").isNull) || (getresourceProfiles("employment_category").isin("PT", "PR")), getresourceProfiles("normal_hours")).
            otherwise(col("hours")).alias("as_rsrc_hours_per_week_qty"),
          getresourceProfiles("effective_start_date"),
          getresourceProfiles("effective_end_date"),
          coalesce(col("responsibility_name"), lit("NO OTL RESPONSIBILITY")).alias("timecard_responsbility_cd"),
          coalesce(getresourceProfiles("job_id"), lit(-999)).cast(IntegerType).alias("bk_as_hr_job_id_int"),
          coalesce(col("mssng_timecard_src_ntfctn_flg"), lit("N")).alias("mssng_timecard_src_ntfctn_flg"),
          getresourceProfiles("as_rsrc_cisco_wrkr_prty_key"),
          getresourceProfiles("as_rsrc_hours_per_week_qty_tgt"),
          getresourceProfiles("bk_as_hr_job_id_int_tgt"),
          getresourceProfiles("mssng_timecard_src_ntfctn_flg_tgt"),
          getresourceProfiles("timecard_responsbility_cd_tgt"),
          getresourceProfiles("start_tv_dt"),
          getresourceProfiles("end_tv_dt"),
          getresourceProfiles("hdp_create_dtm"),
          getresourceProfiles("hdp_update_dtm")))

      //***************** Populating start_tv_dt for the incremental data *******************************//

      val getCdcASResourcetmp2 = getCdcASResourcetmp1.select(
        col("cisco_worker_party_key"),
        col("as_rsrc_hours_per_week_qty"),
        col("bk_as_hr_job_id_int"),
        col("timecard_responsbility_cd"),
        col("mssng_timecard_src_ntfctn_flg"),
        when(col("as_rsrc_cisco_wrkr_prty_key").isNull, col("effective_start_date")).
          otherwise(
            when(col("effective_end_date") === col("end_tv_dt") &&
              col("bk_as_hr_job_id_int") === col("bk_as_hr_job_id_int_tgt") &&
              col("as_rsrc_hours_per_week_qty") === col("as_rsrc_hours_per_week_qty_tgt") &&
              col("mssng_timecard_src_ntfctn_flg") === col("mssng_timecard_src_ntfctn_flg_tgt") &&
              col("timecard_responsbility_cd") === col("timecard_responsbility_cd_tgt"), col("start_tv_dt")).otherwise(
              when(
                ((col("effective_end_date") === col("end_tv_dt"))&& (col("end_tv_dt")===("3500-01-01"))) &&//filter end dated profile
                  (col("bk_as_hr_job_id_int").!==(col("bk_as_hr_job_id_int_tgt")) ||
                    col("as_rsrc_hours_per_week_qty").!==(col("as_rsrc_hours_per_week_qty_tgt")) ||
                    col("mssng_timecard_src_ntfctn_flg").!==(col("mssng_timecard_src_ntfctn_flg_tgt")) ||
                    col("timecard_responsbility_cd").!==(col("timecard_responsbility_cd_tgt"))), current_date()).otherwise(
                  when(
                    (col("effective_end_date").!==(col("end_tv_dt")))
                      && ((col("end_tv_dt")).!==("3500-01-01")), col("effective_start_date")).
                    otherwise(col("start_tv_dt"))))).alias("final_start_tv_dt"),
        col("effective_end_date").alias("final_end_tv_dt"),
        col("as_rsrc_cisco_wrkr_prty_key"),
        col("as_rsrc_hours_per_week_qty_tgt"),
        col("bk_as_hr_job_id_int_tgt"),
        col("mssng_timecard_src_ntfctn_flg_tgt"),
        col("timecard_responsbility_cd_tgt"),
        col("start_tv_dt"),
        col("end_tv_dt"),
        col("hdp_create_dtm"),
        col("hdp_update_dtm"))

      //******************************* Merging the incremental and Corresponding target table data and populating END_TV_DT **************************************************                                                                 

      val getMergeResourceProfileData = getCdcASResourcetmp2. //CDC
        filter(
          (coalesce(col("start_tv_dt"), lit("9999-12-31")).!==(col("final_start_tv_dt")))
            .||(
              (coalesce(col("start_tv_dt"), lit("9999-12-31")).!==(col("final_start_tv_dt")))
                && (coalesce(col("end_tv_dt"), lit("9999-12-31")).!==(col("final_end_tv_dt")))
                && (col("end_tv_dt").===("3500-01-01")))). //filter to discard  the existing "AS" records from CDC
          select(
            col("cisco_worker_party_key").alias("as_rsrc_cisco_wrkr_prty_key"),
            col("as_rsrc_hours_per_week_qty"),
            col("bk_as_hr_job_id_int"),
            col("timecard_responsbility_cd"),
            col("mssng_timecard_src_ntfctn_flg"),
            col("final_start_tv_dt").alias("start_tv_dt"),
            col("final_end_tv_dt").alias("end_tv_dt"),
            when(
              (col("final_end_tv_dt").===("3500-01-01")), current_timestamp()).
              otherwise(
                coalesce(col("hdp_create_dtm"), current_timestamp())).alias("hdp_create_dtm"),
               current_timestamp().alias("hdp_update_dtm")).union(getCdcASResourcetmp2. //Existing data in TgtTable
              filter(
                (col("as_rsrc_cisco_wrkr_prty_key").isNotNull)
                  && ((coalesce(col("start_tv_dt"), lit("9999-12-31")) === (col("final_start_tv_dt")))
                    .||((col("end_tv_dt")).===("3500-01-01")))).
                select(
                  col("as_rsrc_cisco_wrkr_prty_key"),
                  col("as_rsrc_hours_per_week_qty_tgt").alias("as_rsrc_hours_per_week_qty"),
                  col("bk_as_hr_job_id_int_tgt").alias("bk_as_hr_job_id_int"),
                  col("timecard_responsbility_cd_tgt").alias("timecard_responsbility_cd"),
                  col("mssng_timecard_src_ntfctn_flg_tgt").alias("mssng_timecard_src_ntfctn_flg"),
                  col("start_tv_dt"),
                  when(
                    col("start_tv_dt") === (col("final_start_tv_dt")), col("final_end_tv_dt")).
                    otherwise(date_sub(col("final_start_tv_dt"), 1)).alias("end_tv_dt"), // Validates if any changes in the start date between CDC & Target table data.
                  col("hdp_create_dtm"),

                  when(col("start_tv_dt") === (col("final_start_tv_dt"))
                    && (col("end_tv_dt") === (col("final_end_tv_dt"))), col("hdp_update_dtm")).
                    otherwise(current_timestamp()).alias("hdp_update_dtm")))

      //*********************************** Writing Incremental data into WI_AS_RESOURCE file *********************************************

      var tbl_Path = s"${as_proj_parent_hive_f}/WI_AS_RESOURCE"
      var option = Map("path" -> tbl_Path)
      getMergeResourceProfileData.write.format("parquet").options(option).mode(SaveMode.Overwrite).saveAsTable(s"$adv_work_db.wi_as_resource")

      val t2 = System.currentTimeMillis()

      println("\n Records inserted in WI_AS_RESOURCE in " + ((t2 - t1) / (1000 * 60)) + " mins")

      //************************************ Final Load********************************************

val getFinalRsrc_tv_intermediate = (gettgttbldatatmp.filter(col("end_tv_dt").!==("3500-01-01")).
select(
gettgttbldatatmp("as_rsrc_cisco_wrkr_prty_key"), gettgttbldatatmp("as_rsrc_hours_per_week_qty"), 
gettgttbldatatmp("bk_as_hr_job_id_int"), gettgttbldatatmp("timecard_responsbility_cd"), 
gettgttbldatatmp("mssng_timecard_src_ntfctn_flg"), gettgttbldatatmp("start_tv_dt"), 
gettgttbldatatmp("end_tv_dt"), gettgttbldatatmp("hdp_create_dtm"), 
gettgttbldatatmp("hdp_update_dtm")).unionAll(getMergeResourceProfileData)).distinct()
//filter to remove duplicates from the final load
	val getFinalRsrc_tv = getFinalRsrc_tv_intermediate.withColumn("row_num", row_number().over(Window.partitionBy(col("as_rsrc_cisco_wrkr_prty_key"),col("as_rsrc_hours_per_week_qty"),col("bk_as_hr_job_id_int"),
col("timecard_responsbility_cd"),col("mssng_timecard_src_ntfctn_flg"),col("start_tv_dt"),col("end_tv_dt")).
orderBy(col("hdp_update_dtm").desc))).filter(col("row_num") === 1).select(
col("as_rsrc_cisco_wrkr_prty_key"),
col("as_rsrc_hours_per_week_qty"),
col("bk_as_hr_job_id_int"),
col("mssng_timecard_src_ntfctn_flg"),
col("timecard_responsbility_cd"),
col("start_tv_dt"),
col("end_tv_dt"),
col("hdp_create_dtm"),
col("hdp_update_dtm"))

      tbl_Path = s"/app/ASBI/Enterprise_Data/Services/Advanced_Services/W_AS_RESOURCE"
      option = Map("path" -> tbl_Path)
      getFinalRsrc_tv.write.format("parquet").options(option).mode(SaveMode.Overwrite).saveAsTable(s"${adv_work_db}.W_AS_RESOURCE")

      val tgttblload = hql.sql(s"""insert overwrite table $adv_service_db.AS_RESOURCE_TV
    SELECT 
        as_rsrc_cisco_wrkr_prty_key
        ,as_rsrc_hours_per_week_qty
        ,bk_as_hr_job_id_int
        ,mssng_timecard_src_ntfctn_flg
        ,timecard_responsbility_cd
        ,start_tv_dt
        ,end_tv_dt
        ,hdp_create_dtm
        ,hdp_update_dtm
    FROM $adv_work_db.w_as_resource""")

      
      val t4 = System.currentTimeMillis()
      println("\n AS Resource TV & AS Resource data refreshed in " + ((t4 - t1) / (1000 * 60)) + " mins")
      println("\n Program Ended Successfully")

     
    } catch { case e: Throwable => println(e.getMessage())
      x="failed"
      }
    finally {
     println("INFO: stopping sparkcontext")
        hql.stop()
        println("************************")
       if(x=="failed"){
println("ERROR:Failed exiting with status code 1.")
         System.exit(1)}
    }

  } //End of main 

  // This method populates the latest active & inactive resources end_dates
  def getResourceProfilesCDC(
    getppf: DataFrame,
    getpaf: DataFrame,
    getpafmax: DataFrame,
    gettgttbldata: DataFrame): DataFrame =
    {
      return (getppf.join(getpafmax, Seq("person_id"), "inner").
        join(getpaf, (getpafmax("assignment_id") === getpaf("assignment_id"))
&& (getpafmax("effective_start_date") === getpaf("effective_start_date"))
&& (getpafmax("effective_end_date") === getpaf("effective_end_date")), "inner").
        filter((current_date().between(getppf("effective_start_date"), getppf("effective_end_date")))).
        select(
getppf("person_id"),
getpafmax("cisco_worker_party_key"),
getppf("employee_number"),
getpafmax("assignment_id"),
getpaf("employment_category"),
getpaf("job_id").cast(IntegerType),
getpaf("organization_id").cast(IntegerType),
getpaf("normal_hours"),
getppf("person_type_id"),
getpafmax("effective_start_date"),
when(
  (getppf("person_type_id") === 9) && (getpafmax("effective_end_date") === "4712-12-31"), date_sub(getppf("effective_start_date"), 1)).
  otherwise(
    when(
      (getppf("person_type_id") === 6)
        && ((getpafmax("effective_end_date") === "4712-12-31") || getpafmax("effective_end_date") > current_date()), lit("3500-01-01")).
      otherwise(getpafmax("effective_end_date"))).cast(DateType).alias("effective_end_date"))).
join(gettgttbldata,
  (col("cisco_worker_party_key") === gettgttbldata("as_rsrc_cisco_wrkr_prty_key")), "left").
  select(
    col("person_id"),
    col("cisco_worker_party_key"),
    col("assignment_id"),
    col("employment_category"),
    col("job_id"),
    col("organization_id"),
    col("normal_hours"),
    col("effective_start_date"),
    col("effective_end_date"),
    gettgttbldata("as_rsrc_cisco_wrkr_prty_key"),
    gettgttbldata("as_rsrc_hours_per_week_qty").alias("as_rsrc_hours_per_week_qty_tgt"),
    gettgttbldata("bk_as_hr_job_id_int").alias("bk_as_hr_job_id_int_tgt"),
    gettgttbldata("mssng_timecard_src_ntfctn_flg").alias("mssng_timecard_src_ntfctn_flg_tgt"),
    gettgttbldata("timecard_responsbility_cd").alias("timecard_responsbility_cd_tgt"),
    gettgttbldata("start_tv_dt"),
    gettgttbldata("end_tv_dt"),
    coalesce(gettgttbldata("hdp_create_dtm"), current_timestamp()).alias("hdp_create_dtm"),
    coalesce(gettgttbldata("hdp_create_dtm"), current_timestamp()).alias("hdp_update_dtm"))
    }

  //Method for identifying the Responsibility details for each profile

  def getResponsibleDetails(getresourceProfiles: DataFrame): DataFrame =
    {
      var as_Query = s"""select distinct employee_id,user_id,last_update_date from $advservice_applsys.fnd_user where employee_id is not null"""

      val getFndUsertmp = runQuery(as_Query)

      val getFndUser = getFndUsertmp.join(getresourceProfiles, (getresourceProfiles("person_id") === getFndUsertmp("employee_id"))).
        select(getresourceProfiles("person_id"),
getFndUsertmp("user_id"),
getFndUsertmp("last_update_date").cast(TimestampType))

as_Query = s"""select distinct user_id,a.responsibility_id from advservices_csfprd_srvc_oth_apps.fnd_user_resp_groups a,advservices_csfprd_srvc_oth_applsys.fnd_responsibility_tl b
where a.responsibility_id=b.responsibility_id and exists ( select 'X' from advservices_csfprd_srvc_oth_applsys.FND_FLEX_VALUES FFV, advservices_csfprd_srvc_oth_applsys.FND_FLEX_VALUE_SETS FFVS where FFV.FLEX_VALUE_SET_ID = FFVS.FLEX_VALUE_SET_ID
AND FFVS.FLEX_VALUE_SET_NAME = 'XXCAS_PRJ_OTL_RESPONSIBILITIES'
AND COALESCE (FFV.ENABLED_FLAG, 'N') = 'Y'
AND FFV.FLEX_VALUE = b.RESPONSIBILITY_NAME)"""

      val getFndUserrespGrps = runQuery(as_Query)

       as_Query = s"""select distinct responsibility_id,responsibility_name,cast(last_update_date as date) as last_update_date from $advservice_applsys.fnd_responsibility_tl"""
      val getFndresp = runQuery(as_Query)

      return getFndUser.join(getFndUserrespGrps, Seq("user_id"), "inner").
        join(getFndresp, Seq("responsibility_id"), "inner").
        withColumn("row_num", row_number().over(Window.partitionBy("person_id").
orderBy(getFndUser("last_update_date").desc, getFndresp("last_update_date").desc, getFndresp("responsibility_name").desc))).
        filter(col("row_num") === 1).
        select(
col("person_id"),
coalesce(col("responsibility_name"), lit("NO OTL RESPONSIBILITY")).alias("responsibility_name"))
    }

  // Method for identifying RESOURCE_MISSING_TIMECARD_NOTIFICATION_FLAG  

  def getMsngTimCrdNtfcsnFlg(): DataFrame =
    {

   var as_Query = s"""select
                            cast(ffv.ATTRIBUTE1 as bigint) as person_id,
                            COALESCE (ffv.enabled_flag, 'N') as mssng_timecard_src_ntfctn_flg,
                            ffv.creation_date
                            from
                              $advservice_applsys.fnd_flex_values ffv
                              inner join 
                            $advservice_applsys.fnd_flex_value_sets ffvs
                            on ffv.flex_value_set_id = ffvs.flex_value_set_id
                            where ffvs.flex_value_set_name = 'XXCAS_PRJ_MISSING_TC_EXCEPTION'
                            AND COALESCE (ffv.enabled_flag, 'N') = 'Y'
                            and (current_timestamp between ffv.start_date_active and coalesce(ffv.end_date_active,'3500-01-01 00:00:00'))"""

      val getmissingtimcrdntfcsn = runQuery(as_Query)

      return getmissingtimcrdntfcsn.
        withColumn("row_num", row_number().over(Window.partitionBy("person_id").
orderBy(col("creation_date").desc))).
        filter(col("row_num") === 1).drop(col("row_num"))
    }

  def getPerJobs(): DataFrame =
    {

      var as_Query = s"""select 
     JOB_ID,
     coalesce(DATE_TO,date '4700-01-01') AS DATE_TO 
     from $advservice_hr.PER_JOBS
     where coalesce(DATE_TO,date '4700-01-01') > CURRENT_DATE"""

      return runQuery(as_Query)
    }
  //************************** Missing hours on organization level ********************************
  def getNormalHrs(): DataFrame =
    {

      var as_Query = s"""select
    cast(organization_id as bigint) as organization_id
    , cast(org_information1 as int) as calendar_id
         
    from $advservice_app.xxcas_prj_hr_org_information_v
    where org_information_context ='Exp Organization Defaults'"""

      val getHrOrgInfo = runQuery(as_Query)

      as_Query = s"""select
     cast(calendar_id as int) as calendar_id,
     calendar_name
     from $advservice_app.xxcas_prj_jtf_calendars_vl
     where end_date_active is null"""

      val getJtfCalVl = runQuery(as_Query)

      as_Query = s"""select tl.calendar_name,cast(tl.calendar_id as int) as calendar_id,cast(asgn.shift_id as bigint) as shift_id 
 from $advservice_jtf.jtf_calendars_tl tl
 inner join $advservice_jtf.jtf_cal_shift_assign asgn
 on asgn.calendar_id = tl.calendar_id"""

      val getJtfCalTl = runQuery(as_Query)

      as_Query = s"""select 
    shift_id
  ,sum(cast(cast((cast((datediff(end_time,begin_time) * 24 * 60 * 60) + 
  (
    (hour(end_time) * 60 * 60) + (minute(end_time) * 60) + second(end_time) - 
    (hour(begin_time) * 60 * 60) + (minute(begin_time) * 60) + second(begin_time)
   ) as double) / cast((60 * 60) as double )) as string) as decimal(4,2))) as hours
   
    from $advservice_jtf.jtf_cal_shift_constructs
    group by shift_id"""

      val getJtfCalShiftConstrcts = runQuery(as_Query)

      return getHrOrgInfo.join(getJtfCalVl, Seq("calendar_id"), "inner").
        join(getJtfCalTl, Seq("calendar_name"), "inner").
        join(getJtfCalShiftConstrcts, Seq("shift_id"), "inner").
        select(
getHrOrgInfo("organization_id"),
getJtfCalVl("calendar_name"),
getJtfCalShiftConstrcts("hours"))
    }

  def getOrgUnits(): DataFrame = {
    val as_Query = s"""SELECT DISTINCT cast(ORGANIZATION_ID as bigint) as ORGANIZATION_ID 
     FROM $advservice_hr.HR_ALL_ORGANIZATION_UNITS"""

    return runQuery(as_Query)
  }

  def runQuery(query: String): DataFrame = {
    return hql.sql(query)
  }

} //End of the Objects