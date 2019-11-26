#!/bin/sh


## Loading childsku_executive_dashboard_fnl table using spark.

spark-shell -i --master yarn --deploy-mode client --files /opt/mapr/spark/spark-2.1.0/conf/hive-site.xml --conf spark.local.dir=/opt/mapr/tmp/ --conf spark.shuffle.spill=true --conf spark.shuffle.service.enabled=true --conf spark.yarn.executor.memoryOverhead=2560 --executor-memory 13g --driver-memory 10g --conf spark.driver.maxResultSize=8g --num-executors 60 --executor-cores 4 /users/hdpasbi/bookings_extraction_data/childsku_executive_dashboard_fnl.scala

## Counts of as_bookings_executive_dashboard and childsku_executive_dashboard_fnl Tables.

bookings_count=$(hive -S -e "set tez.task.resource.memory.mb=13000;set hive.tez.container.size=15000;set hive.tez.java.opts=-Xmx12000m; select count(*) from ADVANCE_SERVICES_DB.AS_BOOKINGS_EXECUTIVE_DASHBOARD")

childsku_count=$(hive -S -e "set tez.task.resource.memory.mb=13000;set hive.tez.container.size=15000;set hive.tez.java.opts=-Xmx12000m; select count(*) from advance_services_bkp_db.CHILDSKU_EXECUTIVE_DASHBOARD_FNL")


## Bookings extraction

hive -f /users/hdpasbi/bookings_extraction_data/bookings_dashboard_extraction.hql | sed 's/[\t]//g' > /users/hdpasbi/bookings_extraction_data/Bookings_extract_hdp_data_FNL.csv

## Childsku extraction

hive -f /users/hdpasbi/bookings_extraction_data/Childsku_dashborad_extraction.hql | sed 's/[\t]//g' > /users/hdpasbi/bookings_extraction_data/Childsku_dashboard_extraction.csv 

cd /users/hdpasbi/bookings_extraction_data

bookings_extract_count=$(wc -l /users/hdpasbi/bookings_extraction_data/Bookings_extract_hdp_data_FNL.csv | awk '{print $1}')
childsku_extract_count=$(wc -l /users/hdpasbi/bookings_extraction_data/Childsku_dashboard_extraction.csv | awk '{print $1}')


## FTP

HOST='css-operations'
USER='ubuntu'
PASSWD='security1'
BOOKINGS_FILE='Bookings_extract_hdp_data_FNL.csv'
CHILDSKU_FILE='Childsku_dashboard_extraction.csv' 


ftp -n $HOST <<END_SCRIPT
quote USER $USER
quote PASS $PASSWD
cd /storage/Bookings_Rev_Hadoop_IT
passive n
put $BOOKINGS_FILE
put $CHILDSKU_FILE
quit
END_SCRIPT


## Sending e-mail..

echo "Hi All, Bookings and Childsku extraction is completed. Table counts: Bookings-$bookings_count, Childsku-$childsku_count and Extraction files count: Bookings-$bookings_extract_count, Childsku-$childsku_extract_count   Thank you, hdpasbi" | mailx -r "no-reply@cisco.com" -s "Bookings and Childsku extraction" "abondre@cisco.com,mumazhar@cisco.com,abmirajk@cisco.com,gkarumur@cisco.com,rpokadal@cisco.com,srednam@cisco.com,lbeeneed@cisco.com,spullase@cisco.com"
