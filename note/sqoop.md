```shell

sqoop import 
--connect jdbc:oracle:thin:@loacalhost:1521:orcl \
--username xxx \
--password xxx \
--query "SELECT * FROM USERDATA.daily_registereduser_record WHERE ${updated} \$CONDITIONS" \
--hive-table user_bhvr.orcl _USERDATA_daily_registereduser_record_delta \
--hive-drop-import-delims \
--null-non-string '\\N' \
--null-string '\\N' \
--target-dir /apps-data/hdpuser007/user_bhvr/orcl _USERDATA_daily_registereduser_record_delta \
--hive-partition-key y,m,d \
--hive-partition-value 2019,07,02 \
--hive-import \
--hive-overwrite \
--delete-target-dir

```