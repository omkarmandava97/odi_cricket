#Droping database if alraedy exist
drop database if exists om_odi_Cricket_landing cascade;
#Creating database
create database om_odi_Cricket_landing;
#Using database om_odi_Cricket_landing
Use om_odi_Cricket_landing;
CREATE EXTERNAL TABLE if not exists odi_cricket_landing (
  match_id STRING,
  season STRING,
  start_date STRING,
  venue STRING,
  innings STRING,
  ball STRING,
  batting_team STRING,
  bowling_team STRING,
  striker STRING,
  non_striker STRING,
  bowler STRING,
  runs_off_bat STRING,
  extras STRING,
  wides STRING,
  noballs STRING,
  byes STRING,
  legbyes STRING,
  penalty STRING,
  wicket_type STRING,
  player_dismissed STRING,
  other_wicket_type STRING,
  other_player_dismissed STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\""
)
LOCATION '/user/bigdatacloudxlab27228/hdfs_odi_cricket_landing'
tblproperties("skip.header.line.count"="1");
