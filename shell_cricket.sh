rm -r /home/bigdatacloudxlab27228/om_odi_cricket_landing
mkdir /home/bigdatacloudxlab27228/om_odi_cricket_landing
hdfs dfs -rm -r /user/bigdatacloudxlab27228/hdfs_odi_cricket_landing
hdfs dfs -mkdir /user/bigdatacloudxlab27228/hdfs_odi_cricket_landing
hdfs dfs -copyFromLocal /home/bigdatacloudxlab27228/om_odi_cricket_landing/odi_cricket.csv /user/bigdatacloudxlab27228/hdfs_odi_cricket_landing
hive -f /home/bigdatacloudxlab27228/om_odi_cricket_landing/hive_cricket.hql
spark-submit /home/bigdatacloudxlab27228/om_odi_cricket_landing/pyspark_cricket.py
sh /home/bigdatacloudxlab27228/om_odi_cricket_landing/sqoop_cricket.sh


#hii this is gnanendra's line of comment