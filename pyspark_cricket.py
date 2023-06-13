import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("appName").enableHiveSupport().getOrCreate()
df=spark.sql("show databases")
df.show()
df=spark.sql("use omkar_cricket")
df=spark.sql("show tables")
df=spark.sql("select * from omkar_odi")
df=spark.sql("Select * from omkar_odi where match_id!='match_id'")
from pyspark.sql.functions import when, col

df_num = df.select(
    when(col("match_id").isNull() | (col("match_id") == ""), "N/A").otherwise(col("match_id")).alias("match_id"),
    when(col("season").isNull() | (col("season") == ""), "N/A").otherwise(col("season")).alias("season"),
    when(col("start_date").isNull() | (col("start_date") == ""), "N/A").otherwise(col("start_date")).alias("start_date"),
    when(col("venue").isNull() | (col("venue") == ""), "N/A").otherwise(col("venue")).alias("venue"),
    when(col("innings").isNull() | (col("innings") == ""), "N/A").otherwise(col("innings")).alias("innings"),
    when(col("ball").isNull() | (col("ball") == ""), "N/A").otherwise(col("ball")).alias("ball"),
    when(col("batting_team").isNull() | (col("batting_team") == ""), "N/A").otherwise(col("batting_team")).alias("batting_team"),
    when(col("bowling_team").isNull() | (col("bowling_team") == ""), "N/A").otherwise(col("bowling_team")).alias("bowling_team"),
    when(col("striker").isNull() | (col("striker") == ""), "N/A").otherwise(col("striker")).alias("striker"),
    when(col("non_striker").isNull() | (col("non_striker") == ""), "N/A").otherwise(col("non_striker")).alias("non_striker"),
    when(col("bowler").isNull() | (col("bowler") == ""), "N/A").otherwise(col("bowler")).alias("bowler"),
    when(col("runs_off_bat").isNull() | (col("runs_off_bat") == ""), "N/A").otherwise(col("runs_off_bat")).alias("runs_off_bat"),
    when(col("extras").isNull() | (col("extras") == ""), "N/A").otherwise(col("extras")).alias("extras"),
    when(col("wides").isNull() | (col("wides") == ""), "N/A").otherwise(col("wides")).alias("wides"),
    when(col("noballs").isNull() | (col("noballs") == ""), "N/A").otherwise(col("noballs")).alias("noballs"),
    when(col("byes").isNull() | (col("byes") == ""), "N/A").otherwise(col("byes")).alias("byes"),
    when(col("legbyes").isNull() | (col("legbyes") == ""), "N/A").otherwise(col("legbyes")).alias("legbyes"),
    when(col("penalty").isNull() | (col("penalty") == ""), "N/A").otherwise(col("penalty")).alias("penalty"),
    when(col("wicket_type").isNull() | (col("wicket_type") == ""), "N/A").otherwise(col("wicket_type")).alias("wicket_type"),
    when(col("player_dismissed").isNull() | (col("player_dismissed") == ""), "N/A").otherwise(col("player_dismissed")).alias("player_dismissed"),
    when(col("other_wicket_type").isNull() | (col("other_wicket_type") == ""), "N/A").otherwise(col("other_wicket_type")).alias("other_wicket_type"),
    when(col("other_player_dismissed").isNull() | (col("other_player_dismissed") == ""), "N/A").otherwise(col("other_player_dismissed")).alias("other_player_dismissed")
)
df_num.count()
df_num.write.mode("overwrite").parquet("/user/bigdatacloudxlab27228/hdfs_odi_cricket_curated")
df_num.write.mode("overwrite").csv("/user/bigdatacloudxlab27228/hdfs_odi_cricket_curated1")
