import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as st

spark = SparkSession.builder.appName("demo").getOrCreate()

# Define schema to read fields for processing
_schema =(st.StructType()
    .add("picks_bans", st.ArrayType(st.StructType()
        .add("hero_id", st.LongType())
        .add("is_pick", st.BooleanType())
        .add("order", st.LongType())
        .add("team", st.LongType())
    ))
    .add("radiant_name", st.StringType())
    .add("dire_name", st.StringType())
    .add("radiant_win", st.BooleanType())
    .add("players", st.ArrayType(st.StructType()
        .add("purchase_log", st.ArrayType(st.StructType()
            .add("time", st.LongType())
            .add("key", st.StringType())
        ))
        .add("name", st.StringType())
        .add("hero_id", st.LongType())
        .add("pings", st.LongType())
        .add("kills", st.LongType())
        .add("deaths", st.LongType())
        .add("assists", st.LongType())
        .add("net_worth", st.LongType())
    ))
)

# Read JSON data file
df = spark.read.format("json").schema(_schema).load("/mount/isnfs/qfileshare/matches/*.json")

# Picks/Bans
pb = (df.withColumn("picks_bans", sf.explode(df.picks_bans))
        .withColumn("teamname",
            sf.when(sf.col("picks_bans.team") == 0,  df.radiant_name)
            .otherwise(df.dire_name))
        .withColumn("winner",
            sf.when(
                ((sf.col("picks_bans.team") == 0) & (df.radiant_win == True)) | ((sf.col("picks_bans.team") == 1) & (df.radiant_win == False)), True)
            .otherwise(False))
        .select("picks_bans.*", "teamname", "winner"))

#pb.write.format("json").save("/mount/isnfs/qfileshare/outputs/pick.json/")

# Totals
picks = pb.filter("is_pick == true").groupBy("hero_id").agg(sf.count("*").alias("picks"))
bans = pb.filter("is_pick == false").groupBy("hero_id").agg(sf.count("*").alias("bans"))
totals = pb.groupBy("hero_id").agg(sf.count("*").alias("totals")).sort(sf.desc("totals"))
wins = pb.filter("is_pick == true").filter("winner == true").groupBy("hero_id").agg(sf.count("*").alias("wins"))

# Calculate win rate
winrates = (picks.join(wins, "hero_id")
        .withColumn("winrate", sf.try_divide(wins.wins, picks.picks))
        .select(picks.hero_id, "picks", "winrate"))

# Join totals into one table and collect
all_stats = totals.join(bans, on="hero_id", how="left").join(winrates, on="hero_id", how="left").sort(sf.asc("hero_id")).collect()
with open("all_stats.txt", "w") as file:
    for line in all_stats:
        file.write(f"{line}\n")

# Get team names
teamnames = pb.select(pb.teamname).distinct().collect()
if not os.path.isdir("teamstats"):
    os.mkdir("teamstats")

# Pick/bans per team
for row in teamnames:
    if not os.path.isdir(f"teamstats/{row.teamname}"):
        os.mkdir(f"teamstats/{row.teamname}")
    teampicks = pb.filter("is_pick == true").filter(pb.teamname == row.teamname).groupBy("hero_id").agg(sf.count("*").alias("picks"))
    teambans = pb.filter("is_pick == false").filter(pb.teamname == row.teamname).groupBy("hero_id").agg(sf.count("*").alias("bans")).sort(sf.desc("bans"))
    teamwins = pb.filter("is_pick == true").filter(pb.teamname == row.teamname).filter("winner == true").groupBy("hero_id").agg(sf.count("*").alias("wins"))
    teamwinrates = (teampicks.join(teamwins, "hero_id")
        .withColumn("winrate", sf.try_divide(teamwins.wins, teampicks.picks))
        .select(teampicks.hero_id, "picks", "winrate")).sort(sf.desc("picks"))
    collected_bans = teambans.collect()
    collected_winrates = teamwinrates.collect()
    with open(f"teamstats/{row.teamname}/picks.txt", "w") as file:
        for line in collected_winrates:
            file.write(f"{line}\n")
    with open(f"teamstats/{row.teamname}/bans.txt", "w") as file:
        for line in collected_bans:
            file.write(f"{line}\n")

# Items
players = df.withColumn("players", sf.explode(df.players)).select("players.*")
purchases = players.withColumn("purchase_log", sf.explode(players.purchase_log)).select("purchase_log.key", "purchase_log.time")
itemtotals = purchases.groupBy("key").agg(sf.count("*").alias("totals"))
itemtimings = purchases.groupBy("key").agg(sf.min("time").alias("timings"))
joined_items = itemtotals.join(itemtimings, "key").sort(sf.desc("totals")).collect()
with open("item_stats.txt", "w") as file:
    for line in joined_items:
        file.write(f"{line}\n")