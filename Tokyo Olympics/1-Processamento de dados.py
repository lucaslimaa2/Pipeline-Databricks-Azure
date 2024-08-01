# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

athletes = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("InferSchema", "true")\
    .load("/mnt/tokyo olympics/raw-data/Athletes")


Coaches = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("InferSchema", "true")\
    .load("/mnt/tokyo olympics/raw-data/Coaches")


EntriesGender = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("InferSchema", "true")\
    .load("/mnt/tokyo olympics/raw-data/EntriesGender")


Medals = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("InferSchema", "true")\
    .load("/mnt/tokyo olympics/raw-data/Medals")


Teams = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("InferSchema", "true")\
    .load("/mnt/tokyo olympics/raw-data/Teams")


# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
    .withColumn("Total", col("Total").cast(IntegerType())) 

# COMMAND ----------

Top_paises_medalha_ouro = Medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()


# COMMAND ----------

média_genero = EntriesGender\
    .withColumn('Avg_Female', EntriesGender['Female'] / EntriesGender['Total'])\
    .withColumn('Avg_Male', EntriesGender['Male'] / EntriesGender['Total'])\

média_genero.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyo olympics/transformed-data/athletes")

Coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyo olympics/transformed-data/coaches")

EntriesGender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyo olympics/transformed-data/entriesgender")

Medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyo olympics/transformed-data/medals")

Teams.repartition(1).write.mode("overwrite").option("header","true").csv("mnt/tokyo olympics/transformed-data/teams")
     
