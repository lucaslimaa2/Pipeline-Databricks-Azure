# Databricks notebook source
# MAGIC %md
# MAGIC ##### Conferindo se os dados foram montados e se o acesso a pasta inbound está ok

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo os dados na camada de inbound

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
# MAGIC val dados = spark.read.json(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removendo Colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_anuncio = dados.drop("imagens", "usuario")
# MAGIC
# MAGIC display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC > ## Criando uma coluna de identificação

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.col

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando na camada bronze no formato delta

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


