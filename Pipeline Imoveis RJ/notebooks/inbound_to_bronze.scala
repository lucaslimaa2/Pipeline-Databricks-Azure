// Databricks notebook source
// MAGIC %md
// MAGIC ##### Conferindo se os dados foram montados e se o acesso a pasta inbound está ok

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/Raw")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo os dados na camada de inbound

// COMMAND ----------

val path = "dbfs:/mnt/dados/Raw/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Removendo Colunas

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")

display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC > ## Criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))

// COMMAND ----------

display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salvando na camada bronze no formato delta

// COMMAND ----------

val path = "dbfs:/mnt/dados/Bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)
