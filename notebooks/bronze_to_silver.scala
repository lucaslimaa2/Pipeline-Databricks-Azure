// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo os dados na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transformando os campos do json em colunas

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")
display(dados_detalhados)
// anuncio.* e endereco.* transformam as "subcolunas" em colunas

// COMMAND ----------

// MAGIC %md
// MAGIC ## Removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salvando na camada Silver

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Checando os nomes das colunas

// COMMAND ----------

var columnNames: Array[String] = df_silver.columns

// COMMAND ----------

columnNames.foreach(println)
