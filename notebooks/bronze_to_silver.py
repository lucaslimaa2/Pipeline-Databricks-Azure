# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo os dados na camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC val df = spark.read.format("delta").load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformando os campos do json em colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")
# MAGIC display(dados_detalhados)
# MAGIC // anuncio.* e endereco.* transformam as "subcolunas" em colunas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removendo colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
# MAGIC display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando na camada Silver

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
# MAGIC df_silver.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checando os nomes das colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC var columnNames: Array[String] = df_silver.columns

# COMMAND ----------

# MAGIC %scala
# MAGIC columnNames.foreach(println)
