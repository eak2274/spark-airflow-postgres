from pyspark.sql import SparkSession, Row
from datetime import datetime, date
from pyspark.sql.functions import col

spark = SparkSession.builder.config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar").getOrCreate()

laureate_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.laureate") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

top_5_countries_df = laureate_df.filter(col("borncountry")!="null").groupBy("borncountry").count()
top_5_countries_df.sort("count",ascending=False).limit(5) \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.top_5_countries") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()