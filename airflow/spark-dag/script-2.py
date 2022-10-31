from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

laureate_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.laureate") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

prize_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.prize") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Get categories from US born Nobel laureates
prize_df = prize_df.alias("prize_df")
laureate_df = laureate_df.alias("laureate_df")
join_df = prize_df.join(laureate_df,prize_df.id == laureate_df.id,"inner") \
    .where(col("borncountry")=="USA") \
    .groupBy(col("prize_df.category")) \
    .count() \
    .orderBy(col("count").desc())

join_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.nobel_usa_categories") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
