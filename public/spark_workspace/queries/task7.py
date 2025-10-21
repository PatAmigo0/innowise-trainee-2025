import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col
from . import BaseTask
from .db import city, address, customer, rental, inventory, film_category, category


class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),\nи которые начинаются на букву “a”.\nТо же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе"

    @staticmethod
    def run():
        cities_with_customers = city \
            .join(address, "city_id") \
            .join(customer, "address_id") \
            .select(customer["customer_id"], city["city"])

        rental_with_duration: DataFrame = rental \
            .filter(col("return_date").isNotNull()) \
            .withColumn("rental_duration_hours",
                        (col("return_date").cast("long") - col("rental_date").cast("long")) / 3600.0) \
            .withColumn("rental_duration_hours", F.round(col("rental_duration_hours"), 2))  # Добавлено округление

        total_hours_table = cities_with_customers \
            .join(rental_with_duration, "customer_id") \
            .join(inventory, "inventory_id") \
            .join(film_category, "film_id") \
            .join(category, "category_id") \
            .groupBy(category["name"]) \
            .agg(
                F.sum(
                    F.when(col("city").startswith("a"), col("rental_duration_hours"))
                    .otherwise(0)
                ).alias("total_hours_rented_l_cities"),

                F.sum(
                    F.when(col("city").contains("-"), col("rental_duration_hours"))
                    .otherwise(0)
                ).alias("total_hours_rented_s_cities")
            )

        window_l_cities = Window.orderBy(F.desc("total_hours_rented_l_cities"))
        window_s_cities = Window.orderBy(F.desc("total_hours_rented_s_cities"))

        # результирующая таблица
        total_hours_table \
            .withColumn("rank_l", F.dense_rank().over(window_l_cities)) \
            .withColumn("rank_s", F.dense_rank().over(window_s_cities)) \
            .withColumn("c1", F.when(col("rank_l") == 1, col("name")).otherwise(None)) \
            .withColumn("c2", F.when(col("rank_s") == 1, col("name")).otherwise(None)) \
            .select("c1", "total_hours_rented_l_cities", "c2", "total_hours_rented_s_cities").agg(
                F.max("c1").alias("most_popular_ct_in_l_cities"),
                F.max("total_hours_rented_l_cities").alias("total_hours_rented_in_l_cities"),
                F.max("c2").alias("most_popular_ct_in_s_cities"),
                F.max("total_hours_rented_s_cities").alias("total_hours_rented_in_s_cities")
            ).show()
