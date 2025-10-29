from pyspark.sql.functions import col, count, desc, dense_rank
from pyspark.sql.window import Window

from . import BaseTask
from .db import category, film_category, film_actor, actor


class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.\nЕсли у нескольких актеров одинаковое кол-во фильмов, вывести всех"

    @staticmethod
    def run():
        children_category = category.filter(col("name") == "Children")

        actor_film_counts = children_category \
            .join(film_category, "category_id") \
            .join(film_actor, "film_id") \
            .join(actor, "actor_id") \
            .groupBy(actor["actor_id"], actor["first_name"], actor["last_name"]) \
            .agg(count("film_id").alias("film_count"))

        window_spec = Window.orderBy(desc("film_count"))

        ranked_actors = actor_film_counts \
            .withColumn("rank", dense_rank().over(window_spec))

        top_actors_children = ranked_actors \
            .filter(col("rank") <= 3) \
            .orderBy("rank", "last_name") \
            .select("first_name", "last_name", "film_count", "rank").show()
