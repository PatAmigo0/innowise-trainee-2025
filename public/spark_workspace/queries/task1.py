from pyspark.sql.functions import col

from . import BaseTask
from .db import film_category, category


class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести количество фильмов в каждой категории, отсортировать по убыванию"

    @staticmethod
    def run():
        film_count_by_category = film_category \
            .join(category, on="category_id") \
            .groupBy("name") \
            .count() \
            .withColumnRenamed("count", "film_count") \
            .orderBy(col("film_count").desc())

        film_count_by_category.show()

