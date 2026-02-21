from pyspark.sql.functions import desc, sum

from queries import BaseTask
from queries.db import payment, rental, inventory, film_category, category

class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести категорию фильмов, на которую потратили больше всего денег"

    @staticmethod
    def run():
        category_spending = payment \
            .join(rental, "rental_id") \
            .join(inventory, "inventory_id") \
            .join(film_category, "film_id") \
            .join(category, "category_id") \
            .groupBy(category["name"]) \
            .agg(sum("amount").alias("total_spent")) \
            .orderBy(desc("total_spent")) \
            .limit(1).show()
