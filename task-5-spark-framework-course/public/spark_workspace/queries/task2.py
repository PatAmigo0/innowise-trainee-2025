from pyspark.sql.functions import desc, concat, col, lit

from . import BaseTask
from .db import rental, inventory, film_actor, actor

class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию"

    @staticmethod
    def run():
        actor_inventory_count = film_actor \
            .join(inventory, "film_id") \
            .join(actor, "actor_id") \
            .groupBy(actor["actor_id"], actor["first_name"], actor["last_name"]) \
            .count() \
            .withColumnRenamed("count", "rentals_amount") \
            .orderBy(desc("rentals_amount")) \
            .limit(10)

        final_output = actor_inventory_count.select(
            "actor_id",
            concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
            "rentals_amount"
        ).show()