
from pyspark.sql.functions import col, desc, sum, when

from . import BaseTask
from .db import customer, address, city

class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).\nОтсортировать по количеству неактивных клиентов по убыванию"

    @staticmethod
    def run():
        client_status_by_city = customer \
            .join(address, "address_id") \
            .join(city, "city_id") \
            .groupBy("city") \
            .agg(
                sum(when(col("active") == 1, 1).otherwise(0)).alias("active_clients"),
                sum(when(col("active") == 0, 1).otherwise(0)).alias("inactive_clients")
            ).orderBy(desc("inactive_clients")).show()
