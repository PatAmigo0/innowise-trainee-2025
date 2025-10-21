from . import BaseTask
from .db import inventory, film

class Task(BaseTask):
    @staticmethod
    def name():
        return "Вывести названия фильмов, которых нет в inventory.\nНаписать запрос без использования оператора IN"

    @staticmethod
    def run():
        films_not_in_inventory = film \
            .join(inventory, on="film_id", how="left_anti") \
            .select("title").show()
