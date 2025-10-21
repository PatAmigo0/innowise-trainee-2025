from pyspark.sql import SparkSession
from queries import TASK_REGISTRY

def main():
    spark = SparkSession.builder \
        .appName("Task 5: Pagila-Spark") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        for i, task_obj in enumerate(TASK_REGISTRY):
            visible_len = len(task_obj.name())
            rows = task_obj.name().split('\n')

            if len(rows) > 0:
                visible_len = max([len(row) for row in rows])
            dstr = '=' * visible_len

            print('\n')
            print(dstr)
            print(task_obj.name())
            print(dstr)

            task_obj.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()