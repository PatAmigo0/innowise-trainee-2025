from pyspark.sql import SparkSession
from queries import TASK_REGISTRY

def main():
    spark = SparkSession.builder \
        .appName("Task 5: Pagila-Spark") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        for i, task_obj in enumerate(TASK_REGISTRY):
            print("=" * 80)
            print(task_obj.name())
            print("=" * 80)

            task_obj.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()