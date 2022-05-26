def main():
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as f
    from pyspark.sql.avro import functions as fa

    schema = open('schema_example4.avsc', 'r').read()

    person = [
        {'name': 'Ivan', 'age': 20},
        {'name': 'Vladimir', 'age': 20},
        {'age': 10, 'name': 'Anastasia'},
        {'age': 18, 'name': 'Kristina'},
    ]

    spark = (SparkSession
             .builder
             .appName('kafka-avro-producer')
             .getOrCreate())

    df: DataFrame = spark.createDataFrame(person)
    df.show()
    df.printSchema()

    df = df.select(fa.to_avro(f.struct('*'), schema).alias('value'))

    (df
     .write
     .format('kafka')
     .option('kafka.bootstrap.servers', 'localhost:9092')
     .option('topic', 'input00')
     .save())


if __name__ == '__main__':
    main()
