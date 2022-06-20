def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.avro import functions as fa

    schema = open('schema_example4.avsc', 'r').read()

    spark = (SparkSession
             .builder
             .appName('kafka-avro-consumer')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    source = (spark
              .read
              .format('kafka')
              .option('kafka.bootstrap.servers', 'localhost:9092')
              .option('subscribe', 'input00')
              .load())

    df = (source
          .select(fa.from_avro('value', schema).alias('data'))
          .select('data.*'))

    df.show()
    df.printSchema()


if __name__ == '__main__':
    main()
