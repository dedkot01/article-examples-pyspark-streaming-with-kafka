def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.avro import functions as fa

    schema = open('schema_example4.avsc', 'r').read()

    spark = (SparkSession
             .builder
             .appName('streaming-kafka')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    source = (spark
              .readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', 'localhost:9092')
              .option('subscribe', 'input00')
              .load())

    df = (source
          .select(fa.from_avro('value', schema).alias('data')))

    print("df schema")
    df.printSchema()

    console = df.select('data.*')
    print("console schema")
    console.printSchema()

    console = (console
               .writeStream
               .format('console')
               .queryName('console-output'))

    df = df.where(df['data.age'] >= 18)
    df = df.withColumn('value', fa.to_avro('data'))

    query = (df
             .writeStream
             .format('kafka')
             .queryName('kafka-output')
             .option('kafka.bootstrap.servers', 'localhost:9092')
             .option('topic', 'output00')
             .option('checkpointLocation', './.local/checkpoint'))

    console.start()
    query.start().awaitTermination()


if __name__ == '__main__':
    main()
