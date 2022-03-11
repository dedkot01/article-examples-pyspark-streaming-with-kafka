def main():
    from pyspark.sql import SparkSession

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
          .selectExpr('CAST(value AS STRING)', 'offset'))

    console = (df
               .writeStream
               .format('console')
               .queryName('console-output'))

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
