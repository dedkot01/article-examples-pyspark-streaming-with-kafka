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
              .option('subscribe', 'input00,input01')
              .load())

    source.printSchema()

    df = (source
          .selectExpr('CAST(value AS STRING)', 'offset'))

    console = (df
               .writeStream
               .format('console')
               .queryName('console output'))
    console.start().awaitTermination()


if __name__ == '__main__':
    main()
