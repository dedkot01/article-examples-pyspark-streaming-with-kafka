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
              .load()
              .selectExpr('CAST(value AS STRING)', 'topic'))

    query = (source
             .writeStream
             .outputMode('append')
             .format('kafka')
             .option('kafka.bootstrap.servers', 'localhost:9092')
             .option('topic', 'output00')
             .option('checkpointLocation', './.local/checkpoint'))

    query.start().awaitTermination()


if __name__ == '__main__':
    main()
