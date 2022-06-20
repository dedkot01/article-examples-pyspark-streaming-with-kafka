Данный репозиторий создан для хранения примеров к [статье](https://github.com/dedkot01/article-pyspark-streaming-with-kafka/blob/main/ARTICLE.md).

Команды запуска:

* Пример 1 - чтение из топика Kafka:

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 example1.py
```

* Пример 2 - чтение и запись в топики Kafka

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 example2.py
```

* Пример 3 - чтение и запись сообщений JSON

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 example3.py
```

* Пример 4 - чтение и запись сообщений AVRO

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 example4.py
```
