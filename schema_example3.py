from pyspark.sql import types as t

schema = t.StructType(
    [
        t.StructField('name', t.StringType(), True),
        t.StructField('age', t.IntegerType(), True),
    ],
)
