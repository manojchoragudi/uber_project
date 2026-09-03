from pyspark import pipelines as dp
import pyspark.sql.types as T
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE = "uberoct"
EH_NAME = "uberevents"

EH_CONN_STR = (
    "Endpoint=sb://uberoct.servicebus.windows.net/;"
    "SharedAccessKeyName=listenpolicy;"
    "SharedAccessKey=K4cEiqF5A6sb3mrxqBb/dnoKziC1UBQEu+AEhNON19I=;"
    "EntityPath=uberevents"
)

# Kafka Consumer configuration
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="$ConnectionString" password="{EH_CONN_STR}";'
    ),
    "kafka.request.timeout.ms": 10000,
    "kafka.session.timeout.ms": 10000,
    "maxOffsetsPerTrigger": 10000,
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}


@dp.table
def rides_raw():
    df = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    # Converting values to string
    df = df.withColumn("rides", col("value").cast("string"))

    return df

