import json

import jsonl
import kafka


class Consumer:
    """
    Se utiliza un grupo de consumidores para permitir el consumo de mensaje de forma paralela,
    es decir, si un nodo de ese grupo consume
    un mensaje el resto no lo hará. Es recomendable tener el mismo número de
    particiones que de consumidores dentro de un grupo.
    """

    def __init__(self, servers, topic, group_id):
        self.topic = topic
        self.group_id = group_id
        self.consumer = kafka.KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers,
            value_deserializer=self.from_json,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=2000,
        )

    @staticmethod
    def from_json(v):
        return json.loads(v.decode("utf-8")) if v else None

    @property
    def partitions(self):
        """Devuelve los ID de las particiones."""

        return self.consumer.partitions_for_topic(self.topic)

    def consume(self, partition_id, output):
        default = {
            "consumer_partition_id": partition_id,
            "group_id": self.group_id,
        }
        partition = kafka.TopicPartition(self.topic, partition_id)
        self.consumer.assign([partition])
        iterable = (msg.value | default for msg in filter(None, self.consumer) if msg.value)
        with open(output, mode="ab") as fp:
            jsonl.dump(iterable, fp)
        self.close()

    def close(self):
        self.consumer.close()
