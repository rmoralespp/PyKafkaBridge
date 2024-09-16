import json

import kafka


class JsonConsumer:
    """
    Se utiliza un grupo de consumidores para permitir el consumo de mensaje en formato JSON
    de forma paralela, si un nodo de ese grupo consume un mensaje el resto no lo hará.
    Es recomendable tener el mismo número de
    particiones que de consumidores dentro de un grupo.

    [auto_offset_reset="earliest"] nos garantiza que los consumidores comenzarán a leer los
    primeros mensajes disponibles en Kafka cuando no exista un offset para ese consumidor.
    """

    def __init__(self, servers, topic, group_id):
        self.topic = topic
        self.group_id = group_id
        self.consumer = kafka.KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers,
            value_deserializer=self.from_json,
            auto_offset_reset="earliest",  # Ajustar el offset al inicio del tópico
            enable_auto_commit=False,
            consumer_timeout_ms=4000,
        )

    @staticmethod
    def from_json(v):
        return json.loads(v.decode("utf-8")) if v else None

    @property
    def partitions(self):
        """Devuelve los ID de las particiones."""

        return self.consumer.partitions_for_topic(self.topic)

    def consume(self, partition_id):
        default = {
            "consumed_partition_id": partition_id,
            "group_id": self.group_id,
        }
        partition = kafka.TopicPartition(self.topic, partition_id)
        self.consumer.assign([partition])
        for msg in self.consumer:
            # Confirmar el offset manualmente
            self.consumer.commit()  # Guarda el offset actual como procesado
            if this := (msg and msg.value):
                yield this | default

    def close(self):
        self.consumer.close()
