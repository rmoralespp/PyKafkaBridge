import json
import time

import kafka
import kafka.errors


class Producer:
    """
    Envia mensajes en formato JSON a un tópico de Kafka a partir de un número
    de mensajes y una clave de partición.

    - Comprime los mensajes con gzip para reducir el tamaño de los mensajes.
    - Enviar los mensajes con una clave fija para garantizar que todos los mensajes
      se envíen a la misma partición.
    - Se utiliza ACK de tipo "all" para confirmar la recepción de mensajes de todas nuestras particiones.
    - Con el fin de aumentar el rendimiento estableceremos el tamaño del
      bloque a 32 KB(se recomienda entre 32 y 64 KB para mejorar el rendimiento) e introduciremos un
      pequeño margen de tiempo de 20 ms para esperar a que los mensajes se incluyan en un bloque listo para enviar.
    """

    def __init__(self, servers, topic, delay):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=self.to_json,
            compression_type="gzip",
            acks="all",
            batch_size=32768,  # 32KB
            linger_ms=20,  # 20ms
        )
        self.topic = topic
        self.delay = delay

    @staticmethod
    def to_json(v):
        return json.dumps(v).encode("utf-8")

    def send(self, num_messages, partition_id):
        """
        :param int num_messages: Número de mensajes a enviar.
        :param str partition_id: Partición a la que se enviarán los mensajes.
        """

        for num in range(num_messages):
            value = {
                "topic": self.topic,
                "msg_id": num,
                "producer_partition_id": partition_id,
            }
            self.producer.send(self.topic, value=value, partition=partition_id)
            time.sleep(self.delay)
        # Antes de cerrar el productor se llama a flush para asegurar de que todos
        # los mensajes pendientes sean enviados y confirmados
        self.producer.flush()
        self.producer.close()
