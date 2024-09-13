import contextlib

import kafka
import kafka.errors


class TopicManager:
    """Administra los tópicos de Kafka."""

    def __init__(self, servers):
        self.client = kafka.KafkaAdminClient(bootstrap_servers=servers)

    def create(self, name, num_partitions=1, replication_factor=1):
        """
        Crea un topic en Kafka solo si no existe.

        :param str name: Nombre del tópico.
        :param int num_partitions: Número de particiones en las que se dividirá el topic.
            Debes tener al menos tantas particiones como consumidores
            si deseas que todos los consumidores puedan procesar datos en paralelo.

            Permite que múltiples producers envíen mensajes al mismo topic en paralelo,
            y múltiples consumers (organizados en un consumer group) procesen las particiones en paralelo.

        :param int replication_factor: 1 por defecto.
            Estos valores cambiarían si deseamos crear una copia del contenido del topic por seguridad
            Para entornos PRO se recomienda un valor mayor o igual a 3
        """

        existing = self.client.list_topics()
        if name not in existing:
            new_topic = kafka.admin.NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            with contextlib.suppress(kafka.errors.TopicAlreadyExistsError):
                self.client.create_topics(new_topics=(new_topic,), validate_only=False)
