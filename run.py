import argparse
import logging
import multiprocessing

import consumer
import producer
import topic_manager


def multiprocess(func, iterable):
    with multiprocessing.Pool() as pool:
        result = pool.starmap(func, iterable)
    # Consuma el resultado para que se detecten las excepciones
    for _ in result:
        pass


def produce_messages(server, topic, number, partition_id, delay):
    return producer.Producer(server, topic, delay).send(number, partition_id)


def consume_messages(server, topic, group_id, partition_id, output):
    return consumer.Consumer(server, topic, group_id).consume(partition_id, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MyKafka stream bridge")
    parser.add_argument("--server", help="Kafka server", default="127.0.0.1:29092")
    parser.add_argument("--topic", help="An topic to produce and consume", default="my-kafka-topic")
    parser.add_argument("--num_messages", help="Number of messages to send", type=int, default=20)
    parser.add_argument("--consumer_group", help="Consumer group ID", default="my-kafka-consumer-group")
    parser.add_argument("--num_partitions", help="Number partitions.", type=int, default=3)
    parser.add_argument("--delay", help="Delay between messages in seconds", type=float, default=0)
    parser.add_argument("--output", help="Output jsonlines file", default="output.jsonl")

    args, _ = parser.parse_known_args()
    logging.basicConfig(level=logging.INFO)
    # Crear un tópico con un número de particiones específico.
    topic_manager.TopicManager(args.server).create(args.topic, num_partitions=args.num_partitions)
    partitions = tuple(range(args.num_partitions))
    # Produce mensajes en paralelo para cada partición del tópico especificado.
    send_args = ((args.server, args.topic, args.num_messages, n, args.delay) for n in partitions)
    multiprocess(produce_messages, send_args)
    # Consume mensajes en paralelo para cada partición del tópico especificado.
    cons_args = ((args.server, args.topic, args.consumer_group, n, args.output) for n in partitions)
    multiprocess(consume_messages, tuple(cons_args))
