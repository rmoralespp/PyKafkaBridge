import argparse
import contextlib
import logging
import multiprocessing
import sys

import jsonl

import consumer
import producer
import topic_manager


def multiprocess(func, iterable):
    with multiprocessing.Pool() as pool:
        pool.starmap(func, iterable)


def produce_messages(server, topic, number, partition_id, delay):
    return producer.JsonProducer(server, topic, delay).send(number, partition_id)


def consume_messages(server, topic, partition_id, output):
    group_id = f"consumer-for-partition-{partition_id}"
    logging.info("Consuming... [%s]", group_id)
    try:
        with contextlib.closing(consumer.JsonConsumer(server, topic, group_id)) as obj:
            fd = output or sys.stdout
            jsonl.dump(obj.consume(partition_id), fd)
    except Exception as e:
        logging.error(f"Error en worker: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MyKafka stream bridge")
    parser.add_argument("--server", help="Kafka server", default="127.0.0.1:29092")
    parser.add_argument("--topic", help="An topic to produce and consume", default="my-kafka-topic")
    parser.add_argument("--num_messages", help="Number of messages to send", type=int, default=20)
    parser.add_argument("--num_partitions", help="Number partitions/consumer-groups.", type=int, default=3)
    parser.add_argument("--delay", help="Delay between messages in seconds", type=float, default=0)
    parser.add_argument("--output", help="Outputs a JSON Lines file. Uses stdout by default.", default=None)

    args, _ = parser.parse_known_args()
    logging.basicConfig(level=logging.INFO)
    # Crear un tópico con un número de particiones específico.
    topic_manager.TopicManager(args.server).create(args.topic, num_partitions=args.num_partitions)
    partitions = range(args.num_partitions)
    # Produce mensajes en paralelo para cada partición del tópico especificado.
    send_args = ((args.server, args.topic, args.num_messages, n, args.delay) for n in partitions)
    multiprocess(produce_messages, send_args)
    # Se utilizan grupos de consumidores que consume los mensajes en paralelo
    # por cada partición del tópico especificado.
    cons_args = ((args.server, args.topic, n, args.output) for n in partitions)
    multiprocess(consume_messages, cons_args)
