# Kafka Producer-Consumer Multiprocessing Script

Este script es un ejemplo sencillo de producción y consumo de mensajes en Apache Kafka, gestionando particiones en
paralelo mediante múltiples procesos, lo que simula una granja de productores y consumidores para ejecutar tareas de
forma concurrente y eficiente.

## Características Principales

- Producción Paralela de Mensajes: Genera y envía mensajes simultáneamente a un conjunto específico de particiones de un
  tópico Kafka.
- Consumo Paralelo de Mensajes: Los mensajes son consumidos por varios consumidores, cada uno asignado a una partición,
  asegurando un procesamiento distribuido en paralelo.
- Multiprocesamiento: Utiliza el módulo multiprocessing de Python para ejecutar tanto la producción como el consumo de
  mensajes en paralelo, proporcionando alta concurrencia y eficiencia.

## Dependencias del Proyecto

- Python 3.11 o superior: Para la ejecución del script.
- Apache Kafka: Para la gestión de tópicos y mensajes.
- Zookeeper: Para la coordinación y gestión de Kafka.
- Docker y Docker Compose: Para el arranque rápido de Kafka y Zookeeper si prefieres usar contenedores.

### Instalación de dependencias de Python.

`pip install -r requirements.txt`

## Arrancar Infraestructura Kafka

`docker-compose up -d`

## Ejecución del script

### Parámetros del Script

El script acepta varios parámetros para configurar la conexión a Kafka y la gestión de tópicos. Aquí están los
parámetros más importantes:

--server: Dirección del servidor Kafka (por defecto: 127.0.0.1:29092).
--topic: Nombre del tópico Kafka (por defecto: my-kafka-topic).
--num_messages: Número de mensajes a enviar a cada partición (por defecto: 20).
--consumer_group: ID del grupo de consumidores (por defecto: my-kafka-consumer-group).
--num_partitions: Número de particiones del tópico (por defecto: 3).
--delay: Retraso entre los mensajes enviados por el productor en segundos (por defecto: 0).
--output: Archivo de salida donde se guardarán los mensajes consumidos (por defecto: output.jsonl).

Primero, asegúrate de que Kafka esté corriendo y que hayas creado el tópico con el número de particiones especificado.
Luego, puedes ejecutar el script de la siguiente manera:

## Ejemplo de Ejecución

```python script.py --topic my-topic --num_messages 50 --num_partitions 4 --delay 1 --output messages.jsonl```

Este comando produce y consume mensajes de un tópico llamado `my-topic`.
Envía 50 mensajes a cada una de las 4 particiones del tópico.
Introduce un retardo de 1 segundo entre cada mensaje producido.
Guarda los mensajes consumidos en `messages.jsonl`.
