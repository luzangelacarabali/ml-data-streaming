import os
import sys
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from time import sleep
import datetime as dt

# Agregar raíz del proyecto al path para importar tus transformaciones
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar funciones de transformación
from transformation.data_transform import load_data, clean_data, preprocess_features

# Cargar variables de entorno
load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

if not KAFKA_TOPIC or not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("❌ Por favor, define KAFKA_TOPIC y KAFKA_BOOTSTRAP_SERVERS en el archivo .env")

# Ruta al archivo CSV de datos crudos
csv_path = os.path.join(
    "C:/Users/Acer/OneDrive/Escritorio/Workshops y Proyectos/workshop3/datos/datos_procesados.csv"
)

def prepare_data(csv_path):
    df = load_data(csv_path)
    df_clean = clean_data(df)
    df_preprocessed = preprocess_features(df_clean)
    return df_preprocessed

def producer_kafka(df):
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(',')],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for i in range(len(df)):
        row_dict = df.iloc[i].to_dict()
        try:
            future = producer.send(KAFKA_TOPIC, value=row_dict)
            record_metadata = future.get(timeout=10)

            print(f"Mensaje enviado fila {i+1}/{len(df)} a las {dt.datetime.utcnow()}")
            print(f"Datos enviados: {row_dict}")
            print(f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}\n")
            
            sleep(2)
        except KafkaError as e:
            print(f"Error enviando mensaje fila {i+1}: {e}")

    producer.flush()
    print("Todos los mensajes fueron enviados correctamente.")

if __name__ == '__main__':
    df_final = prepare_data(csv_path)
    producer_kafka(df_final)
