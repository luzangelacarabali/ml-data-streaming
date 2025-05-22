import os
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

if not KAFKA_TOPIC or not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("Por favor, define KAFKA_TOPIC y KAFKA_BOOTSTRAP_SERVERS en el archivo .env")

# Inicializar el productor Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Ruta al CSV con los datos procesados
csv_path = r"C:\Users\Acer\OneDrive\Escritorio\Workshops y Proyectos\workshop3\datos\datos_procesados.csv"
df = pd.read_csv(csv_path)

# Eliminar columnas que no necesita el modelo (si existen)
columns_to_exclude = ['Unnamed: 0']  # agrega otras si es necesario
df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns])

# Enviar cada fila como mensaje al topic de Kafka con manejo básico de errores
for _, row in df.iterrows():
    data = row.to_dict()
    try:
        producer.send(KAFKA_TOPIC, value=data).get(timeout=10)
        print(f"✅ Enviado: {data}")
    except KafkaError as e:
        print(f"❌ Error al enviar mensaje: {e}")

producer.flush()
print("✅ Todos los datos han sido enviados.")
