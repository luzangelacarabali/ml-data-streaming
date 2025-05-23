import os
import sys
import json
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
from joblib import load
import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.conexion_db import get_connection, create_estimates_table

load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

if not KAFKA_TOPIC or not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("❌ Define KAFKA_TOPIC y KAFKA_BOOTSTRAP_SERVERS en .env")

modelo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modelo', 'ridge_model.pkl'))
objeto_modelo = load(modelo_path)
modelo = objeto_modelo['modelo']
features = objeto_modelo['features']

def insert_prediction_to_db(conn, values):
    cursor = None
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO Estimates (
            continent_Africa, continent_Asia, continent_Europe, continent_North_America,
            continent_Central_America, continent_South_America, continent_Oceania,
            GDP_per_Capita, Social_Support, Life_Expectancy, Freedom,
            Corruption_Perception, Generosity,
            Happiness_Score, Predicted_Happiness_Score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, values)
        conn.commit()
        print("✅ Predicción guardada en la base de datos.")
    except psycopg2.errors.UndefinedTable as e:
        print(f"❌ La tabla 'Estimates' no existe: {e}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        print(f"❌ Error en la base de datos: {e}")
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inesperado al insertar: {e}")
        raise
    finally:
        if cursor:
            cursor.close()

def convert_to_bool(value):
    """Convierte un valor numérico a booleano seguro para la BD"""
    try:
        return bool(int(float(value)))
    except (ValueError, TypeError):
        return False

def main():
    try:
        create_estimates_table()
    except Exception as e:
        print(f"❌ No se pudo crear la tabla Estimates: {e}")
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(',')],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    conn = None
    try:
        conn = get_connection()
        print("🚀 Esperando mensajes...")
    except Exception as e:
        print(f"❌ No se pudo conectar a la base de datos: {e}")
        return

    for message in consumer:
        data = message.value
        print(f"📩 Mensaje recibido: {data}")

        try:
            # Preparar datos de entrada para el modelo
            X = pd.DataFrame([{col: float(data.get(col, 0)) for col in features}])
            prediction = modelo.predict(X)[0]
            print("🔮 Predicción:", prediction)

            # Convertir continentes a booleanos para la BD, y mantener nombres con mayúsculas
            values = (
                convert_to_bool(data.get("continent_Africa", 0)),
                convert_to_bool(data.get("continent_Asia", 0)),
                convert_to_bool(data.get("continent_Europe", 0)),
                convert_to_bool(data.get("continent_North_America", 0)),
                convert_to_bool(data.get("continent_Central_America", 0)),
                convert_to_bool(data.get("continent_South_America", 0)),
                convert_to_bool(data.get("continent_Oceania", 0)),
                float(data.get("GDP_per_Capita", 0.0)),
                float(data.get("Social_Support", 0.0)),
                float(data.get("Life_Expectancy", 0.0)),
                float(data.get("Freedom", 0.0)),
                float(data.get("Corruption_Perception", 0.0)),
                float(data.get("Generosity", 0.0)),
                float(data.get("Happiness_Score", 0.0)),
                float(prediction)
            )

            insert_prediction_to_db(conn, values)

        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")
            if isinstance(e, psycopg2.errors.UndefinedTable):
                print("❌ La tabla 'Estimates' no existe. Intentando crearla nuevamente...")
                try:
                    create_estimates_table()
                    if conn:
                        conn.close()
                    conn = get_connection()
                except Exception as create_error:
                    print(f"❌ No se pudo crear la tabla: {create_error}")
                    break
            elif isinstance(e, psycopg2.Error):
                if conn:
                    try:
                        conn.rollback()
                        conn.close()
                        conn = get_connection()
                        print("🔄 Conexión reiniciada.")
                    except Exception as reconnect_error:
                        print(f"❌ Error al reconectar: {reconnect_error}")
                        break

    if conn:
        conn.close()

if __name__ == '__main__':
    main()
