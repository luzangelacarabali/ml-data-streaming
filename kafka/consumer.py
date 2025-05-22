import os
import json
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2
from joblib import load

# Cargar variables de entorno
load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

create_table_query = """
CREATE TABLE IF NOT EXISTS Estimates (
    region TEXT,
    happiness_score DOUBLE PRECISION,
    gdp_per_capita DOUBLE PRECISION,
    social_support DOUBLE PRECISION,
    life_expectancy DOUBLE PRECISION,
    freedom DOUBLE PRECISION,
    corruption_perception DOUBLE PRECISION,
    generosity DOUBLE PRECISION,
    dystopia_residual DOUBLE PRECISION,
    cluster INTEGER,
    happiness_level TEXT,
    happiness_category TEXT,
    predicted_happiness_score DOUBLE PRECISION
);
"""

try:
    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()

    # Cargar modelo entrenado con joblib
    modelo = load(r'C:\Users\Acer\OneDrive\Escritorio\Workshops y Proyectos\workshop3\modelo\ranf_model_model.pkl')

    # Obtener features esperadas por el modelo
    try:
        features = list(modelo.feature_names_in_)
    except AttributeError:
        # Si no está definido, poner las features manualmente (ajustar según tu modelo)
        features = [
            'GDP_per_Capita', 'Social_Support', 'Life_Expectancy', 'Freedom',
            'Corruption_Perception', 'Generosity', 'Cluster',
            'Region_Central and Eastern Europe', 'Region_Eastern Asia',
            'Region_Latin America and Caribbean', 'Region_Middle East and Northern Africa',
            'Region_North America', 'Region_Southeastern Asia', 'Region_Southern Asia',
            'Region_Sub-Saharan Africa', 'Region_Unknown', 'Region_Western Europe',
            'Happiness_Level_Bajo', 'Happiness_Level_Medio'
        ]

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {}
    )

    print("🟢 Esperando mensajes en Kafka...")

    for message in consumer:
        try:
            data = message.value
            if not data:
                print("⚠️ Mensaje vacío recibido, ignorando.")
                continue

            required_fields = ['Region', 'Happiness_Score', 'GDP_per_Capita']
            if not all(field in data for field in required_fields):
                print(f"⚠️ Datos incompletos recibidos: {data}")
                continue

            print("📥 Recibido:", data)
            df = pd.DataFrame([data])

            # Crear dummies para Region
            region_cols = [col for col in features if col.startswith('Region_')]
            for col in region_cols:
                region_name = col.replace('Region_', '')
                df[col] = (df['Region'] == region_name).astype(int)

            # Crear dummies para Happiness_Level (Bajo, Medio)
            happiness_level_cols = [col for col in features if col.startswith('Happiness_Level_')]
            for col in happiness_level_cols:
                level_name = col.replace('Happiness_Level_', '')
                df[col] = (df['Happiness_Level'] == level_name).astype(int)

            # Asegurar que las columnas numéricas existan y sean float, rellenar NaN con 0
            numeric_cols = ['GDP_per_Capita', 'Social_Support', 'Life_Expectancy', 'Freedom',
                            'Corruption_Perception', 'Generosity', 'Cluster']
            for col in numeric_cols:
                if col not in df.columns:
                    df[col] = 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            # Si Cluster no es entero, convertir
            df['Cluster'] = df['Cluster'].astype(int)

            # Agregar columnas faltantes con 0 para evitar error al predecir
            for f in features:
                if f not in df.columns:
                    df[f] = 0

            # Reordenar columnas según el orden esperado por el modelo
            X = df[features].copy()

            # Predecir
            prediction = modelo.predict(X)[0]
            print("🔮 Predicción:", prediction)

            # Preparar valores para insertar en DB
            dystopia = data.get("Dystopia_Residual")
            if dystopia is None or (isinstance(dystopia, float) and pd.isna(dystopia)):
                dystopia = 0.0

            cluster_val = data.get("Cluster")
            cluster_val = int(cluster_val) if cluster_val is not None else 0

            values = (
                data.get("Region"),
                float(data.get("Happiness_Score", 0)),
                float(data.get("GDP_per_Capita", 0)),
                float(data.get("Social_Support", 0)),
                float(data.get("Life_Expectancy", 0)),
                float(data.get("Freedom", 0)),
                float(data.get("Corruption_Perception", 0)),
                float(data.get("Generosity", 0)),
                dystopia,
                cluster_val,
                data.get("Happiness_Level"),
                data.get("Happiness Category"),
                float(prediction)
            )

            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO Estimates (
                        region, happiness_score, gdp_per_capita, social_support, life_expectancy, freedom,
                        corruption_perception, generosity, dystopia_residual, cluster, happiness_level,
                        happiness_category, predicted_happiness_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    values
                )
                conn.commit()
            print("💾 Datos insertados en la base de datos.")

        except Exception as e:
            print("❌ Error procesando mensaje:", e)

except Exception as e:
    print("❌ Error en conexión o configuración:", e)

finally:
    if 'conn' in locals():
        conn.close()
