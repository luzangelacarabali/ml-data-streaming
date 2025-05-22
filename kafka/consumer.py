import os
import json
import pickle
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2

# Cargar variables de entorno
load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Consulta para crear la tabla si no existe (todo en minúsculas)
create_table_query = """
CREATE TABLE IF NOT EXISTS predictions (
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

    # Cargar modelo entrenado
    with open(r'C:\Users\Acer\OneDrive\Escritorio\Workshops y Proyectos\workshop3\modelo\mejor_modelo.pkl', 'rb') as f:
        modelo = pickle.load(f)

    # Obtener features esperadas por el modelo
    try:
        features = list(modelo.feature_names_in_)
    except AttributeError:
        features = [
            'GDP_per_Capita', 'Social_Support', 'Life_Expectancy', 'Cluster',
            'Happiness_Level_Bajo', 'GDP_per_Capita Generosity',
            'GDP_per_Capita Cluster', 'GDP_per_Capita Happiness_Level_Bajo',
            'Social_Support Generosity', 'Life_Expectancy Generosity',
            'Life_Expectancy Cluster', 'Generosity Cluster',
            'Generosity Happiness_Level_Bajo', 'Region_Sub-Saharan Africa^2',
            'Happiness_Level_Bajo^2'
        ]

    # Inicializar Kafka Consumer
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

            # Validar campos obligatorios
            required_fields = ['Region', 'Happiness_Score', 'GDP_per_Capita']
            if not all(field in data for field in required_fields):
                print(f"⚠️ Datos incompletos recibidos: {data}")
                continue

            print("📥 Recibido:", data)
            df = pd.DataFrame([data])

            # Evitar SettingWithCopyWarning usando .loc y asegurarse que todas las columnas existan antes
            # Crear columnas dummy y transformaciones que espera el modelo
            if 'Happiness_Level_Bajo' in features:
                df.loc[:, 'Happiness_Level_Bajo'] = (df['Happiness_Level'] == 'Bajo').astype(int)
            if 'Region_Sub-Saharan Africa^2' in features:
                df.loc[:, 'Region_Sub-Saharan Africa^2'] = ((df['Region'] == 'Sub-Saharan Africa').astype(int)) ** 2
            if 'Happiness_Level_Bajo^2' in features:
                if 'Happiness_Level_Bajo' in df.columns:
                    df.loc[:, 'Happiness_Level_Bajo^2'] = df['Happiness_Level_Bajo'] ** 2
                else:
                    df.loc[:, 'Happiness_Level_Bajo^2'] = 0

            # Interacciones
            interaction_features = [
                'GDP_per_Capita Generosity',
                'GDP_per_Capita Cluster',
                'GDP_per_Capita Happiness_Level_Bajo',
                'Social_Support Generosity',
                'Life_Expectancy Generosity',
                'Life_Expectancy Cluster',
                'Generosity Cluster',
                'Generosity Happiness_Level_Bajo'
            ]

            for feat in interaction_features:
                if feat in features:
                    cols = feat.split()
                    if len(cols) == 2:
                        col1, col2 = cols
                        if col1 in df.columns and col2 in df.columns:
                            df.loc[:, feat] = df[col1] * df[col2]
                        else:
                            df.loc[:, feat] = 0
                    else:
                        df.loc[:, feat] = 0

            df = df.fillna(0)

            # Seleccionar solo las columnas que el modelo espera
            available_features = [f for f in features if f in df.columns]
            X = df[available_features].copy()

            # Añadir columnas faltantes con 0
            missing_features = [f for f in features if f not in available_features]
            for mf in missing_features:
                X[mf] = 0

            # Reordenar columnas
            X = X[features]

            prediction = modelo.predict(X)[0]
            print("🔮 Predicción:", prediction)

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
                    INSERT INTO predictions (
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
