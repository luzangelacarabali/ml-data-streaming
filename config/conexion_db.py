# En config/conexion_db.py
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    try:
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        
        if not all([db_user, db_password, db_host, db_port, db_name]):
            raise ValueError("❌ Faltan variables de entorno para la conexión a la base de datos")

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        return engine.raw_connection()
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        raise


def create_estimates_table():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Estimates (
            id SERIAL PRIMARY KEY,
            continent_Africa BOOLEAN NOT NULL,
            continent_Asia BOOLEAN NOT NULL,
            continent_Europe BOOLEAN NOT NULL,
            continent_North_America BOOLEAN NOT NULL,
            continent_Central_America BOOLEAN NOT NULL,
            continent_South_America BOOLEAN NOT NULL,
            continent_Oceania BOOLEAN NOT NULL,
            GDP_per_Capita DOUBLE PRECISION NOT NULL,
            Social_Support DOUBLE PRECISION NOT NULL,
            Life_Expectancy DOUBLE PRECISION NOT NULL,
            Freedom DOUBLE PRECISION NOT NULL,
            Corruption_Perception DOUBLE PRECISION NOT NULL,
            Generosity DOUBLE PRECISION NOT NULL,
            Happiness_Score DOUBLE PRECISION NOT NULL,
            Predicted_Happiness_Score DOUBLE PRECISION NOT NULL
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Tabla 'Estimates' creada o verificada correctamente.")
    except Exception as e:
        print(f"❌ Error al crear la tabla Estimates: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
