# Predicción del Happiness Score por País (2015-2019)

## Descripción del Proyecto

Este proyecto tiene como objetivo predecir el **Happiness Score** (índice de felicidad) de países a partir de datos históricos de los años 2015 a 2019. Se implementan técnicas de análisis de datos, aprendizaje automático y transmisión de datos en tiempo real utilizando un enfoque modular y escalable.

Tecnologías utilizadas:

* **Python 3.10**
* **Jupyter Notebook**
* **Apache Kafka**
* **PostgreSQL**
* **Docker**

---

## Objetivo

Desarrollar un sistema que integre procesamiento de datos, modelado predictivo y almacenamiento en base de datos para estimar el índice de felicidad a partir de variables socioeconómicas.

---

## Estructura del Proyecto

```
WORKSHOP3/
│
├── config/
│   └── conexion_db.py
├── cuaderno/
│   ├── 01_combine_data.ipynb
│   ├── 02_EDA.ipynb
│   ├── 03_modelo.ipynb
│   └── 04_db_model.ipynb
├── datos/
│   ├── 2015.csv - 2019.csv
│   ├── combined_happiness_data.csv
│   └── datos_procesados.csv
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── modelo/
│   └── ridge_modelo.pkl
├── transformación/
├── pdf/
├── .env
├── .gitignore
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## Requisitos e Instalación

1. Crear entorno virtual:

```bash
python -m venv venv
source venv/bin/activate      # En Windows: venv\Scripts\activate
```

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

---

## Flujo de Trabajo

1. Limpieza y combinación de datos: `01_combine_data.ipynb`
2. Análisis exploratorio: `02_EDA.ipynb`
3. Entrenamiento de modelos: `03_modelo.ipynb`
4. Envío y predicción en tiempo real con Kafka: `producer.py` y `consumer.py`
5. Almacenamiento en PostgreSQL: `04_db_model.ipynb`
6. Orquestación de servicios con Docker:

```bash
docker-compose up -d
docker ps
```

---

## Modelado Predictivo

Se evaluaron tres modelos de regresión: **Linear Regression**, **Ridge Regression** y **Random Forest**. A continuación, se presenta un resumen de las métricas evaluadas:

| Métrica                             | Linear Regression | Ridge Regression | Random Forest |
| ----------------------------------- | ----------------- | ---------------- | ------------- |
| **MSE**                             | 0.0723            | 0.0996           | 0.2033        |
| **R²**                              | 0.9406            | 0.9181           | 0.8329        |
| **R² global (cross\_val\_predict)** | 0.8583            | 0.9098           | 0.8295        |
| **MAE (cross\_val\_predict)**       | 0.2485            | 0.2559           | 0.3541        |
| **R² promedio (cross\_val\_score)** | 0.8527            | 0.9087           | 0.8258        |
| **Desv. estándar R²**               | 0.0598            | 0.0092           | 0.0295        |

**Modelo seleccionado:** *Ridge Regression* fue el modelo elegido gracias a su alto desempeño en precisión y estabilidad (R² = 0.9181), con baja desviación estándar, lo que demuestra consistencia entre diferentes subconjuntos de datos.

---

## Base de Datos

* Motor: **PostgreSQL**
* Librería de conexión: `psycopg2`
* Conexión gestionada en: `config/conexion_db.py`
Se almacenan:
* Predicciones del Happiness Score


---

## Conclusiones Generales

* El modelo Ridge mostró alta precisión y generalización, ideal para este tipo de predicción.
* La limpieza de datos y análisis previo fueron clave para obtener un dataset robusto.
* La arquitectura con Kafka y Docker permite procesamiento en tiempo real y escalabilidad.
* Este enfoque puede escalarse o adaptarse a otras métricas sociales o económicas, sirviendo como herramienta para análisis de bienestar a gran escala.

---

## Contacto

* **GitHub:** [@luzangelacarabali](https://github.com/luzangelacarabali)
* **LinkedIn:** [Luz Ángela Carabalí](https://www.linkedin.com/in/luz-angela-carabali-mulato-12b561306?utm_source=share&utm_campaign=share_via&utm_content=profile&utm_medium=ios_app)

