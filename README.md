# Proyecto Final de ETL — Stream de Indicadores Económicos
##### Este proyecto es la tercera y última entrega del proyecto de ETL (Extract, Transform, Load) para la materia de Ingeniería de Datos. El objetivo es integrar distintas herramientas del ecosistema de procesamiento de datos para construir un flujo de trabajo real que incluye adquisición de datos, transformación, almacenamiento, transmisión en tiempo real y visualización.

## Componentes del Proyecto
#### 1. Extracción de Datos
Se utiliza la API del Banco Mundial para obtener datos económicos clave como la tasa de desempleo, para todos los países desde el año 2000 hasta 2023.

#### 2. Transformación
- Limpieza y normalización de los datos obtenidos.

- Selección y modelado dimensional de los indicadores clave.

- Uso de pandas para procesamiento intermedio.

#### 3. Carga
- Carga de datos históricos a una base de datos PostgreSQL.

- Datos de referencia como premios Grammy también se cargan como parte del entorno analítico.

#### 4. Transmisión de Datos en Tiempo Real
- Kafka es utilizado para simular un flujo en tiempo real de datos de desempleo.

- Se publica un mensaje por país al tópico unemployment_topic.

#### 5. Visualización Interactiva
- Streamlit se emplea para construir una interfaz que escucha el stream de Kafka y actualiza dinámicamente un gráfico de barras con la tasa de desempleo por país.

- El gráfico inicia con un país y va acumulando nuevas entradas conforme llegan.

## Dashboard
![alt text](Dashboard_project.png)

## Tecnologías Utilizadas
- Python 3

- Kafka (Apache Kafka + Kafka-Python)

- PostgreSQL

- Pandas

- Streamlit

- Requests (para consumo de API)

- SQLAlchemy

- Docker (opcional para levantar el ecosistema)

## Cómo ejecutar el proyecto

#### 1. Clona el repositorio
````
git clone https://github.com/dcontreras1/ETL_final_project
cd ETL_final_project
````

#### 2. Instala las dependencias
`pip install -r requirements.txt`

#### 3. Levanta Kafka y Zookeeper
`docker-compose up -d`

#### 4. Ejecuta el productor de Kafka
`python Producer_kafka.py`

#### 5. Ejecuta el consumidor con Streamlit
`streamlit run Consumer_kafka.py`