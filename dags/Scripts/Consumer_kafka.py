import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    'unemployment_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='unemployment-stream'
)

# Inicializar el DataFrame vacío una sola vez
if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=['country', 'year', 'indicator', 'value'])


# Título del dashboard
st.title("Stream de Desempleo Global en Tiempo Real")

# Zona para mostrar tabla y gráfico
placeholder_table = st.empty()
placeholder_chart = st.empty()

# Función para actualizar el gráfico de barras acumulado
def update_bar_chart():
    if not st.session_state.data.empty:
        chart_data = st.session_state.data.set_index('country')['value']
        placeholder_chart.bar_chart(chart_data)

# Inicializar la tabla vacía la primera vez
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    placeholder_table.dataframe(st.session_state.data, use_container_width=True)

st.write("Gráfico de Tasa de Desempleo por País")

# Escuchar mensajes de Kafka y actualizar la vista
for message in consumer:
    new_entry = message.value

    if new_entry.get('value') is not None:
        country = new_entry['country']
        unemployment_value = new_entry['value']

        # Revisar si el país ya está en el DataFrame
        existing_idx = st.session_state.data[st.session_state.data['country'] == country].index

        if not existing_idx.empty:
            # Si ya existe, actualizar su valor
            st.session_state.data.loc[existing_idx, 'value'] = unemployment_value
        else:
            # Si no existe, agregar nueva fila
            st.session_state.data = pd.concat([
                st.session_state.data,
                pd.DataFrame([{'country': country, 'value': unemployment_value}])
            ], ignore_index=True)

        # Actualizar gráfico y tabla
        update_bar_chart()
        placeholder_table.dataframe(st.session_state.data, use_container_width=True)

    time.sleep(1)  # Delay para visualizar cambios gradualmente
