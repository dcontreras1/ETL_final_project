import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
import sys
import os

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

class TestTransform(unittest.TestCase):
    def setUp(self):
        """Configuración inicial para las pruebas"""
        # Crear datos de prueba
        self.test_data = pd.DataFrame({
            ' CountryID ': [1, 2, 3],
            ' Country ': ['Country1', 'Country2', 'Country3'],
            ' Year ': [2020, 2021, 2022],
            ' AMA exchange rate ': [1.0, 1.1, 1.2],
            ' IMF based exchange rate ': [1.0, 1.1, 1.2],
            ' Population ': [1000, 2000, 0],
            ' Currency ': ['USD', 'EUR', 'GBP'],
            ' Per capita GNI ': [1000, 2000, 3000],
            ' Gross National Income(GNI) in USD ': [1000000, 2000000, 3000000],
            ' Gross Domestic Product (GDP) ': [2000000, 4000000, 6000000],
            ' Imports of goods and services ': [500000, 1000000, 1500000],
            ' Household consumption expenditure (including Non-profit instit': [300000, 600000, 900000]
        })

    def test_calculated_columns(self):
        """Prueba los cálculos de las columnas derivadas"""
        # Crear una versión transformada de los datos de prueba
        transformed_data = self.test_data.copy()
        
        # Calcular las columnas derivadas
        transformed_data['gdp_per_capita'] = np.where(
            transformed_data[' Population '] > 0,
            transformed_data[' Gross Domestic Product (GDP) '] / transformed_data[' Population '],
            None
        )
        
        transformed_data['imports_per_capita'] = np.where(
            transformed_data[' Population '] > 0,
            transformed_data[' Imports of goods and services '] / transformed_data[' Population '],
            None
        )
        
        transformed_data['household_expenditure_per_capita'] = np.where(
            transformed_data[' Population '] > 0,
            transformed_data[' Household consumption expenditure (including Non-profit instit'] / transformed_data[' Population '],
            None
        )
        
        # Verificar los cálculos
        self.assertEqual(transformed_data['gdp_per_capita'][0], 2000)  # 2000000/1000
        self.assertEqual(transformed_data['gdp_per_capita'][1], 2000)  # 4000000/2000
        self.assertIsNone(transformed_data['gdp_per_capita'][2])  # División por cero
        
        self.assertEqual(transformed_data['imports_per_capita'][0], 500)  # 500000/1000
        self.assertEqual(transformed_data['imports_per_capita'][1], 500)  # 1000000/2000
        self.assertIsNone(transformed_data['imports_per_capita'][2])  # División por cero

    def test_column_renaming(self):
        """Prueba el renombramiento de columnas"""
        # Crear una versión transformada de los datos de prueba
        transformed_data = self.test_data.copy()
        
        # Renombrar las columnas
        column_mapping = {
            ' CountryID ': 'country_id',
            ' Country ': 'country_name',
            ' Year ': 'year',
            ' AMA exchange rate ': 'ama_exchange_rate',
            ' IMF based exchange rate ': 'imf_exchange_rate',
            ' Population ': 'population',
            ' Currency ': 'currency',
            ' Per capita GNI ': 'per_capita_gni',
            ' Gross National Income(GNI) in USD ': 'gni',
            ' Gross Domestic Product (GDP) ': 'gdp',
            ' Imports of goods and services ': 'imports_of_goods_and_services',
            ' Household consumption expenditure (including Non-profit instit': 'household_expenditure'
        }
        
        transformed_data = transformed_data.rename(columns=column_mapping)
        
        # Verificar que las columnas se renombraron correctamente
        for old_name, new_name in column_mapping.items():
            self.assertIn(new_name, transformed_data.columns)
            self.assertNotIn(old_name, transformed_data.columns)

    def test_data_types(self):
        """Prueba los tipos de datos de las columnas"""
        # Crear una versión transformada de los datos de prueba
        transformed_data = self.test_data.copy()
        
        # Verificar tipos de datos
        self.assertTrue(pd.api.types.is_numeric_dtype(transformed_data[' CountryID ']))
        self.assertTrue(pd.api.types.is_string_dtype(transformed_data[' Country ']))
        self.assertTrue(pd.api.types.is_numeric_dtype(transformed_data[' Year ']))
        self.assertTrue(pd.api.types.is_numeric_dtype(transformed_data[' Population ']))
        self.assertTrue(pd.api.types.is_string_dtype(transformed_data[' Currency ']))
        self.assertTrue(pd.api.types.is_numeric_dtype(transformed_data[' Gross Domestic Product (GDP) ']))

if __name__ == '__main__':
    unittest.main()
