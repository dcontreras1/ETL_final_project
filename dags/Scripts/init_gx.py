import great_expectations as gx
import json
import os

def init_gx():
    # Crear el directorio de Great Expectations si no existe
    gx_dir = "/home/dcontreras/ETL_final_project/great_expectations"
    os.makedirs(gx_dir, exist_ok=True)
    
    # Crear la estructura básica de directorios
    os.makedirs(os.path.join(gx_dir, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(gx_dir, "checkpoints"), exist_ok=True)
    os.makedirs(os.path.join(gx_dir, "plugins"), exist_ok=True)
    os.makedirs(os.path.join(gx_dir, "uncommitted"), exist_ok=True)
    
    # Crear el archivo de configuración básico
    config = {
        "config_version": 3,
        "datasources": {
            "economy_data": {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine"
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource.data_connector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
        },
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations"
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "uncommitted/validations"
                }
            }
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "checkpoint_store_name": "checkpoint_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store"
    }
    
    # Guardar la configuración
    with open(os.path.join(gx_dir, "great_expectations.yml"), "w") as f:
        json.dump(config, f, indent=4)
    
    # Obtener el contexto con la configuración correcta
    context = gx.get_context(context_root_dir=gx_dir)
    
    # Crear la suite de expectativas
    suite = context.create_expectation_suite("economy_data_suite")
    
    # Definir las expectativas
    expectations = [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "country_id"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "year"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "gdp"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "population"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "country_id",
                "min_value": 4,
                "max_value": 894
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "population",
                "min_value": 4359,
                "max_value": 1425893465
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "inflation",
                "min_value": -17,
                "max_value": 558
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "unemployment",
                "min_value": 0,
                "max_value": 38,
                "parse_strings_as_datetimes": False,
                "allow_null": True
            }
        }
    ]
    
    # Agregar las expectativas a la suite
    for expectation in expectations:
        suite.add_expectation(**expectation)
    
    # Guardar la suite
    context.save_expectation_suite(suite)
    
    print("Great Expectations inicializado correctamente")

if __name__ == "__main__":
    init_gx() 