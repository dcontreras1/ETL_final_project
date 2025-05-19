def validate_data():
    import great_expectations as gx
    import json
    from sqlalchemy import create_engine

    # Ruta a las credenciales
    with open("/home/dcontreras/ETL_final_project/credentials.json") as f:
        creds = json.load(f)

    # Crear conexión SQLAlchemy
    connection_string = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    engine = create_engine(connection_string)

    # Inicializar contexto
    context = gx.get_context(context_root_dir="/home/dcontreras/ETL_final_project/great_expectations")

    # Registrar datasource si es necesario
    context.add_or_update_sqlalchemy_datasource(
        name="economy_pg",
        connection_string=connection_string
    )

    # Crear o cargar suite
    suite = context.create_expectation_suite(
        expectation_suite_name="economy_suite",
        overwrite_existing=True
    )

    # Crear validador y agregar expectativas
    validator = context.get_validator(
        datasource_name="economy_pg",
        data_asset_name="economy_raw",
        query="SELECT * FROM economy_raw",
        expectation_suite=suite
    )
    validator.expect_column_values_to_not_be_null(" Country ")
    validator.expect_column_values_to_be_between(" Year ", min_value=2000, max_value=2025)

    validator.save_expectation_suite()

    # Ejecutar validación
    checkpoint = context.run_checkpoint(
        name="validate_economy_raw_checkpoint",
        validations=[{
            "batch_request": validator.batch_request,
            "expectation_suite_name": suite.expectation_suite_name
        }]
    )

    if not checkpoint["success"]:
        raise Exception("La validación de datos falló")
    else:
        print("Validación exitosa")
