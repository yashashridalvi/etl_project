from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CONTROL_TABLE = ("project_data.practice_schema.migration_details")


# --------------------------------------------------
# Helper: Check if this is first run for entity
# --------------------------------------------------
def is_first_run(source_name, source_entity_name) -> bool:
    df = spark.sql(f"""
        SELECT 1
        FROM CONTROL_TABLE
        WHERE source_name = '{source_name}'
          AND source_entity_name = '{source_entity_name}'
        LIMIT 1
    """)
    return df.count() == 0


# --------------------------------------------------
# Helper: Check dependency layer success
# --------------------------------------------------
def check_previous_layer_success(
    source_name,
    source_entity_name,
    batch_date,
    layer
):
    # First ever run → allow
    if is_first_run(source_name, source_entity_name):
        # print("First run detected → skipping dependency check")
        return True

    df = spark.sql(f"""
        SELECT status
        FROM CONTROL_TABLE
        WHERE source_name = '{source_name}'
          AND source_entity_name = '{source_entity_name}'
          AND batch_date = '{batch_date}'
          AND layer = '{layer}'
          AND status = 'SUCCESS'
        LIMIT 1
    """)

    if df.count() == 0:
        raise Exception(
            f"Dependency failure: {layer} is not SUCCESS for batch_date {batch_date}"
        )

    return True


# --------------------------------------------------
# Main: Check current batch eligibility
# --------------------------------------------------
def check_current_batch_status(
    source_name,
    source_entity_name,
    batch_date,
    layer
):
    dependency_map = {
        "Bronze": "Extraction",
        "Silver": "Bronze"
    }

    # Dependency enforcement
    if layer in dependency_map:
        check_previous_layer_success(
            source_name,
            source_entity_name,
            batch_date,
            dependency_map[layer]
        )

    # Prevent duplicate SUCCESS
    df = spark.sql(f"""
        SELECT status
        FROM CONTROL_TABLE
        WHERE source_name = '{source_name}'
          AND source_entity_name = '{source_entity_name}'
          AND batch_date = '{batch_date}'
          AND layer = '{layer}'
        LIMIT 1
    """)

    if df.count() > 0:
        status = df.collect()[0]["status"]
        if status == "SUCCESS":
            raise Exception(f"{layer} already completed successfully")

    return "OK"


# --------------------------------------------------
# Logger: Insert status
# --------------------------------------------------
def log_migration_status(
    source_name,
    source_entity_name,
    batch_date,
    layer,
    status,
    error_details=""
):
    error_details = error_details.replace("'", "''")

    spark.sql(f"""
        INSERT INTO CONTROL_TABLE
        (source_name, source_entity_name, batch_date, layer, status, error_details)
        VALUES
        ('{source_name}', '{source_entity_name}', '{batch_date}', '{layer}', '{status}', '{error_details}')
    """)