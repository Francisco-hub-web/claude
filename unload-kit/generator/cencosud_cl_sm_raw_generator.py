from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.models import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from abc import ABC
from datetime import datetime, timedelta
import os
import glob
import json

# Conexion por defecto. Un JSON puede sobreescribirla con "redshift_conn_id"
# (ej: "redshift_corporativo_edw_easy" para tablas de chi_easy_dim_vw).
DEFAULT_REDSHIFT_CONN_ID = "catman_redshift_cl_edw_prod"

# ---------------------------------------------------------------------------
# MODO ESTRICTO (esquema dictado por la tabla destino)
#
# Se activa cuando el JSON trae "only_unload": true y "schema_source" (lo
# escribe `flow` al sincronizar el JSON contra el Glue Catalog del destino).
# En ese modo el columns_mapping es la verdad:
#   - cada columna se castea EXPLICITAMENTE al tipo del destino, asi el
#     parquet sale con exactamente los tipos que espera la tabla raw;
#   - el primer elemento de cada par es la expresion de origen: el nombre de
#     la columna, o una expresion calculada (current_timestamp, current_date,
#     NULL) para columnas que el destino tiene y el origen no;
#   - no se agrega ninguna columna por fuera del mapping.
# Sin "schema_source", only_unload conserva el comportamiento anterior
# (columnas sin castear + auditoria fija), asi los JSON viejos no cambian.
#
# El DAG generado en modo estricto lleva el tag SCHEMA-STRICT: `flow` lo usa
# para confirmar que MWAA ya parseo el JSON nuevo con este generador.
# ---------------------------------------------------------------------------
STRICT_TAG = "SCHEMA-STRICT"


def rs_cast_type(glue_type):
    """Tipo del Glue Catalog -> tipo de CAST en Redshift (parquet equivalente)."""
    t = (glue_type or "").strip().lower().replace(" ", "")
    if t in ("string", "time") or t.startswith("varchar") or t.startswith("char"):
        return "VARCHAR(65535)"  # sin largo, Redshift usaria VARCHAR(256) y truncaria
    if t in ("int", "integer"):
        return "INTEGER"
    if t == "bigint":
        return "BIGINT"
    if t in ("smallint", "tinyint"):
        return "SMALLINT"
    if t == "double":
        return "DOUBLE PRECISION"
    if t == "float":
        return "REAL"
    if t.startswith("decimal"):
        return t.upper()
    if t == "boolean":
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    return "VARCHAR(65535)"


def _ref(expr, col):
    """Si la expresion es la columna misma, la cita (nombres reservados)."""
    return f'"{expr}"' if expr == col else expr

default_args = {
    "owner": "fcsm",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
}

class RedshiftLoaderBuilder(ABC):
    def set_schema(self, schema):
        self.schema = schema
        return self

    def set_table(self, table):
        self.table = table
        self.table_real = table[:-5] if table.endswith('_fcsm') else table
        return self

    def set_column_dt(self, column_dt):
        self.column_dt = column_dt
        return self

    def set_dag_args(self, dag_args):
        self.dag_args = dag_args
        return self

    def set_columns_mapping(self, mapping):
        self.mapping = mapping
        return self

    def set_redshift_conn_id(self, conn_id):
        # Si el JSON no trae "redshift_conn_id", se usa la conexion por defecto.
        self.redshift_conn_id = conn_id or DEFAULT_REDSHIFT_CONN_ID
        return self

    def set_only_unload(self, only_unload):
        # "only_unload": true en el JSON  ->  modo backfill hacia la tabla raw
        # del mass_ingestion. Cambia TRES cosas a la vez, porque van juntas:
        #
        #   1. UNLOAD con PARTITION BY (column_dt): genera calendar_dt=YYYY-MM-DD/
        #      y saca esa columna de los datos, que es como la espera el Glue
        #      Catalog (ahi es partition key, no columna).
        #   2. Agrega fecha_ejecucion y extraction_date al SELECT, que la tabla
        #      raw tiene y el UNLOAD normal no trae.
        #   3. Salta iceberg_load y s3_clean: el parquet queda en el landing
        #      para moverlo a mano a la otra cuenta.
        #
        # Los puntos 1 y 2 NO se pueden aplicar al flujo normal: romperian el
        # iceberg_load, que espera column_dt dentro del parquet y arma su
        # "datatypes" desde columns_mapping (sin las columnas de auditoria).
        self.only_unload = bool(only_unload)
        return self

    def set_schema_source(self, schema_source):
        # Presente solo en JSON sincronizados por flow: activa el modo estricto.
        self.schema_source = schema_source
        return self

    def build(self):
        self.strict = bool(self.only_unload and getattr(self, "schema_source", None))
        with DAG(
            dag_id=f"cencosud_cl_sm_raw_{self.schema}_{self.table}_full",
            schedule=None,
            default_args=self.dag_args,
            max_active_runs=1,
            catchup=False,
            params={
                "load_start": Param(
                    default=f"2025-01-01",
                    type="string",
                    format="date",
                    title="Desde",
                    description="Fecha inicio para la carga (YYYY-MM-DD)."
                ),
                "load_end": Param(
                    default=f"{datetime.today().date() - timedelta(days=1)}",
                    type="string",
                    format="date",
                    title="Hasta",
                    description="Fecha fin para la carga (YYYY-MM-DD)."
                )
            },
            tags=["RAW-DATA-PIPELINE", "FCSM"] + ([STRICT_TAG] if self.strict else []),
        ) as dag:
            ENVIRONMENT = Variable.get("/toolkit-jdbc/Env")
            BUCKET_RAW = Variable.get("BUCKET_RAW")
            BUCKET_LANDING = Variable.get("BUCKET_LANDING")
            BUCKET_LANDING_SSE_KEY_ARN = Variable.get("BUCKET_LANDING_SSE_KEY_ARN")
            BUCKET_ARTIFACTS = Variable.get("BUCKET_ARTIFACTS")
            GLUE_RAW_DB = Variable.get(f"GLUEDB_SM_RAW")

            DG_CORP_ACCOUNT = "595738433757" if ENVIRONMENT == "dev" else "525143576690"
            BUCKET_DG_CORP = f"cencosud-rev-pii-hash-{DG_CORP_ACCOUNT}-us-east-1"
            ROLE_DG_CORP = f"arn:aws:iam::{DG_CORP_ACCOUNT}:role/tr_etl_datos_datamasking"

            JOB_ROLE_ARN = Variable.get("CENCOSUD_EMR_SERVERLSS_ROLE_ARN_APPLICATION_SIZE_EXTRALARGE_EMR_7.1.0")
            APPLICATION_ID = Variable.get("CENCOSUD_EMR_SERVERLSS_APP_ID_APPLICATION_SIZE_EXTRALARGE_EMR_7.1.0")

            PATH_DATA_LANDING = f"s3://{BUCKET_LANDING}/landing/unload/cl_edw_prod/{self.schema}/{self.table}/"
            ICEBERG_TABLE_NAME = f"cl_edw_prod_{self.schema}_{self.table}"
            PATH_DATA_RAW = f"s3://{BUCKET_RAW}/raw/sm/redshift/cl_edw_prod/{ICEBERG_TABLE_NAME}/"
            COLUMNS_TO_CAST_AS_VARCHAR = [
                col for col, typ in self.mapping.items() if typ[1] == "time"
            ]

            if self.strict:
                partes = []
                for col, spec in self.mapping.items():
                    expr, typ = spec[0], spec[1]
                    if col == self.column_dt:
                        # columna de particion: nativa (PARTITION BY la saca de los datos)
                        partes.append(_ref(expr, col) if expr == col
                                      else f'{expr} AS "{col}"')
                    else:
                        partes.append(f'CAST({_ref(expr, col)} AS {rs_cast_type(typ)}) AS "{col}"')
                select_clause = ",\n    ".join(partes)
            else:
                select_clause = ",\n    ".join([
                    f"{col}::varchar AS {col}" if col in COLUMNS_TO_CAST_AS_VARCHAR else col
                    for col in self.mapping
                ])

            unload_options = [
                "FORMAT AS PARQUET",
                "CLEANPATH",
                "ENCRYPTED",
                f"KMS_KEY_ID '{BUCKET_LANDING_SSE_KEY_ARN}'",
                "PARALLEL ON",
            ]

            if self.strict:
                unload_options.insert(0, f"PARTITION BY ({self.column_dt})")
            elif self.only_unload:
                # Columnas de auditoria que tiene la tabla raw del mass_ingestion.
                # Ambas van como varchar: en el Glue Catalog son string.
                select_clause += ",\n    current_timestamp::varchar AS fecha_ejecucion"
                select_clause += ",\n    current_date::varchar AS extraction_date"
                # PARTITION BY sin INCLUDE: la columna sale de los datos y queda
                # solo en el nombre de la carpeta (calendar_dt=YYYY-MM-DD/).
                unload_options.insert(0, f"PARTITION BY ({self.column_dt})")

            unload_task = RedshiftToS3Operator(
                task_id="redshift_unload",
                select_query=f"""
                    WITH data_table AS (
                        SELECT
                            {select_clause}
                        FROM {self.schema}.{self.table_real}
                        WHERE {self.column_dt} BETWEEN '{{{{ params.load_start }}}}' AND '{{{{ params.load_end }}}}'
                    )
                    SELECT
                        *
                    FROM data_table
                    """,
                s3_bucket=BUCKET_LANDING,
                s3_key=f"landing/unload/cl_edw_prod/{self.schema}/{self.table}/",
                unload_options=unload_options,
                redshift_conn_id=self.redshift_conn_id,
                aws_conn_id="aws_default",
                verify=True,
            )

            start_task = EmptyOperator(task_id="start")
            end_task = EmptyOperator(task_id="end")

            if self.only_unload:
                # Modo backfill: el parquet queda en el landing y se mueve a mano.
                # iceberg_load y s3_clean NO se instancian (si se crearan sin
                # dependencias, Airflow igual los registraria como tareas sueltas
                # y las ejecutaria).
                start_task >> unload_task >> end_task
                return dag

            DEFAULT_MONITORING_CONFIG = {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET_ARTIFACTS}/emr_serverless/logs/"}
                },
                "applicationConfiguration": [{
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.hadoop.fs.s3.customAWSCredentialsProvider": "com.amazonaws.emr.serverless.credentialsprovider.BucketLevelAssumeRoleCredentialsProvider",
                        "spark.hadoop.fs.s3.bucketLevelAssumeRoleMapping": f"{BUCKET_DG_CORP}->{ROLE_DG_CORP}",
                        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "LEGACY",
                        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY"
                    }
                }]
            }

            DEFAULT_JOBS_CONFIG = {
                "tags": {
                    "proyecto": "catmansm",
                    "capa": "raw",
                    "ambiente": ENVIRONMENT,
                    "pais": "cl",
                    "udn": "sm",
                    "bandera": "sm",
                    "cuenta": DG_CORP_ACCOUNT
                }
            }

            S3_JOBS_PATH = f"s3://{BUCKET_ARTIFACTS}/etl/raw_layer/sm/foundations/scripts/v_1.1"

            adjusted_mapping = {
                col: [col, "string"] if typ[1] == "time" else typ
                for col, typ in self.mapping.items()
            }

            CUSTOM_PARAMS_CONFIG = {
                "target_glue_db": GLUE_RAW_DB,
                "iceberg_table_name": ICEBERG_TABLE_NAME,
                "datatypes": str(list(adjusted_mapping.values())),
                "process_partition_column_optional": str([self.column_dt]),
                "process_partitions_transformations_optional": str([(self.column_dt, "day")]),
                "target_s3_path": PATH_DATA_RAW,
                "source_s3_path": PATH_DATA_LANDING,
                "iceberg_writing_mode": "overwrite"
            }

            run_EMR_SERVERLESS_raw_job = EmrServerlessStartJobOperator(
                task_id="iceberg_load",
                application_id=APPLICATION_ID,
                execution_role_arn=JOB_ROLE_ARN,
                job_driver={
                    "sparkSubmit": {
                        "entryPoint": f"{S3_JOBS_PATH}/write_iceberg_job.py",
                        "entryPointArguments": ["--dict_config", json.dumps(CUSTOM_PARAMS_CONFIG)],
                        "sparkSubmitParameters": f"--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse={PATH_DATA_RAW} --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
                configuration_overrides=DEFAULT_MONITORING_CONFIG,
                config=DEFAULT_JOBS_CONFIG,
                waiter_max_attempts=120,
                name=f"RAW-DATA-PIPELINE-{self.schema}-{self.table}-raw"
            )

            s3_clean_task = S3DeleteObjectsOperator(
                task_id="s3_clean",
                bucket=BUCKET_LANDING,
                prefix=f"landing/unload/cl_edw_prod/{self.schema}/{self.table}/"
            )

            start_task >> unload_task >> run_EMR_SERVERLESS_raw_job >> s3_clean_task >> end_task

            return dag

root_dir = os.path.dirname(os.path.abspath(__file__))
for json_file in glob.glob(os.path.join(root_dir, "definitions", "**", "*.json"), recursive=True):
    # Un JSON invalido no debe tumbar el resto de los DAGs de este archivo.
    try:
        with open(json_file) as json_handler:
            config = json.load(json_handler)
        print(config)
        globals()[f"cencosud_cl_sm_raw_{config['schema']}_{config['table']}_new"] = (
            RedshiftLoaderBuilder()
            .set_schema(config["schema"])
            .set_table(config["table"])
            .set_column_dt(config["column_dt"])
            .set_columns_mapping(config["columns_mapping"])
            .set_redshift_conn_id(config.get("redshift_conn_id"))
            .set_only_unload(config.get("only_unload", False))
            .set_schema_source(config.get("schema_source"))
            .set_dag_args(default_args)
            .build()
        )
    except Exception as e:  # noqa: BLE001
        print(f"[generator] ERROR construyendo DAG desde {json_file}: {e}")
