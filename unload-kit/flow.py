#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
flow - backfill hacia una tabla raw, de una sola corrida.

El esquema lo dicta la TABLA DESTINO. Antes de disparar nada, flow lee el
Glue Catalog del destino y deja el JSON del loader identico a el: mismas
columnas, mismo orden, mismos tipos, misma clave de particion y las columnas
de auditoria que el destino tenga. El generador (modo estricto) castea cada
columna a ese tipo, asi el UNLOAD baja EXACTAMENTE lo que la tabla espera.

Pasos:
  1. Sincroniza el JSON con la tabla destino (muestra el diff, respalda el viejo)
  2. Lo publica en S3 si cambio y espera a que MWAA lo tome; verifica que el
     generador desplegado soporte el modo estricto (tag SCHEMA-STRICT)
  3. Revisa el landing: las particiones del rango que ya estan y son validas
     (run OK, posteriores al JSON, parquet con el esquema del destino) NO se
     vuelven a bajar de Redshift
  4. Dispara el DAG de UNLOAD solo para lo que falta (conf load_start / load_end)
     y espera; si falla, corta sin mover nada
  5. Mueve particion por particion (bajar, reemplazar, subir, verificar, liberar
     disco) y registra. Si se corta, re-correr el mismo comando sigue donde quedo
  6. Verifica y limpia

Tabla nueva (sin JSON): lo genera desde la tabla destino del Glue Catalog y
lo publica (Redshift solo si hay FLOW_RS_PASS, para validar columnas).

Varias tablas, en texto libre (una tras otra, sin preguntas en el medio):
    flow "tran_item del 4 de julio 2025 a fin de año, despues fact_x de enero a marzo 2026"
    flow --pegar                              pegas el pedido (termina con Ctrl-D)
    flow --cola                               retoma la ultima cola (VPN, SSO, Ctrl-C)
    flow "chi_easy_dim_vw__fact_x — 2025-01-05, 2025-01-17, 2025-05-01 → 2025-05-03"
                                              dias especificos (lista, sub-rangos con →)
  Lo interpreta Claude Code (claude -p) si esta instalado, si no un parser
  local; muestra el plan con fechas explicitas y pide confirmacion.
  Si una sesion SSO vence, hace el login solo (FLOW_LOGIN_CMD para cambiarlo).

Uso:
    flow 7 --desde 2025-01-01 --hasta 2025-12-31
    flow chi_easy_dim_vw.fact_nueva --desde ... --hasta ...
    flow 7 --solo-sync                        sincroniza y publica el JSON, nada mas
    flow 7 --solo-dag --desde ... --hasta ... dispara y espera, no mueve
    flow 7 --run manual__2026-09-16T19:12:17Z retoma un run ya disparado
    flow 7 --solo-mover [--desde ... --hasta ...]
    flow 7 --particion 2025-01-05,2025-05-01..2025-05-03
                                              solo esos dias (pipeline completo: UNLOAD
                                              filtrado con load_dates, mover y verificar)
    flow 7 ... --no-sync                      no toca el JSON (solo valida)
    flow 7 ... --forzar-unload                baja todo el rango de Redshift aunque
                                              ya este en el landing
    flow 7 ... --auto                         sin confirmaciones (salvo borrados)
    flow 7 ... --auto-borrar                  sin confirmaciones, incluidos borrados
    flow schema.tabla --conn <conn_id> --column-dt <col>
    flow --desplegar-generador                despliega el generador del kit en MWAA
    flow --ayuda

Requiere unload.py en el mismo directorio, boto3 + requests (como mwaa_cert)
y psycopg2 solo si genera o valida contra Redshift (FLOW_RS_PASS).
"""

__version__ = "2.5"

import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import unload as U  # noqa: E402
import pedido as P  # noqa: E402

C = U.C

# ─── config ───────────────────────────────────────────────────────────────────

MWAA_PROFILE = os.environ.get("FLOW_MWAA_PROFILE", U.PROFILE_SRC)
MWAA_REGION = os.environ.get("AWS_REGION", "us-east-1")
MWAA_ENV = os.environ.get(
    "FLOW_MWAA_ENV", f"cencosud-dev-cl-airflow-{U.ACCOUNT_SRC}-us-east-1")

# El generador arma:  cencosud_cl_sm_raw_{schema}_{table}_full
DAG_ID_TPL = os.environ.get("FLOW_DAG_ID", "cencosud_cl_sm_raw_{schema}_{table}_full")

REDSHIFT = {
    "host": os.environ.get("FLOW_RS_HOST",
                           "edw-prod.csmzzf6sb3pd.us-east-1.redshift.amazonaws.com"),
    "port": int(os.environ.get("FLOW_RS_PORT", "5439")),
    "dbname": os.environ.get("FLOW_RS_DB", "cl_edw_prod"),
    "user": os.environ.get("FLOW_RS_USER", "usr_read_sm_txd_mdh"),
    "password": os.environ.get("FLOW_RS_PASS", ""),
}

LOADERS_PATH = Path(U.DEFS_DIR)
S3_DAGS_BUCKET = os.environ.get(
    "FLOW_S3_DAGS_BUCKET", f"cencosud-dev-cl-airflow-{U.ACCOUNT_SRC}-us-east-1")
S3_LOADERS_PREFIX = os.environ.get(
    "FLOW_S3_LOADERS",
    "dags/cicd/raw_layer/sm/fcsm/dags/loaders/definitions/redshift")

BACKUP_DIR = Path(__file__).resolve().parent / "backups"

# Debe coincidir con STRICT_TAG del generador.
STRICT_TAG = "SCHEMA-STRICT"

# Columnas de auditoria: si el destino las tiene y el origen no, se calculan.
AUDIT_EXPRS = {
    "fecha_ejecucion": "current_timestamp",
    "extraction_date": "current_date",
}

TYPE_MAPPING = {
    "char": "string", "character": "string", "character varying": "string",
    "varchar": "string", "text": "string", "binary varying": "string", "name": "string",
    "bpchar": "string",
    "smallint": "integer", "integer": "integer", "int2vector": "integer", "int4": "integer",
    "bigint": "integer", "int8": "integer", "int2": "integer", "oid": "integer",
    "oidvector": "integer", "regproc": "integer", "tid": "integer", "xid": "integer",
    "numeric": "double", "double precision": "double", "real": "double",
    "float4": "double", "float8": "double",
    "date": "date",
    "timestamp without time zone": "timestamp", "timestamp with time zone": "timestamp",
    "timestamp": "timestamp", "timestamptz": "timestamp",
    "time without time zone": "time", "time": "time",
    "boolean": "string", "bool": "string", "bytea": "string", "interval": "string",
    "abstime": "string", "anyarray": "string", "array": "string",
}

# Conexion de Airflow segun el esquema (se puede sobreescribir con --conn)
CONN_POR_ESQUEMA = {
    "chi_easy_dim_vw": "redshift_corporativo_edw_easy",
    "chi_easy_dim_tb": "redshift_corporativo_edw_easy",
}
CONN_DEFAULT = "catman_redshift_cl_edw_prod"


class MwaaNoDisponible(Exception):
    """MWAA no responde (tipicamente: VPN caida) o no se pudo autenticar."""


PASO_ACTUAL = ""      # ultimo paso de run_flow (para el resumen de la cola)


class Pasos:
    def __init__(self, plan):
        self.plan, self.i = plan, 0

    def __call__(self, label):
        global PASO_ACTUAL
        PASO_ACTUAL = label
        self.i += 1
        print()
        print(f"{C.B}━━ [{self.i}/{len(self.plan)}] {label} {C.END}")
        print("─" * 60)

    def saltar(self, label, motivo=""):
        self.i += 1
        print()
        print(f"{C.DIM}━━ [{self.i}/{len(self.plan)}] {label}: no hace falta"
              + (f" ({motivo})" if motivo else "") + f"{C.END}")


def parse_iso(s):
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime
            d = parsedate_to_datetime(str(s))   # formato de la CLI v1
        except (TypeError, ValueError):
            return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def fmt_utc(d):
    return f"{d.astimezone(timezone.utc):%Y-%m-%d %H:%M}" if d else "?"


# ─── MWAA ─────────────────────────────────────────────────────────────────────

def _net_errors():
    import requests
    return (requests.exceptions.ConnectionError, requests.exceptions.Timeout)


def _explicar_red(host, e):
    txt = str(e)
    if "vpce" in host and any(k in txt for k in ("resolve", "nodename", "Name or service")):
        return ("El webserver de MWAA es privado (VPC endpoint) y su nombre no "
                "resuelve: la VPN no esta conectada.")
    if "timed out" in txt.lower() or "timeout" in txt.lower():
        return "MWAA no responde (timeout). Revisa la VPN."
    return f"Sin conexion con MWAA: {txt[:160]}"


def mwaa_session(timeout=15):
    try:
        import boto3
        import requests
    except ImportError as e:
        U.bad(f"Falta una dependencia: {e.name}")
        U.info("pip3 install boto3 requests")
        sys.exit(1)
    try:
        cli = boto3.session.Session(
            profile_name=MWAA_PROFILE, region_name=MWAA_REGION).client("mwaa")
        r = cli.create_web_login_token(Name=MWAA_ENV)
    except Exception as e:  # noqa: BLE001
        raise MwaaNoDisponible(f"No pude pedir el token de MWAA ({e}). "
                               f"Sesion SSO de {U.ACCOUNT_SRC} vencida?")
    host = r["WebServerHostname"]
    s = requests.Session()
    try:
        resp = s.post(f"https://{host}/aws_mwaa/login",
                      data={"token": r["WebToken"]}, allow_redirects=True,
                      timeout=timeout)
    except _net_errors() as e:
        raise MwaaNoDisponible(_explicar_red(host, e))
    if resp.status_code not in (200, 302):
        raise MwaaNoDisponible(f"Login MWAA HTTP {resp.status_code}")
    return s, host


def api(s, host, method, path, body=None, silencioso=False):
    try:
        r = s.request(method, f"https://{host}/api/v1/{path}", json=body, timeout=30)
    except _net_errors() as e:
        raise MwaaNoDisponible(_explicar_red(host, e))
    if r.status_code not in (200, 201, 204):
        if not silencioso:
            U.bad(f"HTTP {r.status_code}: {r.text[:200]}")
        return None
    if r.status_code == 204 or not r.text:
        return {}
    try:
        return r.json()
    except ValueError:
        return r.text


def tags_de(d):
    return {t.get("name") if isinstance(t, dict) else t for t in (d.get("tags") or [])}


def trigger_dag(s, host, dag_id, desde, hasta, fechas=None):
    """Dispara el DAG por conf ('Trigger w/ config'): el rango, o dias sueltos."""
    d = api(s, host, "GET", f"dags/{dag_id}")
    if d is None:
        U.bad(f"No encontre el DAG '{dag_id}' en MWAA.")
        return None, None
    if d.get("is_paused", True):
        print("  DAG pausado: activandolo...")
        api(s, host, "PATCH", f"dags/{dag_id}", {"is_paused": False})

    t0 = datetime.now(timezone.utc)
    conf = {"load_start": desde, "load_end": hasta}
    if fechas:
        conf["load_dates"] = ",".join(fechas)
    body = {"dag_run_id": f"manual__{t0:%Y-%m-%dT%H:%M:%SZ}", "conf": conf}
    if fechas:
        print(f"  conf: load_dates={len(fechas)} dias ({U.rangos(fechas, 4)})")
    else:
        print(f"  conf: load_start={desde}  load_end={hasta}")
    r = api(s, host, "POST", f"dags/{dag_id}/dagRuns", body)
    if not r:
        return None, None
    U.ok(f"Run disparado: {r.get('dag_run_id')}")
    return r.get("dag_run_id"), t0


def wait_run(s, host, dag_id, run_id, poll=20, max_fallos=8):
    """Espera el run. Tolera cortes breves de VPN (max_fallos x poll)."""
    terminal = {"success", "failed", "upstream_failed"}
    last, fallos, t0 = None, 0, time.time()
    while True:
        try:
            d = api(s, host, "GET", f"dags/{dag_id}/dagRuns/{run_id}")
            fallos = 0
        except MwaaNoDisponible:
            fallos += 1
            if fallos >= max_fallos:
                raise
            print(f"  [{datetime.now():%H:%M:%S}] sin conexion con MWAA "
                  f"({fallos}/{max_fallos}), reintento en {poll}s...")
            time.sleep(poll)
            continue
        if not d:
            return "unknown"
        st = (d.get("state") or "?").lower()
        if st != last:
            mins = int((time.time() - t0) / 60)
            print(f"  [{datetime.now():%H:%M:%S}] {st.upper()}  ({mins} min)")
            last = st
        if st in terminal:
            return st
        time.sleep(poll)


def show_failed_tasks(s, host, dag_id, run_id):
    d = api(s, host, "GET", f"dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    if not d:
        return
    for t in d.get("task_instances", []):
        if (t.get("state") or "").lower() in ("failed", "upstream_failed"):
            U.bad(f"task {t['task_id']}: {t.get('state')}")
    U.info(f"Detalle:  mwaa_cert log {dag_id}")


def esperar_dag(s, host, dag_id, desde_utc, exigir_strict, minutos=6):
    """Espera a que MWAA re-parsee el DAG despues de publicar el JSON.

    Senal de "ya tomo el JSON nuevo": last_parsed_time posterior a la subida.
    Si el JSON es estricto, ademas exige el tag SCHEMA-STRICT: si falta, el
    generador desplegado es el viejo y un JSON sincronizado le rompe el UNLOAD.
    """
    print(f"  Esperando que MWAA tome el JSON (hasta {minutos} min)", end="", flush=True)
    inicio = time.time()
    limite = inicio + minutos * 60
    while time.time() < limite:
        d = api(s, host, "GET", f"dags/{dag_id}", silencioso=True)
        if d:
            lp = parse_iso(d.get("last_parsed_time"))
            nuevo = (lp is not None and lp > desde_utc) or \
                    (lp is None and time.time() - inicio > 90)
            if nuevo:
                print()
                if d.get("has_import_errors"):
                    U.bad("MWAA reporta errores de import en el archivo del DAG.")
                    return False
                if exigir_strict and STRICT_TAG not in tags_de(d):
                    U.bad("El DAG no trae el tag SCHEMA-STRICT: en MWAA sigue el "
                          "generador viejo.")
                    return "viejo"
                U.ok("MWAA ya tomo el JSON nuevo.")
                return True
        print(".", end="", flush=True)
        time.sleep(15)
    print()
    U.bad(f"MWAA no re-parseo el DAG en {minutos} min.")
    U.info("Reintenta en un rato: el JSON ya esta en S3.")
    return False


# ─── Redshift y JSON ──────────────────────────────────────────────────────────

def rs_columns(schema, table):
    """Columnas y tipos de la tabla en Redshift."""
    try:
        import psycopg2
    except ImportError:
        U.bad("Falta psycopg2 (lo usa ing.py tambien).")
        U.info("pip3 install psycopg2-binary")
        sys.exit(1)

    cfg = dict(REDSHIFT)
    if not cfg["password"]:
        import getpass
        cfg["password"] = os.environ.get("FLOW_RS_PASS") or getpass.getpass(
            f"  Password de {cfg['user']}@{cfg['dbname']}: ")

    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 0")
    cols = [(d.name, d.type_code) for d in cur.description]
    out = []
    for name, code in cols:
        cur.execute("SELECT typname FROM pg_type WHERE oid = %s", (code,))
        r = cur.fetchone()
        out.append((name, r[0] if r else "varchar"))
    cur.close()
    conn.close()
    return out


def elegir_column_dt(columns, preferida=None):
    fechas = [c for c, t in columns
              if t.lower() in ("date", "timestamp", "timestamptz",
                               "timestamp without time zone")]
    if not fechas:
        U.bad("La tabla no tiene columnas de fecha; no se puede particionar.")
        sys.exit(1)
    if preferida and preferida in fechas:
        return preferida
    if "calendar_dt" in fechas:
        U.info("column_dt detectada: calendar_dt")
        return "calendar_dt"
    if len(fechas) == 1:
        U.info(f"column_dt detectada: {fechas[0]}")
        return fechas[0]
    print("\n  Columnas de fecha disponibles:")
    for i, f in enumerate(fechas, 1):
        print(f"    [{i}] {f}")
    while True:
        r = input("  Columna de particion: ").strip()
        if r.isdigit() and 1 <= int(r) <= len(fechas):
            return fechas[int(r) - 1]


def escribir_json(path, d):
    """Escribe el JSON del loader (columns_mapping compacto) y lo valida."""
    orden = ["schema", "table", "redshift_conn_id", "only_unload", "column_dt",
             "schema_source", "schema_synced_at"]
    claves = [k for k in orden if k in d] + [
        k for k in d if k not in orden and k != "columns_mapping" and not k.startswith("_")]
    lines = ["{"]
    for k in claves:
        lines.append(f"  {json.dumps(k)}: {json.dumps(d[k], ensure_ascii=False)},")
    lines.append('  "columns_mapping": {')
    items = list(d["columns_mapping"].items())
    for i, (k, v) in enumerate(items):
        coma = "," if i < len(items) - 1 else ""
        lines.append(f"    {json.dumps(k)}: [{json.dumps(v[0])}, {json.dumps(v[1])}]{coma}")
    lines += ["  }", "}"]
    txt = "\n".join(lines) + "\n"
    json.loads(txt)  # nunca escribir un JSON invalido: tumbaria el generador
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(txt)


def respaldar(path):
    if not path.exists():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    dst = BACKUP_DIR / f"{path.stem}.{datetime.now():%Y%m%d-%H%M%S}.json"
    dst.write_text(path.read_text())
    return dst


def ruta_json_nuevo(schema, table):
    """Donde va el JSON de backfill de una tabla nueva.

    {table}.json, salvo que ese nombre ya lo use otro loader (tipicamente el
    FCSM normal de la misma tabla, que ing.py guarda asi): ahi va
    {table}_backfill.json, para no pisarlo.
    """
    path = LOADERS_PATH / f"{table}.json"
    if path.exists():
        try:
            d = json.loads(path.read_text())
        except ValueError:
            d = {}
        if not (d.get("schema") == schema and d.get("table") == table
                and d.get("only_unload") is True):
            path = LOADERS_PATH / f"{table}_backfill.json"
    return path


def json_desde_destino(schema, table, glue_tbl, fuente, conn_id):
    """JSON de backfill armado desde el Glue Catalog de la tabla destino.

    fuente: columnas del origen (Redshift) si se pudieron leer; sin ellas se
    asume que el origen tiene todas las columnas del destino (salvo las de
    auditoria, que se calculan). Si alguna falta, el UNLOAD falla con
    "column ... does not exist" y no se mueve nada.
    """
    sd = glue_tbl["StorageDescriptor"]
    cols = [c["Name"] for c in sd.get("Columns", [])]
    pks = [p["Name"] for p in (glue_tbl.get("PartitionKeys") or [])]
    if fuente is None:
        fuente = (set(cols) - set(AUDIT_EXPRS)) | set(pks)
        U.warn("Sin acceso a Redshift (FLOW_RS_PASS): asumo que el origen tiene las")
        U.warn("columnas del destino. Si falta alguna, el UNLOAD falla y no se mueve nada.")
    mapping, col_dt, acciones, errores = sincronizar({"columns_mapping": {}}, glue_tbl, fuente)
    if errores:
        for e in errores:
            U.bad(e)
        return None
    nulas = [a[1] for a in acciones if a[0] == "sin_origen"]
    if nulas:
        U.warn(f"{len(nulas)} columnas del destino no estan en el origen: iran con NULL "
               f"({', '.join(nulas[:5])}{'...' if len(nulas) > 5 else ''})")
    path = ruta_json_nuevo(schema, table)
    if path.exists():
        respaldar(path)
    escribir_json(path, {
        "schema": schema, "table": table, "redshift_conn_id": conn_id,
        "only_unload": True, "column_dt": col_dt,
        "schema_source": f"glue:{U.GLUE_DB}.{glue_tbl.get('Name') or U.GLUE_TABLE_TPL.format(schema=schema, table=table)}",
        "schema_synced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "columns_mapping": mapping,
    })
    U.ok(f"JSON generado desde el destino: {path.name}  "
         f"({len(mapping) - 1} columnas + particion '{col_dt}')")
    U.info(f"{path}")
    return path


def generar_json(cfg, prep):
    """Crea el JSON de una tabla que no lo tiene. Devuelve (ok, columnas_origen).

    Con tabla destino en el Catalog, el JSON sale de ahi (no hace falta
    Redshift). Redshift se usa solo si hay FLOW_RS_PASS (para detectar
    columnas que el origen no tiene), o si no hay tabla destino.
    """
    schema, table = cfg["schema"], cfg["table"]
    conn_id = prep.get("conn_id") or CONN_POR_ESQUEMA.get(schema, CONN_DEFAULT)
    print(f"  redshift_conn_id: {conn_id}")
    g = U.aws(["glue", "get-table", "--database-name", U.GLUE_DB,
               "--name", cfg["_glue_table"]], U.PROFILE_DST, check=False)
    columns = fuente = None
    if g is None or os.environ.get("FLOW_RS_PASS"):
        try:
            columns = rs_columns(schema, prep["table_real"])
            fuente = {c for c, _ in columns}
            print(f"  {len(columns)} columnas leidas de Redshift ({schema}.{prep['table_real']})")
        except Exception as e:  # noqa: BLE001
            if g is None:
                U.bad(f"No pude leer Redshift: {e}")
                return False, None
            U.warn(f"No pude leer Redshift ({e}): se arma solo con el destino.")
    if g is None:
        U.warn(f"{U.GLUE_DB}.{cfg['_glue_table']} no existe: el JSON sale solo de Redshift.")
        column_dt = elegir_column_dt(columns, prep.get("column_dt"))
        path, _ = escribir_loader_json(schema, table, column_dt, columns, conn_id)
    else:
        print(f"  Destino: {U.GLUE_DB}.{cfg['_glue_table']}")
        path = json_desde_destino(schema, table, g["Table"], fuente, conn_id)
        if path is None:
            return False, None
    cfg.update(json.loads(path.read_text()))
    cfg["_file"] = path
    return True, fuente


def escribir_loader_json(schema, table, column_dt, columns, conn_id):
    """JSON inicial del loader (backfill) a partir de las columnas de Redshift."""
    path = ruta_json_nuevo(schema, table)
    if path.exists():
        U.warn(f"Ya existe {path.name}")
        if not U.confirm("Sobreescribir? [y/N]", riesgo=True):
            U.info("Se usa el JSON existente.")
            return path, False
        respaldar(path)
    escribir_json(path, {
        "schema": schema, "table": table, "redshift_conn_id": conn_id,
        "only_unload": True, "column_dt": column_dt,
        "columns_mapping": {c: [c, TYPE_MAPPING.get(t.lower(), "string")]
                            for c, t in columns},
    })
    U.ok(f"JSON generado: {path}")
    return path, True


def subir_loader_s3(path):
    uri = f"s3://{S3_DAGS_BUCKET}/{S3_LOADERS_PREFIX}/{path.name}"
    print(f"  {path.name}  ->  {uri}")
    r = U.aws(["s3", "cp", str(path), uri, "--only-show-errors"],
              MWAA_PROFILE, parse=False, check=False)
    if r is None:
        U.bad("Fallo la subida a S3.")
        return False
    U.ok("JSON publicado en S3.")
    return True


def json_publicado(path):
    """(dict, texto crudo) del JSON que hoy esta en S3, o (None, None)."""
    uri = f"s3://{S3_DAGS_BUCKET}/{S3_LOADERS_PREFIX}/{path.name}"
    txt = U.aws(["s3", "cp", uri, "-"], MWAA_PROFILE, parse=False, check=False)
    if not txt:
        return None, None
    try:
        return json.loads(txt), txt
    except ValueError:
        return None, txt


def revertir_publicacion(path, crudo_anterior):
    """Deja en S3 el JSON que habia antes (o lo retira si no habia)."""
    uri = f"s3://{S3_DAGS_BUCKET}/{S3_LOADERS_PREFIX}/{path.name}"
    if crudo_anterior is None:
        U.aws(["s3", "rm", uri, "--only-show-errors"], MWAA_PROFILE, parse=False, check=False)
        U.info("Se retiro el JSON de S3: MWAA vuelve a como estaba.")
        return
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = BACKUP_DIR / f".restaurar.{path.name}"
    tmp.write_text(crudo_anterior)
    U.aws(["s3", "cp", str(tmp), uri, "--only-show-errors"], MWAA_PROFILE, parse=False, check=False)
    tmp.unlink(missing_ok=True)
    U.info("Se restauro en S3 el JSON anterior: MWAA vuelve a como estaba.")


def publicar(path, s, host, dag_id, exigir_strict):
    """Sube el JSON solo si difiere del publicado y espera que MWAA lo tome."""
    local = json.loads(path.read_text())
    remoto, crudo = json_publicado(path)
    if remoto == local:
        U.ok("El JSON publicado en S3 ya es igual al local.")
        d = api(s, host, "GET", f"dags/{dag_id}", silencioso=True)
        if d is None:
            U.bad(f"El DAG '{dag_id}' no existe en MWAA.")
            U.info("Revisa errores de import del generador en la UI de MWAA.")
            return False
        if exigir_strict and STRICT_TAG not in tags_de(d):
            U.bad("El DAG no trae el tag SCHEMA-STRICT: en MWAA sigue el generador viejo.")
            U.info("Desplegalo con:  flow --desplegar-generador")
            return False
        return True

    # Antes de subir un JSON estricto: el generador desplegado tiene que entenderlo.
    # Si no, el DAG quedaria roto en MWAA.
    if exigir_strict:
        k, txt = buscar_generador_s3()
        if k and not generador_es_estricto(txt):
            U.bad("El generador desplegado en MWAA es el viejo: no entiende JSON sincronizados.")
            U.info("No se publico nada. Desplegalo con:  flow --desplegar-generador")
            return False
        if not k:
            U.warn("No pude revisar el generador desplegado; se verifica despues de subir.")

    U.info("El JSON local difiere del publicado." if remoto is not None
           else "El JSON todavia no esta publicado en S3.")
    t_up = datetime.now(timezone.utc)
    if not subir_loader_s3(path):
        return False
    r = esperar_dag(s, host, dag_id, t_up, exigir_strict)
    if r == "viejo":
        revertir_publicacion(path, crudo)
        U.info("Desplega el generador con:  flow --desplegar-generador")
        return False
    return r is True


# ─── generador desplegado ─────────────────────────────────────────────────────

S3_GENERATOR_DIR = S3_LOADERS_PREFIX.rsplit("/definitions", 1)[0] + "/"
KIT_GENERATOR = Path(__file__).resolve().parent / "generator" / "cencosud_cl_sm_raw_generator.py"


def buscar_generador_s3():
    """(key, texto) del generador de loaders desplegado en S3, o (None, None)."""
    out = U.aws(["s3api", "list-objects-v2", "--bucket", S3_DAGS_BUCKET,
                 "--prefix", S3_GENERATOR_DIR, "--delimiter", "/"],
                MWAA_PROFILE, check=False)
    if not isinstance(out, dict):
        return None, None
    for o in out.get("Contents", []) or []:
        k = o["Key"]
        if not k.endswith(".py"):
            continue
        txt = U.aws(["s3", "cp", f"s3://{S3_DAGS_BUCKET}/{k}", "-"],
                    MWAA_PROFILE, parse=False, check=False)
        if txt and "RedshiftLoaderBuilder" in txt:
            return k, txt
    return None, None


def generador_es_estricto(txt):
    return bool(txt) and "set_schema_source" in txt and STRICT_TAG in txt


def generador_con_dias(txt):
    """El generador acepta dias sueltos (param load_dates, kit v9.5+)."""
    return bool(txt) and "load_dates" in txt and "fechas_sql" in txt


# Lineas de versiones anteriores del generador que el del kit reemplaza a
# proposito: no son personalizaciones tuyas.
KIT_REEMPLAZADAS = {
    'tags=["RAW-DATA-PIPELINE", "FCSM"],',
    "WHERE {self.column_dt} BETWEEN '{{{{ params.load_start }}}}' AND "
    "'{{{{ params.load_end }}}}'",
}


def cmd_desplegar_generador():
    """Reemplaza el generador desplegado por el del kit, en su MISMA ruta."""
    U.title("Desplegar el generador del kit")
    if not KIT_GENERATOR.exists():
        U.bad(f"No encuentro el generador del kit: {KIT_GENERATOR}")
        return False
    k, actual = buscar_generador_s3()
    if not k:
        U.bad(f"No encontre el generador en s3://{S3_DAGS_BUCKET}/{S3_GENERATOR_DIR}")
        return False
    uri = f"s3://{S3_DAGS_BUCKET}/{k}"
    print(f"  Desplegado : {uri}")
    if generador_es_estricto(actual) and generador_con_dias(actual):
        U.ok("Ya tiene modo estricto y dias sueltos: no hay nada que desplegar.")
        return True
    U.info("Le falta: " + ", ".join(
        f for f, ok in (("modo estricto", generador_es_estricto(actual)),
                        ("dias sueltos (load_dates)", generador_con_dias(actual))) if not ok))

    nuevo = KIT_GENERATOR.read_text()
    # lineas del desplegado que el nuevo no trae: si hay personalizaciones, se ven aca
    norm = lambda s: {l.strip() for l in s.splitlines() if l.strip() and not l.strip().startswith("#")}
    todas = norm(actual) - norm(nuevo)
    conocidas = sorted(todas & KIT_REEMPLAZADAS)
    perdidas = sorted(todas - KIT_REEMPLAZADAS)
    print(f"  Nuevo      : {KIT_GENERATOR}")
    if conocidas:
        U.info(f"{len(conocidas)} linea(s) de la version anterior del kit se reemplazan "
               "(esperado):")
        for l in conocidas:
            U.info(f"  {l[:100]}")
    if perdidas:
        print()
        U.warn(f"{len(perdidas)} lineas del generador desplegado no estan en el nuevo:")
        for l in perdidas[:12]:
            U.info(l[:100])
        if len(perdidas) > 12:
            U.info(f"... y {len(perdidas) - 12} mas")
        U.info("Si alguna es una personalizacion tuya, no despliegues y avisame.")

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    bak = BACKUP_DIR / f"generador.{Path(k).stem}.{datetime.now():%Y%m%d-%H%M%S}.py"
    bak.write_text(actual)
    U.info(f"Respaldo del desplegado: {bak}")
    print()
    if not U.confirm("Reemplazar el generador desplegado (misma ruta)? [y/N]", destructivo=True):
        print("  Cancelado.")
        return False

    # misma key: nunca un segundo archivo (duplicaria los dag_id en MWAA)
    r = U.aws(["s3", "cp", str(KIT_GENERATOR), uri, "--only-show-errors"],
              MWAA_PROFILE, parse=False, check=False)
    txt = U.aws(["s3", "cp", uri, "-"], MWAA_PROFILE, parse=False, check=False)
    if r is None or not (generador_es_estricto(txt) and generador_con_dias(txt)):
        U.bad("No pude verificar el despliegue.")
        return False
    U.ok("Generador desplegado y verificado en S3.")

    local = LOADERS_PATH.parent.parent / Path(k).name
    if local.exists() and local.read_text() != nuevo:
        local.with_suffix(".py.bak").write_text(local.read_text())
        local.write_text(nuevo)
        U.ok(f"Repo local actualizado: {local}  (respaldo .py.bak)")
    print()
    U.warn("Commitea en el repo el generador y los JSON sincronizados: si el")
    U.warn("CI/CD despliega desde git, pisaria el generador y MWAA volveria al viejo.")
    U.info("MWAA lo toma en 1-2 min. Despues:  flow <tabla> --solo-sync")
    return True


# ─── sincronizacion con la tabla destino ──────────────────────────────────────

def tipo_norm(t):
    t = (t or "").strip().lower().replace(" ", "")
    return {"integer": "int"}.get(t, t)


def columnas_origen(cfg, prep_cols=None):
    """Columnas que existen en la tabla de origen (Redshift)."""
    if prep_cols is not None:
        return set(prep_cols)
    if os.environ.get("FLOW_RS_PASS"):
        tabla = cfg["table"][:-5] if cfg["table"].endswith("_fcsm") else cfg["table"]
        try:
            return {c for c, _ in rs_columns(cfg["schema"], tabla)}
        except Exception as e:  # noqa: BLE001
            U.warn(f"No pude leer Redshift ({e}); uso las columnas del JSON como origen.")
    m = cfg.get("columns_mapping", {}) or {}
    return {c for c, spec in m.items() if spec and spec[0] == c}


def sincronizar(cfg, glue_tbl, fuente):
    """Arma el columns_mapping que produce EXACTAMENTE lo que espera el destino.

    Devuelve (mapping, column_dt, acciones, errores).
    """
    sd = glue_tbl["StorageDescriptor"]
    cat = [(c["Name"], c.get("Type", "string")) for c in sd.get("Columns", [])]
    pks = [(p["Name"], p.get("Type", "string"))
           for p in (glue_tbl.get("PartitionKeys") or [])]
    actual = cfg.get("columns_mapping", {}) or {}
    acciones, errores = [], []

    if len(pks) != 1:
        errores.append(
            "El destino no esta particionado por una sola columna "
            f"({len(pks)} claves): el backfill por UNLOAD necesita exactamente una.")
        return None, None, acciones, errores
    pk, pk_tipo = pks[0]

    nuevo = {}
    for name, typ in cat:
        if name in actual:
            expr, t_old = actual[name][0], actual[name][1]
            nuevo[name] = [expr, typ]
            if tipo_norm(t_old) != tipo_norm(typ):
                acciones.append(("tipo", name, t_old, typ))
        elif name in fuente:
            nuevo[name] = [name, typ]
            acciones.append(("agregar", name, None, typ))
        elif name in AUDIT_EXPRS:
            nuevo[name] = [AUDIT_EXPRS[name], typ]
            acciones.append(("auditoria", name, None, f"{typ} = {AUDIT_EXPRS[name]}"))
        else:
            nuevo[name] = ["NULL", typ]
            acciones.append(("sin_origen", name, None, typ))

    if pk in actual or pk in fuente:
        nuevo[pk] = [actual[pk][0] if pk in actual else pk, pk_tipo]
        if cfg.get("column_dt") != pk:
            acciones.append(("particion", pk, cfg.get("column_dt"), pk))
    else:
        errores.append(f"La clave de particion del destino '{pk}' no existe en el origen.")

    for c in actual:
        if c not in nuevo:
            acciones.append(("quitar", c, actual[c][1], None))

    comunes_antes = [c for c in actual if c in nuevo]
    comunes_despues = [c for c in nuevo if c in actual]
    if comunes_antes != comunes_despues:
        acciones.append(("orden", None, None, None))

    return nuevo, pk, acciones, errores


def mostrar_diff(acciones):
    fmt = {
        "tipo": lambda a: f"tipo       {a[2]}  ->  {a[3]}",
        "agregar": lambda a: f"agregar    {a[3]}",
        "auditoria": lambda a: f"agregar    {a[3]}   (auditoria)",
        "sin_origen": lambda a: f"agregar    NULL::{a[3]}   (no existe en el origen)",
        "quitar": lambda a: "quitar     (el destino no la tiene)",
        "particion": lambda a: f"particion  {a[2]}  ->  {a[3]}",
    }
    filas = [a for a in acciones if a[0] != "orden"]
    if filas:
        w = max(len(a[1]) for a in filas)
        for a in filas:
            print(f"    {a[1]:<{w}}  {fmt[a[0]](a)}")
    if any(a[0] == "orden" for a in acciones):
        print("    (orden de columnas ajustado al del destino)")
    cnt = Counter(a[0] for a in acciones if a[0] != "orden")
    if cnt:
        print()
        print("  " + ", ".join(f"{v} {k}" for k, v in cnt.items()))


def paso_sync(cfg, glue_tbl, fuente):
    """Deja el JSON identico al destino. True = cambio, False = igual, None = error."""
    if cfg.get("only_unload") is not True:
        U.bad('Solo se sincronizan JSON de backfill ("only_unload": true).')
        return None
    mapping, col_dt, acciones, errores = sincronizar(cfg, glue_tbl, fuente)
    if errores:
        for e in errores:
            U.bad(e)
        return None

    marcador = f"glue:{U.GLUE_DB}.{cfg['_glue_table']}"
    n_datos = len([c for c in mapping if c != col_dt])
    print(f"  Destino : {marcador}")
    print(f"  Esquema : {n_datos} columnas + particion '{col_dt}'")
    print()

    sin_marcar = cfg.get("schema_source") != marcador or cfg.get("only_unload") is not True
    if not acciones and not sin_marcar:
        U.ok("El JSON ya es identico al destino.")
        return False

    if acciones:
        mostrar_diff(acciones)
    else:
        U.info("Columnas y tipos ya coinciden; falta marcar el JSON como sincronizado.")
    if any(a[0] == "sin_origen" for a in acciones):
        print()
        U.warn("Hay columnas del destino que el origen no tiene: iran con NULL.")

    print()
    if U.AUTO:
        print(f"  Aplicar al JSON? [Y/n] {C.DIM}[auto: si]{C.END}")
    elif input("  Aplicar al JSON? [Y/n] ").strip().lower() in ("n", "no"):
        U.warn("JSON sin cambios.")
        return False

    path = Path(cfg["_file"])
    bak = respaldar(path)
    nuevo = {k: v for k, v in cfg.items() if not k.startswith("_")}
    nuevo.update({
        "only_unload": True,
        "column_dt": col_dt,
        "schema_source": marcador,
        "schema_synced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "columns_mapping": mapping,
    })
    escribir_json(path, nuevo)
    cfg.update(nuevo)
    U.ok(f"JSON actualizado: {path}")
    if bak:
        U.info(f"Respaldo del anterior: {bak}")
    return True


# ─── landing: que se puede reutilizar ─────────────────────────────────────────
#
# Una particion que ya esta en el landing se mueve SIN volver a correr el
# UNLOAD solo si se puede confiar en ella:
#   1. es posterior al JSON vigente (schema_synced_at): si el JSON cambio
#      despues, esa particion salio con otro SELECT;
#   2. la escribio un run del DAG que termino OK: un run fallido puede dejar
#      archivos a medias;
#   3. su parquet tiene exactamente las columnas y los tipos de la tabla
#      destino. Se lee solo el footer de un archivo por particion (unos KB).
# Lo que no pasa las tres se vuelve a bajar de Redshift.

# Desfase tolerado entre el reloj de S3 (LastModified) y el de MWAA.
MARGEN_RELOJ = timedelta(seconds=60)


class _Thrift:
    """Lector minimo del protocolo compacto de Thrift (footer de parquet)."""

    def __init__(self, b):
        self.b, self.i = b, 0

    def _byte(self):
        v = self.b[self.i]
        self.i += 1
        return v

    def _varint(self):
        r = sh = 0
        while True:
            x = self._byte()
            r |= (x & 0x7F) << sh
            if not x & 0x80:
                return r
            sh += 7

    def _zz(self):
        n = self._varint()
        return (n >> 1) ^ -(n & 1)

    def _valor(self, t):
        if t in (1, 2):                     # bool: en un struct viaja en el header
            return t == 1
        if t == 3:                          # i8
            v = self._byte()
            return v - 256 if v > 127 else v
        if t in (4, 5, 6):                  # i16 / i32 / i64
            return self._zz()
        if t == 7:                          # double (no hace falta el valor)
            self.i += 8
            return None
        if t == 8:                          # binary / string
            n = self._varint()
            v = self.b[self.i:self.i + n]
            self.i += n
            return v
        if t in (9, 10):                    # list / set
            h = self._byte()
            n, et = h >> 4, h & 0x0F
            if n == 15:
                n = self._varint()
            return [self._elem(et) for _ in range(n)]
        if t == 11:                         # map
            n = self._varint()
            if not n:
                return []
            kv = self._byte()
            return [(self._elem(kv >> 4), self._elem(kv & 0x0F)) for _ in range(n)]
        if t == 12:
            return self.struct()
        raise ValueError(f"tipo thrift desconocido: {t}")

    def _elem(self, t):
        if t in (1, 2):                     # bool dentro de una coleccion: 1 byte
            return self._byte() == 1
        return self._valor(t)

    def struct(self):
        out, fid = {}, 0
        while True:
            h = self._byte()
            if h == 0:
                return out
            d, t = h >> 4, h & 0x0F
            fid = fid + d if d else self._zz()
            out[fid] = self._valor(t)


# SchemaElement: 1 type, 4 name, 5 num_children, 6 converted_type,
# 7 scale, 8 precision, 10 logicalType
PQ_FISICO = {0: "boolean", 1: "int", 2: "bigint", 3: "timestamp", 4: "float",
             5: "double", 6: "binary", 7: "binary"}
PQ_CONVERTIDO = {0: "string", 4: "string", 19: "string", 6: "date", 7: "time",
                 8: "time", 9: "timestamp", 10: "timestamp", 11: "tinyint",
                 12: "smallint", 13: "int", 14: "bigint", 15: "tinyint",
                 16: "smallint", 17: "int", 18: "bigint"}
PQ_LOGICO = {1: "string", 4: "string", 12: "string", 6: "date", 7: "time",
             8: "timestamp"}


def familia_parquet(el):
    lt = el.get(10) or {}
    if 5 in lt:
        return f"decimal({lt[5].get(2)},{lt[5].get(1, 0)})"
    if el.get(6) == 5:
        return f"decimal({el.get(8)},{el.get(7) or 0})"
    for k, fam in PQ_LOGICO.items():
        if k in lt:
            return fam
    if 10 in lt:
        return {8: "tinyint", 16: "smallint", 32: "int", 64: "bigint"}.get(
            lt[10].get(1), "int")
    if el.get(6) in PQ_CONVERTIDO:
        return PQ_CONVERTIDO[el[6]]
    return PQ_FISICO.get(el.get(1), "?")


def columnas_parquet(footer):
    """[(columna, familia)] del footer (FileMetaData) de un parquet."""
    els = _Thrift(footer).struct().get(2) or []
    if not els:
        raise ValueError("el footer no trae esquema")

    def saltar(i):
        n = els[i].get(5) or 0
        i += 1
        for _ in range(n):
            i = saltar(i)
        return i

    cols, i = [], 1                         # els[0] es la raiz
    while i < len(els):
        el = els[i]
        nombre = el.get(4, b"").decode("utf-8", "replace")
        if el.get(5):                       # grupo anidado: Redshift no los escribe
            cols.append((nombre, "anidada"))
            i = saltar(i)
        else:
            cols.append((nombre, familia_parquet(el)))
            i += 1
    return cols


def leer_esquema_parquet(s3, bucket, key, size):
    """Lee SOLO el final del archivo (rango de bytes) y devuelve sus columnas."""
    def cola(desde):
        return s3.get_object(Bucket=bucket, Key=key,
                             Range=f"bytes={desde}-{size - 1}")["Body"].read()
    b = cola(max(0, size - 65536))
    if len(b) < 12 or b[-4:] != b"PAR1":
        raise ValueError("no es un parquet")
    n = int.from_bytes(b[-8:-4], "little")
    if n + 8 > size:
        raise ValueError("footer invalido")
    if n + 8 > len(b):
        b = cola(size - n - 8)
    return columnas_parquet(b[-8 - n:-8])


def familia_glue(t):
    t = (t or "").strip().lower().replace(" ", "")
    if t == "string" or t.startswith(("varchar", "char")):
        return "string"
    if t == "integer":
        return "int"
    if t == "decimal":
        return "decimal(10,0)"
    return t


ENTEROS_32 = {"int", "smallint", "tinyint"}   # en parquet los tres son INT32


def tipos_compatibles(destino, parquet):
    if destino == parquet:
        return True
    if destino == "string":
        return parquet in ("string", "binary")
    return destino in ENTEROS_32 and parquet in ENTEROS_32


def comparar_esquema(pq_cols, glue_cols):
    """None si el parquet se lee tal cual en la tabla destino; si no, el motivo."""
    pq = {n.lower(): f for n, f in pq_cols}
    dest = [(c["Name"].lower(), familia_glue(c.get("Type"))) for c in glue_cols]
    faltan = [n for n, _ in dest if n not in pq]
    malos = [(n, t, pq[n]) for n, t in dest if n in pq and not tipos_compatibles(t, pq[n])]
    sobran = [n for n in pq if n not in dict(dest)]
    if faltan:
        return f"le faltan {len(faltan)} columnas del destino (ej. {', '.join(faltan[:3])})"
    if malos:
        n, t, f = malos[0]
        return f"{len(malos)} columnas con otro tipo (ej. {n}: parquet {f}, destino {t})"
    if sobran:
        return f"trae {len(sobran)} columnas que el destino no tiene (ej. {', '.join(sobran[:3])})"
    return None


def _esquemas_que_no_calzan(cfg, glue_tbl, enc):
    """{fecha: (motivo, detalle)} de las particiones cuyo parquet no calza."""
    try:
        import boto3
        s3 = boto3.session.Session(profile_name=U.PROFILE_SRC,
                                   region_name=MWAA_REGION).client("s3")
    except Exception as e:  # noqa: BLE001
        return {f: ("no pude revisar el parquet", str(e)[:160]) for f in enc}
    from concurrent.futures import ThreadPoolExecutor
    col = cfg.get("column_dt", "calendar_dt")
    p_src = U.PREFIX_SRC_TPL.format(schema=cfg["schema"], table=cfg["table"])
    glue_cols = glue_tbl["StorageDescriptor"].get("Columns", [])

    def una(f):
        arch = {n: t for n, t in enc[f]["archivos"].items() if t > 0}
        cand = sorted(n for n in arch if n.endswith(".parquet")) or sorted(arch)
        if not cand:
            return f, ("particion sin archivos", "")
        try:
            cols = leer_esquema_parquet(s3, U.BUCKET_SRC, f"{p_src}{col}={f}/{cand[0]}",
                                        arch[cand[0]])
        except Exception as e:  # noqa: BLE001
            return f, ("no pude leer el parquet", f"{cand[0]}: {str(e)[:140]}")
        m = comparar_esquema(cols, glue_cols)
        return f, (("esquema distinto al destino", m) if m else None)

    print(f"  Leyendo el esquema del parquet de {len(enc)} particiones...", end=" ", flush=True)
    with ThreadPoolExecutor(max_workers=8) as ex:
        res = dict(ex.map(una, sorted(enc)))
    print("listo")
    return {f: m for f, m in res.items() if m}


def runs_del_dag(s, host, dag_id, desde):
    """[(inicio, fin, estado, run_id)] de los runs creados despues de 'desde'.

    Filtra por execution_date (y no start_date) para incluir los que estan en
    cola, que todavia no tienen start_date.
    """
    q = (f"dags/{dag_id}/dagRuns?limit=100"
         f"&execution_date_gte={desde.astimezone(timezone.utc):%Y-%m-%dT%H:%M:%S}Z")
    d = api(s, host, "GET", q, silencioso=True)
    if not isinstance(d, dict):
        return None
    return [(parse_iso(r.get("start_date")), parse_iso(r.get("end_date")),
             (r.get("state") or "?").lower(), r.get("dag_run_id"))
            for r in d.get("dag_runs", [])]


def revisar_landing(cfg, glue_tbl, fechas=None, s=None, host=None, dag_id=None):
    """Clasifica las particiones del landing. Devuelve (sirven, descartes, faltan).

    fechas: las pedidas (None = todo lo que haya en el landing).
    sirven: se pueden mover tal cual.  descartes: {fecha: (motivo, detalle)}.
    faltan: pedidas que no estan en el landing.
    """
    col = cfg.get("column_dt", "calendar_dt")
    p_src = U.PREFIX_SRC_TPL.format(schema=cfg["schema"], table=cfg["table"])
    tam = U.particiones_s3(U.BUCKET_SRC, p_src, U.PROFILE_SRC)
    enc = {p.split("=", 1)[1]: v for p, v in tam.items() if p.startswith(col + "=")}
    faltan = []
    if fechas is not None:
        pedidas = set(fechas)
        enc = {f: v for f, v in enc.items() if f in pedidas}
        faltan = sorted(pedidas - set(enc))
    if not enc:
        print("  El landing no tiene particiones" + (" de esas fechas." if fechas else "."))
        return [], {}, faltan

    nunca = datetime.min.replace(tzinfo=timezone.utc)   # fecha ilegible: se trata como vieja
    lm = {f: (parse_iso(v["lm_min"]) or nunca, parse_iso(v["lm_max"]) or nunca)
          for f, v in enc.items()}
    t_min = min(a for a, _ in lm.values())
    print(f"  En el landing : {len(enc)} particiones, "
          f"{U.human(sum(v['bytes'] for v in enc.values()))}, escritas "
          f"{fmt_utc(t_min)} .. {fmt_utc(max(z for _, z in lm.values()))} UTC")

    descartes = {}
    vigente = parse_iso(cfg.get("schema_synced_at"))
    if vigente:
        for f, (a, _) in lm.items():
            if a < vigente:
                descartes[f] = ("anteriores al JSON vigente",
                                f"el JSON se sincronizo {fmt_utc(vigente)} UTC")

    if s is None:
        U.warn("Sin conexion con MWAA: no puedo confirmar que run las escribio;")
        U.info("se valida solo la fecha del JSON y el esquema del parquet.")
    else:
        runs = runs_del_dag(s, host, dag_id, t_min - timedelta(days=2))
        if runs is None:
            U.warn("No pude leer los runs del DAG en MWAA.")
        for f, (a, z) in lm.items():
            if f in descartes:
                continue
            if runs is None:
                descartes[f] = ("no pude confirmar el run que las escribio", "")
                continue
            # quien la escribio: el ULTIMO run que empezo antes de su primer
            # archivo (los runs no se solapan: max_active_runs=1). Con un margen
            # amplio, un tramo que falla justo despues de otro exitoso quedaria
            # atribuido al exitoso.
            previos = [r for r in runs if r[0] and r[0] - MARGEN_RELOJ <= a]
            run = max(previos, key=lambda r: r[0]) if previos else None
            if run is None or (run[1] is not None and z > run[1] + MARGEN_RELOJ):
                descartes[f] = ("no las escribio ningun run del DAG", "")
            elif run[2] != "success":
                descartes[f] = ("escritas por un run que no termino OK", f"{run[3]}: {run[2]}")

    pendientes = {f: enc[f] for f in enc if f not in descartes}
    if pendientes and glue_tbl is None:
        U.warn("La tabla destino no esta en el Catalog: no se valida el esquema del parquet.")
    elif pendientes:
        descartes.update(_esquemas_que_no_calzan(cfg, glue_tbl, pendientes))

    sirven = sorted(f for f in enc if f not in descartes)
    print(f"  {C.OK}✓{C.END} {len(sirven)} se pueden mover tal cual"
          + (f": {U.rangos(sirven, 3)}" if sirven else ""))
    if descartes:
        U.warn(f"{len(descartes)} no se reutilizan:")
        por_motivo = {}
        for f, (m, det) in sorted(descartes.items()):
            por_motivo.setdefault(m, ([], det))[0].append(f)
        for m, (fs, det) in por_motivo.items():
            U.info(f"{m}: {U.rangos(fs, 3)}")
            if det:
                U.info(f"  ej. {det}")
    if faltan:
        print(f"  · {len(faltan)} fechas no estan en el landing: {U.rangos(faltan, 3)}")
    return sirven, descartes, faltan


def particiones_de_la_corrida(cfg, desde_utc, margen_min=5):
    """Fechas del landing escritas desde desde_utc (una corrida del DAG)."""
    col = cfg.get("column_dt", "calendar_dt")
    p_src = U.PREFIX_SRC_TPL.format(schema=cfg["schema"], table=cfg["table"])
    corte = desde_utc - timedelta(minutes=margen_min)
    tam = U.particiones_s3(U.BUCKET_SRC, p_src, U.PROFILE_SRC)
    return sorted(p.split("=", 1)[1] for p, v in tam.items()
                  if p.startswith(col + "=") and parse_iso(v["lm_max"]) >= corte)


def rango_a_parts(desde, hasta):
    d = datetime.strptime(desde, "%Y-%m-%d").date()
    h = datetime.strptime(hasta, "%Y-%m-%d").date()
    out = []
    while d <= h:
        out.append(str(d))
        d += timedelta(days=1)
    return out


# ─── pipeline ─────────────────────────────────────────────────────────────────

def esperar_unload(cfg, s, host, dag_id, run_id, t0, sel):
    """Espera un run. Devuelve el dagRun (dict) si termino OK, si no None."""
    if t0 is None and not api(s, host, "GET", f"dags/{dag_id}/dagRuns/{run_id}"):
        U.bad(f"No encontre el run '{run_id}'.")
        return None
    U.info("Ctrl-C corta la espera; el DAG sigue corriendo en MWAA.")
    try:
        state = wait_run(s, host, dag_id, run_id)
    except MwaaNoDisponible as e:
        U.bad(str(e))
        U.info(f"El DAG sigue corriendo en MWAA (run {run_id}).")
        U.info(f"Cuando vuelva la VPN:  flow {sel} --run {run_id}")
        return None
    if state != "success":
        U.bad(f"El DAG termino en {state.upper()}. No se mueve nada.")
        show_failed_tasks(s, host, dag_id, run_id)
        return None
    U.ok("UNLOAD completado.")
    r = api(s, host, "GET", f"dags/{dag_id}/dagRuns/{run_id}", silencioso=True) or {}
    t_ini = parse_iso(r.get("start_date")) or t0
    if t_ini:
        conf = r.get("conf") or {}
        parts = [p for p in particiones_de_la_corrida(cfg, t_ini)
                 if conf.get("load_start", "") <= p <= conf.get("load_end", "9999")]
        pedidas = {f.strip() for f in str(conf.get("load_dates") or "").split(",") if f.strip()}
        if pedidas:
            extra = [p for p in parts if p not in pedidas]
            parts = [p for p in parts if p in pedidas]
            if extra:
                U.warn(f"El UNLOAD escribio {len(extra)} dias que no se pidieron "
                       f"({U.rangos(extra, 3)}): no se mueven.")
                U.info("El DAG ignoro load_dates: desplega el generador del kit "
                       "(flow --desplegar-generador).")
        if parts:
            print(f"  Esta corrida escribio {len(parts)} particiones: {U.rangos(parts, 3)}")
        else:
            U.warn("Esta corrida no escribio particiones: el origen no tiene datos para ese rango.")
    return r


_SOPORTA_DIAS = {}


def dag_soporta_dias(s, host, dag_id):
    """True si el DAG que MWAA tiene parseado acepta load_dates (generador v9.5+).

    Se pregunta a MWAA y no al archivo en S3: si el generador se desplego pero
    MWAA todavia no lo re-parseo, un conf con load_dates se ignoraria y el
    UNLOAD bajaria el rango completo de punta a punta.
    """
    if dag_id not in _SOPORTA_DIAS:
        d = api(s, host, "GET", f"dags/{dag_id}/details", silencioso=True)
        _SOPORTA_DIAS[dag_id] = isinstance(d, dict) and "load_dates" in (d.get("params") or {})
    return _SOPORTA_DIAS[dag_id]


def tramos_unload(fechas, exactas, soporta_dias):
    """Corridas de UNLOAD para bajar 'fechas': [{desde, hasta, fechas}].

    Un tramo continuo: un run con desde/hasta. Varios tramos: si el DAG acepta
    load_dates, UN solo run con la lista exacta. Si no: un run por tramo
    (dias pedidos a mano), o en un rango con muchos huecos uno de punta a punta.
    """
    bs = U.bloques(fechas)
    if len(bs) == 1:
        return [{"desde": bs[0][0], "hasta": bs[0][1], "fechas": None}]
    if soporta_dias:
        return [{"desde": fechas[0], "hasta": fechas[-1], "fechas": list(fechas)}]
    if exactas or len(bs) <= 3:
        return [{"desde": a, "hasta": b, "fechas": None} for a, b, _ in bs]
    return [{"desde": min(fechas), "hasta": max(fechas), "fechas": None}]


def expandir_fechas(vals):
    """['2025-01-05', 'calendar_dt=2025-01-17', '2025-05-01..2025-05-03'] -> dias ordenados."""
    out = set()
    for v in vals:
        v = v.split("=", 1)[-1].strip()
        a, _, b = v.partition("..")
        d = datetime.strptime(a.strip(), "%Y-%m-%d").date()
        h = datetime.strptime(b.strip(), "%Y-%m-%d").date() if b else d
        if h < d:
            raise ValueError(f"rango al reves: {v}")
        out.update(rango_a_parts(str(d), str(h)))
    return sorted(out)


def esperar_run_en_curso(cfg, s, host, dag_id, sel):
    """Si el DAG ya tiene un run corriendo o en cola, lo espera en vez de
    disparar otro (ej.: retomar una cola despues de un corte de VPN).
    False si ese run termina mal."""
    runs = runs_del_dag(s, host, dag_id, datetime.now(timezone.utc) - timedelta(days=3)) or []
    activos = [r for r in runs if r[2] in ("running", "queued")]
    if not activos:
        return True
    rid = activos[-1][3]
    U.warn(f"El DAG ya tiene un run en curso ({rid}): lo espero en vez de disparar otro.")
    return esperar_unload(cfg, s, host, dag_id, rid, None, sel) is not None


def fechas_cli(fechas):
    """['2025-05-01','2025-05-02','2025-07-11'] -> '2025-05-01..2025-05-02,2025-07-11'."""
    return ",".join(a if n == 1 else f"{a}..{b}" for a, b, n in U.bloques(fechas))


SIN_DATOS = []        # dias pedidos que el UNLOAD no encontro en el origen


def run_flow(cfg, o):
    global PASO_ACTUAL, SIN_DATOS
    PASO_ACTUAL = ""
    schema, table = cfg["schema"], cfg["table"]
    dag_id = DAG_ID_TPL.format(schema=schema, table=table)
    sel = cfg.get("_sel", table)
    publica = o["solo_sync"] or o["solo_json"]
    # Lo que ya esta en el landing (y es valido) no se vuelve a bajar de Redshift.
    reusa = not (o["solo_dag"] or o["forzar_unload"] or o["run_id"]
                 or o["solo_mover"] or publica)

    plan = []
    if o["prep"]:
        plan.append("prep")
    if not o["solo_mover"] and not o["run_id"]:
        plan += ["sync", "publicar"]
    if not (o["solo_mover"] or publica):
        if reusa:
            plan.append("landing")
        if not o["run_id"]:
            plan.append("disparar")
        plan.append("esperar")
    if not (o["solo_dag"] or publica):
        plan += ["mover", "verificar"]
    paso = Pasos(plan)

    print()
    print(f"{C.B}Pipeline: {schema}.{table}{C.END}")
    print(f"  DAG    : {dag_id}")
    pedidas = o.get("fechas")            # dias especificos (None = rango continuo)
    if pedidas:
        print(f"  Dias   : {len(pedidas)} especificos: {U.rangos(pedidas, 6)}")
    elif o["desde"] and ("disparar" in plan or o["solo_mover"]):
        print(f"  Rango  : {o['desde']}  ->  {o['hasta']}")
    if o["run_id"]:
        print(f"  Run    : {o['run_id']}")
    print(f"  Destino: s3://{U.BUCKET_DST}/"
          + U.PREFIX_DST_TPL.format(schema=schema, table=table))

    # flow solo opera sobre loaders de backfill. Un loader FCSM normal
    # (UNLOAD -> iceberg_load -> s3_clean) NO se toca: sincronizarlo le pondria
    # only_unload y el DAG dejaria de cargar Iceberg.
    if not o["prep"] and cfg.get("only_unload") is not True:
        print()
        U.bad('Este JSON no es de backfill: no tiene "only_unload": true.')
        U.info("Es un loader FCSM normal (UNLOAD -> iceberg_load -> s3_clean).")
        U.info("flow no lo modifica: convertirlo cortaria la carga a Iceberg de ese DAG.")
        U.info("En el listado, los de backfill aparecen como [only_unload].")
        return False

    SIN_DATOS = []
    rango = pedidas or (rango_a_parts(o["desde"], o["hasta"]) if o["desde"] and o["hasta"] else None)
    okey = False
    with U.KeepAwake():
        s = host = None
        if "sync" in plan or "esperar" in plan:
            print("\n  Conectando con MWAA...", end=" ", flush=True)
            s, host = mwaa_session(timeout=10)
            print("OK")
        elif "mover" in plan:
            # --solo-mover: MWAA es opcional (confirma que run escribio el landing)
            print("\n  Conectando con MWAA...", end=" ", flush=True)
            try:
                s, host = mwaa_session(timeout=8)
                print("OK")
            except MwaaNoDisponible:
                print("sin conexion (VPN?)")

        # ── generar JSON (tabla nueva) ──
        fuente = None
        if "prep" in plan:
            paso("Generando el JSON de la tabla nueva")
            creado, fuente = generar_json(cfg, o["prep"])
            if not creado:
                return False

        g = U.aws(["glue", "get-table", "--database-name", U.GLUE_DB,
                   "--name", cfg["_glue_table"]], U.PROFILE_DST, check=False)
        glue_tbl = g["Table"] if g else None

        # ── sincronizar con el destino y publicar ──
        if "sync" in plan:
            paso("Sincronizando el JSON con la tabla destino" if not o["no_sync"]
                 else "Validando el JSON contra la tabla destino")
            if glue_tbl is None:
                U.warn(f"{U.GLUE_DB}.{cfg['_glue_table']} no existe en el Catalog.")
                U.info("Tabla nueva: no hay esquema que replicar; el JSON queda como esta.")
                if not U.confirm("Seguir igual? [y/N]", riesgo=True):
                    return False
            else:
                if not o["no_sync"]:
                    if fuente is None:
                        fuente = columnas_origen(cfg)
                    if paso_sync(cfg, glue_tbl, fuente) is None:
                        return False
                print()
                if not U.analyze(cfg, glue_tbl):
                    if not U.confirm("El JSON no calza con el destino. Seguir igual? [y/N]",
                                     riesgo=True):
                        return False

            paso("Publicando el JSON en MWAA")
            exigir = bool(cfg.get("only_unload") and cfg.get("schema_source"))
            if not publicar(Path(cfg["_file"]), s, host, dag_id, exigir):
                return False
            if publica:
                print()
                U.ok("JSON sincronizado y publicado.")
                U.info(f"Para cargar:  flow {sel} --desde AAAA-MM-DD --hasta AAAA-MM-DD")
                return True

        def a_bajar(fechas):
            """Corridas de UNLOAD para esas fechas (una sola si el DAG acepta load_dates)."""
            varios = len(U.bloques(fechas)) > 1
            soporta = varios and dag_soporta_dias(s, host, dag_id)
            if varios and pedidas and not soporta:
                U.warn("El DAG todavia no acepta dias sueltos (generador anterior a v9.5):")
                U.info(f"corro un UNLOAD por tramo ({len(U.bloques(fechas))} runs). Para "
                       "hacerlo en uno solo:  flow --desplegar-generador")
            return tramos_unload(fechas, bool(pedidas), soporta)

        # ── landing: lo que ya esta y es valido no se vuelve a bajar ──
        tramos = []
        if "disparar" in plan and "landing" not in plan:
            tramos = a_bajar(rango)
        sirven = None
        if "landing" in plan:
            paso("Revisando lo que ya esta en el landing")
            if not esperar_run_en_curso(cfg, s, host, dag_id, sel):
                return False
            sirven, descartes, faltan = revisar_landing(cfg, glue_tbl, rango, s, host, dag_id)
            necesitan = sorted(set(faltan) | set(descartes))
            print()
            if not necesitan:
                U.ok(("Todos los dias pedidos ya estan" if pedidas else "Todo el rango ya esta")
                     + " en el landing y son validos: no se corre el UNLOAD.")
                U.info("Para bajarlo igual de Redshift:  --forzar-unload")
            else:
                tramos = a_bajar(necesitan)
                print(f"  UNLOAD solo para lo que falta ({len(necesitan)} fechas):")
                for t in tramos:
                    U.info(f"{len(t['fechas'])} dias sueltos en un solo UNLOAD: "
                           f"{U.rangos(t['fechas'], 4)}" if t["fechas"]
                           else f"{t['desde']} .. {t['hasta']}")

        # ── disparar y esperar ──
        corrio = False
        if "disparar" in plan and not tramos:
            paso.saltar("Disparar el DAG de UNLOAD", "todo el rango esta en el landing")
            paso.saltar("Esperar el UNLOAD")
        elif "esperar" in plan:
            lista = tramos if "disparar" in plan else [None]   # None: retomar --run
            for k, tramo in enumerate(lista):
                run_id, t0 = o["run_id"], None
                if tramo:
                    if k == 0:
                        paso("Disparando el DAG de UNLOAD")
                        if "landing" not in plan and not esperar_run_en_curso(
                                cfg, s, host, dag_id, sel):
                            return False
                    else:
                        print()
                        print(f"  {C.B}Tramo {k + 1}/{len(lista)}{C.END}")
                    run_id, t0 = trigger_dag(s, host, dag_id, tramo["desde"],
                                             tramo["hasta"], tramo["fechas"])
                    if not run_id:
                        return False
                if k == 0:
                    paso("Esperando el UNLOAD")
                r = esperar_unload(cfg, s, host, dag_id, run_id, t0, sel)
                if r is None:
                    return False
                corrio = True
                if o["run_id"]:
                    conf = r.get("conf") or {}
                    if conf.get("load_dates"):
                        rango = expandir_fechas(str(conf["load_dates"]).split(","))
                    elif conf.get("load_start") and conf.get("load_end"):
                        rango = rango_a_parts(conf["load_start"], conf["load_end"])
                    else:
                        t_ini = parse_iso(r.get("start_date"))
                        rango = particiones_de_la_corrida(cfg, t_ini) if t_ini else None
            if o["solo_dag"]:
                print()
                if pedidas:
                    U.info(f"Para mover:  flow {sel} --solo-mover --particion {fechas_cli(rango)}")
                else:
                    U.info(f"Para mover:  flow {sel} --solo-mover"
                           + (f" --desde {rango[0]} --hasta {rango[-1]}" if rango else ""))
                return True

        # ── mover y verificar ──
        paso("Moviendo a la tabla raw y registrando particiones")
        if o["solo_mover"] and not rango and U.SOLO_PARTS:
            rango = sorted({p.split("=", 1)[-1] for p in U.SOLO_PARTS})
        if sirven is None or corrio:
            print("  Validando el landing antes de mover:")
            try:
                sirven, _, faltan = revisar_landing(cfg, glue_tbl, rango, s, host, dag_id)
            except RuntimeError as e:
                U.bad(str(e))
                return False
            if corrio and faltan:
                SIN_DATOS = faltan
                (U.warn if pedidas else U.info)(
                    f"{len(faltan)} fechas sin datos en el origen (el UNLOAD no escribio "
                    f"nada): {U.rangos(faltan, 4)}")
        if not sirven:
            U.bad("No hay particiones validas para mover en el landing.")
            return False
        U.SOLO_PARTS, U.SOLO_PARTS_RANGO = sirven, True
        if not U.move(cfg, use_msck=False):
            print()
            U.bad("El movimiento no termino (detalle arriba): no se verifica todavia.")
            return False
        paso("Verificacion final")
        okey = U.cmd_verificar(cfg)

    print()
    if okey:
        print(f"{C.OK}{C.B}  Pipeline completo.{C.END}")
    else:
        U.warn("Pipeline termino con observaciones; revisa el detalle arriba.")
    return okey


# ─── sesiones SSO ─────────────────────────────────────────────────────────────

# Lo que hacian inic / inht. Con {profile} se corre una vez por cuenta vencida;
# para usar tus alias:  export FLOW_LOGIN_CMD='zsh -ic "inic && inht"'
LOGIN_CMD = os.environ.get("FLOW_LOGIN_CMD", "aws sso login --profile {profile}")
CUENTAS = ((U.PROFILE_SRC, U.ACCOUNT_SRC), (U.PROFILE_DST, U.ACCOUNT_DST))


def sesiones_activas(perfiles=CUENTAS):
    return all(U.check_session(p) for p, _ in perfiles)


def asegurar_sesiones(perfiles=CUENTAS):
    """Revisa las sesiones SSO y hace login de las vencidas."""
    for prof, acc in perfiles:
        if U.check_session(prof):
            continue
        cmd = LOGIN_CMD.replace("{profile}", prof)
        print()
        U.warn(f"Sesion SSO de {acc} vencida: hago login  ({cmd})")
        U.info("Se abre el navegador: aproba el acceso y sigo solo.")
        subprocess.run(cmd, shell=True)
        if not U.check_session(prof):
            U.bad(f"No quedo activa la sesion de {acc}.")
            U.info(f"Proba a mano:  aws sso login --profile {prof}")
            return False
        U.ok(f"Sesion de {acc} activa.")
    return True


def vpn_ok():
    try:
        mwaa_session(timeout=8)
        return True
    except MwaaNoDisponible:
        return False


# ─── cola: varias tablas, una tras otra ──────────────────────────────────────

ESTADO_DIR = Path(os.environ.get("FLOW_ESTADO_DIR", Path(__file__).resolve().parent))
COLA_FILE = ESTADO_DIR / "cola.json"
LOG_DIR = ESTADO_DIR / "logs"
HECHO = ("ok", "obs")
_TABLAS_DESTINO = None


def tablas_destino():
    """Nombres de las tablas del Glue Catalog destino (una sola lectura)."""
    global _TABLAS_DESTINO
    if _TABLAS_DESTINO is None:
        try:
            out = U.aws(["glue", "get-tables", "--database-name", U.GLUE_DB,
                         "--query", "TableList[].Name"], U.PROFILE_DST)
        except RuntimeError as e:
            U.warn(f"No pude listar las tablas de {U.GLUE_DB}: {e}")
            out = []
        _TABLAS_DESTINO = [n.lower() for n in (out or [])]
    return _TABLAS_DESTINO


def resolver_tabla(ref, defs):
    """(tabla, error). tabla = {schema, table, nuevo, archivo}.

    Busca primero un JSON de backfill (nombre exacto, numero del listado o
    parte del nombre); si no hay, la tabla destino en el Glue Catalog: en ese
    caso el JSON se genera desde el destino al correrla.
    """
    r = ref.strip().lower().strip(".,;:*")
    if "__" in r and "." not in r:          # nombre de Glue: esquema__tabla
        r = r.replace("__", ".", 1)
    bf = [d for d in defs if d.get("only_unload") is True]

    def de_json(d):
        return {"schema": d["schema"], "table": d["table"], "nuevo": False,
                "archivo": Path(d["_file"]).name}, None

    def nueva(schema, table):
        return {"schema": schema, "table": table, "nuevo": True, "archivo": None}, None

    def nombres(ds):
        return ", ".join(f"{d['schema']}.{d['table']}" for d in ds[:5])

    if r.isdigit():
        i = int(r)
        if not 1 <= i <= len(defs):
            return None, f"no hay tabla #{i} (el listado tiene {len(defs)})"
        d = defs[i - 1]
        if d.get("only_unload") is True:
            return de_json(d)
        # es el loader FCSM normal: se usa (o se crea) el de backfill de esa tabla
        t = d["table"][:-5] if d["table"].endswith("_fcsm") else d["table"]
        r = f"{d['schema']}.{t}"
    if "." in r:
        schema, table = r.split(".", 1)
        hit = [d for d in bf if d["schema"] == schema and d["table"] == table]
        if hit:
            return de_json(hit[0])
        if U.GLUE_TABLE_TPL.format(schema=schema, table=table).lower() in tablas_destino():
            return nueva(schema, table)
        return None, (f"no hay JSON de backfill ni tabla destino "
                      f"{U.GLUE_DB}.{U.GLUE_TABLE_TPL.format(schema=schema, table=table)}")
    exactas = [d for d in bf if d["table"] == r]
    if len(exactas) > 1:
        return None, f"esta en varios esquemas: {nombres(exactas)} (usa esquema.tabla)"
    if exactas:
        return de_json(exactas[0])
    parciales = [d for d in bf if r in d["table"]]
    if len(parciales) > 1:
        return None, f"coincide con varias: {nombres(parciales)}"
    if parciales:
        return de_json(parciales[0])
    en_destino = [n for n in tablas_destino() if "__" in n]
    cands = ([n for n in en_destino if n.split("__", 1)[1] == r]
             or [n for n in en_destino if r in n.split("__", 1)[1]])
    if len(cands) > 1:
        return None, (f"no tiene JSON y en {U.GLUE_DB} coincide con varias: "
                      f"{', '.join(cands[:5])} (usa esquema.tabla)")
    if cands:
        return nueva(*cands[0].split("__", 1))
    return None, f"no hay JSON de backfill ni tabla destino que coincida en {U.GLUE_DB}"


def interpretar(texto, defs):
    """Items del pedido [{tabla, desde, hasta, ...}] o None. Muestra quien lo interpreto."""
    hoy = date.today()
    modo = os.environ.get("FLOW_INTERPRETE", "auto").lower()
    local = err_local = None
    if modo != "claude":
        try:
            local = P.interpretar_local(texto, [d["table"] for d in defs], hoy)
        except P.PedidoInvalido as e:
            err_local = str(e)
    items = dudas = err_claude = None
    if modo in ("auto", "claude"):
        conocidas = [(i, d["schema"], d["table"], d.get("only_unload") is True)
                     for i, d in enumerate(defs, 1)]
        print("  Interpretando el pedido con Claude Code...", end=" ", flush=True)
        items, dudas, err_claude = P.interpretar_claude(texto, conocidas, hoy)
        print("listo" if items else "no disponible")
    if items and local and len(local) == len(items):
        # Claude convirtio en rango una lista de dias que el parser local leyo
        # exacta: se usa la lista (nunca se baja mas de lo pedido).
        for k, (lo, cl) in enumerate(zip(local, items)):
            if lo.get("fechas") and not cl.get("fechas") and \
                    cl["desde"] <= lo["fechas"][0] and lo["fechas"][-1] <= cl["hasta"]:
                U.warn(f"Claude Code leyo un rango para {cl['tabla']}; uso los "
                       f"{len(lo['fechas'])} dias exactos del pedido.")
                items[k] = {**cl, "fechas": lo["fechas"], "desde": lo["desde"],
                            "hasta": lo["hasta"]}
    if items:
        fuente = "Claude Code"
        if local and not P.mismo_plan(local, items):
            U.warn("Ojo: el parser local lo entendio distinto:")
            for it in local:
                U.info(f"{it['tabla']}  " + (f"{len(it['fechas'])} dias: {U.rangos(it['fechas'], 5)}"
                                             if it.get("fechas") else f"{it['desde']} .. {it['hasta']}"))
        elif local:
            fuente += " (el parser local coincide)"
    elif local:
        fuente, items = "parser local", local
        if err_claude and modo != "local":
            U.info(f"Claude Code: {err_claude}")
    else:
        U.bad("No pude interpretar el pedido.")
        if err_local:
            U.info(err_local)
        if err_claude:
            U.info(f"Claude Code: {err_claude}")
        U.info('Ej:  flow "fact_x del 4 de julio 2025 a fin de año y despues '
               'fact_y de enero a marzo 2026"')
        return None
    for d in dudas or []:
        U.warn(f"Claude Code: {d}")
    U.info(f"Interpretado por: {fuente}")
    return items


def mostrar_plan(items):
    U.title(f"Plan: {len(items)} tabla{'s' if len(items) != 1 else ''}")
    w = max(len(f"{i['schema']}.{i['table']}") for i in items)
    for k, it in enumerate(items, 1):
        sel = f"{it['schema']}.{it['table']}"
        hecho = f"  {C.OK}(ya hecha){C.END}" if it.get("estado") in HECHO else ""
        if it.get("fechas"):
            print(f"  {C.B}{k}.{C.END} {sel:<{w}}  {len(it['fechas'])} dias especificos{hecho}")
            for a, b, n in U.bloques(it["fechas"]):
                print(f"       {C.DIM}{a}{f' .. {b}  ({n} dias)' if n > 1 else ''}{C.END}")
        else:
            dias = (date.fromisoformat(it["hasta"]) - date.fromisoformat(it["desde"])).days + 1
            print(f"  {C.B}{k}.{C.END} {sel:<{w}}  {it['desde']} .. {it['hasta']}  "
                  f"{C.DIM}{dias} dias{C.END}{hecho}")
        if it.get("nuevo"):
            U.info("sin JSON: se genera desde la tabla destino y se publica")
        if it.get("forzar_unload"):
            U.info("forzar unload: baja todo de Redshift aunque este en el landing")
        if it.get("solo_mover"):
            U.info("solo mover lo que ya esta en el landing (sin UNLOAD)")
        if it.get("nota"):
            U.info(it["nota"])


def _si(prompt):
    try:
        return input(f"  {prompt} ").strip().lower() in ("y", "s", "si", "yes")
    except EOFError:
        print()
        return False


def leer_cola():
    try:
        return json.loads(COLA_FILE.read_text())
    except (OSError, ValueError):
        return None


def guardar_cola(cola):
    tmp = COLA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cola, ensure_ascii=False, indent=2))
    tmp.replace(COLA_FILE)


def _ahora():
    return datetime.now().strftime("%Y-%m-%d %H:%M")


class _Tee:
    """Copia lo que se imprime a un log (sin colores)."""
    ANSI = re.compile(r"\x1b\[[0-9;]*m")

    def __init__(self, consola, archivo):
        self.c, self.f = consola, archivo

    def write(self, s):
        self.c.write(s)
        self.f.write(self.ANSI.sub("", s))
        return len(s)

    def flush(self):
        self.c.flush()
        self.f.flush()

    def isatty(self):
        return self.c.isatty()


def cmd_cola(texto):
    """flow "pedido en texto"  |  flow --cola (retoma la ultima)."""
    if texto is not None:
        U.title("Pedido")
        for linea in texto.strip().splitlines():
            print(f"  {C.DIM}{linea}{C.END}")
    if not asegurar_sesiones():
        return False
    defs = U.load_defs()

    if texto is not None:
        print()
        items = interpretar(texto, defs)
        if not items:
            return False
        plan, errores = [], []
        for it in items:
            t, err = resolver_tabla(it["tabla"], defs)
            if err:
                errores.append(f'"{it["tabla"]}": {err}')
                continue
            plan.append({**t, "desde": it["desde"], "hasta": it["hasta"],
                         "fechas": it.get("fechas") or None,
                         "nota": it.get("nota", ""), "estado": "pendiente",
                         "forzar_unload": bool(it.get("forzar_unload")),
                         "solo_mover": bool(it.get("solo_mover"))})
        if errores:
            print()
            for e in errores:
                U.bad(e)
            U.info("Corregi el pedido (nombre exacto o esquema.tabla) y volve a correrlo.")
            return False
        cola = {"pedido": texto.strip(), "creada": _ahora(), "items": plan}
    else:
        cola = leer_cola()
        if not cola:
            U.bad(f"No hay una cola guardada para retomar ({COLA_FILE}).")
            return False
        U.info(f"Cola del {cola['creada']}: {cola['pedido'][:100]}")
        if all(i["estado"] in HECHO for i in cola["items"]):
            mostrar_plan(cola["items"])
            print()
            U.ok("Esa cola ya termino.")
            return True

    mostrar_plan(cola["items"])
    print()
    print("  Conectando con MWAA...", end=" ", flush=True)
    try:
        mwaa_session(timeout=10)
        print("OK")
    except MwaaNoDisponible as e:
        print()
        U.bad(str(e))
        U.info("Conecta la VPN y volve a correr el mismo comando.")
        return False

    print()
    if not U.AUTO_BORRAR:
        U.AUTO_BORRAR = _si("Si una particion ya existe en destino con otros datos, "
                            "la reemplazo? [y/N]")
        if not U.AUTO_BORRAR:
            U.info("Las tablas que necesiten reemplazar se cortan sin tocar el destino.")
    if not _si("Ejecutar la cola? [y/N]"):
        print("  Cancelado.")
        return False
    U.AUTO = U.NO_INTERACTIVO = True
    guardar_cola(cola)
    return run_cola(cola)


def _correr_item(it):
    """Corre una tabla de la cola: "ok" | "obs" | "fallo" | "sin_vpn"."""
    sel = f"{it['schema']}.{it['table']}"
    for intento in (1, 2):
        if not asegurar_sesiones():
            return "fallo"
        defs = U.load_defs()          # recarga: toma los JSON recien creados
        hit = [d for d in defs if d["schema"] == it["schema"]
               and d["table"] == it["table"] and d.get("only_unload") is True]
        prep = None
        if hit:
            cfg = hit[0]
        else:
            cfg = {"schema": it["schema"], "table": it["table"], "columns_mapping": {},
                   "only_unload": True, "_file": ruta_json_nuevo(it["schema"], it["table"])}
            prep = {"table_real": it["table"], "conn_id": None, "column_dt": None}
        cfg["_sel"] = sel
        cfg["_glue_table"] = U.GLUE_TABLE_TPL.format(schema=cfg["schema"], table=cfg["table"])
        o = {"desde": it["desde"], "hasta": it["hasta"], "run_id": None,
             "solo_dag": False, "solo_mover": bool(it.get("solo_mover")),
             "solo_sync": False, "solo_json": False, "no_sync": False, "prep": prep,
             "forzar_unload": bool(it.get("forzar_unload")), "fechas": it.get("fechas")}
        U.SOLO_PARTS, U.SOLO_PARTS_RANGO = None, False
        del U.ERRORES[:]
        try:
            ok = run_flow(cfg, o)
        except MwaaNoDisponible as e:
            U.bad(str(e))
            return "sin_vpn"
        except RuntimeError as e:
            U.bad(str(e))
            ok = False
        if prep and Path(cfg["_file"]).exists():
            it["json_nuevo"] = str(cfg["_file"])
        it["sin_datos"] = list(SIN_DATOS)
        if ok:
            return "ok"
        if PASO_ACTUAL == "Verificacion final":
            return "obs"
        if not sesiones_activas():
            if intento == 1:
                U.warn("La sesion SSO vencio en el medio: login y reintento la tabla "
                       "(sigue donde quedo).")
                continue
            return "fallo"
        if not vpn_ok():
            return "sin_vpn"
        return "fallo"
    return "fallo"


def run_cola(cola):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"cola-{datetime.now():%Y%m%d-%H%M%S}.log"
    fh = open(log, "a", encoding="utf-8")
    consola, sys.stdout = sys.stdout, _Tee(sys.stdout, fh)
    cola.setdefault("logs", []).append(str(log))
    items, t0 = cola["items"], time.time()
    try:
        for k, it in enumerate(items, 1):
            if it["estado"] in HECHO:
                continue
            print()
            print(f"{C.B}{'═' * 64}{C.END}")
            cuando = (f"{len(it['fechas'])} dias especificos" if it.get("fechas")
                      else f"{it['desde']} .. {it['hasta']}")
            print(f"{C.B}  Tabla {k}/{len(items)}: {it['schema']}.{it['table']}   {cuando}{C.END}")
            print(f"{C.B}{'═' * 64}{C.END}")
            it.update(estado="en curso", inicio=_ahora(), detalle="")
            guardar_cola(cola)
            t = time.time()
            res = _correr_item(it)
            it["duracion"] = U.dur(time.time() - t)
            if res == "sin_vpn":
                it["estado"] = "pendiente"
                guardar_cola(cola)
                print()
                U.bad("Sin conexion con MWAA: la cola queda pausada en esta tabla.")
                U.info("Cuando vuelva la VPN:  flow --cola")
                break
            it["estado"] = res
            if res == "fallo":
                it["detalle"] = (U.ERRORES[0] if U.ERRORES else f"en: {PASO_ACTUAL}")[:140]
            elif res == "obs" and U.ERRORES:
                it["detalle"] = U.ERRORES[-1][:140]
            guardar_cola(cola)
    finally:
        resumen_cola(cola, t0)
        guardar_cola(cola)
        sys.stdout = consola
        fh.close()
    return all(i["estado"] in HECHO for i in items)


def resumen_cola(cola, t0):
    items = cola["items"]
    U.title(f"Resumen de la cola  ({len(items)} tablas, {U.dur(time.time() - t0)})")
    icono = {"ok": f"{C.OK}✓{C.END}", "obs": f"{C.WARN}!{C.END}", "fallo": f"{C.ERR}✗{C.END}"}
    texto = {"ok": "completa", "obs": "movida; la verificacion dejo observaciones",
             "fallo": "fallo", "pendiente": "pendiente", "en curso": "interrumpida"}
    w = max(len(f"{i['schema']}.{i['table']}") for i in items)
    for it in items:
        e = it["estado"]
        cuando = (f"{len(it['fechas'])} dias especificos" if it.get("fechas")
                  else f"{it['desde']} .. {it['hasta']}")
        print(f"  {icono.get(e, '·')} {it['schema'] + '.' + it['table']:<{w}}  "
              f"{cuando}  {texto.get(e, e)}  {C.DIM}{it.get('duracion', '')}{C.END}")
        if it.get("detalle") and e in ("fallo", "obs"):
            U.info(it["detalle"])
        if it.get("sin_datos") and e in HECHO:
            U.info(f"{len(it['sin_datos'])} dias sin datos en el origen: "
                   f"{U.rangos(it['sin_datos'], 4)}")
    if any(i["estado"] not in HECHO for i in items):
        print()
        U.info("Para seguir con lo que falta:  flow --cola")
    nuevos = [i["json_nuevo"] for i in items if i.get("json_nuevo")]
    if nuevos:
        print()
        U.warn("JSON nuevos: commitealos en el repo (si no, el proximo deploy de CI/CD no los tiene):")
        for n in nuevos:
            U.info(n)
    if cola.get("logs"):
        U.info(f"Log: {cola['logs'][-1]}")


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    flags = {a for a in args if a.startswith("-")}

    if flags & {"--ayuda", "--help", "-h"}:
        print(f"flow v{__version__} (unload v{U.__version__})")
        print(__doc__)
        return

    def opt(name):
        if name in args:
            i = args.index(name)
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                return args[i + 1]
        return None

    valores = {opt(n) for n in ("--desde", "--hasta", "--conn", "--column-dt", "--run",
                                "--particion", "--particiones-archivo")} - {None}
    pos = [a for a in args if not a.startswith("-") and a not in valores]

    o = {
        "desde": opt("--desde"), "hasta": opt("--hasta"), "run_id": opt("--run"),
        "solo_dag": "--solo-dag" in flags, "solo_mover": "--solo-mover" in flags,
        "solo_sync": "--solo-sync" in flags, "solo_json": "--solo-json" in flags,
        "no_sync": "--no-sync" in flags, "prep": None,
        "forzar_unload": "--forzar-unload" in flags, "fechas": None,
    }
    U.AUTO = bool(flags & {"--auto", "--auto-borrar"})
    U.AUTO_BORRAR = "--auto-borrar" in flags
    U.SOLO_PARTS = U.parse_solo_parts(args)
    # --particion / --particiones-archivo: dias especificos (tambien "a..b"),
    # para el pipeline completo o para --solo-mover
    if U.SOLO_PARTS and not o["run_id"]:
        try:
            o["fechas"] = expandir_fechas(U.SOLO_PARTS)
        except ValueError as e:
            U.bad(f"--particion invalida: {e} (formato AAAA-MM-DD o AAAA-MM-DD..AAAA-MM-DD)")
            sys.exit(1)
        o["desde"], o["hasta"] = o["fechas"][0], o["fechas"][-1]

    print(f"{C.DIM}flow v{__version__} (unload v{U.__version__}){C.END}")

    if "--desplegar-generador" in flags:
        if not asegurar_sesiones(((MWAA_PROFILE, U.ACCOUNT_SRC),)):
            sys.exit(1)
        sys.exit(0 if cmd_desplegar_generador() else 1)

    # pedido en texto libre -> cola:  flow "tabla x de julio a diciembre 2025, despues y ..."
    texto = None
    if "--pegar" in flags:
        print("  Pega el pedido y termina con Ctrl-D en una linea vacia:")
        texto = sys.stdin.read()
        if not sys.stdin.isatty():
            try:
                sys.stdin = open("/dev/tty")      # para poder confirmar despues
            except OSError:
                pass
    elif len(pos) > 1 or (pos and re.search(r"\s", pos[0])):
        texto = " ".join(pos)
    if texto is not None or "--cola" in flags:
        if texto is not None and not texto.strip():
            U.bad("El pedido esta vacio.")
            sys.exit(1)
        sys.exit(0 if cmd_cola(texto) else 1)

    defs = U.load_defs()
    if not defs:
        U.bad(f"No hay JSON en {U.DEFS_DIR}")
        sys.exit(1)

    arg = pos[0] if pos else None
    if arg and "__" in arg and "." not in arg:        # nombre de Glue: esquema__tabla
        arg = arg.replace("__", ".", 1)
    if arg and "." in arg and not arg.isdigit():
        schema, table = arg.strip().lower().split(".", 1)
        ya = [d for d in defs if d["schema"] == schema and d["table"] == table]
        if ya:
            cfg = ya[0]
            cfg["_sel"] = str(defs.index(cfg) + 1)
        else:
            cfg = {"schema": schema, "table": table, "columns_mapping": {},
                   "only_unload": True, "_file": LOADERS_PATH / f"{table}.json",
                   "_sel": f"{schema}.{table}"}
            o["prep"] = {"table_real": table, "conn_id": opt("--conn"),
                         "column_dt": opt("--column-dt")}
    else:
        cfg = U.pick(defs, arg)
        cfg["_sel"] = str(defs.index(cfg) + 1)
    cfg["_glue_table"] = U.GLUE_TABLE_TPL.format(schema=cfg["schema"], table=cfg["table"])

    necesita_rango = not (o["solo_mover"] or o["solo_sync"] or o["solo_json"] or o["run_id"]
                          or o["fechas"])
    if necesita_rango:
        if not o["desde"]:
            o["desde"] = input("  Desde [2026-01-01]: ").strip() or "2026-01-01"
        if not o["hasta"]:
            ayer = (datetime.today() - timedelta(days=1)).date()
            o["hasta"] = input(f"  Hasta [{ayer}]: ").strip() or str(ayer)
    for f in ("desde", "hasta"):
        if o[f]:
            try:
                datetime.strptime(o[f], "%Y-%m-%d")
            except ValueError:
                U.bad(f"--{f} invalida: '{o[f]}' (formato AAAA-MM-DD)")
                sys.exit(1)

    if not asegurar_sesiones():
        sys.exit(1)

    sys.exit(0 if run_flow(cfg, o) else 1)


if __name__ == "__main__":
    try:
        main()
    except MwaaNoDisponible as e:
        print()
        U.bad(str(e))
        U.info("No se disparo ni se movio nada. Conecta la VPN y volve a correr.")
        sys.exit(2)
    except KeyboardInterrupt:
        print("\n  Interrumpido.")
        print("  - Si estaba esperando el DAG, sigue en MWAA:  flow <tabla> --run <run_id>")
        print("  - Si estaba moviendo: lo movido ya quedo registrado; volve a correr el")
        print("    mismo comando y sigue donde quedo.")
        print("  - Si era una cola:  flow --cola  la retoma desde esa tabla.")
        sys.exit(130)
    except RuntimeError as e:
        U.bad(str(e))
        sys.exit(1)
