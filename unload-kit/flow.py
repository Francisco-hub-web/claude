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
  3. Dispara el DAG de UNLOAD con el rango (conf load_start / load_end)
  4. Espera; si falla, corta sin mover nada
  5. Mueve SOLO las particiones que escribio esta corrida (ignora restos
     viejos del landing), registra particiones
  6. Verifica y limpia

Tabla nueva (schema.tabla sin JSON): antes genera el JSON desde Redshift.

Uso:
    flow 7 --desde 2025-01-01 --hasta 2025-12-31
    flow chi_easy_dim_vw.fact_nueva --desde ... --hasta ...
    flow 7 --solo-sync                        sincroniza y publica el JSON, nada mas
    flow 7 --solo-dag --desde ... --hasta ... dispara y espera, no mueve
    flow 7 --run manual__2026-09-16T19:12:17Z retoma un run ya disparado
    flow 7 --solo-mover [--desde ... --hasta ...]
    flow 7 ... --no-sync                      no toca el JSON (solo valida)
    flow 7 ... --auto                         sin confirmaciones (salvo borrados)
    flow 7 ... --auto-borrar                  sin confirmaciones, incluidos borrados
    flow schema.tabla --conn <conn_id> --column-dt <col>
    flow --desplegar-generador                despliega el generador del kit en MWAA
    flow --ayuda

Requiere unload.py en el mismo directorio, boto3 + requests (como mwaa_cert)
y psycopg2 solo si genera o valida contra Redshift (FLOW_RS_PASS).
"""

__version__ = "2.2"

import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import unload as U  # noqa: E402

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


class Pasos:
    def __init__(self, plan):
        self.plan, self.i = plan, 0

    def __call__(self, label):
        self.i += 1
        print()
        print(f"{C.B}━━ [{self.i}/{len(self.plan)}] {label} {C.END}")
        print("─" * 60)


def parse_iso(s):
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


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


def trigger_dag(s, host, dag_id, desde, hasta):
    """Dispara el DAG con el rango por conf ('Trigger w/ config')."""
    d = api(s, host, "GET", f"dags/{dag_id}")
    if d is None:
        U.bad(f"No encontre el DAG '{dag_id}' en MWAA.")
        return None, None
    if d.get("is_paused", True):
        print("  DAG pausado: activandolo...")
        api(s, host, "PATCH", f"dags/{dag_id}", {"is_paused": False})

    t0 = datetime.now(timezone.utc)
    body = {"dag_run_id": f"manual__{t0:%Y-%m-%dT%H:%M:%SZ}",
            "conf": {"load_start": desde, "load_end": hasta}}
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


def escribir_loader_json(schema, table, column_dt, columns, conn_id):
    """JSON inicial del loader (backfill) a partir de las columnas de Redshift."""
    path = LOADERS_PATH / f"{table}.json"
    if path.exists():
        U.warn(f"Ya existe {path.name}")
        if not U.confirm("Sobreescribir? [y/N]", destructivo=True):
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


def cmd_desplegar_generador():
    """Reemplaza el generador desplegado por el del kit, en su MISMA ruta."""
    U.title("Desplegar generador con modo estricto")
    if not KIT_GENERATOR.exists():
        U.bad(f"No encuentro el generador del kit: {KIT_GENERATOR}")
        return False
    k, actual = buscar_generador_s3()
    if not k:
        U.bad(f"No encontre el generador en s3://{S3_DAGS_BUCKET}/{S3_GENERATOR_DIR}")
        return False
    uri = f"s3://{S3_DAGS_BUCKET}/{k}"
    print(f"  Desplegado : {uri}")
    if generador_es_estricto(actual):
        U.ok("Ya tiene el modo estricto: no hay nada que desplegar.")
        return True

    nuevo = KIT_GENERATOR.read_text()
    # lineas del desplegado que el nuevo no trae: si hay personalizaciones, se ven aca
    norm = lambda s: {l.strip() for l in s.splitlines() if l.strip() and not l.strip().startswith("#")}
    perdidas = sorted(norm(actual) - norm(nuevo))
    print(f"  Nuevo      : {KIT_GENERATOR}")
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
    if r is None or not generador_es_estricto(txt):
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


# ─── landing ──────────────────────────────────────────────────────────────────

def particiones_de_la_corrida(cfg, desde_utc, margen_min=5):
    """Particiones del landing escritas por esta corrida (LastModified >= inicio).

    Asi el movimiento ignora restos de corridas anteriores que sigan en el
    landing (CLEANPATH con PARTITION BY solo limpia las carpetas que reescribe).
    """
    col = cfg.get("column_dt", "calendar_dt")
    p_src = U.PREFIX_SRC_TPL.format(schema=cfg["schema"], table=cfg["table"])
    corte = desde_utc - timedelta(minutes=margen_min)
    parts, token = set(), None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", U.BUCKET_SRC,
                "--prefix", p_src, "--max-items", "1000"]
        if token:
            args += ["--starting-token", token]
        out = U.aws(args, U.PROFILE_SRC, check=False)
        if out is None:
            raise RuntimeError("No pude listar el landing (sesion SSO de origen vencida?)")
        if not isinstance(out, dict):
            break
        for o in out.get("Contents", []) or []:
            lm = parse_iso(o.get("LastModified"))
            if lm and lm >= corte:
                seg = o["Key"][len(p_src):].split("/", 1)[0]
                if seg.startswith(col + "="):
                    parts.add(seg.split("=", 1)[1])
        token = out.get("NextToken")
        if not token:
            break
    return sorted(parts)


def rango_a_parts(desde, hasta):
    d = datetime.strptime(desde, "%Y-%m-%d").date()
    h = datetime.strptime(hasta, "%Y-%m-%d").date()
    out = []
    while d <= h:
        out.append(str(d))
        d += timedelta(days=1)
    return out


# ─── pipeline ─────────────────────────────────────────────────────────────────

def run_flow(cfg, o):
    schema, table = cfg["schema"], cfg["table"]
    dag_id = DAG_ID_TPL.format(schema=schema, table=table)
    sel = cfg.get("_sel", table)
    publica = o["solo_sync"] or o["solo_json"]

    plan = []
    if o["prep"]:
        plan.append("prep")
    if not o["solo_mover"] and not o["run_id"]:
        plan += ["sync", "publicar"]
    if not (o["solo_mover"] or publica):
        if not o["run_id"]:
            plan.append("disparar")
        plan.append("esperar")
    if not (o["solo_dag"] or publica):
        plan += ["mover", "verificar"]
    paso = Pasos(plan)

    print()
    print(f"{C.B}Pipeline: {schema}.{table}{C.END}")
    print(f"  DAG    : {dag_id}")
    if o["desde"] and "disparar" in plan:
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

    okey = False
    with U.KeepAwake():
        s = host = None
        if "sync" in plan or "esperar" in plan:
            print("\n  Conectando con MWAA...", end=" ", flush=True)
            s, host = mwaa_session(timeout=10)
            print("OK")

        # ── generar JSON (tabla nueva) ──
        fuente = None
        if "prep" in plan:
            paso("Generando el JSON desde Redshift")
            columns = rs_columns(schema, o["prep"]["table_real"])
            fuente = {c for c, _ in columns}
            print(f"  {len(columns)} columnas leidas de {schema}.{o['prep']['table_real']}")
            g0 = U.aws(["glue", "get-table", "--database-name", U.GLUE_DB,
                        "--name", cfg["_glue_table"]], U.PROFILE_DST, check=False)
            pks = [p["Name"] for p in
                   ((g0 or {}).get("Table", {}).get("PartitionKeys") or [])]
            if len(pks) == 1 and pks[0] in fuente and not o["prep"].get("column_dt"):
                column_dt = pks[0]
                U.info(f"column_dt tomada del destino: {column_dt}")
            else:
                column_dt = elegir_column_dt(columns, o["prep"].get("column_dt"))
            conn_id = o["prep"].get("conn_id") or CONN_POR_ESQUEMA.get(schema, CONN_DEFAULT)
            print(f"  redshift_conn_id: {conn_id}")
            path, _ = escribir_loader_json(schema, table, column_dt, columns, conn_id)
            cfg.update(json.loads(path.read_text()))
            cfg["_file"] = path

        # ── sincronizar con el destino y publicar ──
        if "sync" in plan:
            paso("Sincronizando el JSON con la tabla destino" if not o["no_sync"]
                 else "Validando el JSON contra la tabla destino")
            g = U.aws(["glue", "get-table", "--database-name", U.GLUE_DB,
                       "--name", cfg["_glue_table"]], U.PROFILE_DST, check=False)
            if g is None:
                U.warn(f"{U.GLUE_DB}.{cfg['_glue_table']} no existe en el Catalog.")
                U.info("Tabla nueva: no hay esquema que replicar; el JSON queda como esta.")
                if not U.confirm("Seguir igual? [y/N]", destructivo=True):
                    return False
            else:
                if not o["no_sync"]:
                    if fuente is None:
                        fuente = columnas_origen(cfg)
                    if paso_sync(cfg, g["Table"], fuente) is None:
                        return False
                print()
                if not U.analyze(cfg, g["Table"]):
                    if not U.confirm("El JSON no calza con el destino. Seguir igual? [y/N]",
                                     destructivo=True):
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

        # ── disparar y esperar ──
        run_id, t0 = o["run_id"], None
        if "disparar" in plan:
            paso("Disparando el DAG de UNLOAD")
            run_id, t0 = trigger_dag(s, host, dag_id, o["desde"], o["hasta"])
            if not run_id:
                return False

        if "esperar" in plan:
            paso("Esperando el UNLOAD")
            if t0 is None:
                r = api(s, host, "GET", f"dags/{dag_id}/dagRuns/{run_id}")
                if not r:
                    U.bad(f"No encontre el run '{run_id}'.")
                    return False
                t0 = parse_iso(r.get("start_date")) or parse_iso(r.get("execution_date"))
                if t0 is None:
                    U.bad("El run no tiene fecha de inicio todavia.")
                    return False
            U.info("Ctrl-C corta la espera; el DAG sigue corriendo en MWAA.")
            try:
                state = wait_run(s, host, dag_id, run_id)
            except MwaaNoDisponible as e:
                U.bad(str(e))
                U.info(f"El DAG sigue corriendo en MWAA (run {run_id}).")
                U.info(f"Cuando vuelva la VPN:  flow {sel} --run {run_id}")
                return False
            if state != "success":
                U.bad(f"El DAG termino en {state.upper()}. No se mueve nada.")
                show_failed_tasks(s, host, dag_id, run_id)
                return False
            U.ok("UNLOAD completado.")

            parts = particiones_de_la_corrida(cfg, t0)
            if not parts:
                U.bad("El UNLOAD termino OK pero no escribio particiones:")
                U.info("el origen no tiene datos para ese rango.")
                return False
            print(f"  Esta corrida escribio {len(parts)} particiones: "
                  f"{parts[0]} .. {parts[-1]}")
            if o["solo_dag"]:
                U.info(f"Para mover:  flow {sel} --solo-mover "
                       f"--desde {parts[0]} --hasta {parts[-1]}")
                return True
            U.SOLO_PARTS, U.SOLO_PARTS_RANGO = parts, True

        elif o["solo_mover"] and o["desde"] and o["hasta"]:
            U.SOLO_PARTS = rango_a_parts(o["desde"], o["hasta"])
            U.SOLO_PARTS_RANGO = True

        # ── mover y verificar ──
        paso("Moviendo a la tabla raw y registrando particiones")
        U.move(cfg, use_msck=False)
        paso("Verificacion final")
        okey = U.cmd_verificar(cfg)

    print()
    if okey:
        print(f"{C.OK}{C.B}  Pipeline completo.{C.END}")
    else:
        U.warn("Pipeline termino con observaciones; revisa el detalle arriba.")
    return okey


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
    }
    U.AUTO = bool(flags & {"--auto", "--auto-borrar"})
    U.AUTO_BORRAR = "--auto-borrar" in flags
    U.SOLO_PARTS = U.parse_solo_parts(args)

    print(f"{C.DIM}flow v{__version__} (unload v{U.__version__}){C.END}")

    if "--desplegar-generador" in flags:
        if not U.check_session(MWAA_PROFILE):
            U.bad(f"Sesion SSO no activa para {U.ACCOUNT_SRC}")
            U.info(f"aws sso login --profile {MWAA_PROFILE}")
            sys.exit(1)
        sys.exit(0 if cmd_desplegar_generador() else 1)

    defs = U.load_defs()
    if not defs:
        U.bad(f"No hay JSON en {U.DEFS_DIR}")
        sys.exit(1)

    arg = pos[0] if pos else None
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

    necesita_rango = not (o["solo_mover"] or o["solo_sync"] or o["solo_json"] or o["run_id"])
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

    for prof, acc in ((U.PROFILE_SRC, U.ACCOUNT_SRC), (U.PROFILE_DST, U.ACCOUNT_DST)):
        if not U.check_session(prof):
            U.bad(f"Sesion SSO no activa para {acc}")
            U.info(f"aws sso login --profile {prof}")
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
        print("\n  Interrumpido. Si el DAG ya estaba corriendo, sigue en MWAA:")
        print("  retoma con  flow <tabla> --run <run_id>")
        sys.exit(130)
    except RuntimeError as e:
        U.bad(str(e))
        sys.exit(1)
