#!/usr/bin/env python3
"""
unload - mueve los parquet del UNLOAD (cuenta origen) a la tabla raw
         (cuenta destino) y registra las particiones.

Lee los JSON de definitions/ (cada uno = un DAG en Airflow = una tabla),
los lista numerados, y para el que elijas:

  1. muestra config del JSON y rutas origen/destino
  2. analiza compatibilidad contra el Glue Catalog del destino
     (columnas que faltan, que sobran, particion, flag only_unload)
  3. detecta solapamiento de particiones y ofrece limpiarlas
  4. mueve two-hop (bajar + subir) porque la copia server-side
     cross-account falla por KMS
  5. registra las particiones nuevas y valida

Uso:
    unload                      lista y pregunta
    unload 2                    elige el #2 del listado
    unload fact_obsolescence    elige por nombre

    unload --listar             solo lista las tablas
    unload --estado             panorama de las tablas con only_unload
    unload 2 --analizar         solo analiza compatibilidad, no mueve
    unload 2 --verificar        chequeo profundo de una tabla ya movida
    unload 2 --particiones      registra particiones sin transferir
    unload 2 --fix-location     corrige el Location de la tabla en el Catalog
    unload 2 --msck             registra con Athena MSCK (suele estar bloqueado por SCP)
    unload 2 --auto             sin confirmaciones (salvo borrados)
    unload 2 --auto-borrar      sin confirmaciones, incluidos borrados
    unload 2 --particion 2025-01-05,2025-01-12
                                mueve SOLO esas particiones
    unload 2 --particiones-archivo dias.txt
                                idem, una por linea
    unload --ayuda              esta ayuda

Solo usa la CLI de aws (nada de boto3 ni pip).
"""

__version__ = "5.0"

import json
import os
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------- config

DEFS_DIR = os.environ.get(
    "UNLOAD_DEFS",
    "/Users/franciscogarciadecortazargallegos/raw_layer_sm/raw_layer/develop/"
    "fcsm/dags/loaders/definitions/redshift",
)

ACCOUNT_SRC = os.environ.get("UNLOAD_ACCOUNT_SRC", "595738433757")
ACCOUNT_DST = os.environ.get("UNLOAD_ACCOUNT_DST", "608614369971")

PROFILE_SRC = os.environ.get("UNLOAD_PROFILE_SRC", f"CencoDataEngineerGov-{ACCOUNT_SRC}")
PROFILE_DST = os.environ.get("UNLOAD_PROFILE_DST", f"CencoDataEngineerGov-{ACCOUNT_DST}")

BUCKET_SRC = os.environ.get("UNLOAD_BUCKET_SRC", f"cencosud-dev-cl-landing-{ACCOUNT_SRC}-us-east-1")
BUCKET_DST = os.environ.get("UNLOAD_BUCKET_DST", f"cencosud-dev-cl-raw-{ACCOUNT_DST}-us-east-1")

# origen:  landing/unload/cl_edw_prod/<schema>/<table>/
# destino: redshift/mdh/<schema>__<table>/
PREFIX_SRC_TPL = os.environ.get("UNLOAD_PREFIX_SRC", "landing/unload/cl_edw_prod/{schema}/{table}/")
PREFIX_DST_TPL = os.environ.get("UNLOAD_PREFIX_DST", "redshift/mdh/{schema}__{table}/")

GLUE_DB = os.environ.get("UNLOAD_GLUE_DB", "cencosud_cl_mdh_raw_dev")
GLUE_TABLE_TPL = os.environ.get("UNLOAD_GLUE_TABLE", "{schema}__{table}")

STAGING = os.environ.get("UNLOAD_STAGING", "/tmp/unload")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Columnas que el generador agrega cuando el JSON trae "only_unload": true
AUDIT_COLUMNS = ["fecha_ejecucion", "extraction_date"]

# ---------------------------------------------------------------- colores

class C:
    OK = "\033[92m"
    WARN = "\033[93m"
    ERR = "\033[91m"
    DIM = "\033[90m"
    B = "\033[1m"
    END = "\033[0m"

    @staticmethod
    def off():
        C.OK = C.WARN = C.ERR = C.DIM = C.B = C.END = ""


if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
    C.off()


def confirm(prompt, destructivo=False):
    """input() salvo en modo AUTO. Los destructivos exigen AUTO_BORRAR."""
    if AUTO and not destructivo:
        print(f"  {prompt} {C.DIM}[auto: si]{C.END}")
        return True
    if AUTO and destructivo and AUTO_BORRAR:
        print(f"  {prompt} {C.DIM}[auto-borrar: si]{C.END}")
        return True
    return input(f"  {prompt} ").strip().lower() in ("y", "s", "si", "yes")


def title(txt):
    print(f"\n{C.B}{txt}{C.END}")
    print("─" * max(len(txt), 40))


def ok(txt):
    print(f"  {C.OK}✓{C.END} {txt}")


def warn(txt):
    print(f"  {C.WARN}!{C.END} {txt}")


def bad(txt):
    print(f"  {C.ERR}✗{C.END} {txt}")


def info(txt):
    print(f"    {C.DIM}{txt}{C.END}")


# ---------------------------------------------------------------- aws

def aws(args, profile, parse=True, check=True, quiet=True):
    """Corre la CLI de aws y devuelve stdout (json parseado si parse)."""
    cmd = ["aws"] + args + ["--profile", profile, "--region", REGION]
    if parse:
        cmd += ["--output", "json"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        if check:
            msg = (r.stderr or r.stdout).strip().splitlines()
            raise RuntimeError(msg[-1] if msg else "error aws")
        return None
    if parse and r.stdout.strip():
        return json.loads(r.stdout)
    return r.stdout


def aws_stream(args, profile):
    """Corre aws mostrando la salida en vivo (para sync)."""
    cmd = ["aws"] + args + ["--profile", profile, "--region", REGION]
    return subprocess.run(cmd).returncode == 0


def check_session(profile):
    try:
        aws(["sts", "get-caller-identity"], profile)
        return True
    except Exception:
        return False


def list_prefixes(bucket, prefix, profile, label="particiones", strict=False):
    """Sub-prefijos (particiones) bajo prefix. Pagina y muestra progreso."""
    res = []
    token = None
    sys.stdout.write(f"  {C.DIM}listando {label}...{C.END}")
    sys.stdout.flush()
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket,
                "--prefix", prefix, "--delimiter", "/", "--max-items", "1000"]
        if token:
            args += ["--starting-token", token]
        out = aws(args, profile, check=False)
        if out is None and strict:
            sys.stdout.write("\r" + " " * 70 + "\r")
            raise RuntimeError(f"No pude listar s3://{bucket}/{prefix} (sesion SSO?)")
        if not out:
            break
        for p in out.get("CommonPrefixes", []) or []:
            name = p["Prefix"][len(prefix):].rstrip("/")
            if name:
                res.append(name)
        sys.stdout.write(f"\r  {C.DIM}listando {label}... {len(res)}{C.END}   ")
        sys.stdout.flush()
        token = out.get("NextToken")
        if not token:
            break
    sys.stdout.write("\r" + " " * 70 + "\r")
    sys.stdout.flush()
    return sorted(res)


MAX_SCAN_PAGES = int(os.environ.get("UNLOAD_MAX_SCAN_PAGES", "200"))


def summarize(bucket, prefix, profile, label="objetos"):
    """(n_objetos, bytes, truncado) bajo prefix. Muestra progreso en vivo."""
    total_n = total_b = 0
    token = None
    pages = 0
    truncated = False
    sys.stdout.write(f"  {C.DIM}escaneando {label}...{C.END}")
    sys.stdout.flush()
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix,
                "--max-items", "1000"]
        if token:
            args += ["--starting-token", token]
        out = aws(args, profile, check=False)
        if not out:
            break
        for o in out.get("Contents", []) or []:
            total_n += 1
            total_b += o.get("Size", 0)
        pages += 1
        sys.stdout.write(f"\r  {C.DIM}escaneando {label}... {total_n} objetos, "
                         f"{human(total_b)}{C.END}   ")
        sys.stdout.flush()
        token = out.get("NextToken")
        if not token:
            break
        if pages >= MAX_SCAN_PAGES:
            truncated = True
            break
    sys.stdout.write("\r" + " " * 70 + "\r")
    sys.stdout.flush()
    return total_n, total_b, truncated


def tamanos_por_particion(bucket, prefix, profile):
    """{particion: (objetos, bytes)} con UN listado paginado del prefijo."""
    res, token = {}, None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix,
                "--max-items", "1000"]
        if token:
            args += ["--starting-token", token]
        out = aws(args, profile, check=False)
        if not isinstance(out, dict):
            break
        for o in out.get("Contents", []) or []:
            seg = o["Key"][len(prefix):].split("/", 1)[0]
            n, b = res.get(seg, (0, 0))
            res[seg] = (n + 1, b + o.get("Size", 0))
        token = out.get("NextToken")
        if not token:
            break
    return res


def quedan_objetos(bucket, prefix, profile):
    """True/False si quedan objetos bajo prefix. Lanza si no puede consultar."""
    out = aws(["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix,
               "--max-keys", "1"], profile, check=False)
    if out is None:
        raise RuntimeError(f"No pude consultar s3://{bucket}/{prefix} (sesion SSO?)")
    return bool(isinstance(out, dict) and out.get("Contents"))


def borrar_particiones(s3_dst, p_dst, parts):
    """Borra particiones del destino y VERIFICA que no quede nada antes de seguir."""
    for i, p in enumerate(parts, 1):
        print(f"    [{i}/{len(parts)}] borrando {p}/")
        aws(["s3", "rm", f"{s3_dst}{p}/", "--recursive", "--only-show-errors"],
            PROFILE_DST, parse=False, check=False)
    try:
        vivas = set(list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "verificando", strict=True))
    except RuntimeError as e:
        bad(str(e))
        info("No se sube nada: no pude confirmar el borrado.")
        return False
    quedan = sorted(set(parts) & vivas)
    if quedan:
        bad(f"{len(quedan)} particiones no se pudieron borrar (ej. {quedan[0]}).")
        info("No se sube nada para no duplicar. Volve a correr: la copia local sigue.")
        return False
    ok(f"{len(parts)} particiones borradas en destino.")
    return True


def human(n):
    for u in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PiB"



def count_partitions(cfg):
    """Cuenta particiones registradas en el Glue Catalog (paginado)."""
    n = 0
    token = None
    while True:
        args = ["glue", "get-partitions", "--database-name", GLUE_DB,
                "--table-name", cfg["_glue_table"], "--max-items", "1000"]
        if token:
            args += ["--starting-token", token]
        out = aws(args, PROFILE_DST, check=False)
        if not out:
            break
        n += len(out.get("Partitions", []) or [])
        token = out.get("NextToken")
        if not token:
            break
    return n


def sample_partition_location(cfg):
    """Location de una particion cualquiera (para detectar rutas mal armadas)."""
    out = aws(["glue", "get-partitions", "--database-name", GLUE_DB,
               "--table-name", cfg["_glue_table"], "--max-items", "1"],
              PROFILE_DST, check=False)
    if not out or not out.get("Partitions"):
        return None
    p = out["Partitions"][0]
    return p["Values"][0], p["StorageDescriptor"].get("Location", "")


class KeepAwake:
    """Impide que el Mac se suspenda durante la transferencia."""

    def __enter__(self):
        self.proc = None
        if sys.platform == "darwin":
            try:
                self.proc = subprocess.Popen(
                    ["caffeinate", "-dimsu", "-w", str(os.getpid())],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                info("caffeinate activo: el Mac no se suspende durante la transferencia")
            except FileNotFoundError:
                pass
        return self

    def __exit__(self, *a):
        if self.proc:
            self.proc.terminate()


def cleanup(cfg, local, s3_src, parts=None):
    """Ofrece liberar staging local y landing de origen, y verifica que se borro."""
    title("Limpieza")
    if local.exists():
        size = sum(f.stat().st_size for f in local.rglob("*") if f.is_file())
        print(f"  Staging local : {local}  ({human(size)})")
        if confirm("Borrar staging local? [y/N]"):
            subprocess.run(["rm", "-rf", str(local)])
            if local.exists():
                bad("No se pudo borrar el staging local.")
            else:
                ok("Staging borrado.")
    print()
    prefix = s3_src[len(f"s3://{BUCKET_SRC}/"):]
    if parts:
        print(f"  Landing origen: {len(parts)} particiones movidas en {s3_src}")
        pregunta = "Borrar del landing las particiones movidas? [y/N]"
    else:
        print(f"  Landing origen: {s3_src}")
        pregunta = "Borrar landing de origen ahora? [y/N]"
    info("Tiene lifecycle (~7 dias): se borra solo. Borrarlo ahora solo libera espacio antes.")
    if not confirm(pregunta):
        return
    if not check_session(PROFILE_SRC):
        bad(f"Sesion SSO de {ACCOUNT_SRC} vencida: NO se borro nada del landing.")
        info(f"aws sso login --profile {PROFILE_SRC}")
        return
    objetivos = [f"{s3_src}{p}/" for p in parts] if parts else [s3_src]
    for t in objetivos:
        aws(["s3", "rm", t, "--recursive", "--only-show-errors"],
            PROFILE_SRC, parse=False, check=False)
    try:
        if parts:
            vivas = set(list_prefixes(BUCKET_SRC, prefix, PROFILE_SRC, "verificando",
                                      strict=True))
            quedan = len(set(parts) & vivas)
        else:
            quedan = 1 if quedan_objetos(BUCKET_SRC, prefix, PROFILE_SRC) else 0
    except RuntimeError as e:
        bad(f"No pude verificar el borrado: {e}")
        return
    if quedan:
        bad("El landing NO quedo limpio; volve a correr la limpieza.")
    else:
        ok("Particiones movidas borradas del landing." if parts else "Landing de origen borrado.")


# ---------------------------------------------------------------- defs

def load_defs():
    d = Path(DEFS_DIR)
    if not d.is_dir():
        bad(f"No existe el directorio de definiciones:\n    {DEFS_DIR}")
        info("Ajustalo con:  export UNLOAD_DEFS=/ruta/a/definitions/redshift")
        sys.exit(1)
    out = []
    for f in sorted(d.glob("*.json")):
        try:
            cfg = json.loads(f.read_text())
        except Exception as e:
            warn(f"{f.name}: JSON invalido ({e})")
            continue
        if "schema" not in cfg or "table" not in cfg:
            warn(f"{f.name}: sin 'schema'/'table', se omite")
            continue
        cfg["_file"] = f
        out.append(cfg)
    return out


def show_list(defs):
    title(f"Tablas disponibles  ({len(defs)})")
    print(f"  {C.DIM}{DEFS_DIR}{C.END}\n")
    w = max((len(c["table"]) for c in defs), default=10)
    for i, c in enumerate(defs, 1):
        flag = f"{C.OK}only_unload{C.END}" if c.get("only_unload") else f"{C.WARN}normal{C.END}"
        conn = c.get("redshift_conn_id", "—")
        print(f"  {C.B}{i:>2}{C.END}. {c['table']:<{w}}  {C.DIM}{c['schema']}{C.END}  "
              f"[{flag}]  {C.DIM}{conn}{C.END}")
    print()


def pick(defs, arg):
    if arg is None:
        show_list(defs)
        try:
            arg = input(f"  Numero o nombre (enter para salir): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not arg:
            sys.exit(0)
    if arg.isdigit():
        i = int(arg)
        if not 1 <= i <= len(defs):
            bad(f"Numero fuera de rango (1-{len(defs)})")
            sys.exit(1)
        return defs[i - 1]
    hits = [c for c in defs if c["table"] == arg]
    if not hits:
        hits = [c for c in defs if arg.lower() in c["table"].lower()]
    if not hits:
        bad(f"No encontre ninguna tabla que matchee '{arg}'")
        sys.exit(1)
    if len(hits) > 1:
        bad(f"'{arg}' matchea varias: " + ", ".join(h["table"] for h in hits))
        sys.exit(1)
    return hits[0]


# ---------------------------------------------------------------- analisis

def analyze(cfg, glue_tbl):
    """Compara lo que produce el UNLOAD contra el Glue Catalog del destino."""
    title("Analisis de compatibilidad")

    if glue_tbl is None:
        bad(f"La tabla {GLUE_DB}.{cfg['_glue_table']} no existe en el Glue Catalog")
        info("Sin tabla destino no se puede validar el esquema ni registrar particiones.")
        return False

    sd = glue_tbl["StorageDescriptor"]
    cat_cols = [c["Name"] for c in sd.get("Columns", [])]
    cat_types = {c["Name"]: c.get("Type", "?") for c in sd.get("Columns", [])}
    part_keys = [p["Name"] for p in glue_tbl.get("PartitionKeys", [])]

    only = bool(cfg.get("only_unload"))
    column_dt = cfg.get("column_dt")
    mapping = list(cfg.get("columns_mapping", {}).keys())

    strict = only and bool(cfg.get("schema_source"))

    # Lo que realmente produce el UNLOAD
    if strict:
        produced = [c for c in mapping if c != column_dt]
    elif only:
        produced = [c for c in mapping if c != column_dt] + AUDIT_COLUMNS
    else:
        produced = list(mapping)

    fmt = sd.get("InputFormat", "")
    is_parquet = "parquet" in fmt.lower() or "parquet" in sd.get(
        "SerdeInfo", {}).get("SerializationLibrary", "").lower()
    print(f"  Catalog  : {GLUE_DB}.{cfg['_glue_table']}")
    print(f"  Location : {sd.get('Location', '?')}")
    print(f"  Formato  : {'parquet' if is_parquet else fmt or '?'}")
    print()

    problems = []

    # -- particion
    if not part_keys:
        warn("La tabla destino NO esta particionada.")
        if only:
            problems.append("only_unload particiona pero el destino no espera particiones")
    elif len(part_keys) == 1 and part_keys[0] == column_dt:
        if only:
            ok(f"Particion '{part_keys[0]}' coincide con column_dt del JSON")
        else:
            bad(f"El destino particiona por '{part_keys[0]}' pero el JSON no tiene only_unload")
            problems.append("falta only_unload: el UNLOAD saldria plano, sin particiones")
    else:
        bad(f"Particion del destino {part_keys} != column_dt del JSON ('{column_dt}')")
        problems.append("la clave de particion no calza")

    # -- columnas
    missing = [c for c in cat_cols if c not in produced]
    extra = [c for c in produced if c not in cat_cols]

    print()
    print(f"  Columnas en el Catalog : {len(cat_cols)}")
    print(f"  Columnas del UNLOAD    : {len(produced)}"
          + (f"  {C.DIM}(modo estricto){C.END}" if strict else
             f"  {C.DIM}(incluye {len(AUDIT_COLUMNS)} de auditoria){C.END}" if only else ""))
    print()

    if not missing and not extra:
        ok("Las columnas calzan exactamente.")
    if missing:
        bad(f"FALTAN en el UNLOAD ({len(missing)}) - se leerian como NULL:")
        for c in missing:
            info(f"- {c} ({cat_types.get(c, '?')})")
        if not only and all(c in AUDIT_COLUMNS for c in missing):
            info("")
            info('Solucion: agrega  "only_unload": true  al JSON.')
            info("El generador agrega fecha_ejecucion y extraction_date, y particiona.")
        problems.append(f"{len(missing)} columnas faltantes")
    if extra:
        warn(f"SOBRAN en el UNLOAD ({len(extra)}) - Athena las ignora:")
        for c in extra:
            info(f"- {c}")

    # -- tipos (solo modo estricto: ahi los tipos del JSON son los del destino
    #    y el generador castea cada columna a ese tipo)
    if strict:
        def _tn(t):
            t = (t or "").strip().lower().replace(" ", "")
            return {"integer": "int"}.get(t, t)
        mt = cfg.get("columns_mapping", {})
        difs = [(col, mt[col][1], cat_types[col]) for col in cat_cols
                if col in mt and _tn(mt[col][1]) != _tn(cat_types[col])]
        if difs:
            bad(f"TIPOS distintos al destino ({len(difs)}):")
            for col, t_json, t_cat in difs[:10]:
                info(f"- {col}: JSON {t_json}  vs  destino {t_cat}")
            problems.append(f"{len(difs)} columnas con tipo distinto")
        else:
            ok("Tipos identicos al destino: el UNLOAD castea cada columna.")

    print()
    if problems:
        bad("NO es seguro mover todavia:")
        for p in problems:
            info(f"- {p}")
    else:
        ok("El UNLOAD calza con la tabla destino. Se puede mover.")
    return not problems


# ---------------------------------------------------------------- particiones

def register_glue(cfg, parts, glue_tbl, real_base):
    """Registra particiones con glue batch-create-partition (lotes de 100).

    IMPORTANTE: la ubicacion de cada particion se arma con real_base (la ruta
    a la que efectivamente subimos), NO con el Location de la tabla. Hay tablas
    cuyo Location quedo mal registrado (schema duplicado) y apunta a una carpeta
    que no existe; la tabla igual funciona porque cada particion lleva su propio
    Location. Usar el de la tabla registraria particiones vacias.
    """
    sd = dict(glue_tbl["StorageDescriptor"])
    base = real_base.rstrip("/")
    tbl = cfg["_glue_table"]

    existing = set()
    token = None
    while True:
        args = ["glue", "get-partitions", "--database-name", GLUE_DB, "--table-name", tbl]
        if token:
            args += ["--next-token", token]
        out = aws(args, PROFILE_DST, check=False)
        if not out:
            break
        for p in out.get("Partitions", []):
            existing.add(tuple(p["Values"]))
        token = out.get("NextToken")
        if not token:
            break

    todo = [p for p in parts if (p.split("=", 1)[1],) not in existing]
    if not todo:
        ok(f"Las {len(parts)} particiones ya estaban registradas.")
        return True

    print(f"  Registrando {len(todo)} particiones nuevas "
          f"({len(parts) - len(todo)} ya existian)...")

    created = failed = 0
    for i in range(0, len(todo), 100):
        chunk = todo[i:i + 100]
        inp = []
        for part in chunk:
            val = part.split("=", 1)[1]
            psd = json.loads(json.dumps(sd))
            psd["Location"] = f"{base}/{part}/"
            psd.pop("Parameters", None)
            inp.append({"Values": [val], "StorageDescriptor": psd})
        payload = {
            "DatabaseName": GLUE_DB,
            "TableName": tbl,
            "PartitionInputList": inp,
        }
        tmp = Path(STAGING) / f"_part_{i}.json"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload))
        out = aws(["glue", "batch-create-partition", "--cli-input-json", f"file://{tmp}"],
                  PROFILE_DST, check=False)
        tmp.unlink(missing_ok=True)
        if out is None:
            failed += len(chunk)
            continue
        errs = out.get("Errors", []) or []
        # AlreadyExists no es un error real
        real = [e for e in errs
                if e.get("ErrorDetail", {}).get("ErrorCode") != "AlreadyExistsException"]
        created += len(chunk) - len(errs)
        failed += len(real)
        if real:
            for e in real[:3]:
                info(f"- {e.get('PartitionValues')}: "
                     f"{e.get('ErrorDetail', {}).get('ErrorMessage', '?')}")
        print(f"    lote {i // 100 + 1}: {len(chunk) - len(errs)} creadas")

    if failed:
        bad(f"{failed} particiones fallaron.")
        return False
    ok(f"{created} particiones registradas en el Glue Catalog.")
    return True


def register_msck(cfg):
    """MSCK REPAIR TABLE via Athena."""
    tbl = cfg["_glue_table"]
    outloc = os.environ.get("ATHENA_OUTPUT")
    if not outloc:
        wg = aws(["athena", "get-work-group", "--work-group", "primary"],
                 PROFILE_DST, check=False)
        if wg:
            outloc = (wg.get("WorkGroup", {}).get("Configuration", {})
                      .get("ResultConfiguration", {}).get("OutputLocation"))
    if not outloc:
        bad("Athena necesita una ubicacion de resultados y no la encontre.")
        info("export ATHENA_OUTPUT=s3://<bucket>/athena-results/")
        return False

    q = f"MSCK REPAIR TABLE `{GLUE_DB}`.`{tbl}`"
    print(f"  {q}")
    print(f"  {C.DIM}resultados -> {outloc}{C.END}")
    start = aws(["athena", "start-query-execution", "--query-string", q,
                 "--result-configuration", f"OutputLocation={outloc}"],
                PROFILE_DST, check=False)
    if not start:
        bad("No pude lanzar la query en Athena.")
        return False
    qid = start["QueryExecutionId"]
    print(f"  QueryExecutionId: {qid}")
    print("  Esperando", end="", flush=True)
    while True:
        time.sleep(3)
        st = aws(["athena", "get-query-execution", "--query-execution-id", qid],
                 PROFILE_DST, check=False)
        state = st["QueryExecution"]["Status"]["State"] if st else "UNKNOWN"
        if state in ("SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"):
            print()
            if state == "SUCCEEDED":
                ok("MSCK REPAIR TABLE completado.")
                return True
            reason = (st["QueryExecution"]["Status"].get("StateChangeReason", "")
                      if st else "")
            bad(f"MSCK {state}. {reason}")
            return False
        print(".", end="", flush=True)


def validate(cfg, parts):  # noqa: legacy
    n = len(aws(["glue", "get-partitions", "--database-name", GLUE_DB,
                 "--table-name", cfg["_glue_table"], "--max-items", "1000"],
                PROFILE_DST, check=False).get("Partitions", []))
    print(f"  Particiones visibles en el Catalog: {n}")
    print()
    print("  Validar en Athena:")
    print(f"{C.DIM}    SELECT {cfg['column_dt']}, count(*) AS filas")
    print(f"    FROM {GLUE_DB}.{cfg['_glue_table']}")
    print(f"    WHERE {cfg['column_dt']} >= DATE '{parts[0].split('=')[1]}'")
    print(f"    GROUP BY 1 ORDER BY 1 LIMIT 20;{C.END}")



def cmd_verificar(cfg):
    """Chequeo profundo de una tabla ya movida."""
    schema, table = cfg["schema"], cfg["table"]
    p_dst = PREFIX_DST_TPL.format(schema=schema, table=table)
    s3_dst = f"s3://{BUCKET_DST}/{p_dst}"

    title(f"Verificacion: {schema}.{table}")
    print(f"  {s3_dst}\n")

    parts_s3 = list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "carpetas en S3")
    n_glue = count_partitions(cfg)

    print(f"  Carpetas calendar_dt= en S3        : {len(parts_s3)}")
    print(f"  Particiones en el Glue Catalog     : {n_glue}")
    print()

    problemas = []
    if len(parts_s3) == 0:
        if not check_session(PROFILE_DST):
            bad(f"La sesion SSO de {ACCOUNT_DST} expiro: no pude leer S3 ni Glue.")
            info("Los ceros de arriba NO son reales, son la sesion caida.")
            info(f"aws sso login --profile {PROFILE_DST}  y volve a verificar.")
            return False
        bad("No hay datos en S3.")
        problemas.append("sin datos")
    elif n_glue == len(parts_s3):
        ok("Coinciden: todas las particiones estan registradas.")
    elif n_glue < len(parts_s3):
        bad(f"Faltan {len(parts_s3) - n_glue} particiones por registrar.")
        info(f"Registralas con:  unload {table} --particiones")
        problemas.append("faltan particiones")
    else:
        warn(f"Hay {n_glue - len(parts_s3)} particiones registradas sin datos en S3.")
        problemas.append("particiones huerfanas")

    # Location de la tabla vs realidad
    g = aws(["glue", "get-table", "--database-name", GLUE_DB,
             "--name", cfg["_glue_table"]], PROFILE_DST, check=False)
    if g:
        cat_loc = g["Table"]["StorageDescriptor"].get("Location", "").rstrip("/")
        print()
        if cat_loc == s3_dst.rstrip("/"):
            ok("El Location de la tabla apunta a la ruta real.")
        else:
            warn("El Location de la TABLA no apunta a la ruta real:")
            info(f"Catalog : {cat_loc}/")
            info(f"Real    : {s3_dst}")
            info("No rompe hoy (cada particion lleva su Location), pero cualquier")
            info("herramienta que derive rutas del Location de la tabla va a fallar.")
            info(f"Corregir con:  unload {table} --fix-location")

    # Location de una particion de muestra
    samp = sample_partition_location(cfg)
    if samp:
        val, loc = samp
        print()
        if loc.rstrip("/").startswith(s3_dst.rstrip("/")):
            ok(f"Particion de muestra ({val}) apunta bien.")
        else:
            bad(f"Particion {val} apunta a una ruta distinta:")
            info(loc)
            problemas.append("particiones con Location incorrecto")

    print()
    if problemas:
        bad("Revisar: " + ", ".join(problemas))
    else:
        ok("Todo consistente.")
    return not problemas


def cmd_fix_location(cfg):
    """Corrige el Location de la tabla cuando quedo mal registrado."""
    p_dst = PREFIX_DST_TPL.format(schema=cfg["schema"], table=cfg["table"])
    s3_dst = f"s3://{BUCKET_DST}/{p_dst}"
    g = aws(["glue", "get-table", "--database-name", GLUE_DB,
             "--name", cfg["_glue_table"]], PROFILE_DST, check=False)
    if not g:
        bad("No encontre la tabla en el Catalog.")
        return
    t = g["Table"]
    cur = t["StorageDescriptor"].get("Location", "").rstrip("/")
    title("Corregir Location de la tabla")
    print(f"  Actual : {cur}/")
    print(f"  Nuevo  : {s3_dst}")
    if cur == s3_dst.rstrip("/"):
        ok("Ya estaba correcto, no hay nada que hacer.")
        return
    if input("\n  Aplicar el cambio? [y/N] ").strip().lower() != "y":
        print("  Cancelado.")
        return
    t["StorageDescriptor"]["Location"] = s3_dst
    inp = {k: v for k, v in t.items() if k in (
        "Name", "Description", "Owner", "StorageDescriptor", "PartitionKeys",
        "TableType", "Parameters", "Retention")}
    tmp = Path(STAGING) / "_tbl.json"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps({"DatabaseName": GLUE_DB, "TableInput": inp}))
    r = aws(["glue", "update-table", "--cli-input-json", f"file://{tmp}"],
            PROFILE_DST, check=False)
    tmp.unlink(missing_ok=True)
    if r is None:
        bad("Fallo el update-table.")
    else:
        ok("Location corregido.")


def cmd_particiones(cfg):
    """Registra particiones sin transferir nada (para datos ya subidos)."""
    p_dst = PREFIX_DST_TPL.format(schema=cfg["schema"], table=cfg["table"])
    s3_dst = f"s3://{BUCKET_DST}/{p_dst}"
    title("Registro de particiones (sin transferir)")
    parts = list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "carpetas en S3")
    if not parts:
        bad("No hay carpetas calendar_dt= en el destino.")
        return
    print(f"  {len(parts)} carpetas en S3: {parts[0]} .. {parts[-1]}")
    g = aws(["glue", "get-table", "--database-name", GLUE_DB,
             "--name", cfg["_glue_table"]], PROFILE_DST, check=False)
    if not g:
        bad("La tabla no existe en el Glue Catalog.")
        return
    register_glue(cfg, parts, g["Table"], s3_dst)


def cmd_estado(defs):
    """Panorama de las tablas del flujo (solo las only_unload)."""
    rel = [c for c in defs if c.get("only_unload")]
    title(f"Estado del backfill  ({len(rel)} tablas con only_unload)")
    if not rel:
        info("Ninguna tabla tiene only_unload: true todavia.")
        return
    for c in rel:
        c["_glue_table"] = GLUE_TABLE_TPL.format(schema=c["schema"], table=c["table"])
        p_src = PREFIX_SRC_TPL.format(schema=c["schema"], table=c["table"])
        p_dst = PREFIX_DST_TPL.format(schema=c["schema"], table=c["table"])
        src = list_prefixes(BUCKET_SRC, p_src, PROFILE_SRC, "origen")
        dst = list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "destino")
        glu = count_partitions(c)
        if not dst:
            estado = f"{C.ERR}sin datos en destino{C.END}"
        elif glu == len(dst):
            estado = f"{C.OK}listo{C.END}"
        elif glu < len(dst):
            estado = f"{C.WARN}faltan {len(dst) - glu} particiones por registrar{C.END}"
        else:
            estado = f"{C.WARN}revisar{C.END}"
        pend = f"  {C.DIM}(landing con {len(src)} particiones sin limpiar){C.END}" if src else ""
        print(f"  {C.B}{c['table']}{C.END}")
        print(f"    S3 destino {len(dst):>5}   Catalog {glu:>5}   {estado}{pend}")
    print()


# ---------------------------------------------------------------- mover

def move(cfg, use_msck):
    schema, table = cfg["schema"], cfg["table"]
    name_part = cfg.get("column_dt", "calendar_dt")
    p_src = PREFIX_SRC_TPL.format(schema=schema, table=table)
    p_dst = PREFIX_DST_TPL.format(schema=schema, table=table)
    s3_src = f"s3://{BUCKET_SRC}/{p_src}"
    s3_dst = f"s3://{BUCKET_DST}/{p_dst}"
    local = Path(STAGING) / table

    title("Origen")
    print(f"  {s3_src}")
    parts_src = list_prefixes(BUCKET_SRC, p_src, PROFILE_SRC)
    n_src, b_src, trunc = summarize(BUCKET_SRC, p_src, PROFILE_SRC, "origen")
    if n_src == 0:
        if ARGS_ANALIZAR:
            # Con --analizar el origen vacio no importa: el analisis compara
            # el JSON contra el Glue Catalog, no contra S3.
            warn("Origen vacio (el UNLOAD todavia no corrio).")
            info("Se analiza igual contra la tabla destino.")
        else:
            bad("No hay nada en el origen.")
            info("Corriste el DAG de UNLOAD para esta tabla?")
            return
    sufijo = "+ (truncado)" if trunc else ""
    print(f"  {n_src} objetos, {human(b_src)} {sufijo}, {len(parts_src)} particiones")

    if SOLO_PARTS:
        pedidas = {p if p.startswith(f"{name_part}=") else f"{name_part}={p}"
                   for p in SOLO_PARTS}
        faltan = sorted(pedidas - set(parts_src))
        parts_src = sorted(pedidas & set(parts_src))
        print()
        if SOLO_PARTS_RANGO:
            info(f"Se mueven solo {len(parts_src)} particiones (las de esta corrida / rango).")
            if parts_src:
                info(f"{parts_src[0]}  ..  {parts_src[-1]}")
            if faltan:
                info(f"{len(faltan)} fechas del rango no tienen datos en el landing.")
        else:
            warn(f"Filtro activo: solo {len(parts_src)} de las particiones del origen.")
            for p in parts_src[:10]:
                info(f"- {p}")
            if len(parts_src) > 10:
                info(f"... y {len(parts_src) - 10} mas")
            if faltan:
                bad(f"{len(faltan)} pedidas NO estan en el origen:")
                for p in faltan[:10]:
                    info(f"- {p}")
                if not confirm("Seguir con las que si estan? [y/N]"):
                    return
        if not parts_src:
            bad("Ninguna de las particiones pedidas esta en el origen.")
            return
    if parts_src:
        info(f"{parts_src[0]}  ..  {parts_src[-1]}")
    else:
        warn("Sin particiones: el UNLOAD salio plano (falta only_unload en el JSON?)")

    title("Destino")
    print(f"  {s3_dst}")
    parts_dst = list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "particiones destino")
    print(f"  {len(parts_dst)} particiones ya existentes")
    if parts_dst:
        info(f"{parts_dst[0]}  ..  {parts_dst[-1]}")

    glue_tbl = None
    g = aws(["glue", "get-table", "--database-name", GLUE_DB,
             "--name", cfg["_glue_table"]], PROFILE_DST, check=False)
    if g:
        glue_tbl = g["Table"]

    if glue_tbl:
        cat_loc = glue_tbl["StorageDescriptor"].get("Location", "").rstrip("/")
        if cat_loc and cat_loc != s3_dst.rstrip("/"):
            title("Aviso: Location del Catalog")
            warn("El Location de la tabla NO coincide con el destino real:")
            info(f"Catalog : {cat_loc}/")
            info(f"Real    : {s3_dst}")
            info("Se usa la ruta real para subir y para registrar particiones.")
            info("(pasa cuando el Location quedo mal registrado; cada particion")
            info(" lleva su propio Location, por eso la tabla igual funciona)")

    compatible = analyze(cfg, glue_tbl)

    if ARGS_ANALIZAR:
        return

    if not compatible:
        print()
        if not confirm("El analisis encontro problemas. Mover igual? [y/N]", destructivo=True):
            print("  Cancelado.")
            return

    # -- solapamiento
    title("Solapamiento")
    over = sorted(set(parts_src) & set(parts_dst))
    print(f"  Particiones solapadas: {len(over)}")

    # Caso tipico: ya moviste esta tabla antes. No hay nada que transferir,
    # solo (quiza) registrar particiones. Borrar seria tirar el trabajo hecho.
    # Con filtro explicito la intencion es reemplazar justo esas particiones,
    # asi que la deteccion de "ya movido" no aplica: seguir al borrado+subida.
    if SOLO_PARTS and over:
        info("Filtro activo: se reemplazan estas particiones aunque ya existan.")
    elif over and len(over) == len(parts_src):
        n_glue = count_partitions(cfg)
        print()
        warn("TODAS las particiones del origen ya estan en el destino.")
        info("Parece que esta tabla ya fue movida.")
        info(f"Particiones en S3 destino: {len(parts_dst)}  |  en el Catalog: {n_glue}")
        print()
        if n_glue >= len(parts_dst):
            ok("Ademas ya estan todas registradas: no hay nada que hacer.")
            info(f"Para revisar en detalle:  unload {table} --verificar")
            return
        print("  Opciones:")
        print("    1. registrar las particiones que faltan (no transfiere nada)")
        print("    2. borrar el destino y volver a subir todo")
        print("    3. cancelar")
        r = input("\n  Que hago? [1/2/3] ").strip()
        if r == "1":
            g = aws(["glue", "get-table", "--database-name", GLUE_DB,
                     "--name", cfg["_glue_table"]], PROFILE_DST, check=False)
            if g:
                register_glue(cfg, parts_dst, g["Table"], s3_dst)
            return
        if r != "2":
            print("  Cancelado.")
            return

    if over:
        warn("Estas fechas ya existen en destino. s3 sync NO las reemplaza:")
        warn("los archivos nuevos tienen otro nombre y las filas quedarian DUPLICADAS.")
        for p in over[:8]:
            info(f"- {p}")
        if len(over) > 8:
            info(f"... y {len(over) - 8} mas")
        print()
        if not confirm(f"Reemplazar esas {len(over)} particiones en destino? [y/N]",
                       destructivo=True):
            bad("Cancelado: subir sin borrar dejaria duplicados.")
            return
        info("Se borran DESPUES de bajar los datos nuevos, justo antes de subirlos:")
        info("si algo falla antes (SSO, red, crash), el destino queda intacto.")
    else:
        ok("Sin solapamiento.")

    print()
    if SOLO_PARTS:
        # b_src se midio sobre el prefijo completo, antes del filtro: recalcular
        # solo sobre las particiones filtradas para no mostrar un total enganoso.
        tam = tamanos_por_particion(BUCKET_SRC, p_src, PROFILE_SRC)
        n_sel = sum(tam.get(pt, (0, 0))[0] for pt in parts_src)
        b_sel = sum(tam.get(pt, (0, 0))[1] for pt in parts_src)
        print(f"  Se van a mover {human(b_sel)} en {len(parts_src)} particiones "
              f"({n_sel} objetos).")
        info(f"El landing completo pesa {human(b_src)}; el filtro mueve solo lo de arriba.")
    else:
        print(f"  Se van a mover {human(b_src)} en {len(parts_src)} particiones.")
    print(f"  {C.DIM}two-hop (bajar+subir): la copia server-side cross-account")
    print(f"  falla por kms:GenerateDataKey.{C.END}")
    print()
    if not confirm("Continuar con la transferencia? [y/N]"):
        print("  Cancelado.")
        return

    with KeepAwake():
        # -- bajar
        title("Bajando desde " + ACCOUNT_SRC)
        local.mkdir(parents=True, exist_ok=True)
        if SOLO_PARTS:
            # Con filtro se sincroniza particion por particion: un sync del
            # prefijo completo se traeria todo el landing.
            for i, part in enumerate(parts_src, 1):
                print(f"  [{i}/{len(parts_src)}] {part}")
                if not aws_stream(["s3", "sync", f"{s3_src}{part}/",
                                   str(local / part) + "/"], PROFILE_SRC):
                    bad(f"Fallo la descarga de {part}.")
                    return
        elif not aws_stream(["s3", "sync", s3_src, str(local) + "/"], PROFILE_SRC):
            bad("Fallo la descarga. Volve a correr: sync es reanudable.")
            return
        nloc = len(list(local.rglob("*.parquet")))
        ok(f"{nloc} parquet en {local}")

        # -- subir
        title("Subiendo a " + ACCOUNT_DST)
        if not check_session(PROFILE_DST):
            bad(f"La sesion SSO de {ACCOUNT_DST} expiro. No se borro ni subio nada.")
            info(f"aws sso login --profile {PROFILE_DST}")
            info("Despues:  flow <tabla> --solo-mover   (no vuelve a bajar)")
            return
        if over:
            print("  Reemplazando particiones existentes:")
            if not borrar_particiones(s3_dst, p_dst, over):
                return
        if SOLO_PARTS:
            for i, part in enumerate(parts_src, 1):
                print(f"  [{i}/{len(parts_src)}] {part}")
                if not aws_stream(["s3", "sync", str(local / part) + "/",
                                   f"{s3_dst}{part}/"], PROFILE_DST):
                    bad(f"Fallo la subida de {part}.")
                    return
        elif not aws_stream(["s3", "sync", str(local) + "/", s3_dst], PROFILE_DST):
            bad("Fallo la subida. Volve a correr: sync es reanudable.")
            return

    # -- verificar: un sync --dryrun vacio = todo subido
    title("Verificacion")
    if SOLO_PARTS:
        pend = []
        for part in parts_src:
            o = aws(["s3", "sync", str(local / part) + "/", f"{s3_dst}{part}/",
                     "--dryrun"], PROFILE_DST, parse=False, check=False)
            pend += [l for l in (o or "").splitlines() if l.strip()]
        out = "\n".join(pend)
    else:
        out = aws(["s3", "sync", str(local) + "/", s3_dst, "--dryrun"],
                  PROFILE_DST, parse=False, check=False)
    pending = [l for l in (out or "").splitlines() if l.strip()]
    if pending:
        bad(f"Quedaron {len(pending)} archivos sin subir. Volve a correr.")
        return
    ok("Todos los archivos estan en destino.")

    # -- particiones
    title("Registro de particiones")
    if use_msck:
        good = register_msck(cfg)
    else:
        if glue_tbl is None:
            bad("Sin tabla en el Catalog no puedo registrar particiones.")
            good = False
        else:
            good = register_glue(cfg, parts_src, glue_tbl, s3_dst)

    if good:
        title("Listo")
        n_glue = count_partitions(cfg)
        parts_final = list_prefixes(BUCKET_DST, p_dst, PROFILE_DST, "destino")
        print(f"  Carpetas en S3 : {len(parts_final)}")
        print(f"  En el Catalog  : {n_glue}")
        if n_glue == len(parts_final):
            ok("Coinciden.")
        else:
            warn("No coinciden; revisa con --verificar")
        cleanup(cfg, local, s3_src, parts_src if SOLO_PARTS else None)


# ---------------------------------------------------------------- main

ARGS_ANALIZAR = False
AUTO = False          # responde solo los pasos seguros
AUTO_BORRAR = False   # responde solo tambien los destructivos
SOLO_PARTS = None     # si no es None: mover solo estas particiones
SOLO_PARTS_RANGO = False  # el filtro viene de un rango/corrida: sin preguntas por faltantes


def parse_solo_parts(args):
    """--particion 2025-01-05,2025-01-12   |   --particiones-archivo lista.txt"""
    vals = []
    if "--particion" in args:
        i = args.index("--particion")
        if i + 1 < len(args):
            vals += [v.strip() for v in args[i + 1].split(",") if v.strip()]
    if "--particiones-archivo" in args:
        i = args.index("--particiones-archivo")
        if i + 1 < len(args):
            f = Path(args[i + 1])
            if not f.exists():
                bad(f"No existe el archivo {f}")
                sys.exit(1)
            vals += [l.strip() for l in f.read_text().splitlines()
                     if l.strip() and not l.startswith("#")]
    return vals or None


def main():
    global ARGS_ANALIZAR
    args = sys.argv[1:]
    flags = {a for a in args if a.startswith("-")}
    pos = [a for a in args if not a.startswith("-")]

    global AUTO, AUTO_BORRAR, SOLO_PARTS
    SOLO_PARTS = parse_solo_parts(args)
    AUTO = "--auto" in flags or "--auto-borrar" in flags
    AUTO_BORRAR = "--auto-borrar" in flags
    use_msck = "--msck" in flags
    ARGS_ANALIZAR = bool(flags & {"--analizar", "--analyze"})

    print(f"{C.DIM}unload v{__version__}{C.END}")
    defs = load_defs()
    if not defs:
        bad(f"No hay JSON en {DEFS_DIR}")
        sys.exit(1)

    if flags & {"--listar", "--list"}:
        show_list(defs)
        return

    if flags & {"--ayuda", "--help", "-h"}:
        print(__doc__)
        return

    # --estado no necesita elegir tabla
    if "--estado" in flags:
        for prof, acc in ((PROFILE_SRC, ACCOUNT_SRC), (PROFILE_DST, ACCOUNT_DST)):
            if not check_session(prof):
                bad(f"Sesion SSO no activa para {acc}")
                info(f"aws sso login --profile {prof}")
                sys.exit(1)
        cmd_estado(defs)
        return

    cfg = pick(defs, pos[0] if pos else None)
    cfg["_glue_table"] = GLUE_TABLE_TPL.format(schema=cfg["schema"], table=cfg["table"])

    title(f"{cfg['schema']}.{cfg['table']}")
    print(f"  JSON        : {cfg['_file'].name}")
    print(f"  conn        : {cfg.get('redshift_conn_id', '(default)')}")
    print(f"  column_dt   : {cfg.get('column_dt', '—')}")
    print(f"  only_unload : {cfg.get('only_unload', False)}")
    print(f"  columnas    : {len(cfg.get('columns_mapping', {}))}")

    for prof, acc in ((PROFILE_SRC, ACCOUNT_SRC), (PROFILE_DST, ACCOUNT_DST)):
        if not check_session(prof):
            bad(f"Sesion SSO no activa para {acc}")
            info(f"aws sso login --profile {prof}")
            sys.exit(1)

    if flags & {"--verificar", "--verify"}:
        cmd_verificar(cfg)
        return
    if "--fix-location" in flags:
        cmd_fix_location(cfg)
        return
    if flags & {"--particiones", "--partitions"}:
        cmd_particiones(cfg)
        return

    move(cfg, use_msck)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Interrumpido. sync es reanudable: volve a correr el mismo comando.")
        sys.exit(130)
    except RuntimeError as e:
        bad(str(e))
        sys.exit(1)
