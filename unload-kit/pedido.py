#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pedido - convierte un pedido en texto libre en una cola de cargas para flow.

    flow "vamos con tran_item del 4 de julio 2025 a fin de año y despues
          fact_x de enero a marzo 2026"

    ->  tran_item  2025-07-04 .. 2025-12-31
        fact_x     2026-01-01 .. 2026-03-31

    flow "chi_easy_dim_vw__fact_y — 5 días: 2025-03-07, 2025-05-01 → 2025-05-03, 2025-07-11"

    ->  fact_y     5 dias: 2025-03-07, 2025-05-01, 2025-05-02, 2025-05-03, 2025-07-11

Rango o lista: fechas unidas por "a", "al", "hasta", "→", "..", "-" (o "entre X
y Y") forman un rango; separadas por coma, "y" o salto de linea, una lista de
dias sueltos (cada elemento puede ser un dia, un sub-rango o un mes entero).

Dos interpretes:
  - Claude Code (claude -p), si esta instalado: entiende cualquier redaccion.
  - Local (reglas de fechas en castellano): sin red e instantaneo. Es el
    respaldo si no hay Claude Code y el control cruzado si lo hay.
flow SIEMPRE muestra el resultado con fechas explicitas y pide confirmacion
antes de ejecutar: un rango mal entendido moveria datos que no son.

Variables: FLOW_INTERPRETE=auto|claude|local, FLOW_CLAUDE_BIN, FLOW_CLAUDE_MODEL.
"""

import json
import os
import re
import shutil
import subprocess
import unicodedata
from calendar import monthrange
from datetime import date, timedelta

MESES = {
    "enero": 1, "ene": 1, "febrero": 2, "feb": 2, "marzo": 3, "mar": 3,
    "abril": 4, "abr": 4, "mayo": 5, "may": 5, "junio": 6, "jun": 6,
    "julio": 7, "jul": 7, "agosto": 8, "ago": 8, "septiembre": 9,
    "setiembre": 9, "sept": 9, "sep": 9, "set": 9, "octubre": 10, "oct": 10,
    "noviembre": 11, "nov": 11, "diciembre": 12, "dic": 12,
}
_MES = "|".join(sorted(MESES, key=len, reverse=True))
_ANIO = r"(?:\s*,?\s+(?:de|del))?\s+(\d{4})"      # " 2025", " de 2025", " del 2025"
_REF_ANIO = r"(?:(?:este|ese|el|aquel)\s+)?an(?:o|io)\b"

# (tipo, regex). Se toman de izquierda a derecha; ante dos que empiezan en el
# mismo lugar gana la mas larga (y a igual largo, la primera de la lista).
PATRONES = [
    ("iso", r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b"),
    ("dmy", r"\b(\d{1,2})[/.-](\d{1,2})[/.-](\d{4}|\d{2})\b"),
    ("ultimos", r"\bultim[oa]s?\s+(\d{1,4})\s+dias\b"),
    # rango de dias del mismo mes: "del 1 al 10 de octubre", "entre el 5 y el 8 de julio 2025"
    ("dias", rf"\b(\d{{1,2}})\s*(?:al|a|-|hasta)\s*(?:el\s+)?(\d{{1,2}})(?:\s+de)?\s+({_MES})\b(?:{_ANIO})?"),
    ("dias", rf"\bentre\s+(?:el\s+)?(\d{{1,2}})\s+y\s+(?:el\s+)?(\d{{1,2}})(?:\s+de)?\s+({_MES})\b(?:{_ANIO})?"),
    # dias sueltos del mismo mes: "5, 17 y 28 de enero 2025"
    ("dias_lista", rf"\b(\d{{1,2}}(?:\s*(?:,|\by\b|\be\b)\s*(?:el\s+)?\d{{1,2}})+)(?:\s+de)?\s+({_MES})\b(?:{_ANIO})?"),
    ("fin_mes", rf"\bfin(?:es|al)?\s+de\s+({_MES})\b(?:{_ANIO})?"),
    ("dia", rf"\b(\d{{1,2}}|primero|1ro|1ero)(?:\s+de)?\s+({_MES})\b(?:{_ANIO})?"),
    ("fin_anio", rf"\bfin(?:es|al)?\s+(?:de|del)\s+{_REF_ANIO}(?:\s+(\d{{4}}))?"),
    ("fin_anio", r"\bfin(?:es|al)?\s+(?:de|del)\s+(\d{4})\b"),
    ("ini_anio", rf"\b(?:inicio|principio|comienzo)s?\s+(?:de|del)\s+{_REF_ANIO}(?:\s+(\d{{4}}))?"),
    ("ini_anio", r"\b(?:inicio|principio|comienzo)s?\s+(?:de|del)\s+(\d{4})\b"),
    ("mes", rf"\b({_MES})\b(?:{_ANIO})?"),
    ("ayer", r"\b(?:ayer|hoy|a la fecha|la fecha|ahora|la actualidad|al dia)\b"),
    ("anio", r"(?<![\w-])(20\d{2})(?![\w-])"),
]
NECESITAN_ANIO = {"dia", "mes", "fin_mes", "fin_anio", "ini_anio"}
CONECTORES = {"y", "e", "la", "las", "el", "los", "tabla", "tablas", "tambien",
              "con", "despues", "luego", "de", "del", "para", "a", "mismo",
              "rango", "igual"}
ABIERTO = r"\b(?:desde|a partir|en adelante)\b"


class PedidoInvalido(ValueError):
    pass


def _norm(t):
    t = unicodedata.normalize("NFKD", t.lower())
    return "".join(c for c in t if not unicodedata.combining(c))


# ─── tablas mencionadas ──────────────────────────────────────────────────────

def _refs(t, conocidas):
    """[(ini, fin, ref)] de las tablas mencionadas, en orden de aparicion.

    Se reconoce: un numero del listado ("tabla 7", "#7"), un identificador con
    guion bajo o esquema.tabla (fact_x, chi_easy_dim_vw.fact_x) o, tras la
    palabra "tabla", un nombre simple que sea una tabla conocida.
    """
    out = []
    for m in re.finditer(r"\btablas?\s+(?:n(?:ro|umero)?\.?\s*|#\s*)?(\d{1,3})\b", t):
        out.append((m.start(), m.end(), m.group(1)))
    for m in re.finditer(r"(?<![\w-])#(\d{1,3})\b", t):
        out.append((m.start(), m.end(), m.group(1)))
    # fact_x, esquema.tabla y el nombre de Glue esquema__tabla
    ident = r"(?<![\w.-])([a-z][a-z0-9]*(?:_+[a-z0-9]+)+(?:\.[a-z][a-z0-9_]*)?|[a-z][a-z0-9_]*\.[a-z][a-z0-9_]+)(?![\w-])"
    for m in re.finditer(ident, t):
        out.append((m.start(), m.end(), m.group(1).strip(".")))
    simples = {c for c in conocidas if "_" not in c}
    for m in re.finditer(r"\btablas?\s+([a-z][a-z0-9]+)\b", t):
        if m.group(1) in simples:
            out.append((m.start(1), m.end(1), m.group(1)))
    out.sort(key=lambda r: (r[0], -(r[1] - r[0])))
    limpio, fin = [], -1
    for r in out:
        if r[0] >= fin:
            limpio.append(r)
            fin = r[1]
    return limpio


# ─── fechas ──────────────────────────────────────────────────────────────────

def _menciones(seg):
    cands = []
    for prio, (tipo, pat) in enumerate(PATRONES):
        for m in re.finditer(pat, seg):
            cands.append((m.start(), -(m.end() - m.start()), prio, m.end(), tipo, m.groups()))
    cands.sort()
    out, fin = [], -1
    for ini, _, _, end, tipo, g in cands:
        if ini < fin:
            continue
        txt = seg[ini:end]
        if tipo == "dias":          # rango: el segundo dia cierra al primero
            nuevas = [_mencion("dia", (g[0], g[2], g[3]), txt),
                      _mencion("dia", (g[1], g[2], g[3]), txt)]
            nuevas[1]["par"] = True
        elif tipo == "dias_lista":  # sueltos: cada uno es su propio dia
            nuevas = [_mencion("dia", (n, g[1], g[2]), txt) for n in re.findall(r"\d{1,2}", g[0])]
            for m in nuevas[1:]:
                m["suelta"] = True
        else:
            nuevas = [_mencion(tipo, g, txt)]
        for m in nuevas:
            m["ini"], m["fin"] = ini, end
        out += nuevas
        fin = end
    return out


SEP_RANGO = r"\b(?:a|al|hasta)\b|→|->|\.\.|[–—-]"
SEP_LISTA = r"[,;\n]|\b(?:y|e)\b"


def _une_rango(sep, seg, ms):
    """True si el texto entre dos fechas las une en un rango (y no en una lista)."""
    if re.search(SEP_RANGO, sep):
        return True
    if re.search(SEP_LISTA, sep):
        # "entre julio y diciembre": la "y" une un rango
        return (len(ms) == 2 and re.search(r"\by\b", sep) is not None
                and re.search(r"\bentre\b", seg[:ms[0]["ini"]]) is not None)
    return len(ms) == 2             # "fact_x 2025-01-01 2025-02-01"


def _grupos(ms, seg):
    """Agrupa las menciones: cada grupo es un dia/mes/año suelto o un rango [desde, hasta].

    "2025-01-05, 2025-01-17, 2025-05-01 → 2025-05-03"  -> 3 grupos (el ultimo, rango)
    "de enero a marzo 2025"                            -> 1 grupo (rango)
    """
    grupos = []
    for i, m in enumerate(ms):
        if i and m.get("par"):
            grupos[-1].append(m)
        elif i == 0 or m.get("suelta"):
            grupos.append([m])
        elif len(grupos[-1]) == 1 and _une_rango(seg[ms[i - 1]["fin"]:m["ini"]], seg, ms):
            grupos[-1].append(m)
        else:
            grupos.append([m])
    return grupos


def _mencion(tipo, g, txt):
    y = lambda s: None if s is None else (2000 + int(s) if len(s) == 2 else int(s))
    if tipo == "iso":
        return {"tipo": "dia", "y": int(g[0]), "m": int(g[1]), "d": int(g[2]), "txt": txt}
    if tipo == "dmy":
        return {"tipo": "dia", "y": y(g[2]), "m": int(g[1]), "d": int(g[0]), "txt": txt}
    if tipo == "ultimos":
        return {"tipo": "ultimos", "n": int(g[0]), "txt": txt}
    if tipo == "fin_mes":
        return {"tipo": "fin_mes", "m": MESES[g[0]], "y": y(g[1]), "txt": txt}
    if tipo == "dia":
        d = 1 if g[0] in ("primero", "1ro", "1ero") else int(g[0])
        return {"tipo": "dia", "d": d, "m": MESES[g[1]], "y": y(g[2]), "txt": txt}
    if tipo in ("fin_anio", "ini_anio", "anio"):
        return {"tipo": tipo, "y": y(g[0]), "txt": txt}
    if tipo == "mes":
        return {"tipo": "mes", "m": MESES[g[0]], "y": y(g[1]), "txt": txt}
    return {"tipo": "ayer", "txt": txt}


def _completar_anios(ms, anio_ctx):
    """Anio faltante: el de la mencion vecina que lo tenga, o el de contexto.

    "de marzo a junio 2024"          -> marzo toma el 2024 de junio (el siguiente)
    "4 de julio 2025 a fin de año"   -> fin de año toma el 2025 del anterior
    """
    for i, m in enumerate(ms):
        if m["tipo"] not in NECESITAN_ANIO or m.get("y"):
            continue
        sig = next((x["y"] for x in ms[i + 1:] if x.get("y")), None)
        ant = next((x["y"] for x in reversed(ms[:i]) if x.get("y")), None)
        preferido = (ant, sig) if m["tipo"] == "fin_anio" or i > 0 else (sig, ant)
        m["y"] = next((v for v in preferido if v), anio_ctx)
        m["inferido"] = True


def _ini(m, ayer):
    t = m["tipo"]
    if t == "dia":
        return date(m["y"], m["m"], m["d"])
    if t == "mes":
        return date(m["y"], m["m"], 1)
    if t == "fin_mes":
        return date(m["y"], m["m"], monthrange(m["y"], m["m"])[1])
    if t in ("anio", "ini_anio"):
        return date(m["y"], 1, 1)
    if t == "fin_anio":
        return date(m["y"], 12, 31)
    if t == "ultimos":
        return ayer - timedelta(days=m["n"] - 1)
    return ayer


def _fin(m, ayer):
    t = m["tipo"]
    if t == "mes":
        return date(m["y"], m["m"], monthrange(m["y"], m["m"])[1])
    if t == "anio":
        return date(m["y"], 12, 31)
    if t == "ini_anio":
        return date(m["y"], 1, 1)
    if t == "ultimos":
        return ayer
    return _ini(m, ayer)


def _rango(ms, seg, ayer):
    """(desde, hasta) de las menciones de un tramo del pedido."""
    if len(ms) == 1:
        m, t = ms[0], ms[0]["tipo"]
        abierto = re.search(ABIERTO, seg)
        if t == "ultimos":
            return _ini(m, ayer), ayer
        if t in ("fin_anio", "fin_mes", "ayer"):
            raise PedidoInvalido("dice hasta cuando, pero no desde cuando")
        if abierto or t == "ini_anio":
            return _ini(m, ayer), ayer
        if t == "dia" and re.search(r"\bhasta\b", seg):
            raise PedidoInvalido("dice hasta cuando, pero no desde cuando")
        return _ini(m, ayer), _fin(m, ayer)
    return _ini(ms[0], ayer), _fin(ms[-1], ayer)


def _rango_valido(ms, seg, ayer):
    try:
        desde, hasta = _rango(ms, seg, ayer)
    except ValueError as e:        # fecha imposible, ej. 31 de junio
        if isinstance(e, PedidoInvalido):
            raise
        raise PedidoInvalido(f"fecha invalida ({e})")
    if desde > hasta and ms[0].get("inferido"):
        ms[0]["y"] -= 1            # "de noviembre a febrero 2026" -> nov 2025
        desde, hasta = _rango(ms, seg, ayer)
    return desde, hasta


def _dias(desde, hasta):
    out, d = [], desde
    while d <= hasta:
        out.append(d)
        d += timedelta(days=1)
    return out


def _fechas_sueltas(grupos, ayer):
    """Dias de una lista de grupos (cada uno: un dia, un mes, un año o un rango)."""
    fechas = set()
    for g in grupos:
        try:
            a, b = _ini(g[0], ayer), _fin(g[-1], ayer)
            if a > b and g[0].get("inferido"):
                g[0]["y"] -= 1
                a = _ini(g[0], ayer)
        except ValueError as e:        # fecha imposible, ej. 31 de junio
            raise PedidoInvalido(f"fecha invalida ({e})")
        if a > b:
            raise PedidoInvalido(f"rango al reves ({a} > {b})")
        fechas.update(_dias(a, b))
    return sorted(fechas)


def _dias_declarados(seg):
    """El "N dias" que el pedido dice tener (ej. "— 8 días:"), para controlar."""
    for m in re.finditer(r"\b(\d{1,4})\s+dias\b", seg):
        if not re.search(r"ultim[oa]s?\s+$", seg[:m.start()]):
            return int(m.group(1))
    return None


def interpretar_local(texto, conocidas=(), hoy=None):
    """[{tabla, desde, hasta, fechas, nota, forzar_unload, solo_mover}] o PedidoInvalido.

    fechas: None si es un rango continuo; si el pedido lista dias sueltos
    (o varios tramos), la lista exacta de dias, y desde/hasta son sus extremos.
    """
    hoy = hoy or date.today()
    ayer = hoy - timedelta(days=1)
    t = _norm(texto)
    refs = _refs(t, set(conocidas))
    if not refs:
        raise PedidoInvalido("no encontre ninguna tabla (usa el nombre con guion bajo, "
                             "esquema.tabla o 'tabla 7')")

    # cada tabla se lleva el texto que viene despues de ella, hasta la siguiente
    tramos = []
    for i, (ini, fin, ref) in enumerate(refs):
        hasta_txt = refs[i + 1][0] if i + 1 < len(refs) else len(t)
        seg = t[fin:hasta_txt]
        palabras = [w for w in re.findall(r"[a-z0-9]+", seg) if w not in CONECTORES]
        tramos.append({"ref": ref, "seg": seg, "ms": _menciones(seg), "corto": not palabras})
    prefijo = t[:refs[0][0]]
    ms_prefijo = _menciones(prefijo)

    # una tabla sin fechas propias comparte las de otra:
    #   "fact_x y fact_y de enero a marzo"  -> la siguiente
    #   "fact_x de enero a marzo, fact_y tambien" -> la anterior
    #   "entre julio y diciembre las tablas fact_x y fact_y" -> las del principio
    for i, tr in enumerate(tramos):
        if tr["ms"]:
            continue
        donante = None
        if tr["corto"]:
            donante = next((x for x in tramos[i + 1:] if x["ms"] and not x.get("heredado")), None)
        if donante is None and i > 0 and tramos[i - 1]["ms"]:
            donante = tramos[i - 1]
        if donante is not None:
            tr.update(ms=[dict(m) for m in donante["ms"]], seg=donante["seg"],
                      heredado=donante["ref"])
        elif ms_prefijo:
            tr.update(ms=[dict(m) for m in ms_prefijo], seg=prefijo)

    anio_ctx, explicito = hoy.year, False
    for m in ms_prefijo:
        if m.get("y"):
            anio_ctx, explicito = m["y"], True
    items, errores = [], []
    for tr in tramos:
        ms = tr["ms"]
        if not ms:
            errores.append(f"{tr['ref']}: no dice fechas")
            continue
        _completar_anios(ms, anio_ctx)
        grupos = _grupos(ms, tr["seg"])
        sin_anio = not explicito and all(m.get("inferido") or not m.get("y") for m in ms)
        try:
            if len(grupos) > 1:
                fechas = _fechas_sueltas(grupos, ayer)
                if sin_anio and fechas[-1] > ayer and any(m.get("inferido") for m in ms):
                    for m in ms:
                        if m.get("inferido"):
                            m["y"] -= 1
                    fechas = _fechas_sueltas(_grupos(ms, tr["seg"]), ayer)
                desde, hasta = fechas[0], fechas[-1]
            else:
                fechas = None
                desde, hasta = _rango_valido(ms, tr["seg"], ayer)
                if sin_anio and hasta > ayer and any(m.get("inferido") for m in ms):
                    # sin año en ningun lado: el año mas reciente en que el rango ya
                    # paso entero ("julio a diciembre" dicho en septiembre = el anterior)
                    for m in ms:
                        if m.get("inferido"):
                            m["y"] -= 1
                    desde, hasta = _rango_valido(ms, tr["seg"], ayer)
        except PedidoInvalido as e:
            errores.append(f"{tr['ref']}: {e}")
            continue
        for m in ms:
            if m.get("y") and not m.get("inferido"):
                anio_ctx, explicito = m["y"], True
        notas = []
        if tr.get("heredado"):
            notas.append(f"mismo rango que {tr['heredado']}")
        declarados = _dias_declarados(tr["seg"])
        n = len(fechas) if fechas else (hasta - desde).days + 1
        if declarados is not None and declarados != n:
            notas.append(f"ojo: el pedido dice {declarados} dias y encontre {n}")
        if fechas:
            futuras = [f for f in fechas if f > ayer]
            if futuras:
                fechas = [f for f in fechas if f <= ayer]
                notas.append(f"sin {len(futuras)} dia(s) desde hoy en adelante: "
                             + ", ".join(str(f) for f in futuras[:3]))
            if not fechas:
                errores.append(f"{tr['ref']}: todos los dias pedidos son de hoy en adelante")
                continue
            desde, hasta = fechas[0], fechas[-1]
        elif hasta > ayer:
            hasta = ayer
            notas.append("hasta recortado a ayer")
        if desde > hasta:
            errores.append(f"{tr['ref']}: el rango queda vacio ({desde} > {hasta})")
            continue
        items.append({
            "tabla": tr["ref"], "desde": str(desde), "hasta": str(hasta),
            "fechas": [str(f) for f in fechas] if fechas else None,
            "nota": "; ".join(notas),
            "forzar_unload": bool(re.search(r"\bforz", tr["seg"])),
            "solo_mover": bool(re.search(r"\bsolo[\s_-]*mov|\bsin (?:correr el )?unload\b", tr["seg"])),
        })
    if errores:
        raise PedidoInvalido("; ".join(errores))
    return items


# ─── Claude Code ─────────────────────────────────────────────────────────────

PROMPT = """Converti este pedido de cargas de datos en JSON. Responde SOLO con el JSON,
sin explicaciones y sin usar herramientas.

Hoy es {hoy} ({dia}). Ayer fue {ayer}.

Pedido:
<<<
{texto}
>>>

Formato exacto:
{{"items": [{{"tabla": "...", "desde": "AAAA-MM-DD", "hasta": "AAAA-MM-DD",
             "fechas": null, "forzar_unload": false, "solo_mover": false}}],
  "dudas": []}}

Reglas:
- Un item por tabla, en el orden del pedido.
- "tabla": si se refiere a una de las tablas conocidas (aunque la abrevie), su
  nombre exacto; si no, como la escribio el usuario (con esquema si lo dio:
  esquema.tabla; "esquema__tabla" es lo mismo). Si la nombra por numero
  ("la 7", "tabla 7"), pone "7".
- Rango continuo ("de enero a marzo"): desde/hasta y "fechas": null.
- Dias especificos o varios tramos sueltos ("2025-01-05, 2025-01-17",
  "5, 17 y 28 de enero", "2025-05-01 → 2025-05-03, 2025-07-11"): en "fechas"
  la lista COMPLETA de dias AAAA-MM-DD (cada sub-rango expandido dia por dia),
  y desde/hasta = el primero y el ultimo. No lo conviertas en un rango.
- Fechas inclusivas. Un mes: del 1 al ultimo dia. Un año: 1-ene a 31-dic.
  "Fin de año": 31-dic del año en contexto.
- Si no dice el año, usa el del contexto del pedido. Si no hay ninguno, el año
  mas reciente en que TODO el rango ya paso (ej. hoy {hoy}: "julio a diciembre"
  es del año anterior; "desde julio" es de este año hasta ayer).
- Si no dice hasta cuando, o dice "hoy" / "a la fecha": hasta = {ayer}.
  Nunca pongas fechas posteriores a {ayer}.
- Si varias tablas comparten un rango ("x e y de enero a marzo"), repetilo en cada una.
- forzar_unload: true solo si pide bajar de nuevo desde Redshift aunque ya este
  en el landing. solo_mover: true solo si pide mover sin correr el UNLOAD.
- No inventes: lo que no se pueda determinar, dejalo afuera y explicalo en "dudas".

Tablas conocidas (numero del listado, tabla, esquema, tipo):
{tablas}
"""

DIAS = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]


def _primer_json(s):
    dec = json.JSONDecoder()
    for i, ch in enumerate(s or ""):
        if ch == "{":
            try:
                obj, _ = dec.raw_decode(s[i:])
                if isinstance(obj, dict):
                    return obj
            except ValueError:
                continue
    return None


def _fecha_ok(s):
    try:
        return str(date.fromisoformat(str(s))) == str(s)
    except ValueError:
        return False


def interpretar_claude(texto, conocidas, hoy=None, timeout=180):
    """(items, dudas, error). conocidas: [(numero, esquema, tabla, es_backfill)]."""
    hoy = hoy or date.today()
    ayer = hoy - timedelta(days=1)
    exe = shutil.which(os.environ.get("FLOW_CLAUDE_BIN", "claude"))
    if not exe:
        return None, [], "Claude Code no esta instalado (no encontre 'claude' en el PATH)"
    tablas = "\n".join(f"  {n}. {t}  ({s})  {'backfill' if bf else 'fcsm normal'}"
                       for n, s, t, bf in conocidas) or "  (ninguna)"
    prompt = PROMPT.format(hoy=hoy, dia=DIAS[hoy.weekday()], ayer=ayer,
                           texto=texto.strip(), tablas=tablas)
    cmd = [exe, "-p", prompt]
    if os.environ.get("FLOW_CLAUDE_MODEL"):
        cmd += ["--model", os.environ["FLOW_CLAUDE_MODEL"]]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                           stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return None, [], f"Claude Code no respondio en {timeout}s"
    except OSError as e:
        return None, [], f"no pude ejecutar Claude Code ({e})"
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip().splitlines()
        return None, [], f"claude salio con codigo {r.returncode}: {err[-1][:160] if err else ''}"
    d = _primer_json(r.stdout)
    if not d or not isinstance(d.get("items"), list):
        return None, [], "Claude Code no devolvio el JSON esperado"
    items, dudas = [], [str(x) for x in (d.get("dudas") or [])]
    for it in d["items"]:
        if not isinstance(it, dict):
            continue
        tabla = str(it.get("tabla") or "").strip()
        desde, hasta = str(it.get("desde") or ""), str(it.get("hasta") or "")
        if not tabla or not _fecha_ok(desde) or not _fecha_ok(hasta):
            dudas.append(f"item descartado por incompleto: {json.dumps(it, ensure_ascii=False)}")
            continue
        nota, fechas = [], None
        if isinstance(it.get("fechas"), list) and it["fechas"]:
            fechas = sorted({str(f) for f in it["fechas"]})
            malas = [f for f in fechas if not _fecha_ok(f)]
            if malas:
                dudas.append(f"{tabla}: fechas invalidas {malas[:3]}; item descartado")
                continue
            futuras = [f for f in fechas if f > str(ayer)]
            if futuras:
                fechas = [f for f in fechas if f <= str(ayer)]
                nota.append(f"sin {len(futuras)} dia(s) desde hoy en adelante: "
                            + ", ".join(futuras[:3]))
            if not fechas:
                dudas.append(f"{tabla}: todos los dias pedidos son de hoy en adelante")
                continue
            desde, hasta = fechas[0], fechas[-1]
        elif hasta > str(ayer):
            hasta = str(ayer)
            nota.append("hasta recortado a ayer")
        if desde > hasta:
            dudas.append(f"{tabla}: rango vacio ({desde} > {hasta})")
            continue
        items.append({"tabla": tabla, "desde": desde, "hasta": hasta, "fechas": fechas,
                      "nota": "; ".join(nota),
                      "forzar_unload": it.get("forzar_unload") is True,
                      "solo_mover": it.get("solo_mover") is True})
    if not items:
        return None, dudas, "Claude Code no encontro ninguna carga en el pedido"
    return items, dudas, None


def mismo_plan(a, b):
    """True si dos interpretaciones piden las mismas fechas para las mismas tablas."""
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        tx, ty = _norm(x["tabla"]).replace("__", "."), _norm(y["tabla"]).replace("__", ".")
        if (x["desde"], x["hasta"]) != (y["desde"], y["hasta"]):
            return False
        if (x.get("fechas") or None) != (y.get("fechas") or None):
            return False
        if tx not in ty and ty not in tx:
            return False
    return True
