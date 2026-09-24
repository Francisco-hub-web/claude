#!/usr/bin/env bash
# Instala 'unload' como alias persistente.
#
# Destino de instalacion: ~/lakehousev2/unload-kit
# (override con:  UNLOAD_HOME=/otra/ruta ./install.sh )
set -euo pipefail

DEST="${UNLOAD_HOME:-${HOME}/lakehousev2/unload-kit}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$DEST"
cp "${SRC}/unload.py" "${DEST}/unload.py"
cp "${SRC}/flow.py"   "${DEST}/flow.py"
cp "${SRC}/pedido.py" "${DEST}/pedido.py"
cp "${SRC}/README.md" "${DEST}/README.md" 2>/dev/null || true
mkdir -p "${DEST}/generator" "${DEST}/backups"
cp "${SRC}/generator/"*.py "${DEST}/generator/" 2>/dev/null || true
chmod +x "${DEST}/unload.py" "${DEST}/flow.py"

# zsh en macOS; si usas bash, cambia a ~/.bashrc
RC="${HOME}/.zshrc"
[[ "${SHELL:-}" == *bash* ]] && RC="${HOME}/.bashrc"

MARK="# >>> unload kit >>>"
if grep -qF "$MARK" "$RC" 2>/dev/null; then
  echo "Ya existe un bloque 'unload kit' en $RC; se actualiza la ruta del alias."
  python3 - "$RC" "$DEST" <<'PY'
import re, sys
rc, dest = sys.argv[1], sys.argv[2]
txt = open(rc).read()
txt = re.sub(r"alias unload='python3 [^']*'",
             "alias unload='python3 \"%s/unload.py\"'" % dest, txt)
open(rc, "w").write(txt)
PY
else
  cat >> "$RC" <<EOF

# >>> unload kit >>>
# Mueve parquet del UNLOAD a la tabla raw y registra particiones.
# UNLOAD_DEFS = donde viven los JSON de los DAGs (repo raw_layer_sm);
# es distinto de donde se instala la herramienta.
export UNLOAD_DEFS="\${HOME}/raw_layer_sm/raw_layer/develop/fcsm/dags/loaders/definitions/redshift"
alias unload='python3 "${DEST}/unload.py"'
alias flow='python3 "${DEST}/flow.py"'
# <<< unload kit <<<
EOF
  echo "Alias agregado a $RC"
fi

echo
echo "Instalado en ${DEST}/unload.py"
echo "Abre una terminal nueva, o corre:  source $RC"
echo
echo "Luego:"
echo "  unload              lista las tablas"
echo "  unload 2            elige la #2"
echo "  unload fact_obsolescence --analizar"
echo "  unload --estado"
echo ""
echo "  flow 6 --desde 2026-01-01 --hasta 2026-08-28   pipeline completo"
echo '  flow "tabla_x de julio a diciembre 2025, despues tabla_y todo 2024"   varias, en texto'
echo "  flow --cola                                    retoma la ultima cola"
