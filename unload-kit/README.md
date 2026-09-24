# unload kit v9.2  (flow 2.2 · unload 5.0 · generador modo estricto)

Backfill desde Redshift (cuenta 595738433757) hacia tablas raw existentes
(cuenta 608614369971). El esquema lo dicta la TABLA DESTINO.

## Instalar

    tar xzf unload-kit-v92.tar.gz && cd unload-kit && ./install.sh && source ~/.zshrc

Queda en ~/lakehousev2/unload-kit (otra ruta: UNLOAD_HOME=/ruta ./install.sh).

## PRIMERO: desplegar el generador (una sola vez)

flow deja los JSON en "modo estricto" (cada columna se castea al tipo del
destino). Eso lo entiende SOLO el generador nuevo (generator/ del kit).

    inic
    flow --desplegar-generador

Busca tu generador desplegado en S3 (el que define RedshiftLoaderBuilder),
lo respalda en backups/, te muestra que lineas del desplegado NO estan en el
nuevo (si ves algo tuyo, no confirmes), lo reemplaza en la MISMA ruta y lo
verifica. Tambien actualiza el archivo en tu repo local.

Es retrocompatible: los JSON sin "schema_source" se comportan igual que
antes y los DAGs FCSM normales no cambian. Commitealo en el repo junto a los
JSON sincronizados, o el proximo deploy de CI/CD lo pisa.

Con el generador viejo desplegado, flow NO publica JSON sincronizados (lo
revisa antes de subir), y si igual algo se colara, revierte la publicacion.

## Alcance

flow solo opera sobre loaders de backfill ("only_unload": true, marcados
[only_unload] en el listado). Los loaders FCSM normales (UNLOAD ->
iceberg_load -> s3_clean) no se tocan: flow corta sin modificarlos, y el
generador los renderiza exactamente igual que antes.

## flow

    flow 7 --desde 2025-01-01 --hasta 2025-12-31     pipeline completo
    flow chi_easy_dim_vw.fact_nueva --desde ... --hasta ...
    flow 7 --solo-sync                               sincroniza + publica el JSON
    flow 7 --solo-dag --desde ... --hasta ...        dispara y espera, no mueve
    flow 7 --run manual__2026-09-16T19:12:17Z        retoma un run (VPN caida)
    flow 7 --solo-mover [--desde ... --hasta ...]    mueve lo que hay en el landing
    ... --no-sync | --auto | --auto-borrar

Pasos:
 1. Sincroniza el JSON con el Glue Catalog del destino: columnas, orden,
    tipos, clave de particion y auditoria (fecha_ejecucion/extraction_date
    solo si el destino las tiene). Muestra el diff y respalda el JSON viejo
    en ~/lakehousev2/unload-kit/backups/. Idempotente.
 2. Publica el JSON en S3 solo si difiere del publicado, y espera a que
    MWAA lo re-parsee (last_parsed_time) con el generador correcto.
 3. Dispara el DAG con el rango por conf.
 4. Espera. Tolera cortes breves de VPN; si se cae del todo te da el
    comando exacto:  flow 7 --run <run_id>
 5. Mueve SOLO las particiones que escribio esta corrida (por fecha de
    escritura en el landing): los restos de corridas anteriores se ignoran.
 6. Registra particiones, verifica y limpia.

Columnas del destino que el origen no tiene -> NULL tipado (con aviso).
Si un CAST es imposible (ej. texto -> int), falla el UNLOAD y no se mueve nada.

## Reemplazo seguro de particiones

Las particiones que ya existen en destino se borran DESPUES de bajar los
datos nuevos, justo antes de subirlos, y se verifica que el borrado ocurrio
antes de subir (si no, no sube: nunca duplica). Un corte a mitad (SSO, red,
crash) deja el destino intacto o recuperable re-corriendo.

## unload (pasos sueltos)

    unload --estado | --listar
    unload 7 --analizar | --verificar | --particiones | --fix-location
    unload 7 --particion 2025-01-05,2025-01-12
    unload 7 --particiones-archivo dias.txt

## Requisitos

aws CLI; boto3 + requests (flow, como mwaa_cert); psycopg2 solo para
generar JSON de tablas nuevas o validar el origen (FLOW_RS_PASS).
MWAA es privado: flow necesita la VPN para disparar/esperar.
