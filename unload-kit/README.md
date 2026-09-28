# unload kit v9.5  (flow 2.5 · unload 5.2 · generador con dias sueltos)

Backfill desde Redshift (cuenta 595738433757) hacia tablas raw existentes
(cuenta 608614369971). El esquema lo dicta la TABLA DESTINO.

## Instalar

    tar xzf unload-kit-v92.tar.gz && cd unload-kit && ./install.sh && source ~/.zshrc

Queda en ~/lakehousev2/unload-kit (otra ruta: UNLOAD_HOME=/ruta ./install.sh).

## PRIMERO: desplegar el generador (una vez por version del kit)

flow deja los JSON en "modo estricto" (cada columna se castea al tipo del
destino) y puede pedir dias sueltos (param "load_dates"). Eso lo entiende
SOLO el generador del kit (generator/). v9.5 agrega load_dates: si ya
desplegaste el de v9.2-9.4, desplegalo de nuevo. La linea del WHERE que
cambia aparece como "reemplazada (esperado)"; cualquier otra que aparezca es
una personalizacion tuya: no confirmes.

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

## Varias tablas, en texto libre

    flow "vamos con tran_item del 4 de julio 2025 a fin de año y despues
          fact_x de enero a marzo 2026"
    flow --pegar          pegas el pedido (varias lineas) y terminas con Ctrl-D
    flow --cola           retoma la ultima cola donde quedo

1. Hace login SSO si hace falta (lo que hacian inic / inht).
2. Interpreta el pedido: con Claude Code (claude -p) si esta instalado; si
   no, con un parser local de fechas en castellano ("del 1 al 10 de octubre",
   "julio a diciembre 2025", "todo 2024", "desde marzo", "ultimos 30 dias",
   "fin de año", 04/07/2025, 2025-07-04...). Si los dos lo entienden
   distinto, te avisa.
3. Resuelve cada tabla: JSON de backfill existente (nombre, parte del nombre
   o numero del listado) o, si no hay, la tabla destino en el Glue Catalog.
4. Muestra el plan con fechas explicitas y pregunta dos cosas: si puede
   reemplazar particiones que ya existan en destino, y si ejecuta.
5. Corre las tablas una tras otra sin preguntar nada mas. Si una falla, sigue
   con la siguiente. Si vence el SSO, hace login y reintenta esa tabla. Si
   se cae la VPN, pausa: `flow --cola` sigue desde ahi.
6. Resumen final, JSON nuevos para commitear y log en logs/.

Sin comillas, zsh puede interpretar caracteres como ? * ( ): usalas.
Variables: FLOW_INTERPRETE=auto|claude|local, FLOW_CLAUDE_MODEL,
FLOW_LOGIN_CMD (ej. 'zsh -ic "inic && inht"').

## Dias especificos

    flow "quiero estos dias
          **chi_easy_dim_vw__fact_daily_inventory_tran_item** — 3 días:
          2025-01-05, 2025-01-17, 2025-02-19
          **chi_easy_dim_vw__fact_daily_mgt_sys_sales** — 5 días:
          2025-03-07, 2025-05-01 → 2025-05-03, 2025-07-11"

    flow 7 --particion 2025-01-05,2025-05-01..2025-05-03       una tabla, por CLI

Fechas separadas por coma, "y" o salto de linea son dias sueltos; unidas por
"a", "al", "hasta", "→" o ".." son un rango. Si el pedido dice "N días", flow
controla que coincida. Las tablas se pueden nombrar como esquema__tabla.

Por tabla hace UN solo UNLOAD con exactamente esos dias (conf "load_dates":
WHERE column_dt IN (...)), reutilizando los que ya esten validos en el
landing, y mueve SOLO esos dias. Los dias sin datos en el origen quedan en el
resumen.

Necesita el generador v9.5 desplegado (una vez):  flow --desplegar-generador
flow le pregunta a MWAA si el DAG ya acepta load_dates; si todavia no (o MWAA
no re-parseo), hace un UNLOAD por tramo continuo: mas lento, mismo resultado.
Si un UNLOAD escribe dias que no se pidieron, avisa y no los mueve.

Tambien aplica a los rangos: si al rango le faltan dias salteados en el
landing, se bajan todos en un solo UNLOAD con la lista exacta.

## Tablas sin JSON

`flow esquema.tabla --desde ... --hasta ...` (o nombrarla en un pedido)
genera el JSON desde la tabla destino del Glue Catalog: columnas, tipos,
particion y auditoria. No hace falta Redshift. Con FLOW_RS_PASS ademas
lee Redshift y manda NULL en las columnas que el origen no tenga; sin eso,
si falta alguna el UNLOAD falla y no se mueve nada. Si ya hay un
{tabla}.json de otro loader (el FCSM normal), el nuevo va a
{tabla}_backfill.json. Commitea los JSON nuevos en el repo.

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
    flow 7 --desde ... --hasta ... --forzar-unload   baja todo de Redshift aunque
                                                     ya este en el landing
    ... --no-sync | --auto | --auto-borrar

Pasos:
 1. Sincroniza el JSON con el Glue Catalog del destino: columnas, orden,
    tipos, clave de particion y auditoria (fecha_ejecucion/extraction_date
    solo si el destino las tiene). Muestra el diff y respalda el JSON viejo
    en ~/lakehousev2/unload-kit/backups/. Idempotente.
 2. Publica el JSON en S3 solo si difiere del publicado, y espera a que
    MWAA lo re-parsee (last_parsed_time) con el generador correcto.
 3. Revisa el landing (ver abajo): lo que ya esta y es valido NO se vuelve
    a bajar de Redshift.
 4. Dispara el DAG solo para las fechas que faltan (un run por tramo
    continuo, hasta 3; si estan muy salteadas, uno de punta a punta) y
    espera. Tolera cortes breves de VPN; si se cae del todo te da el
    comando exacto:  flow 7 --run <run_id>
 5. Vuelve a validar el landing y mueve particion por particion.
 6. Registra particiones, verifica y limpia.

## Reutiliza lo que ya esta en el landing

Una particion del landing se mueve sin volver a correr el UNLOAD solo si:
  - es posterior al JSON vigente (schema_synced_at);
  - la escribio un run del DAG que termino OK (un run fallido puede dejar
    archivos a medias);
  - su parquet tiene exactamente las columnas y tipos de la tabla destino
    (lee solo el footer de un archivo por particion: unos KB).
Lo que no pasa, se vuelve a bajar, y flow muestra el motivo de cada una.
Con --solo-mover y sin VPN no se puede consultar MWAA: valida solo fecha y
esquema, y lo avisa.

## Preguntas en modo automatico

--auto contesta si a lo seguro; --auto-borrar tambien a los reemplazos.
"Seguir igual pese al problema?" (el analisis no calza, el JSON no calza,
tabla sin destino) en modo automatico es siempre NO: esa tabla se corta.

## Si se corta (SSO, red, disco, Ctrl-C)

Volve a correr el MISMO comando. Lo ya movido queda en destino y
registrado; al re-correr se salta (mismos archivos y tamanos que el
landing) y lo que quedo bajado en disco no se vuelve a bajar. Si aws
falla, flow muestra su codigo y mensaje, reintenta 2 veces y revisa si
la sesion SSO vencio (UNLOAD_REINTENTOS para cambiar la cantidad).

Columnas del destino que el origen no tiene -> NULL tipado (con aviso).
Si un CAST es imposible (ej. texto -> int), falla el UNLOAD y no se mueve nada.

## Reemplazo seguro de particiones

Se mueve una particion a la vez: bajar (copia local identica al landing,
sin restos viejos) -> borrar la de destino si existia -> subir -> verificar
archivo por archivo -> liberar el disco. El disco nunca necesita mas de
una particion (+2 GB de margen, UNLOAD_MARGEN_DISCO_GB). La de destino se
borra recien cuando la nueva esta completa en disco, y se confirma el
borrado antes de subir: nunca duplica. Si la subida falla despues de
borrar, flow lo avisa y la copia sigue en disco: re-correr la sube.

`unload 7 --verificar` lista las particiones registradas sin datos (y cual
comando las restaura: desde el landing si siguen ahi, o re-bajandolas).

## unload (pasos sueltos)

    unload --estado | --listar
    unload 7 --analizar | --verificar | --particiones | --fix-location
    unload 7 --particion 2025-01-05,2025-01-12
    unload 7 --particiones-archivo dias.txt

## Requisitos

aws CLI; boto3 + requests (flow: MWAA y lectura del footer de los parquet
del landing, como mwaa_cert); psycopg2 solo para
generar JSON de tablas nuevas o validar el origen (FLOW_RS_PASS).
MWAA es privado: flow necesita la VPN para disparar/esperar.
