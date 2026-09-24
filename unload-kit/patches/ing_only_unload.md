# Parche para ing.py — generar loaders con only_unload

Dos cambios. Reemplaza la funcion `build_loader_json` y su llamada.

## 1. Reemplazar build_loader_json (linea ~236)

```python
CONNS = {
    "1": None,                              # usa el default del generador
    "2": "redshift_corporativo_edw_easy",   # esquemas chi_easy_dim_vw
    "3": "catman_redshift_cl_edw_prod",
}


def choose_conn() -> str:
    print("\n  🔌 Conexion Redshift del loader:")
    print("  [1] default del generador")
    print("  [2] redshift_corporativo_edw_easy   (chi_easy_dim_vw / Easy)")
    print("  [3] catman_redshift_cl_edw_prod")
    return CONNS.get(input("  Conexion [1]: ").strip(), None)


def build_loader_json(schema, table, date_col, columns_mapping,
                      only_unload=False, conn_id=None):
    """
    only_unload=True  -> backfill hacia la tabla raw del mass_ingestion:
      el DAG solo hace el UNLOAD (sin iceberg_load ni s3_clean), sale
      particionado por column_dt y agrega fecha_ejecucion/extraction_date.
    only_unload=False -> flujo FCSM normal (UNLOAD + iceberg + clean).
    """
    d = OrderedDict()
    d["schema"] = schema
    d["table"] = table + "_fcsm" if not only_unload else table
    if conn_id:
        d["redshift_conn_id"] = conn_id
    if only_unload:
        d["only_unload"] = True
    d["column_dt"] = date_col
    d["columns_mapping"] = columns_mapping
    return d
```

Nota: con only_unload NO se agrega el sufijo `_fcsm`, porque el backfill
apunta a la tabla real, no a la de FCSM.

## 2. Reemplazar la llamada en main() (linea ~430)

```python
        # Si incremental → también loader
        if mode == "i" and date_col:
            print("\n  📦 Tipo de loader:")
            print("  [1] normal   (UNLOAD + iceberg_load + s3_clean)")
            print("  [2] backfill (solo UNLOAD, particionado, para mover a raw)")
            tipo = input("  Loader [1]: ").strip()
            only_unload = tipo == "2"
            conn_id = choose_conn() if only_unload else None

            loader_data = build_loader_json(
                schema, table, date_col, columns_mapping, only_unload, conn_id)
            suffix = "" if not only_unload else "_backfill"
            loader_path = LOADERS_PATH / f"{table}{suffix}.json"
            save_json(loader_data, loader_path)
            all_generated.append((loader_path, "loader"))
```

## 3. save_json ya soporta las llaves nuevas

Agrega "redshift_conn_id" y "only_unload" a la lista de `ordered`:

```python
    for key in ["engine", "database", "schema", "table", "redshift_conn_id",
                "only_unload", "aws_account", "dag",
                "column_dt", "columns_mapping", "extraction", "load"]:
```
