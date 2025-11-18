import pandas as pd
import numpy as np
from pathlib import Path

# ------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------
fecha_manual = None  
current_dir = Path(__file__).parent.absolute()

# Directorios de entrada
ink_base = current_dir / "data/processed/inkafarma"
mif_base = current_dir / "data/processed/mi_farma"

# Nuevo directorio de salida
output_dir = current_dir / "data/processed/consolidado_salud"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "precios_consolidados.xlsx"

# Si no se indica fecha, buscar todas las carpetas disponibles
if fecha_manual is None:
    fechas_disponibles = sorted([f.name for f in ink_base.iterdir() if f.is_dir()])
else:
    fechas_disponibles = [fecha_manual]

# Lista para guardar todos los consolidado de fechas
lista_consolidados = []

# ------------------------------------------
# PROCESAR TODAS LAS FECHAS
# ------------------------------------------
for fecha in fechas_disponibles:
    print(f"\nProcesando fecha: {fecha}")

    ink_file = ink_base / fecha / f"inkafarma_limpio_{fecha}.xlsx"
    mif_file = mif_base / fecha / f"mifarma_limpio_{fecha}.xlsx"

    if not ink_file.exists() or not mif_file.exists():
        print(f"Archivos no encontrados para la fecha {fecha}")
        continue

    # ------------------------------------------
    # CARGA DE DATOS
    # ------------------------------------------
    ink_df = pd.read_excel(ink_file)
    mif_df = pd.read_excel(mif_file)

    ink_df.columns = ink_df.columns.str.upper().str.strip()
    mif_df.columns = mif_df.columns.str.upper().str.strip()

    ink_df["FUENTE"] = "INKAFARMA"
    mif_df["FUENTE"] = "MIFARMA"

    # ------------------------------------------
    # FILTRAR PRECIOS EXTREMOS (>300)
    # ------------------------------------------
    ink_df = ink_df[ink_df["PRECIO_FINAL"] <= 300]
    mif_df = mif_df[mif_df["PRECIO_FINAL"] <= 300]

    df = pd.concat([ink_df, mif_df], ignore_index=True)

    df = df.dropna(subset=["CLAVE_MATCHING", "PRECIO_FINAL"])
    df = df[df["PRECIO_FINAL"] > 0]

    # ------------------------------------------
    # NORMALIZAR SUBCATEGORÍAS
    # ------------------------------------------
    df["SUBCATEGORIA"] = df["SUBCATEGORIA"].str.strip().replace({
        "Antibióticos Respiratorios": "Antibióticos"
    })

    # ------------------------------------------
    # CONSOLIDAR POR PRODUCTO (PROMEDIO GEOMÉTRICO)
    # ------------------------------------------
    resultados = []

    for clave, grupo in df.groupby("CLAVE_MATCHING"):
        precios = grupo["PRECIO_FINAL"].dropna()

        if len(precios) == 0:
            continue

        n = len(precios)
        promedio_geom = np.exp(np.mean(np.log(precios)))

        fila_ref = grupo.iloc[0]

        resultados.append({
            "FECHA": fecha,
            "CLAVE_MATCHING": clave,
            "CODIGO": fila_ref.get("CODIGO"),
            "CATEGORIA": fila_ref.get("CATEGORIA"),
            "SUBCATEGORIA": fila_ref.get("SUBCATEGORIA"),
            "NOMBRE": fila_ref.get("NOMBRE"),
            "NOMBRE_COMERCIAL": fila_ref.get("NOMBRE_COMERCIAL"),
            "CONCENTRACION": fila_ref.get("CONCENTRACION"),
            "DESCRIPCION": fila_ref.get("DESCRIPCION"),
            "LABORATORIO": fila_ref.get("LABORATORIO"),
            "PRESENTACION": fila_ref.get("PRESENTACION"),
            "TIPO_ENVASE": fila_ref.get("TIPO_ENVASE"),
            "CANTIDAD_ENVASE": fila_ref.get("CANTIDAD_ENVASE"),
            "UNIDAD_ENVASE": fila_ref.get("UNIDAD_ENVASE"),
            "PROMEDIO_GEOM": promedio_geom,
            "PRODUCTOS_DISPONIBLES": n,
        })

    consolidado = pd.DataFrame(resultados)

    # Contar matching
    matching_por_producto = (
        df.groupby("CLAVE_MATCHING")["FUENTE"]
        .nunique()
        .reset_index(name="MATCHING_POR_PRODUCTO")
    )
    consolidado = consolidado.merge(matching_por_producto, on="CLAVE_MATCHING", how="left")

    # Guardar para juntar después
    lista_consolidados.append(consolidado)

# ------------------------------------------
# UNIR TODAS LAS FECHAS
# ------------------------------------------
if lista_consolidados:
    final_df = pd.concat(lista_consolidados, ignore_index=True)
    final_df.to_excel(output_file, index=False)
    print(f"\nArchivo UNICO guardado en: {output_file.name}")
else:
    print("No se generaron resultados.")