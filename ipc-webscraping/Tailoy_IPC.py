# -*- coding: utf-8 -*-
"""
===============================================================================
IPC TAILOY POR NIVEL EDUCATIVO (INICIAL / PRIMARIA / SECUNDARIA)
===============================================================================
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional

import pandas as pd

# =============================================================================
# 1. CONFIGURACIÓN (RUTAS OBLIGATORIAS)
# =============================================================================

# RUTA OBLIGATORIA 1: carpeta raíz de los NORMALIZADOS (por fecha)
NORMALIZED_ROOT = Path(
    r"C:\Users\Hp\Desktop\WebScraping\Personal\tailoy-ipc-webscraping\normalized"
)

# RUTA OBLIGATORIA 2: carpeta donde se guardará el IPC final
IPC_OUTPUT_DIR = Path(
    r"C:\Users\Hp\Desktop\WebScraping\Personal\tailoy-ipc-webscraping\ipc_tailoy"
)

# Fecha base para el índice (DEBE existir como carpeta dentro de NORMALIZED_ROOT)
BASE_DATE_STR = "2025-10-29"
BASE_DATE: date = datetime.strptime(BASE_DATE_STR, "%Y-%m-%d").date()

# Nombre de la columna de precio en los archivos normalizados
COL_PRECIO = "precio_por_unidad_estandar"
COL_PRODUCTO_ESPECIFICO = "producto_especifico"

# =============================================================================
# 2. MAPEO FIJO producto_especifico -> nivel_educativo  (NO TOCAR)
# =============================================================================

PRODUCTO_A_NIVEL: Dict[str, str] = {
    # INICIAL
    "cartulinas": "INICIAL",
    "colores": "INICIAL",
    "crayones-y-oleos": "INICIAL",
    "limpiatipos": "INICIAL",
    "loncheras": "INICIAL",
    "plastilinas": "INICIAL",
    "reglas": "INICIAL",
    "cuaderno triple reglon": "INICIAL",

    # PRIMARIA
    "borradores": "PRIMARIA",
    "cartucheras": "PRIMARIA",
    "colas": "PRIMARIA",
    "cuaderno doble reglon": "PRIMARIA",
    "forros": "PRIMARIA",
    "gomas": "PRIMARIA",
    "lapices": "PRIMARIA",
    "mochila niño": "PRIMARIA",
    "cuaderno rayado": "PRIMARIA",
    "tajadores": "PRIMARIA",
    "tijeras": "PRIMARIA",

    # SECUNDARIA
    "calculadoras": "SECUNDARIA",
    "compases": "SECUNDARIA",
    "correctores": "SECUNDARIA",
    "cuaderno cuadriculado": "SECUNDARIA",
    "escuadras": "SECUNDARIA",
    "lapiceros": "SECUNDARIA",
    "mochila escolar": "SECUNDARIA",
    "plumones-para-papel": "SECUNDARIA",
    "resaltadores": "SECUNDARIA",
    "files-y-folders-de-manila": "SECUNDARIA",
}

NIVELES_ORDEN = ["INICIAL", "PRIMARIA", "SECUNDARIA"]

# =============================================================================
# 3. FUNCIONES AUXILIARES
# =============================================================================


def leer_normalizados_dia(carpeta_dia: Path) -> Optional[pd.DataFrame]:
    """
    Lee todos los .xlsx de una carpeta de fecha, los concatena y devuelve
    un DataFrame con:
      - producto_especifico
      - precio_por_unidad_estandar
    """
    archivos: List[Path] = sorted(carpeta_dia.glob("*.xlsx"))
    archivos = [f for f in archivos if not f.name.startswith("~$")]  

    if not archivos:
        print(f"⚠️  No se encontraron .xlsx en: {carpeta_dia}")
        return None

    df_list: List[pd.DataFrame] = []
    for f in archivos:
        try:
            df = pd.read_excel(f)
            df_list.append(df)
        except Exception as e:
            print(f" Error leyendo {f.name}: {e}")

    if not df_list:
        print(f"No se pudo leer ningún archivo en: {carpeta_dia}")
        return None

    df_all = pd.concat(df_list, ignore_index=True)

    columnas_necesarias = {COL_PRODUCTO_ESPECIFICO, COL_PRECIO}
    faltan = columnas_necesarias.difference(df_all.columns)
    if faltan:
        raise ValueError(
            f"Faltan columnas {faltan} en los archivos de: {carpeta_dia}"
        )

    # Normaliza tipos
    df_all[COL_PRODUCTO_ESPECIFICO] = (
        df_all[COL_PRODUCTO_ESPECIFICO].astype(str).str.strip()
    )
    df_all[COL_PRECIO] = pd.to_numeric(df_all[COL_PRECIO], errors="coerce")

    # Filtra a solo productos que están en el mapeo
    df_all = df_all[df_all[COL_PRODUCTO_ESPECIFICO].isin(PRODUCTO_A_NIVEL.keys())].copy()
    df_all = df_all.dropna(subset=[COL_PRECIO])

    if df_all.empty:
        print(f"Después de filtrar, no quedan filas en: {carpeta_dia}")
        return None

    # Añade nivel educativo
    df_all["nivel_educativo"] = df_all[COL_PRODUCTO_ESPECIFICO].map(PRODUCTO_A_NIVEL)

    return df_all


def calcular_precios_promedio_por_nivel(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    A partir de un DataFrame de un solo día, calcula el precio promedio
    por nivel educativo.
    """
    grp = (
        df.groupby("nivel_educativo")[COL_PRECIO]
        .mean()
        .rename("precio_promedio")
        .reset_index()
    )
    return grp


# =============================================================================
# 4. PROCESO PRINCIPAL
# =============================================================================


def main() -> None:
    print("===================================================================")
    print(" IPC TAILOY POR NIVEL EDUCATIVO")
    print(" Carpeta NORMALIZED_ROOT :", NORMALIZED_ROOT)
    print(" Carpeta IPC_OUTPUT_DIR  :", IPC_OUTPUT_DIR)
    print(" Fecha base (índice=100):", BASE_DATE_STR)
    print("===================================================================\n")

    if not NORMALIZED_ROOT.exists():
        raise FileNotFoundError(f"No existe la carpeta NORMALIZED_ROOT: {NORMALIZED_ROOT}")

    # Crear carpeta de salida si no existe
    IPC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Encontrar subcarpetas de fecha ---
    carpetas_fecha: Dict[date, Path] = {}
    for sub in NORMALIZED_ROOT.iterdir():
        if not sub.is_dir():
            continue
        try:
            d = datetime.strptime(sub.name, "%Y-%m-%d").date()
        except ValueError:
            # Nombre de carpeta que no es fecha -> se ignora
            continue
        carpetas_fecha[d] = sub

    if not carpetas_fecha:
        raise RuntimeError("No se encontraron carpetas de fecha en NORMALIZED_ROOT.")

    if BASE_DATE not in carpetas_fecha:
        raise RuntimeError(
            f"La fecha base {BASE_DATE_STR} no tiene carpeta dentro de NORMALIZED_ROOT."
        )

    fechas_ordenadas = sorted(carpetas_fecha.keys())

    # --- Procesar cada fecha ---
    registros: List[pd.DataFrame] = []
    for d in fechas_ordenadas:
        carpeta_dia = carpetas_fecha[d]
        print(f"Procesando fecha {d}  ({carpeta_dia}) ...")

        df_dia = leer_normalizados_dia(carpeta_dia)
        if df_dia is None:
            print(f"   → Sin datos utilizables para {d}, se omite.\n")
            continue

        df_prom = calcular_precios_promedio_por_nivel(df_dia)
        df_prom["fecha"] = d
        registros.append(df_prom)

        print(
            "   → Niveles con datos:",
            ", ".join(sorted(df_prom["nivel_educativo"].unique())),
        )
        print()

    if not registros:
        raise RuntimeError("No se pudo calcular ningún promedio; revisa los archivos.")

    df_all = pd.concat(registros, ignore_index=True)

    # --- Tabla de precios: filas = nivel, columnas = fecha ---
    tabla_precios = df_all.pivot_table(
        index="nivel_educativo",
        columns="fecha",
        values="precio_promedio",
    ).reindex(index=NIVELES_ORDEN)

    # Ordena columnas por fecha
    tabla_precios = tabla_precios.sort_index(axis=1)

    # --- Convertir a índice (base 100 en BASE_DATE) ---
    if BASE_DATE not in tabla_precios.columns:
        raise RuntimeError(
            f"La columna de la fecha base {BASE_DATE_STR} no existe en la tabla de precios."
        )

    precios_base = tabla_precios[BASE_DATE]

    # División fila a fila (axis=0) para obtener índices
    tabla_ipc = tabla_precios.divide(precios_base, axis=0) * 100.0

    # Renombrar fila/columnas como en tu ejemplo
    tabla_precios.index.name = "SUBCATEGORIA"
    tabla_ipc.index.name = "SUBCATEGORIA"

    tabla_precios.columns = [d.strftime("%d/%m/%Y") for d in tabla_precios.columns]
    tabla_ipc.columns = [d.strftime("%d/%m/%Y") for d in tabla_ipc.columns]

    # --- Guardar a Excel ---
    nombre_archivo = f"ipc_tailoy_nivel_educativo_base_{BASE_DATE_STR}.xlsx"
    salida = IPC_OUTPUT_DIR / nombre_archivo

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        tabla_precios.to_excel(writer, sheet_name="precios_promedio")
        tabla_ipc.to_excel(writer, sheet_name="ipc_nivel_educativo")

    print("===================================================================")
    print("Proceso terminado.")
    print("   Archivo guardado en:")
    print(f"   → {salida}")
    print("===================================================================")


# Ejecutar solo si es script principal
if __name__ == "__main__":
    main()
