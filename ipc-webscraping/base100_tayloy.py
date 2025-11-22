# -*- coding: utf-8 -*-
"""
===============================================================================
IPC TAILOY POR NIVEL EDUCATIVO CON PROMEDIO GEOMÉTRICO
===============================================================================
- Calcula el precio promedio geométrico por nivel educativo (INICIAL, PRIMARIA,
  SECUNDARIA) para cada fecha.
- Construye una tabla de IPC tomando como base 100 la fecha BASE_DATE_STR.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# =============================================================================
# 1. CONFIGURACIÓN (RUTAS OBLIGATORIAS)
# =============================================================================

# Carpeta raíz de los NORMALIZADOS (por fecha) (processed)
NORMALIZED_ROOT: Path = Path("data\processed\tailoy")

# Carpeta donde se guardará el IPC final
IPC_OUTPUT_DIR: Path = Path("data\base100")

# Fecha base para el índice (DEBE existir como carpeta dentro de NORMALIZED_ROOT)
BASE_DATE_STR: str = "2025-10-29"
BASE_DATE: date = datetime.strptime(BASE_DATE_STR, "%Y-%m-%d").date()

# Nombre de columnas esperadas en los archivos normalizados
COL_PRECIO: str = "precio_por_unidad_estandar"
COL_PRODUCTO_ESPECIFICO: str = "producto_especifico"

# =============================================================================
# 2. MAPEO FIJO producto_especifico -> nivel_educativo  (NO TOCAR)
# =============================================================================

PRODUCTO_A_NIVEL: Dict[str, str] = {
    # INICIAL
    "cartulinas": "INICIAL",
    "colores": "INICIAL",
    "crayones-y-oleos": "INICIAL",
    "cuaderno triple reglon": "INICIAL",
    "limpiatipos": "INICIAL",
    "loncheras": "INICIAL",
    "plastilinas": "INICIAL",
    "reglas": "INICIAL",   

    # PRIMARIA
    "borradores": "PRIMARIA",
    "cartucheras": "PRIMARIA",
    "colas": "PRIMARIA",
    "cuaderno doble reglon": "PRIMARIA",
    "cuaderno rayado": "PRIMARIA",
    "forros": "PRIMARIA",
    "gomas": "PRIMARIA",
    "lapices": "PRIMARIA",
    "mochila niño": "PRIMARIA",
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
    "papel-bond": "SECUNDARIA",
    "plumones-para-papel": "SECUNDARIA",
    "resaltadores": "SECUNDARIA",
}

NIVELES_ORDEN: List[str] = ["INICIAL", "PRIMARIA", "SECUNDARIA"]

# =============================================================================
# 3. FUNCIONES AUXILIARES
# =============================================================================


def leer_normalizados_dia(carpeta_dia: Path) -> Optional[pd.DataFrame]:

    archivos: List[Path] = sorted(carpeta_dia.glob("*.xlsx"))
    archivos = [f for f in archivos if not f.name.startswith("~$")]  

    if not archivos:
        print(f"[AVISO] No se encontraron archivos .xlsx en: {carpeta_dia}")
        return None

    df_list: List[pd.DataFrame] = []
    for f in archivos:
        try:
            df = pd.read_excel(f)
            df_list.append(df)
        except Exception as e:
            print(f"[ERROR] No se pudo leer '{f.name}': {e}")

    if not df_list:
        print(f"[AVISO] No se pudo leer ningún archivo en: {carpeta_dia}")
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
        print(f"[AVISO] Después de filtrar, no quedan filas en: {carpeta_dia}")
        return None

    # Añade nivel educativo
    df_all["nivel_educativo"] = df_all[COL_PRODUCTO_ESPECIFICO].map(PRODUCTO_A_NIVEL)

    return df_all


def calcular_precios_promedio_geometrico_por_nivel(df: pd.DataFrame) -> pd.DataFrame:

    def promedio_geometrico(series: pd.Series) -> float:
        valores = series[series > 0]
        if len(valores) == 0:
            return float("nan")
        return float(np.exp(np.log(valores).mean()))

    grp = (
        df.groupby("nivel_educativo")[COL_PRECIO]
        .apply(promedio_geometrico)
        .rename("precio_promedio_geometrico")
        .reset_index()
    )
    return grp


# =============================================================================
# 4. PROCESO PRINCIPAL
# =============================================================================


def main() -> None:
    print("===================================================================")
    print(" IPC TAILOY POR NIVEL EDUCATIVO (PROMEDIO GEOMÉTRICO)")
    print("-------------------------------------------------------------------")
    print(f" Carpeta NORMALIZED_ROOT : {NORMALIZED_ROOT}")
    print(f" Carpeta IPC_OUTPUT_DIR  : {IPC_OUTPUT_DIR}")
    print(f" Fecha base (índice=100): {BASE_DATE_STR}")
    print("===================================================================\n")

    if not NORMALIZED_ROOT.exists():
        raise FileNotFoundError(f"No existe la carpeta NORMALIZED_ROOT: {NORMALIZED_ROOT}")

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

        df_prom = calcular_precios_promedio_geometrico_por_nivel(df_dia)
        df_prom["fecha"] = d
        registros.append(df_prom)

        niveles_con_datos = sorted(df_prom["nivel_educativo"].unique())
        print(f"   → Niveles con datos: {', '.join(niveles_con_datos)}\n")

    if not registros:
        raise RuntimeError("No se pudo calcular ningún promedio; revisa los archivos normalizados.")

    df_all = pd.concat(registros, ignore_index=True)

    # --- Tabla de precios: filas = nivel, columnas = fecha ---
    tabla_precios = df_all.pivot_table(
        index="nivel_educativo",
        columns="fecha",
        values="precio_promedio_geometrico",
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

    # Renombrar filas/columnas
    tabla_precios.index.name = "CATEGORIA"
    tabla_ipc.index.name = "CATEGORIA"

    tabla_precios.columns = [d.strftime("%d/%m/%Y") for d in tabla_precios.columns]
    tabla_ipc.columns = [d.strftime("%d/%m/%Y") for d in tabla_ipc.columns]

    # --- Guardar a Excel ---
    salida: Path = IPC_OUTPUT_DIR / "base_educacion.xlsx"

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        tabla_ipc.to_excel(writer, sheet_name="ipc_geometrico")

    print("===================================================================")
    print(" Proceso finalizado (PROMEDIO GEOMÉTRICO).")
    print(f" Archivo generado: {salida}")
    print("===================================================================")


if __name__ == "__main__":
    main()