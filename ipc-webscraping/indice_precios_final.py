import pandas as pd
from pathlib import Path
import re
import numpy as np

# ---------------- CONFIGURACIÓN ----------------
CARPETA = Path(r"d:\PEU-CD-Grupo-1-Proyecto-Integrador-main\ipc-webscraping\data\processed\consolidado")
BASE_FECHA = "20251031"  # fecha base = 31 de octubre 2025

# ---------------- FUNCION PARA EXTRAER FECHA ----------------
def extraer_fecha_archivo(nombre):
    """
    Extrae la fecha (AAAAMMDD) del nombre del archivo.
    Detecta patrones como:
    - precios_normalizados_multitienda_20251031.xlsx
    """
    match = re.search(r"(\d{8})", nombre)
    return match.group(1) if match else None

# ---------------- CARGAR ARCHIVOS ----------------
archivos = sorted(CARPETA.glob("precios_normalizados_multitienda_*.xlsx"))

dfs = []
for archivo in archivos:
    fecha = extraer_fecha_archivo(archivo.name)
    if not fecha:
        print(" No se detectó fecha en:", archivo.name)
        continue

    df = pd.read_excel(archivo)

    # verificar columnas
    if "subclase_inei" not in df.columns or "precio_por_unidad_estandar" not in df.columns:
        print(" Archivo ignorado, faltan columnas clave:", archivo.name)
        continue

    df["fecha"] = fecha
    dfs.append(df)

if not dfs:
    raise ValueError("No se cargó ningún archivo válido. Revisa la carpeta o columnas.")

data = pd.concat(dfs, ignore_index=True)

# ---------------- PROMEDIO GEOMÉTRICO ----------------
def geom_promedio(x):
    x = x[x > 0]
    return np.exp(np.log(x).mean()) if len(x) > 0 else np.nan

agrupado = (
    data.groupby(["fecha", "subclase_inei"], as_index=False)
    .agg(precio_gmean=("precio_por_unidad_estandar", geom_promedio))
)

# ---------------- CALCULAR TRANFORMACIÓN BASE ----------------
base = agrupado[agrupado["fecha"] == BASE_FECHA][["subclase_inei", "precio_gmean"]]
base = base.rename(columns={"precio_gmean": "precio_base"})

merged = agrupado.merge(base, on="subclase_inei", how="left")

merged["indice"] = (merged["precio_gmean"] / merged["precio_base"]) * 100

merged["fecha"] = pd.to_datetime(merged["fecha"], format="%Y%m%d")

# ---------------- TABLA FINAL ----------------
tabla = merged.pivot(index="subclase_inei", columns="fecha", values="indice")
tabla = tabla.sort_index()

# ---------------- GUARDAR ----------------
output = CARPETA / "ipc_subclase_base_31oct.xlsx"
tabla.to_excel(output)

print(" IPC generado correctamente:")
print(output)
print("Subclases incluidas:", len(tabla))

