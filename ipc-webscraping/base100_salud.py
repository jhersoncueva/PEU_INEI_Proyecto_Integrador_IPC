import pandas as pd
from pathlib import Path
import numpy as np

# ------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------
current_dir = Path(__file__).parent.absolute()

# Archivo generado por el primer script
input_file = current_dir / "data/consolidated/consolidado_salud.xlsx"

# Carpeta y archivo de salida
output_dir = current_dir / "data/base100"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "base_salud.xlsx"

# Fecha base
BASE_FECHA = "2025-10-29"

# ------------------------------------------
# CARGAR DATA
# ------------------------------------------
df = pd.read_excel(input_file)

# Normalizar nombres de columnas
df.columns = df.columns.map(lambda x: str(x).strip().upper())

# Validación
obligatorias = ["FECHA", "PRECIO_FINAL", "CATEGORIA"]
for col in obligatorias:
    if col not in df.columns:
        raise ValueError(f"Falta la columna obligatoria: {col}")

# ------------------------------------------
# FORMATO DE FECHA
# ------------------------------------------
df["FECHA"] = df["FECHA"].astype(str).str.replace("/", "-").str.strip()

# ------------------------------------------
# PROMEDIO GEOMÉTRICO POR CATEGORIA Y FECHA
# ------------------------------------------
def geom_promedio(x):
    x = x[x > 0]
    return np.exp(np.log(x).mean()) if len(x) > 0 else np.nan

geom = (
    df.groupby(["CATEGORIA", "FECHA"])["PRECIO_FINAL"]
      .apply(geom_promedio)
      .reset_index(name="precio_gmean")
)

# ------------------------------------------
# OBTENER PRECIO BASE POR CATEGORIA
# ------------------------------------------
if BASE_FECHA not in geom["FECHA"].unique():
    raise ValueError(f"La fecha base {BASE_FECHA} no existe en el dataset")

base_vals = geom[geom["FECHA"] == BASE_FECHA][["CATEGORIA", "precio_gmean"]]
base_vals = base_vals.rename(columns={"precio_gmean": "precio_base"})

# Unir precio base a toda la tabla (por categoría)
geom = geom.merge(base_vals, on="CATEGORIA", how="left")

# ------------------------------------------
# CALCULAR IPC Y RENOMBRARLO A VALOR
# ------------------------------------------
geom["VALOR"] = (geom["precio_gmean"] / geom["precio_base"]) * 100

# ------------------------------------------
# TABLA FINAL (FORMATO LARGO)
# FECHA | CATEGORIA | VALOR
# ------------------------------------------
resultado = geom[["FECHA", "CATEGORIA", "VALOR"]].copy()

# Ordenar
resultado = resultado.sort_values(["FECHA", "CATEGORIA"])

# ------------------------------------------
# GUARDAR OUTPUT
# ------------------------------------------
resultado.to_excel(output_file, index=False)
print("Archivo guardado:", output_file)
