import pandas as pd
import numpy as np
from pathlib import Path

# --------------------------------------------------
# CONFIGURACIÓN
# --------------------------------------------------
current_dir = Path(__file__).parent.absolute()

consol_subcat_dir = current_dir / "data/consolidated/salud2"
output_dir = current_dir / "data/consolidated/salud3"
output_dir.mkdir(parents=True, exist_ok=True)  # ← crea salud3 si no existe

# Fechas a procesar
fechas = pd.date_range("2025-10-29", "2025-11-16").strftime("%Y-%m-%d")

# --------------------------------------------------
# CARGAR PONDERACIONES INEI
# --------------------------------------------------
inei_file = current_dir / "base_period" / "IPCLM-Div. Alimen+Alquil+Salud+Educa (Base Dic. 2021=100).xlsx"

df_inei = pd.read_excel(inei_file)
df_inei = df_inei[df_inei["CLASIFICACION"].str.upper().str.strip() == "PRODUCTO"]
df_inei["DESCRIPCION"] = df_inei["DESCRIPCION"].str.upper().str.strip()

# Mapeos
mapeo = {
    "Antianémicos": "ANTIANÉMICOS",
    "Antiarrítmicos": "ANTIARRÍTMICOS (APARATO CARDIOVASCULAR)",
    "Antiasmáticos": "ANTIASMÁTICOS Y BRONCODILATADORES",
    "Antibióticos": "ANTIBIÓTICOS/ANTIBACTERIANOS",
    "Anticonceptivos": "ANTICONCEPTIVOS",
    "Antidepresivos": "ANTIDEPRESIVOS (SISTEMA NERVIOSO)",
    "Antiepilépticos": "ANTICONVULSIVOS, ANTIEPILÉPTICOS (SISTEMA NERVIOSO)",
    "Antiflatulentos": "ANTIÁCIDOS, ANTIFLATULENTOS",
    "Antigripales": "ANTIGRIPALES",
    "Antihipertensivos": "ANTIHIPERTENSIVOS",
    "Antiulcerosos/Protector Gástrico": "ANTIULCEROSOS, ANTISECRETORES",
    "Descongestionante Oftálmico": "DESCONGESTIVOS OFTÁLMICOS",
    "Reguladores de Hormonas": "HORMONAS NATURALES Y SINTÉTICAS (APARATO REPRODUCTOR)",
    "cicatrizantes /Heridas/ Otros": "ANTISÉPTICOS Y CICATRIZANTES (PIEL Y MUCOSAS)"
}

# --------------------------------------------------
# CARGAR PRECIOS BASE (tomados de salud2)
# --------------------------------------------------
file_base = consol_subcat_dir / "2025-10-29" / "subcategorias_consolidadas_2025-10-29.xlsx"
df_base = pd.read_excel(file_base)

precios_base = df_base.set_index("SUBCATEGORIA")["PROMEDIO_GEOM_SUBCATEGORIA"]

# --------------------------------------------------
# TABLA WIDE: IPC SUBCATEGORÍAS
# --------------------------------------------------
lista_wide = []

for fecha in fechas:

    file_path = consol_subcat_dir / fecha / f"subcategorias_consolidadas_{fecha}.xlsx"
    
    if not file_path.exists():
        print(f"⚠️ No se encontró archivo para {fecha}")
        continue

    df = pd.read_excel(file_path)

    # Precio base
    df["P_BASE"] = df["SUBCATEGORIA"].map(precios_base)
    df = df.dropna(subset=["P_BASE", "PROMEDIO_GEOM_SUBCATEGORIA"])

    # IPC SUBCATEGORÍA
    df["IPC_SUBCAT"] = (df["PROMEDIO_GEOM_SUBCATEGORIA"] / df["P_BASE"]) * 100

    # Wide format
    df_temp = df[["SUBCATEGORIA", "IPC_SUBCAT"]].copy()
    df_temp = df_temp.rename(columns={"IPC_SUBCAT": fecha})
    lista_wide.append(df_temp)

# --------------------------------------------------
# UNIR TABLA WIDE (IPC POR SUBCATEGORÍA)
# --------------------------------------------------
df_wide = lista_wide[0]
for df in lista_wide[1:]:
    df_wide = df_wide.merge(df, on="SUBCATEGORIA", how="outer")

# Guardar en SALUD3
df_wide.to_excel(output_dir / "ipc_subcategorias.xlsx", index=False)