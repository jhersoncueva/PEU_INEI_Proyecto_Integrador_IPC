import pandas as pd
import numpy as np
from pathlib import Path

# ------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------
fecha_manual = None  
current_dir = Path(__file__).parent.absolute()

base_input_dir = current_dir / "data/consolidated/salud"
base_output_dir = current_dir / "data/consolidated/salud2"
base_output_dir.mkdir(parents=True, exist_ok=True)

# Si no hay fecha manual, buscar solo carpetas con formato fecha YYYY-MM-DD
if fecha_manual is None:
    fechas_disponibles = sorted([
        f.name
        for f in base_input_dir.iterdir()
        if f.is_dir() and len(f.name) == 10 and f.name[4] == "-" and f.name[7] == "-"
    ])
else:
    fechas_disponibles = [fecha_manual]

# ------------------------------------------
# PROCESAR CADA FECHA
# ------------------------------------------
for fecha in fechas_disponibles:
    print(f"\nProcesando fecha: {fecha}")

    # Archivo de entrada desde SALUD
    input_file = base_input_dir / fecha / f"precios_consolidados_{fecha}.xlsx"

    # Archivo de salida dentro de SALUD2
    output_dir = base_output_dir / fecha
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"subcategorias_consolidadas_{fecha}.xlsx"

    if not input_file.exists():
        print(f"No se encontró el archivo: {input_file}")
        continue

    # ------------------------------------------
    # CARGA DE DATOS
    # ------------------------------------------
    df = pd.read_excel(input_file)
    df = df.dropna(subset=["SUBCATEGORIA", "PROMEDIO_GEOM"])

    resultados = []

    for subcat, grupo in df.groupby("SUBCATEGORIA"):
        precios_pg = grupo["PROMEDIO_GEOM"].dropna()
        precios_pg = precios_pg[precios_pg > 0]   # Eliminar ceros

        n = len(precios_pg)
        if n == 0:
            continue

        # Promedio geométrico estable
        pg_subcat = np.exp(np.mean(np.log(precios_pg)))

        fila_ref = grupo.iloc[0]

        resultados.append({
            "SUBCATEGORIA": subcat,
            "CATEGORIA": fila_ref.get("CATEGORIA"),
            "N_PRODUCTOS": n,
            "PROMEDIO_GEOM_SUBCATEGORIA": pg_subcat
        })

    subcat_df = pd.DataFrame(resultados)

    # ------------------------------------------
    # GUARDAR RESULTADO
    # ------------------------------------------
    subcat_df.to_excel(output_file, index=False)
    print(f"Archivo guardado en: {output_file}")
