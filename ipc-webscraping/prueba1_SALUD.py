import pandas as pd
from pathlib import Path

# ------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------
fecha_manual = None  
current_dir = Path(__file__).parent.absolute()

# Directorios de entrada
ink_base = current_dir / "data/processed/inkafarma"
mif_base = current_dir / "data/processed/mi_farma"

output_dir = current_dir / "data/consolidated/consolidado_salud"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "precios_consolidados.xlsx"

# Fechas
if fecha_manual is None:
    fechas_disponibles = sorted([f.name for f in ink_base.iterdir() if f.is_dir()])
else:
    fechas_disponibles = [fecha_manual]

# Lista general
lista_final = []

# ------------------------------------------
# 1) MAPEO DE SUBCATEGORÍAS (PRODUCTO INEI)
# ------------------------------------------

mapa_subcat = {
    "Antibióticos": "ANTIBIÓTICOS/ANTIBACTERIANOS",
    "Antibióticos Respiratorios": "ANTIBIÓTICOS/ANTIBACTERIANOS",

    "Antiarrítmicos": "ANTIARRÍTMICOS (APARATO CARDIOVASCULAR)",
    "Antihipertensivos": "ANTIHIPERTENSIVOS",

    "Antiflatulentos": "ANTIÁCIDOS, ANTIFLATULENTOS",
    "Antiulcerosos/Protector Gástrico": "ANTIULCEROSOS, ANTISECRETORES",

    "Antiasmáticos": "ANTIASMÁTICOS Y BRONCODILATADORES",
    "Antigripales": "ANTIGRIPALES",

    "Reguladores de Hormonas": "HORMONAS NATURALES Y SINTÉTICAS (APARATO REPRODUCTOR)",
    "Descongestionante Oftálmico": "DESCONGESTIVOS OFTÁLMICOS",
    "cicatrizantes /Heridas/ Otros": "ANTISÉPTICOS Y CICATRIZANTES (PIEL Y MUCOSAS)",

    "Antianémicos": "ANTIANÉMICOS",

    "Antiepilépticos": "ANTICONVULSIVOS, ANTIEPILÉPTICOS (SISTEMA NERVIOSO)",
    "Antidepresivos": "ANTIDEPRESIVOS (SISTEMA NERVIOSO)",

    "Anticonceptivos": "ANTICONCEPTIVOS"
}

# ------------------------------------------
# 2) MAPEO DE CATEGORÍA (RUBRO INEI)
# ------------------------------------------

mapa_cat_from_subcat = {
    "ANTIBIÓTICOS/ANTIBACTERIANOS": "ANTIINFECCIOSOS SISTÉMICOS",

    "ANTIARRÍTMICOS (APARATO CARDIOVASCULAR)": "APARATO CARDIOVASCULAR",
    "ANTIHIPERTENSIVOS": "APARATO CARDIOVASCULAR",

    "ANTIÁCIDOS, ANTIFLATULENTOS": "APARATO DIGESTIVO",
    "ANTIULCEROSOS, ANTISECRETORES": "APARATO DIGESTIVO",

    "ANTIASMÁTICOS Y BRONCODILATADORES": "APARATO RESPIRATORIO",
    "ANTIGRIPALES": "APARATO RESPIRATORIO",

    "HORMONAS NATURALES Y SINTÉTICAS (APARATO REPRODUCTOR)": "APARATO GENITOURINARIO Y REPRODUCTOR",

    "DESCONGESTIVOS OFTÁLMICOS": "OFTALMOLÓGICOS",

    "ANTISÉPTICOS Y CICATRIZANTES (PIEL Y MUCOSAS)": "PIEL Y MUCOSAS",

    "ANTIANÉMICOS": "SANGRE, LÍQUIDOS Y ELECTROLITOS",

    "ANTICONVULSIVOS, ANTIEPILÉPTICOS (SISTEMA NERVIOSO)": "SISTEMA NERVIOSO",
    "ANTIDEPRESIVOS (SISTEMA NERVIOSO)": "SISTEMA NERVIOSO",

    "ANTICONCEPTIVOS": "ANTICONCEPTIVOS"
}

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

    # Cargar datos
    ink_df = pd.read_excel(ink_file)
    mif_df = pd.read_excel(mif_file)

    # Normalizar columnas
    ink_df.columns = ink_df.columns.str.upper().str.strip()
    mif_df.columns = mif_df.columns.str.upper().str.strip()

    # Etiqueta de farmacia
    ink_df["FUENTE"] = "INKAFARMA"
    mif_df["FUENTE"] = "MIFARMA"

    # Filtrar extremos
    ink_df = ink_df[ink_df["PRECIO_FINAL"] <= 300]
    mif_df = mif_df[mif_df["PRECIO_FINAL"] <= 300]

    # Unir ambas
    df = pd.concat([ink_df, mif_df], ignore_index=True)

    # Guardar fecha
    df["FECHA"] = fecha

    # ---------------------------
    # SUBCATEGORÍA → INEI
    # ---------------------------
    df["SUBCATEGORIA"] = df["SUBCATEGORIA"].str.strip()
    df["SUBCATEGORIA_EST"] = df["SUBCATEGORIA"].replace(mapa_subcat)

    # Verificar faltantes
    faltantes = df[df["SUBCATEGORIA_EST"].isna()]["SUBCATEGORIA"].unique()
    if len(faltantes) > 0:
        print("\n SUBCATEGORÍAS NO RECONOCIDAS:")
        print(faltantes)

    # ---------------------------
    # CATEGORÍA (RUBRO INEI)
    # ---------------------------
    df["CATEGORIA"] = df["SUBCATEGORIA_EST"].map(mapa_cat_from_subcat)

    # Agregar
    lista_final.append(df)

# ------------------------------------------
# UNIR TODO Y GUARDAR
# ------------------------------------------
if lista_final:
    final_df = pd.concat(lista_final, ignore_index=True)
    final_df = final_df.drop(columns=["SUBCATEGORIA"], errors="ignore")
    final_df.to_excel(output_file, index=False)
    print(f"\nArchivo FINAL guardado")
else:
    print("No se generaron resultados.")