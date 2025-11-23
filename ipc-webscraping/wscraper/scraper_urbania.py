import os
import datetime
import pandas as pd
from selenium.webdriver.chrome.options import Options

# Importar tu clase de scraper
from scrapers.selenium_scraper import WebScraper_Selenium

# ============================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================

# current_dir = os.path.dirname(os.path.abspath(__file__))
current_date = datetime.datetime.now().strftime("%Y_%m_%d")

# config_path = os.path.join(current_dir, "config_websites", "urbania.xml")
# csv_path = os.path.join(current_dir, "base_period", "urbania_precios.csv")
# output_path = os.path.join(current_dir, "data", "raw", "urbania", current_date)
config_path = "config_websites/urbania.xml"
csv_path = "base_period/urbania_precios.csv"
output_path = f"../data/raw/urbania/{current_date}"
os.makedirs(output_path, exist_ok=True)

# Verificar archivos
if not os.path.exists(config_path):
    print(f"❌ ERROR: No se encuentra {config_path}")
    exit(1)

if not os.path.exists(csv_path):
    print(f"❌ ERROR: No se encuentra {csv_path}")
    exit(1)

# ============================================================
# LEER CSV DE ENTRADA
# ============================================================

df = pd.read_csv(csv_path)
print(f"📋 Se encontraron {len(df)} búsquedas a realizar\n")

# ============================================================
# CONFIGURACIÓN DEL NAVEGADOR CHROME
# ============================================================

chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# ============================================================
# FUNCIÓN PARA CONSTRUIR URLS DE URBANIA
# ============================================================

def construir_urls_urbania(ciudad, tipo_operacion, tipo_inmueble, max_paginas=10):
    # Normalizar textos
    ciudad = ciudad.lower().strip().replace(" ", "-")
    tipo_operacion = tipo_operacion.lower().strip()
    tipo_inmueble = tipo_inmueble.lower().strip()

    # Pluralizar inmueble
    if tipo_inmueble == "departamento":
        tipo_inmueble = "departamentos"
    elif tipo_inmueble == "casa":
        tipo_inmueble = "casas"
    elif tipo_inmueble == "oficina":
        tipo_inmueble = "oficinas"
    elif tipo_inmueble == "local":
        tipo_inmueble = "locales"

    # Crear lista de URLs por página con filtro últimos 7 días
    base_url = f"https://urbania.pe/buscar/{tipo_operacion}-de-{tipo_inmueble}-en-{ciudad}?publicationDate=0"
    urls = [base_url]

    for i in range(2, max_paginas + 1):
        urls.append(f"{base_url}&page={i}")

    return urls

# ============================================================
# SCRAPING PRINCIPAL
# ============================================================
data_acumulada = pd.DataFrame()
scraper = WebScraper_Selenium(config_path, chrome_options)

for idx, fila in df.iterrows():
    ciudad = fila["CIUDAD"]
    tipo = fila["TIPO_OPERACION"]
    inmueble = fila["TIPO_INMUEBLE"]

    urls = construir_urls_urbania(ciudad, tipo, inmueble, max_paginas=10)

    print(f"\n{'='*70}")
    print(f"🏙️ [{idx+1}/{len(df)}] Procesando:")
    print(f" Ciudad: {ciudad}")
    print(f" Tipo: {tipo}")
    print(f" Inmueble: {inmueble}")
    print(f" 🔗 Total de páginas a scrapear: {len(urls)}")
    print(f"{'='*70}\n")


    for num, url in enumerate(urls, start=1):
        if "publicationDate=" not in url:
            url += "&publicationDate=0"
        try:
            print(f" → Página {num}: {url}")

            # scraper = WebScraper_Selenium(config_path, chrome_options)
            website = scraper.config.find(".//website[@name='urbania']")
            if website is not None:
                website.set('base_url', url)

            

            search_term = f"{ciudad}_{tipo}_{inmueble}_p{num}"

            # Ejecutar el scraping
            resultado = scraper.scrape_and_save("urbania", search_term, output_path)
                
            if not resultado or not os.path.exists(resultado):
                print(f" ⚠️ Página {num} vacía. Se detiene la búsqueda aquí.")
                break
            # if resultado and os.path.exists(resultado):
            temp_df = pd.read_csv(resultado)
            temp_df["Ciudad"] = ciudad
            temp_df["Tipo"] = tipo
            temp_df["Inmueble"] = inmueble
            temp_df["Pagina"] = num

                # Columna nueva para la fecha de publicación
            if 'fecha_publicacion' in temp_df.columns:
                temp_df["Fecha_Publicacion"] = temp_df["fecha_publicacion"]
            else:
                temp_df["Fecha_Publicacion"] = ""

            data_acumulada = pd.concat([data_acumulada, temp_df], ignore_index=True)
            print(f" ✅ Página {num} guardada ({len(temp_df)} registros)")

            # else:
            #     print(f" ⚠️ Sin resultados en página {num}")

        except Exception as e:
            print(f" ❌ Error en página {num}: {str(e)}")
            continue

# ============================================================
# ELIMINAR DUPLICADOS Y GUARDAR CONSOLIDADO FINAL
# ============================================================

total_registros = len(data_acumulada)
data_acumulada.drop_duplicates(subset=['url'], inplace=True)
registros_finales = len(data_acumulada)
duplicados_eliminados = total_registros - registros_finales

csv_final = os.path.join(output_path, f"urbania_completo_{current_date}.csv")
data_acumulada.to_csv(csv_final, index=False, encoding='utf-8-sig')

print("\n🎯 Scraping finalizado correctamente.")
print(f"📦 Archivo consolidado guardado en:\n{csv_final}")
print(f"📊 Total registros obtenidos: {total_registros}")
print(f"📊 Registros duplicados eliminados: {duplicados_eliminados}")
print(f"📊 Registros únicos finales: {registros_finales}")
print("="*70)

# ============================================================
# ELIMINAR CSV INDIVIDUALES
# ============================================================

print("\n🧹 Eliminando archivos CSV intermedios...")

for archivo in os.listdir(output_path):
    if archivo.endswith(".csv") and not archivo.startswith("urbania_completo_"):
        ruta = os.path.join(output_path, archivo)
        os.remove(ruta)
        print(f" 🗑️ Eliminado: {archivo}")

print("✅ Limpieza completada. Solo queda el consolidado final.")
print("="*70)
