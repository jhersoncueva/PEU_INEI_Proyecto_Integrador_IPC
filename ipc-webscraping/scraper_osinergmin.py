from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import os
from datetime import datetime

# ==================== CONFIGURACIÓN ====================

# Parámetros fijos
DEPARTAMENTO = "LIMA"
PROVINCIA = "LIMA"
RESULTADOS = "50"

# XPaths
XPATH_SELECT_DEPTO = '//*[@id="contact"]/div/div[1]/div/div[1]/select'
XPATH_SELECT_PROV  = '//*[@id="contact"]/div/div[1]/div/div[2]/select'
XPATH_SELECT_DIST  = '//*[@id="contact"]/div/div[1]/div/div[3]/select'
XPATH_SELECT_PROD  = '//*[@id="contact"]/div/div[1]/div/div[4]/select'
XPATH_LEN_TABLE    = '//*[@id="tblPreciosAutomotor_length"]/label/select'
XPATH_TABLE        = '//*[@id="tblPreciosAutomotor"]'
URL = "https://www.facilito.gob.pe/facilito/pages/facilito/buscadorEESS.jsp"

def click_center_of_viewport(driver):
    js = """
    const x = Math.floor(window.innerWidth / 2);
    const y = Math.floor(window.innerHeight / 2);
    const el = document.elementFromPoint(x, y);
    if (el) {
        const ev = new MouseEvent('click', {bubbles: true, clientX: x, clientY: y});
        el.dispatchEvent(ev);
        return true;
    }
    return false;
    """
    driver.execute_script(js)

def gather_data(productos, folder_suffix, all_data):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    
    # --- MODO HEADLESS (oculta la ventana) ---
    options.add_argument("--headless=new")  # para Chrome moderno
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(URL)
        time.sleep(2)

        click_center_of_viewport(driver)
        time.sleep(2)

        # 1️⃣ Departamento
        dept = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_DEPTO)))
        Select(dept).select_by_visible_text(DEPARTAMENTO)
        time.sleep(2)

        # 2️⃣ Provincia
        prov = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_PROV)))
        Select(prov).select_by_visible_text(PROVINCIA)
        time.sleep(2)

        # 3️⃣ Obtener lista completa de distritos disponibles
        dist_elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_DIST)))
        all_distritos = [opt.text.strip() for opt in dist_elem.find_elements(By.TAG_NAME, "option") if opt.text.strip() and "Seleccione" not in opt.text]
        # print(f"🔍 Distritos encontrados: {len(all_distritos)} → {all_distritos}")

        # 4️⃣ Iterar sobre cada distrito
        for distrito in all_distritos[1:2]:  # Limitar a los primeros 5 distritos para pruebas
            dist_elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_DIST)))
            Select(dist_elem).select_by_visible_text(distrito)
            time.sleep(2)

            for producto in productos:
                prod_elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_PROD)))
                Select(prod_elem).select_by_visible_text(producto)
                time.sleep(3)

                # Mostrar 50 resultados si está disponible
                try:
                    len_select = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LEN_TABLE)))
                    Select(len_select).select_by_visible_text(RESULTADOS)
                    time.sleep(2)
                except Exception:
                    pass

                # Obtener tabla
                try:
                    table_elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_TABLE)))
                    table_html = table_elem.get_attribute("outerHTML")
                    tables = pd.read_html(table_html)

                    if len(tables) > 0:
                        df = tables[0]
                        df["Departamento"] = DEPARTAMENTO
                        df["Provincia"] = PROVINCIA
                        df["Distrito"] = distrito
                        df["Producto"] = producto
                        all_data.append(df)  # Agregar el DataFrame al conjunto de datos
                        print(f"✅ {distrito} | {producto}: {len(df)} filas capturadas")
                    else:
                        print(f"⚠️ {distrito} | {producto}: tabla vacía")

                except Exception as e:
                    print(f"⚠️ Error capturando datos en {distrito}: {e}")

        # # 5️⃣ Concatenar y guardar todo
        # if all_data:
        #     final_df = pd.concat(all_data, ignore_index=True)

        #     # Crear carpeta con fecha actual
        #     today = datetime.now()
        #     folder_name = f"data/osinergmin/osin_{folder_suffix}_{today.strftime('%d_%m_%Y')}"
        #     os.makedirs(folder_name, exist_ok=True)

        #     file_path = os.path.join(folder_name, f"{DEPARTAMENTO}_{PROVINCIA}.csv")
        #     final_df.to_csv(file_path, index=False, encoding="utf-8-sig")

        #     # print(f"\n✅ Archivo guardado correctamente en: {file_path}")
        # else:
        #     print("\n⚠️ No se obtuvieron datos para guardar.")
    finally:
        driver.quit()

# Lista para almacenar todos los datos
all_data = []

# Parámetros para cada producto
productos_1 = ["DB5 S-50 UV"]
productos_2 = ["Gasohol Premium"]
productos_3 = ["Gasohol Regular"]

# Llamadas a la función para cada caso y almacenar en all_data
gather_data(productos_1, "DB5_UV", all_data)
gather_data(productos_2, "GasPremium", all_data)
gather_data(productos_3, "GasRegular", all_data)


######################################################################################
######################################################################################
######################################################################################
######################################################################################

today = datetime.now()

final_df = pd.concat(all_data, ignore_index=True)
final_df["Fecha"] = today.date()
final_df['Precio'] = pd.to_numeric(final_df['Precio de Venta (Soles por galón)'], errors="coerce")

#FOLDER
folder_name = f"data/raw/osinergmin/"
os.makedirs(folder_name, exist_ok=True)

# Ruta del archivo consolidado
file_path = os.path.join(folder_name, f"osinergmin_{today.strftime('%d_%m_%Y')}.csv")

final_df.to_csv(file_path, index=False, encoding="utf-8-sig")
final_df
