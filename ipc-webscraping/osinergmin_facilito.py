from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
import os

URL = "https://www.facilito.gob.pe/facilito/pages/facilito/buscadorEESS.jsp"

# Parámetros
DEPARTAMENTO = "LIMA"
PROVINCIA = "LIMA"
DISTRITOS = ["LIMA"]
PRODUCTOS = ["Gasohol Regular"]
RESULTADOS = "50"

# XPaths
XPATH_SELECT_DEPTO = '//*[@id="contact"]/div/div[1]/div/div[1]/select'
XPATH_SELECT_PROV  = '//*[@id="contact"]/div/div[1]/div/div[2]/select'
XPATH_SELECT_DIST  = '//*[@id="contact"]/div/div[1]/div/div[3]/select'
XPATH_SELECT_PROD  = '//*[@id="contact"]/div/div[1]/div/div[4]/select'
XPATH_LEN_TABLE    = '//*[@id="tblPreciosAutomotor_length"]/label/select'
XPATH_TABLE        = '//*[@id="tblPreciosAutomotor"]'  # Changed to full table XPath (was tbody)

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

# ---- CONFIGURAR DRIVER ----
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    driver.get(URL)
    time.sleep(2)

    # 1️⃣ Clic inicial en el mapa
    click_center_of_viewport(driver)
    time.sleep(2)

    # 2️⃣ Departamento
    dept = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_DEPTO)))
    Select(dept).select_by_visible_text(DEPARTAMENTO)
    time.sleep(2)

    # 3️⃣ Provincia
    prov = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_PROV)))
    Select(prov).select_by_visible_text(PROVINCIA)
    time.sleep(2)

    all_data = []

    for distrito in DISTRITOS:
        # 4️⃣ Distrito
        dist = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_DIST)))
        Select(dist).select_by_visible_text(distrito)
        time.sleep(2)

        for producto in PRODUCTOS:
            # 5️⃣ Producto
            prod = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECT_PROD)))
            Select(prod).select_by_visible_text(producto)
            time.sleep(4)

            # 6️⃣ Mostrar 50 resultados
            try:
                len_select = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LEN_TABLE)))
                Select(len_select).select_by_visible_text(RESULTADOS)
                time.sleep(2)
            except Exception:
                pass

            # 7️⃣ Obtener HTML completo de la tabla (changed from tbody)
            table_elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_TABLE)))
            table_html = table_elem.get_attribute("outerHTML")

            # 8️⃣ Convertir HTML a DataFrame (now parses full table)
            tables = pd.read_html(table_html)
            if len(tables) > 0:
                df = tables[0]
                # If the site's thead doesn't match perfectly, uncomment and adjust:
                # df.columns = ["Distrito", "Establecimiento", "Dirección", "Teléfono", "Precio (S/ galón)"]
                df["Producto"] = producto
                all_data.append(df)
                print(f"✅ {distrito} | {producto}: {len(df)} filas capturadas")
            else:
                print(f"⚠️ {distrito} | {producto}: tabla vacía")

    # 9️⃣ Guardar
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = f"facilito_{DEPARTAMENTO}_{PROVINCIA}.xlsx"
        final_df.to_excel(filename, index=False)
        # print(f"\n✅ Archivo guardado correctamente: {filename}")
        # print(final_df.head())
    else:
        print("\n⚠️ No se obtuvieron datos para guardar.")

finally:
    driver.quit()