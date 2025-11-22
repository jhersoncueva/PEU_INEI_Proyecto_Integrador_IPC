# ============================================
# TAILOY SCRAPER 
# EXCEL CONSOLIDADO: categoria, subcategoria, producto, codigo, marca, nombre, precio, fecha
# ============================================

# (Opcional) Instalar librerías la 1ra vez:
INSTALL_LIBS = False
if INSTALL_LIBS:
    import sys, subprocess
    for pkg in ["requests", "beautifulsoup4", "lxml", "pandas", "openpyxl"]:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

import os, re, time, json, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

# ---------- CONFIG ----------
# Carpeta OBLIGATORIA donde guardar resultados
OUTPUT_BASE_DIR = "data/raw/tailoy"

# Dominio fijo
BASE_ROOT = "https://www.tailoy.com.pe"

# Páginas por productos (reducido - la mayoría tiene 1-3 páginas)
MAX_PAGES_PER_CATEGORY = 10

# Pausa entre requests (mínima)
REQUEST_DELAY_SEC = 0.05

# Número de hilos simultáneos (ajusta: 20-30 es óptimo, baja si te bloquean)
MAX_WORKERS = 25

# Headers básicos para parecer navegador real
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36")
}

# Lock para print thread-safe
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# ---------- SEMILLA DE CATEGORÍAS ----------
KEY_ROWS = [
    {"categoria": "escolar", "subcategoria": "utiles",
     "productos": "borradores, colores, compases, correctores, crayones-y-oleos, escuadras, lapiceros, lapices, motas-y-tizas, pizarras, plastilinas, plumones-indelebles, plumones-para-papel, plumones-para-pizarra, portaminas-y-minas, reglas, resaltadores, tajadores, temperas-y-acuarelas-escolares, tijeras, sets-escolares"},
    {"categoria": "escolar", "subcategoria": "cuadernos-y-blocks",
     "productos": "blocks-de-escritura-y-libretas, blocks-de-manualidades, cuadernos-anillados, cuadernos-cosidos, cuadernos-grapados-a4, cuadernos-grapados-a5, libretas, sketchbooks"},
    {"categoria": "escolar", "subcategoria": "mochilas-cartucheras-y-loncheras",
     "productos": "cartucheras, loncheras, maletas-y-mochilas, tapers, tomatodos"},
    {"categoria": "escolar", "subcategoria": "manualidades",
     "productos": "accesorios-pintura, blocks-de-manualidades, ceramica-en-frio, cinta-de-agua, microporoso-y-corrospum, papeles-para-manualidades, pasamaneria-y-otros, pinceles-escolares, plastilinas, tijeras"},
    {"categoria": "escolar", "subcategoria": "papeleria",
     "productos": "cartones, cartulinas, papel-bond, papel-celofan, papel-copia, papel-fotocopia, papel-fotografico-y-otros, papel-periodico, papeles-adhesivos, papeles-para-manualidades, papelografos"},
    {"categoria": "escolar", "subcategoria": "materiales-didacticos",
     "productos": "juegos-didacticos, rompecabezas-didacticos"},
    {"categoria": "escolar", "subcategoria": "forros-y-etiquetas",
     "productos": "cintas-adhesivas, etiquetas, forros, stickers"},
    {"categoria": "escolar", "subcategoria": "archivo",
     "productos": "carpetas-y-portatodos, folders-escolares, pioners-escolares, archivadores-de-palanca"},
    {"categoria": "escolar", "subcategoria": "textos",
     "productos": "biblias, diccionarios, plan-lector, refuerzo-en-casa, tablas, textos-escolares"},
    {"categoria": "escolar", "subcategoria": "cintas-y-pegamentos",
     "productos": "cintas-adhesivas-y-masking-tape, gomas-siliconas-y-colas, limpiatipos, otros-pegamentos"},
    {"categoria": "escolar", "subcategoria": "utiles-para-el-salon",
     "productos": "desinfectantes, engrapadores-sacagrapas-y-grapas, limpiatipos, motas-y-tizas, notas-adhesivas-y-banderitas, pa-os-de-limpieza, perforadores, plumones-de-pizarra"},
    {"categoria": "oficina", "subcategoria": "utiles",
     "productos": "borradores, correctores, lapiceros, lapices, plumones-indelebles, plumones-para-papel, plumones-para-pizarra, portaminas-y-minas, resaltadores, tajadores, otros-plumones"},
    {"categoria": "oficina", "subcategoria": "articulos-de-escritorio",
     "productos": "clips-alfiler-chinches-y-ligas, cuchillas, dispensadores, engrapadores-sacagrapas-y-grapas, notas-adhesivas-y-banderitas, otros-accesorios, perforadores, sellos-y-tampones, tijeras"},
    {"categoria": "oficina", "subcategoria": "anillados-y-enmicados",
     "productos": "anillados-y-enmicados"},
    {"categoria": "oficina", "subcategoria": "muebles-de-oficina",
     "productos": "sillas"},
    {"categoria": "oficina", "subcategoria": "escritura-fina",
     "productos": "boligrafos-finos"},
    {"categoria": "oficina", "subcategoria": "papeleria",
     "productos": "papel-bond, papel-copia, papel-fotocopia, papel-periodico, papeles-adhesivos, papeles-finos, papelografos"},
    {"categoria": "oficina", "subcategoria": "accesorios-de-oficina",
     "productos": "motas-y-tizas, pizarras, plumones-para-pizarra"},
    {"categoria": "oficina", "subcategoria": "embalaje-cintas-y-pegamentos",
     "productos": "cintas-adhesivas-y-masking-tape, cintas-de-embalaje, gomas-siliconas-y-colas, limpiatipos, otros-pegamentos, stretch-film, etiquetas"},
    {"categoria": "oficina", "subcategoria": "archivo",
     "productos": "archivadores-de-palanca, carpetas-y-archivo, files-y-folders-de-manila, pioners, portapapel-y-micas, separadores, sobres-de-papel"},
    {"categoria": "oficina", "subcategoria": "contabilidad-y-administracion",
     "productos": "cuadernos-empastados, libros-contables, papel-carbon, talonarios"},
    {"categoria": "oficina", "subcategoria": "agendas-y-libretas",
     "productos": "agendas, libretas"},
    {"categoria": "universitario", "subcategoria": "cuadernos-y-blocks",
     "productos": "blocks-de-escritura-y-libretas, cuadernos-anillados, libretas, sketchbooks"},
    {"categoria": "universitario", "subcategoria": "archivo",
     "productos": "carpetas-y-archivo, files-y-folders-de-manila, pioners, portapapel-y-micas, separadores, sobres-de-papel"},
    {"categoria": "universitario", "subcategoria": "papeleria",
     "productos": "papel-bond, papel-fotocopia, papelografos, papel-fotografico"},
    {"categoria": "universitario", "subcategoria": "utiles",
     "productos": "borradores, correctores, lapiceros, lapices, pizarras, plumones-indelebles, plumones-para-papel, plumones-para-pizarra, resaltadores, tajadores, tijeras, portaminas-y-minas"},
    {"categoria": "universitario", "subcategoria": "cartucheras-mochilas-y-loncheras",
     "productos": "cartucheras, loncheras, maletas-y-mochilas, tapers, tomatodos"},
    {"categoria": "universitario", "subcategoria": "cintas-y-pegamentos",
     "productos": "cintas-adhesivas-y-masking-tape, gomas-siliconas-y-colas, limpiatipos, otros-pegamentos"},
    {"categoria": "universitario", "subcategoria": "otros",
     "productos": "calculadoras, engrapadores-sacagrapas-y-grapas, limpiatipos, motas-y-tizas, notas-adhesivas-y-banderitas, perforadores"},
    {"categoria": "lectura", "subcategoria": "actividades-y-estilo-de-vida",
     "productos": "bienestar-y-salud, cocina, deporte-y-estilo-de-vida"},
    {"categoria": "lectura", "subcategoria": "agendas-y-planners",
     "productos": "planners"},
    {"categoria": "lectura", "subcategoria": "albumes-coleccionables",
     "productos": "panini"},
    {"categoria": "lectura", "subcategoria": "best-sellers-y-promociones",
     "productos": "lo-vendido, solo-por-hoy, top-ofertas"},
    {"categoria": "lectura", "subcategoria": "economia-y-negocios",
     "productos": "economia, empresa, finanzas"},
    {"categoria": "lectura", "subcategoria": "libreria-infantil",
     "productos": "cuentos, libros-de-actividades, literatura-infantil"},
    {"categoria": "lectura", "subcategoria": "libros-y-cuentos",
     "productos": "actualidad, bienestar-y-salud, cocina, cuentos, empresa, libros-de-actividades, libros-y-novelas, literatura-infantil, literatura-juvenil"},
    {"categoria": "lectura", "subcategoria": "literatura-juvenil-y-novelas",
     "productos": "literatura-juvenil, novelas"},
    {"categoria": "lectura", "subcategoria": "libros-en-ingles",
     "productos": "libros-en-ingles"},
    {"categoria": "arte-y-dise-o", "subcategoria": "dibujo",
     "productos": "lapices-de-grafito-y-portaminas, estilografos, lapices-de-color, carboncillo, tinta-china, gomas-de-borrar-y-sacapuntas, oleos-y-tizas-pastel"},
    {"categoria": "arte-y-dise-o", "subcategoria": "pintura",
     "productos": "pinturas-acrilicas, auxiliares-de-pintura, pinturas-oleos, gouache-y-temperas, acuarelas, pintura-en-spray, pintura-en-tela"},
    {"categoria": "arte-y-dise-o", "subcategoria": "blocks-de-arte",
     "productos": "blocks-para-dibujo, blocks-para-acuarela, blocks-para-oleo-y-acrilico, blocks-para-marcadores, bitacoras-y-libretas, papel-vegetal"},
    {"categoria": "arte-y-dise-o", "subcategoria": "pinceles",
     "productos": "pinceles-redondos, pinceles-planos, pinceles-lengua-de-gato, pinceles-de-agua, sets-de-pinceles"},
    {"categoria": "arte-y-dise-o", "subcategoria": "lienzos-y-caballetes",
     "productos": "lienzos-blancos, lienzos-de-lino, caballetes-de-mesa, caballetes-de-piso"},
    {"categoria": "arte-y-dise-o", "subcategoria": "marcadores",
     "productos": "marcadores-acrilicos, marcadores-a-base-de-alcohol, marcadores-a-base-de-agua, marcadores-permanentes, marcadores-de-tiza"},
    {"categoria": "arte-y-dise-o", "subcategoria": "herramientas-y-accesorios",
     "productos": "accesorios-de-pintura, accesorios-de-dibujo, planchas-de-corte"}
]
# ---------- HELPERS ----------
def split_productos(kw: str):
    if not isinstance(kw, str) or not kw.strip():
        return []
    parts = [p.strip() for p in kw.split(",")]
    return [p for p in parts if p]

def clean_part(part: str) -> str:
    if not isinstance(part, str):
        return ""
    t = part.strip().strip("/").lower()
    t = re.sub(r"\.html?$", "", t)
    return t

def build_category_url(categoria: str, subcategoria: str, producto: str) -> str:
    s1 = clean_part(categoria)
    s2 = clean_part(subcategoria)
    k1 = clean_part(producto)
    parts = [p for p in [s1, s2, k1] if p]
    if not parts:
        parts = [s1 or "jugueteria"]
    return f"{BASE_ROOT}/{'/'.join(parts)}.html"

def build_page_url(url: str, page: int) -> str:
    parts = urlparse(url)
    q = parse_qs(parts.query, keep_blank_values=True)
    if page <= 1:
        q.pop("p", None)
    else:
        q["p"] = [str(page)]
    return urlunparse(parts._replace(query=urlencode(q, doseq=True), fragment=""))

def get_soup(session, url):
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ---------- PARSERS ----------
def parse_listing(session, list_url):
    soup = get_soup(session, list_url)
    items = []
    for li in soup.select("li.product-item"):
        a = li.select_one("a.product-item-link")
        href = a.get("href").strip() if a and a.has_attr("href") else ""
        name = a.get_text(strip=True) if a else ""
        price_el = (li.select_one('[data-price-type="finalPrice"] .price') or
                    li.select_one('[data-price-type="finalPrice"]') or
                    li.select_one("span.price"))
        price_text = price_el.get_text(strip=True) if price_el else ""
        
        brand_el = li.select_one(".brand-label .label, .product-item-brand, .brand, .marca")
        brand_list = brand_el.get_text(strip=True) if brand_el else ""
        
        codigo = ""
        if href:
            match = re.search(r'-(\d+)\.html', href)
            if match:
                codigo = match.group(1)
        
        if href and name:
            items.append({"url": href, "nombre": name, "precio": price_text, "marca_list": brand_list, "codigo": codigo})
    return items

def parse_brand_from_jsonld(soup: BeautifulSoup) -> str:
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or tag.text or "")
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            if "@graph" in obj and isinstance(obj["@graph"], list):
                for g in obj["@graph"]:
                    if isinstance(g, dict) and g.get("@type") in ("Product", "schema:Product"):
                        b = g.get("brand")
                        if isinstance(b, dict):
                            name = b.get("name") or b.get("@id") or ""
                            if name: return name.strip()
                        elif isinstance(b, str):
                            return b.strip()
            if obj.get("@type") in ("Product", "schema:Product"):
                b = obj.get("brand")
                if isinstance(b, dict):
                    name = b.get("name") or b.get("@id") or ""
                    if name: return name.strip()
                elif isinstance(b, str):
                    return b.strip()
    return ""

def parse_brand_from_attributes(soup: BeautifulSoup) -> str:
    for tr in soup.select("table tr, .additional-attributes-wrapper tr"):
        th, td = tr.find("th"), tr.find("td")
        if th and td and "marca" in th.get_text(" ", strip=True).lower():
            return td.get_text(" ", strip=True).strip()
    for dt in soup.select("dt, .product.attribute"):
        label = dt.get_text(" ", strip=True).lower()
        if "marca" in label:
            dd = dt.find_next("dd")
            if dd:
                return dd.get_text(" ", strip=True).strip()
    m = re.search(r"Marca\s*[:\-]\s*([A-Za-z0-9\-\s&\.\+_/]+)", soup.get_text(" ", strip=True), re.I)
    return m.group(1).strip() if m else ""
def parse_detail_brand(session, product_url: str) -> str:
    try:
        soup = get_soup(session, product_url)
    except Exception:
        return ""
    brand = parse_brand_from_jsonld(soup) or parse_brand_from_attributes(soup)
    return re.sub(r"\s{2,}", " ", (brand or "")).strip()

# ---------- SCRAPER PARALELO ----------
def scrape_producto(categoria, subcat, kw, today):
    """Scrapea un producto completo y retorna sus productos"""
    session = requests.Session()
    session.headers.update(HEADERS)
    
    base_url = build_category_url(categoria, subcat, kw)
    seen_urls = set()
    results = []
    
    for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
        url = build_page_url(base_url, page)
        safe_print(f"[{categoria}] {subcat or '-'} | {kw or '-'} | Pág {page}")
        
        try:
            listing = parse_listing(session, url)
        except Exception as e:
            safe_print(f"  (x) Error: {e}")
            break
        
        if not listing:
            if page == 1:
                safe_print("  (i) Sin productos")
            break
        
        nuevos_en_esta_pagina = 0
        
        for it in listing:
            prod_url = it.get("url", "")
            if not prod_url or prod_url in seen_urls:
                continue
            seen_urls.add(prod_url)
            nuevos_en_esta_pagina += 1
            
            # Marca: primero del listado (90%+ casos)
            brand = (it.get("marca_list") or "").strip()
            # Solo si falta marca, buscar en detalle
            if not brand and prod_url:
                try:
                    brand = parse_detail_brand(session, prod_url)
                except Exception:
                    brand = ""
            
            results.append({
                "categoria": categoria,
                "subcategoria": subcat,
                "producto": kw,
                "codigo": it.get("codigo", ""),
                "marca": brand,
                "nombre": (it.get("nombre") or "").strip(),
                "precio": (it.get("precio") or "").strip(),
                "fecha": today
            })
        
        if nuevos_en_esta_pagina == 0:
            break
        
        # Sleep por página (no por producto)
        time.sleep(REQUEST_DELAY_SEC)
    
    session.close()
    return results

# ---------- MAIN ----------
def main():
    out_base = OUTPUT_BASE_DIR.strip()
    if not out_base:
        raise ValueError("Debes definir OUTPUT_BASE_DIR con una ruta válida.")

    today = datetime.now().strftime("%Y-%m-%d")
    base_out = os.path.join(out_base, today)
    os.makedirs(base_out, exist_ok=True)

    # Construir DataFrame desde la semilla embebida
    key_df = pd.DataFrame(KEY_ROWS)
    if key_df.empty:
        raise ValueError("La semilla KEY_ROWS está vacía.")

    # Lista única para TODOS los productos
    all_products = []
    tasks = []
    
    for _, row in key_df.iterrows():
        categoria = (row.get("categoria") or "SinCategoria").strip() or "SinCategoria"
        subcat = (row.get("subcategoria") or "").strip()
        kw_list = split_productos(row.get("productos") or "")
        if not kw_list:
            kw_list = [""]  # Si no hay productos, scrapea la subcategoría directa

        for kw in kw_list:
            tasks.append((categoria, subcat, kw))
    
    safe_print(f"\n Scraping con {MAX_WORKERS} hilos | {len(tasks)} tareas | Delay {REQUEST_DELAY_SEC}s\n")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_producto, cat, sub, kw, today): (cat, sub, kw)
            for cat, sub, kw in tasks
        }
        
        completed = 0
        for future in as_completed(futures):
            try:
                results = future.result()
                all_products.extend(results)  # Agregar a la lista única
                completed += 1
                elapsed = time.time() - start_time
                avg_time = elapsed / max(completed, 1)
                remaining = (len(tasks) - completed) * avg_time
                safe_print(f"✓ {completed}/{len(tasks)} | Estimado restante: {remaining/60:.1f} min")
            except Exception as e:
                safe_print(f"(x) Error: {e}")

    # Guardar TODO en un solo Excel
    safe_print("\n Guardando Excel consolidado...")
    if all_products:
        df_consolidated = pd.DataFrame(
            all_products,
            columns=["categoria","subcategoria","producto","codigo","marca","nombre","precio","fecha"]
        )
        excel_output = os.path.join(base_out, f"tailoy_consolidado_{today}.xlsx")
        df_consolidated.to_excel(excel_output, index=False)
        safe_print(f"  ✓ {excel_output} ({len(df_consolidated)} productos)")
    else:
        safe_print("  (i) No se encontraron productos para guardar")

    total_time = time.time() - start_time
    safe_print(f"\n Finalizado en {total_time/60:.1f} minutos | Carpeta: {base_out}")

if __name__ == "__main__":
    main()
