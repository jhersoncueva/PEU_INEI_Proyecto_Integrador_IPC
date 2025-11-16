"""
SCRAPER INKAFARMA - API (Algolia) 
"""
import os
import time
import random
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------- CONFIG ----------------
current_dir = Path(__file__).parent.absolute()
csv_path = current_dir / 'base_period' / 'IPC_INKAFARMA.csv'

current_date = datetime.now().strftime('%Y-%m-%d')
output_path = current_dir / 'data' / 'raw' / 'inkafarma' / current_date
output_path.mkdir(parents=True, exist_ok=True)

# Algolia / Inkafarma
ALGOLIA_APP_ID = "15W622LAQ4"
ALGOLIA_API_KEY = "ccd8cbda203928003f7fe6f44ddbfc3a"
ALGOLIA_INDEX = "products"
ALGOLIA_BASE = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes"
ALGOLIA_QUERY_URL = f"{ALGOLIA_BASE}/{ALGOLIA_INDEX}/query"

HEADERS = {
    "x-algolia-api-key": ALGOLIA_API_KEY,
    "x-algolia-application-id": ALGOLIA_APP_ID,
    "Content-Type": "application/json",
    "Referer": "https://inkafarma.pe"
}

HITS_PER_PAGE = 100
SLEEP_BETWEEN_REQ = 0.4

# ---------------- HELPERS ----------------
def choose_price(hit):
    promo = hit.get('pricePromo')
    price_list = hit.get('priceList')

    if promo:
        try:
            val = float(promo)
            if val > 0:
                return val
        except Exception:
            pass

    if price_list is not None:
        if isinstance(price_list, (int, float)):
            return price_list
        if isinstance(price_list, dict):
            for k in ('price', 'value', 'priceValue', 'amount'):
                if k in price_list and price_list[k] not in (None, ""):
                    try:
                        return float(price_list[k])
                    except Exception:
                        pass

    vp = hit.get('validPrice')
    if vp is not None:
        try:
            return float(vp)
        except Exception:
            pass

    return None

def clean_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, '')
    name = name.strip().replace(' ', '_')
    return name[:200]

def build_smart_query(categoria, subcategoria, presentacion):
    if subcategoria and subcategoria.lower() not in ['', 'na', 'n/a']:
        return subcategoria.strip()
    if categoria and categoria.lower() not in ['', 'na', 'n/a']:
        return categoria.strip()
    return ""

def fetch_page(query, page, facet_filters=None):
    params_list = [
        f"query={query}",
        f"hitsPerPage={HITS_PER_PAGE}",
        f"page={page}"
    ]
    if facet_filters:
        import json
        params_list.append(f"facetFilters={json.dumps(facet_filters)}")
    params = "&".join(params_list)
    payload = {"params": params}

    r = requests.post(ALGOLIA_QUERY_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_all_hits_for_query(query, categoria="", subcategoria="", presentacion=""):
    all_hits = []
    seen_ids = set()

    try:
        resp = fetch_page(query, 0)
    except Exception as e:
        print(f" Error en query '{query}': {e}")
        return []

    nb_pages = resp.get('nbPages', 1)
    nb_hits = resp.get('nbHits', 0)
    hits = resp.get('hits', [])

    print(f" Query: '{query}' | nbHits={nb_hits} | nbPages={nb_pages} | hits_page0={len(hits)}")

    for h in hits:
        oid = h.get('objectID') or str(h.get('sku') or h.get('skuMifarma') or h.get('skuSap') or '')
        if oid and oid not in seen_ids:
            seen_ids.add(oid)
            all_hits.append(h)

    if nb_pages > 1:
        for p in range(1, min(nb_pages, 20)):
            print(f" Página {p+1}/{nb_pages} ...")
            time.sleep(SLEEP_BETWEEN_REQ + random.random() * 0.1)
            try:
                resp_p = fetch_page(query, p)
            except Exception as e:
                print(f" Error en página {p}: {e}")
                continue
            hits_p = resp_p.get('hits', [])
            for h in hits_p:
                oid = h.get('objectID') or str(h.get('sku') or h.get('skuMifarma') or h.get('skuSap') or '')
                if oid and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_hits.append(h)

    print(f"  → Total sin duplicados: {len(all_hits)}")
    return all_hits

def filter_hits_locally(hits, categoria, subcategoria, presentacion):
    return hits

# ---------------- MAIN ----------------
def main():
    all_rows = []

    if not csv_path.exists():
        raise FileNotFoundError(f"No encuentro el CSV en: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna('')
    print(f"{len(df)} filas leídas desde {csv_path}")

    for idx, row in df.iterrows():
        categoria = row.get('CATEGORIA', '').strip()
        subcategoria = row.get('SUBCATEGORIA', '').strip()
        presentacion_csv = row.get('PRESENTACION', '').strip()

        query = build_smart_query(categoria, subcategoria, presentacion_csv)
        if not query:
            print(f"({idx+1}) Saltando fila sin query válida: {row.to_dict()}")
            continue

        print(f"\n{'='*80}")
        print(f"  Categoría: {categoria}")
        print(f"  Subcategoría: {subcategoria}")

        hits = fetch_all_hits_for_query(query, categoria, subcategoria, presentacion_csv)
        print(f"  → {len(hits)} productos obtenidos de Algolia")

        hits_filtered = filter_hits_locally(hits, categoria, subcategoria, presentacion_csv)
        print(f"  → {len(hits_filtered)} productos después de filtrar localmente")

        rows = []
        for h in hits_filtered:
            codigo_app = h.get('objectID') or ''
            nombre = h.get('name') or ''
            marca = h.get('brand') or h.get('lab') or ''
            precio_lista = h.get('priceList')
            precio_oferta = h.get('pricePromo')
            precio_final = choose_price(h)
            present_real = h.get('presentation') or ''

            rows.append({
                "CODIGO": codigo_app,
                "CATEGORIA": categoria,
                "SUBCATEGORIA": subcategoria,
                "NOMBRE": nombre,
                "LABORATORIO": marca,
                "PRECIO LISTA": precio_lista,
                "PRECIO OFERTA": precio_oferta,
                "PRECIO FINAL": precio_final,
                "PRESENTACION": present_real,
                "FECHA": current_date
            })
        all_rows.extend(rows)
        
        if all_rows:
            out_df = pd.DataFrame(all_rows).drop_duplicates(subset=['NOMBRE'])
            filename = f"inkafarma_consolidado_{current_date}.xlsx"
            out_file = output_path / clean_filename(filename)
            out_df.to_excel(out_file, index=False)
            print(f"\n Consolidado guardado con {len(out_df)} productos")
        else:
            print("\n No se encontraron productos en ninguna categoría.")

    print("\n" + "="*80)
    print("Proceso terminado")


if __name__ == "__main__":
    main()

 