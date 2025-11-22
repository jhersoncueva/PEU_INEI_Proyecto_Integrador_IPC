"""
SCRAPER WONG - Por Categorías y Subcategorías
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
current_date = datetime.now().strftime('%Y-%m-%d')
output_path = current_dir / 'data' / 'raw' / 'wong' / current_date
output_path.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.wong.pe"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
}

SLEEP_BETWEEN_REQ = 1.2
TIENDA = "Wong"

# ---------------- CATEGORÍAS (Supermercado Wong Perú) ----------------
CATEGORIAS = {
    "frutas-y-verduras": {
        "nombre": "Frutas y Verduras",
        "subcategorias": {
            "frutas": "Frutas",
            "verduras": "Verduras"
        }
    },
    "congelados": {
        "nombre": "Congelados",
        "subcategorias": {
            "masas-y-bocaditos": "Masas y Bocaditos",
            "helados": "Helados",
            "pastas-y-salsas": "Pastas y Salsas",
            "frutas-y-verduras": "Frutas y Verduras",
            "comida-congelada": "Comida Congelada"
        }
    },
    "embutidos-y-fiambres": {
        "nombre": "Embutidos y Fiambres",
        "subcategorias": {
            "embutidos": "Embutidos",
            "fiambres": "Fiambres",
            "delicatessen": "Delicatessen"
        }
    },
    "abarrotes": {
        "nombre": "Abarrotes",
        "subcategorias": {
            "aceites": "Aceites",
            "alimentos-en-conserva": "Alimentos en Conserva",
            "fideos-pastas-y-salsas": "Fideos, Pastas y Salsas",
            "chocolateria": "Chocolatería",
            "reposteria": "Repostería",
            "arroz": "Arroz",
            "menestras": "Menestras",
            "condimentos-vinagres-y-comida-instantanea": "Condimentos, Vinagres y Comida Instantánea",
            "galletas-snacks-y-golosinas": "Galletas, Snacks y Golosinas"
        }
    },
    "limpieza": {
        "nombre": "Limpieza",
        "subcategorias": {
            "limpieza-de-cocina": "Limpieza de Cocina",
            "cuidado-de-ropa": "Cuidado de Ropa",
            "accesorios-de-limpieza": "Accesorios de Limpieza",
            "cuidado-del-hogar": "Cuidado del Hogar",
            "limpieza-de-bano": "Limpieza de Baño",
            "limpieza-del-calzado": "Limpieza del Calzado",
            "limpieza-de-automoviles": "Limpieza de Automóviles",
            "aerosoles": "Aerosoles",
            "papel-para-el-hogar": "Papel para el Hogar"
        }
    },
    "cervezas-vinos-y-licores": {
        "nombre": "Cervezas, Vinos y Licores",
        "subcategorias": {
            "vinos": "Vinos",
            "champagne-espumantes-y-cavas": "Champagne, Espumantes y Cavas",
            "cervezas": "Cervezas",
            "cigarrillos": "Cigarrillos",
            "licores": "Licores"
        }
    },
    "higiene-salud-y-belleza": {
        "nombre": "Higiene, Salud y Belleza",
        "subcategorias": {
            "cuidado-bucal": "Cuidado Bucal",
            "cuidado-del-cabello": "Cuidado del Cabello",
            "cuidado-femenino": "Cuidado Femenino",
            "proteccion-incontinencia": "Protección Incontinencia",
            "salud": "Salud",
            "cuidado-personal": "Cuidado Personal",
            "cuidado-facial-y-corporal": "Cuidado Facial y Corporal",
            "afeitado-y-depilacion": "Afeitado y Depilación",
            "belleza": "Belleza",
            "packs": "Packs"
        }
    },
    "desayuno": {
        "nombre": "Desayuno",
        "subcategorias": {
            "cafe-e-infusiones": "Café e Infusiones",
            "azucar-y-edulcorantes": "Azúcar y Edulcorantes",
            "cereales-y-avenas": "Cereales y Avenas",
            "mermeladas-y-mieles": "Mermeladas y Mieles",
            "panes-y-tortillas-empacadas": "Panes y Tortillas Empacadas",
            "panetones-kekes-y-bizcochos-empacados": "Panetones, Kekes y Bizcochos Empacados",
            "modificadores-de-leche": "Modificadores de Leche",
            "suplementos-nutricionales-para-adultos": "Suplementos Nutricionales para Adultos"
        }
    },
    "panaderia-y-pasteleria": {
        "nombre": "Panadería y Pastelería",
        "subcategorias": {
            "la-panaderia": "La Panadería",
            "pasteleria": "Pastelería",
            "confiteria": "Confitería"
        }
    },
    "comidas-y-rostizados": {
        "nombre": "Comidas y Rostizados",
        "subcategorias": {
            "comidas": "Comidas",
            "pollo-delivery": "Pollo Delivery",
            "rostizados": "Rostizados"
        }
    },
    "dermocosmetica": {
        "nombre": "Dermocosmética",
        "subcategorias": {
            "marcas": "Marcas"
        }
    },
    "carnes-aves-y-pescados": {
        "nombre": "Carnes, Aves y Pescados",
        "subcategorias": {
            "aves-y-huevos": "Aves y Huevos",
            "res-y-otras-carnes": "Res y Otras Carnes",
            "cerdo": "Cerdo",
            "pescados-y-mariscos": "Pescados y Mariscos",
            "hamburguesas-y-apanados": "Hamburguesas y Apanados"
        }
    },
    "lacteos-y-huevos": {
        "nombre": "Lácteos y Huevos",
        "subcategorias": {
            "aves-y-huevos": "Aves y Huevos",
            "la-queseria": "La Quesería",
            "leches": "Leches",
            "mantequillas-y-margarinas": "Mantequillas y Margarinas",
            "yogures": "Yogures"
        }
    },
    "bebidas": {
        "nombre": "Bebidas",
        "subcategorias": {
            "aguas": "Aguas",
            "bebidas-regeneradoras": "Bebidas Regeneradoras",
            "gaseosas": "Gaseosas",
            "hielo": "Hielo",
            "jugos-y-otras-bebidas": "Jugos y Otras Bebidas"
        }
    }
}


# ---------------- FUNCIONES AUXILIARES ----------------

def get_products_from_category(cat_slug, subcat_slug):
    """
    Extrae los productos desde la API de Wong (VTEX)
    """
    all_products = []
    page = 1
    max_pages = 15
    page_size = 50

    while page <= max_pages:
        start = (page - 1) * page_size
        end = page * page_size - 1
        url = f"{BASE_URL}/api/catalog_system/pub/products/search/{cat_slug}/{subcat_slug}?_from={start}&_to={end}"

        try:
            print(f"  Página {page} → {url}")
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            products = resp.json()

            if not products:
                print("  ✗ No hay más productos")
                break

            for p in products:
                producto = parse_product(p)
                if producto:
                    all_products.append(producto)

            page += 1
            time.sleep(SLEEP_BETWEEN_REQ + random.random() * 0.5)

        except Exception as e:
            print(f"   Error en página {page}: {e}")
            break

    return all_products


def parse_product(p):
    """Procesa y limpia la info de un producto incluyendo precios y descuentos"""
    try:
        nombre = p.get("productName", "").strip()
        marca = p.get("brand", "").strip()
        sku = p.get("productId", "")

        precio_lista = None
        precio_final = None
        descuento_porcentaje = None
        descuento_monto = None
        disponible = False
        presentacion = ""

        if p.get("items"):
            item = p["items"][0]
            presentacion = item.get("name", "")
            if item.get("sellers"):
                seller = item["sellers"][0]
                oferta = seller.get("commertialOffer", {})
                
                # Extraer precios
                precio_lista = oferta.get("ListPrice")
                precio_final = oferta.get("Price")
                disponible = oferta.get("IsAvailable", False)
                
                # Calcular descuento si existe diferencia de precios
                if precio_lista and precio_final and precio_lista > precio_final:
                    descuento_monto = round(precio_lista - precio_final, 2)
                    descuento_porcentaje = round(((precio_lista - precio_final) / precio_lista) * 100, 2)
                else:
                    if not precio_final and precio_lista:
                        precio_final = precio_lista

        if not nombre:
            return None

        return {
            "sku": sku,
            "nombre": nombre,
            "marca": marca,
            "precio_lista": precio_lista,
            "precio_final": precio_final,
            "descuento_monto": descuento_monto,
            "descuento_porcentaje": descuento_porcentaje,
            "presentacion": presentacion,
            "disponible": disponible,
        }

    except Exception as e:
        print(f"    Error procesando producto: {e}")
        return None


# ---------------- MAIN ----------------
def main():
    print("=" * 80)
    print("SCRAPER WONG - Generación de Archivo Consolidado")
    print("=" * 80)

    todos_los_productos = []

    for cat_slug, cat_data in CATEGORIAS.items():
        cat_nombre = cat_data["nombre"]
        print(f"\nCATEGORÍA: {cat_nombre}")

        for sub_slug, sub_nombre in cat_data["subcategorias"].items():
            print(f"\n→ Subcategoría: {sub_nombre}")

            productos = get_products_from_category(cat_slug, sub_slug)
            print(f"  ✓ Productos obtenidos: {len(productos)}")

            if productos:
                for producto in productos:
                    producto["CATEGORIA"] = cat_nombre
                    producto["SUBCATEGORIA"] = sub_nombre
                    todos_los_productos.append(producto)
            else:
                print("  ✗ Sin resultados")

            time.sleep(SLEEP_BETWEEN_REQ)

    # Crear DataFrame consolidado
    if todos_los_productos:
        df_consolidado = pd.DataFrame(todos_los_productos)
        
        # Reordenar columnas poniendo CATEGORIA y SUBCATEGORIA al inicio
        cols = ['CATEGORIA', 'SUBCATEGORIA', 'sku', 'nombre', 'marca', 'precio_lista', 
                'precio_final', 'descuento_monto', 'descuento_porcentaje', 'presentacion', 'disponible']
        df_consolidado = df_consolidado[cols]
        
        # Eliminar duplicados por SKU
        df_consolidado.drop_duplicates(subset=["sku"], inplace=True)
        
        # Agregar columnas de fecha y tienda
        df_consolidado['fecha_scraping'] = current_date
        df_consolidado['tienda'] = TIENDA
        
        # Guardar archivo consolidado
        archivo_consolidado = output_path / f"CONSOLIDADO_wong_{current_date}.xlsx"
        df_consolidado.to_excel(archivo_consolidado, index=False)
        
        print("\n" + "=" * 80)
        print(f"✓ SCRAPING COMPLETADO")
        print(f" Total productos únicos: {len(df_consolidado)}")
        print(f" Archivo: {archivo_consolidado.name}")
        print(f" Ubicación: {output_path}")
        print("=" * 80)
    else:
        print("\n No se encontraron productos para consolidar")


if __name__ == "__main__":
    main()