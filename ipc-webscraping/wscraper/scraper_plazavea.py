"""
SCRAPER PLAZA VEA - Por Categorías y Subcategorías
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
output_path = current_dir / 'data' / 'raw' / 'plazavea' / current_date
output_path.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.plazavea.com.pe"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
}

SLEEP_BETWEEN_REQ = 1.2
TIENDA = "Plaza Vea"

# ---------------- CATEGORÍAS (Supermercado Plaza Vea) ----------------

CATEGORIAS = {
    "bebidas": {
        "nombre": "Bebidas",
        "subcategorias": {
            "aguas": "Aguas",
            "gaseosas": "Gaseosas",
            "jugos-y-otras-bebidas": "Jugos y Otras Bebidas",
            "bebidas-funcionales": "Bebidas Funcionales"
        }
    },
    "cuidado-personal-y-salud": {
        "nombre": "Cuidado Personal y Salud",
        "subcategorias": {
            "cuidado-del-cabello": "Cuidado del Cabello",
            "cuidado-bucal": "Cuidado Bucal",
            "afeitado": "Afeitado",
            "higiene-personal": "Higiene Personal",
            "proteccion-femenina": "Protección Femenina",
            "proteccion-adulta": "Protección Adulta",
            "bienestar-sexual": "Bienestar Sexual",
            "packs-de-regalo": "Packs de Regalo",
            "vitaminas-y-nutricion": "Vitaminas y Nutrición",
            "salud": "Salud",
            "depilacion": "Depilación"
        }
    },
    "limpieza": {
        "nombre": "Limpieza",
        "subcategorias": {
            "cuidado-de-la-ropa": "Cuidado de la Ropa",
            "limpieza-de-calzado": "Limpieza de Calzado",
            "cuidado-del-hogar": "Cuidado del Hogar",
            "limpieza-de-cocina": "Limpieza de Cocina",
            "accesorios-de-limpieza": "Accesorios de Limpieza",
            "limpieza-de-bano": "Limpieza de Baño",
            "papel-para-el-hogar": "Papel para el Hogar"
        }
    },
    "mascotas": {
        "nombre": "Mascotas",
        "subcategorias": {
            "otras-mascotas": "Otras Mascotas",
            "comida-para-perros": "Comida para perros",
            "salud-e-higiene-para-perros": "Salud e higiene para perros",
            "accesorios-para-perros": "Accesorios para perros",
            "comida-para-gatos": "Comida para gatos",
            "salud-e-higiene-para-gatos": "Salud e higiene para gatos",
            "accesorios-para-gatos": "Accesorios para gatos"
        }
    },
    "abarrotes": {
        "nombre": "Abarrotes",
        "subcategorias": {
            "snacks-y-piqueos": "Snacks y Piqueos",
            "arroz": "Arroz",
            "aceite": "Aceite",
            "azucar-y-endulzantes": "Azúcar y Endulzantes",
            "menestras": "Menestras",
            "fideos-pastas-y-salsas": "Fideos, Pastas y Salsas",
            "conservas": "Conservas",
            "salsas-cremas-y-condimentos": "Salsas, Cremas y Condimentos",
            "comidas-instantaneas": "Comidas Instantáneas",
            "galletas-y-golosinas": "Galletas y Golosinas",
            "canastas-navidenas": "Canastas Navideñas",
            "chocolateria": "Chocolatería"
        }
    },
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
            "helados-y-postres": "Helados y Postres",
            "panes-pastas-bocaditos-y-salsas": "Panes, Pastas, Bocaditos y Salsas",
            "hamburguesas-nuggets-y-apanados": "Hamburguesas, Nuggets y Apanados",
            "enrollados": "Enrollados"
        }
    },
    "quesos-y-fiambres": {
        "nombre": "Quesos y Fiambres",
        "subcategorias": {
            "quesos-blandos": "Quesos Blandos",
            "quesos-duros": "Quesos Duros",
            "quesos-procesados": "Quesos Procesados",
            "otros-fiambres": "Otros Fiambres",
            "jamones-madurados": "Jamones Madurados",
            "salames-y-salchichones": "Salames y Salchichones",
            "quesos-semiduros": "Quesos Semiduros",
            "embutidos": "Embutidos",
            "jamonadas-y-jamones-cocidos": "Jamonadas y Jamones Cocidos",
            "tablas-y-piqueos": "Tablas y Piqueos"
        }
    },
    "panaderia-y-pasteleria": {
        "nombre": "Panadería y Pastelería",
        "subcategorias": {
            "reposteria": "Repostería",
            "pan-de-la-casa": "Pan de la Casa",
            "pan-envasado": "Pan Envasado",
            "tortillas-y-masas": "Tortillas y Masas",
            "postres": "Postres",
            "panetones": "Panetones",
            "pasteleria": "Pastelería"
        }
    },
    "carnes-aves-y-pescados": {
        "nombre": "Carnes, Aves y Pescados",
        "subcategorias": {
            "pollo": "Pollo",
            "cerdo": "Cerdo",
            "res": "Res",
            "pavo-pavita-y-otras-aves": "Pavo, Pavita y Otras Aves",
            "pescados-y-mariscos": "Pescados y Mariscos"
        }
    },
    "lacteos-y-huevos": {
        "nombre": "Lácteos y Huevos",
        "subcategorias": {
            "huevos": "Huevos",
            "leche": "Leche",
            "mantequilla-y-margarina": "Mantequilla y Margarina",
            "yogurt": "Yogurt"
        }
    },
    "desayunos": {
        "nombre": "Desayunos",
        "subcategorias": {
            "cereales": "Cereales",
            "cafe-e-infusiones": "Café e Infusiones",
            "modificadores-de-leche": "Modificadores de Leche",
            "mermeladas-mieles-y-dulces": "Mermeladas, Mieles y Dulces"
        }
    },
    "pollo-rostizado-y-comidas-preparadas": {
        "nombre": "Pollo Rostizado y Comidas Preparadas",
        "subcategorias": {
            "tamales-y-humitas": "Tamales y Humitas",
            "pizzas-y-pastas-frescas": "Pizzas y Pastas Frescas",
            "cenas-navidenas": "Cenas Navideñas",
            "cremas-salsas-y-condimentos-a-granel": "Cremas, Salsas y Condimentos a Granel",
            "pollo-rostizado": "Pollo Rostizado",
            "comidas-preparadas": "Comidas Preparadas"
        }
    },
    "mercado-saludable": {
        "nombre": "Mercado Saludable",
        "subcategorias": {
            "cosmetica-natural": "Cosmética Natural",
            "snacks-organicos": "Snacks Orgánicos",
            "vitaminas-y-suplementos-organicos": "Vitaminas y Suplementos Orgánicos",
            "desayunos-organicos": "Desayunos Orgánicos",
            "alimentos-organicos": "Alimentos Orgánicos",
            "cuidado-personal-sostenible": "Cuidado Personal Sostenible"
        }
    },
    "vinos-licores-y-cervezas": {
        "nombre": "Vinos, licores y cervezas",
        "subcategorias": {
            "licores": "Licores",
            "vinos": "Vinos",
            "espumantes": "Espumantes",
            "cervezas": "Cervezas",
            "cigarros": "Cigarros",
            "hielo": "Hielo"
        }
    },
    "cuidado-del-bebe": {
        "nombre": "Cuidado del Bebé",
        "subcategorias": {
            "panales-y-toallitas-para-bebe": "Pañales y Toallitas para Bebé",
            "alimentacion-del-bebe": "Alimentación del Bebé",
            "cuidado-y-aseo-del-bebe": "Cuidado y Aseo del Bebé",
            "marcas": "Marcas"
        }
    }
}


# ---------------- FUNCIONES AUXILIARES ----------------

def get_products_from_category(cat_slug, subcat_slug):
    """
    Extrae los productos desde la API de Plaza Vea (VTEX)
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
    print("SCRAPER PLAZA VEA - Generación de Archivo Consolidado")
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
        archivo_consolidado = output_path / f"consolidado_plazavea_{current_date}.xlsx"
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