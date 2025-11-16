"""
SCRAPER VIVANDA – Por Categorías y Subcategorías
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
output_path = current_dir / 'data' / 'raw' / 'vivanda' / current_date
output_path.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.vivanda.com.pe"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
}

SLEEP_BETWEEN_REQ = 1.2
TIENDA = "Vivanda"

# ---------------- CATEGORÍAS (Supermercado Vivanda Perú) ----------------
CATEGORIAS = {
    "frutas-y-verduras": {
        "nombre": "Frutas y Verduras",
        "subcategorias": {
            "frutas": "Frutas",
            "verduras": "Verduras"
        }
    },
    "carnes-aves-y-pescados": {
        "nombre": "Carnes, Aves y Pescados",
        "subcategorias": {
            "pollo": "Pollo",
            "res": "Res",
            "cerdo": "Cerdo",
            "pescados-y-mariscos": "Pescados y Mariscos",
            "pavo-pavita-y-otras-aves": "Pavo, Pavita y Otras Aves"
        }
    },
    "desayunos": {
        "nombre": "Desayunos",
        "subcategorias": {
            "cafe-e-infusiones": "Café e Infusiones",
            "cereales": "Cereales",
            "mermeladas-mieles-y-dulces": "Mermeladas, Mieles y Dulces",
            "modificadores-de-leche": "Modificadores de Leche",
            "leche": "Leche"
        }
    },
    "lacteos-y-huevos": {
        "nombre": "Lácteos y Huevos",
        "subcategorias": {
            "leche": "Leche",
            "yogurt": "Yogurt",
            "mantequilla-y-margarina": "Mantequilla y Margarina",
            "huevos": "Huevos"
        }
    },
    "quesos-y-fiambres": {
        "nombre": "Quesos y Fiambres",
        "subcategorias": {
            "quesos-blandos": "Quesos Blandos",
            "quesos-duros": "Quesos Duros",
            "quesos-semiduros": "Quesos Semiduros",
            "quesos-procesados": "Quesos Procesados",
            "embutidos": "Embutidos",
            "salames-y-salchichones": "Salames y Salchichones",
            "otros-fiambres": "Otros Fiambres",
            "jamones-madurados": "Jamones Madurados",
            "jamonadas-y-jamones-cocidos": "Jamonadas y Jamones Cocidos",
            "tablas-y-piqueos": "Tablas y Piqueos"
        }
    },
    "abarrotes": {
        "nombre": "Abarrotes",
        "subcategorias": {
            "aceite": "Aceite",
            "arroz": "Arroz",
            "azucar-y-endulzantes": "Azúcar y Endulzantes",
            "comidas-instantaneas": "Comidas Instantáneas",
            "galletas-y-golosinas": "Galletas y Golosinas",
            "conservas": "Conservas",
            "salsas-cremas-y-condimentos": "Salsas, Cremas y Condimentos",
            "fideos-pastas-y-salsas": "Fideos, Pastas y Salsas",
            "menestras": "Menestras",
            "snacks-y-piqueos": "Snacks y Piqueos",
            "canastas-navidenas": "Canastas Navideñas",
            "chocolateria": "Chocolatería"
        }
    },
    "panaderia-y-pasteleria": {
        "nombre": "Panadería y Pastelería",
        "subcategorias": {
            "pan-de-la-casa": "Pan de la Casa",
            "postres": "Postres",
            "pan-envasado": "Pan Envasado",
            "reposteria": "Repostería",
            "tortillas-y-masas": "Tortillas y Masas",
            "panetones": "Panetones",
            "pasteleria": "Pastelería"
        }
    },
    "pollo-rostizado-y-comidas-preparadas": {
        "nombre": "Pollo Rostizado y Comidas Preparadas",
        "subcategorias": {
            "pollo-rostizado": "Pollo Rostizado",
            "comidas-preparadas": "Comidas Preparadas",
            "cremas-salsas-y-condimentos-a-granel": "Cremas, Salsas y Condimentos a Granel",
            "pizzas-y-pastas-frescas": "Pizzas y Pastas Frescas",
            "tamales-y-humitas": "Tamales y Humitas"
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
    "bebidas": {
        "nombre": "Bebidas",
        "subcategorias": {
            "aguas": "Aguas",
            "bebidas-funcionales": "Bebidas Funcionales",
            "gaseosas": "Gaseosas",
            "jugos-y-otras-bebidas": "Jugos y Otras Bebidas"
        }
    },
    "vinos-licores-y-cervezas": {
        "nombre": "Vinos, Licores y Cervezas",
        "subcategorias": {
            "licores": "Licores",
            "vinos": "Vinos",
            "cigarros": "Cigarros",
            "espumantes": "Espumantes",
            "cervezas": "Cervezas",
            "hielo": "Hielo"
        }
    },
    "limpieza": {
        "nombre": "Limpieza",
        "subcategorias": {
            "accesorios-de-limpieza": "Accesorios de Limpieza",
            "cuidado-de-la-ropa": "Cuidado de la Ropa",
            "cuidado-del-hogar": "Cuidado del Hogar",
            "limpieza-de-cocina": "Limpieza de Cocina",
            "limpieza-de-bano": "Limpieza de Baño",
            "limpieza-de-calzado": "Limpieza de Calzado",
            "papel-para-el-hogar": "Papel para el Hogar"
        }
    },
    "cuidado-personal-y-salud": {
        "nombre": "Cuidado Personal y Salud",
        "subcategorias": {
            "cuidado-del-cabello": "Cuidado del Cabello",
            "higiene-personal": "Higiene Personal",
            "cuidado-bucal": "Cuidado Bucal",
            "cuidado-de-la-piel": "Cuidado de la Piel",
            "proteccion-femenina": "Protección Femenina",
            "proteccion-adulta": "Protección Adulta",
            "afeitado-y-depilacion": "Afeitado y Depilación",
            "bienestar-sexual": "Bienestar Sexual",
            "packs-de-regalo": "Packs de Regalo",
            "salud": "Salud",
            "vitaminas-y-nutricion": "Vitaminas y Nutrición"
        }
    },
    "cuidado-del-bebe": {
        "nombre": "Cuidado del Bebé",
        "subcategorias": {
            "panales-y-toallitas-para-bebe": "Pañales y Toallitas para Bebé",
            "alimentacion-del-bebe": "Alimentación del Bebé",
            "cuidado-y-aseo-del-bebe": "Cuidado y Aseo del Bebé"
        }
    },
    "mascotas": {
        "nombre": "Mascotas",
        "subcategorias": {
            "comida-para-perros": "Comida para Perros",
            "accesorios-para-perros": "Accesorios para Perros",
            "salud-e-higiene-para-perros": "Salud e Higiene para Perros",
            "comida-para-gatos": "Comida para Gatos",
            "accesorios-para-gatos": "Accesorios para Gatos",
            "salud-e-higiene-para-gatos": "Salud e Higiene para Gatos",
            "otras-mascotas": "Otras Mascotas"
        }
    }
}



# ---------------- FUNCIONES AUXILIARES ----------------

def get_products_from_category(cat_slug, subcat_slug):
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
                precio_lista = oferta.get("ListPrice")  # Precio original/normal
                precio_final = oferta.get("Price")      # Precio con descuento
                disponible = oferta.get("IsAvailable", False)
                
                # Calcular descuento si existe diferencia de precios
                if precio_lista and precio_final and precio_lista > precio_final:
                    descuento_monto = round(precio_lista - precio_final, 2)
                    descuento_porcentaje = round(((precio_lista - precio_final) / precio_lista) * 100, 2)
                else:
                    # Si no hay descuento, el precio final es igual al precio de lista
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
    print("SCRAPER VIVANDA - Generación de Archivo Consolidado")
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
        archivo_consolidado = output_path / f"CONSOLIDADO_vivanda_{current_date}.xlsx"
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
