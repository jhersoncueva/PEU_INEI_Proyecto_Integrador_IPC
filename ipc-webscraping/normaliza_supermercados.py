"""
NORMALIZADOR MULTI-TIENDA CON MAPEO A SUBCLASES INEI
Procesa Plaza Vea, Metro, Wong y Vivanda
Mapea subcategorías a las subclases de alimentos del INEI
"""
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
current_dir = Path(__file__).parent.absolute()
FECHA_ESPECIFICA = "2025-11-16"

# Rutas de entrada
RUTAS_TIENDAS = {
    'plazavea': current_dir / 'data' / 'raw' / 'plazavea' / FECHA_ESPECIFICA,
    'metro': current_dir / 'data' / 'raw' / 'metro' / FECHA_ESPECIFICA,
    'wong': current_dir / 'data' / 'raw' / 'wong' / FECHA_ESPECIFICA,
    'vivanda': current_dir / 'data' / 'raw' / 'vivanda' / FECHA_ESPECIFICA
}

# Ruta de salida
output_path = current_dir / 'data' / 'processed' / 'consolidado'
output_path.mkdir(parents=True, exist_ok=True)

# Excepciones de precio
PRECIO_REAL_POR_KG = [
    "pan francés", "pan ciabatta", "pan caracol", "pan pizza",
    "pan baguettino", "pan de aceituna", "pan de jamón", "pan de yema",
    "pan cachito", "pan carioca", "pan de pasas", "pan coliza", "pan hamburguesa"
]

# ============================================================================
# MAPEO A SUBCLASES INEI
# ============================================================================
MAPEO_INEI = {
    # ARROZ Y CEREALES
    "ARROZ DE TODAS CLASES": ["Arroz"],
    "CEREALES EN GRANO": ["Menestras", "Cereales"],
    "HARINA FINA Y HARINA GRUESA": ["Repostería"],
    "CEREALES EN HOJUELA": ["Cereales y Avenas", "Cereales"],
    
    # PRODUCTOS DE PANADERÍA
    "PRODUCTOS DE PANADERÍA Y PASTELERÍA": [
        "Pan de la Casa", "La Panadería", "Pan Envasado",
        "Panes y Tortillas Empacadas", "Panetones", "Pastelería",
        "Panetones, Kekes y Bizcochos Empacados", "Confitería",
        "Panes, Pastas, Bocaditos y Salsas"   
    ],
    "PASTAS": ["Fideos, Pastas y Salsas", "Pastas y Salsas"],
    
    # CARNES
    "CARNE DE RES FRESCA, REFRIGERADA Y CONGELADA": [
        "Res", "Res y Otras Carnes",
        "Cerdo"   
    ],
    "CARNE DE AVE FRESCA, REFRIGERADA Y CONGELADA": [
        "Pollo", "Aves y Huevos", "Pavo, Pavita y Otras Aves"
    ],
    "MENUDENCIA FRESCA, REFRIGERADA Y CONGELADA": ["Res y Otras Carnes"],
    
    "EMBUTIDO Y CARNE SECA": [
        "Embutidos", "Salames y Salchichones", "Fiambres",
        "Delicatessen",                       
        "Jamonadas y Jamones Cocidos",        
        "Jamones Madurados"                   
    ],
    
    "PREPARADOS Y PRODUCTOS CÁRNICOS": [
        "Hamburguesas y Apanados", "Hamburguesas, Nuggets y Apanados",
        "Rostizados"   
    ],
    
    # PESCADOS Y MARISCOS
    "PESCADO FRESCO, REFRIGERADO O CONGELADO": ["Pescados y Mariscos"],
    "MARISCOS": ["Pescados y Mariscos"],
    
    # LÁCTEOS
    "LECHE": ["Leche", "Leches"],
    "YOGUR, CREMA Y DERIVADOS LÁCTEOS": ["Yogurt", "Yogures"],
    "QUESOS": [
        "La Quesería", "Quesos Blandos", "Quesos Duros",
        "Quesos Semiduros", "Quesos Procesados"
    ],
    "HUEVOS": ["Huevos"],
    
    # ACEITES Y GRASAS
    "ACEITES COMESTIBLES": ["Aceite", "Aceites"],
    "GRASAS COMESTIBLES": ["Mantequilla y Margarina", "Mantequillas y Margarinas"],
    
    # FRUTAS Y VERDURAS
    "FRUTAS FRESCAS": ["Frutas"],
    "HORTALIZAS FRESCAS DE HOJAS O TALLO": ["Verduras"],
    "HORTALIZAS CULTIVADAS POR SU FRUTO": ["Verduras"],
    "HORTALIZAS DE RAÍZ OR BULBO Y HONGOS": ["Verduras"],
    
    # CONGELADOS
    "VERDURAS Y LEGUMBRES PROCESADAS": ["Frutas y Verduras"],
    
    # AZÚCAR Y DULCES
    "AZÚCAR Y OTRAS PRESENTACIONES": [
        "Azúcar y Endulzantes", 
        "Azucar y Edulcorantes",
        "Azúcar y Edulcorantes"   
    ],
    "MERMELADAS, MIEL Y PREPARADOS DULCES": [
        "Mermeladas, Mieles y Dulces", "Mermeladas y Mieles"
    ],
    "CHOCOLATE Y PRODUCTOS DE CONFITERÍA": [
        "Chocolatería", "Galletas y Golosinas",
        "Galletas, Snacks y Golosinas"
    ],
    "HIELO COMESTIBLE, HELADOS Y SORBETES": ["Helados", "Helados y Postres"],
    
    # CONDIMENTOS
    "SAL, ESPECIES Y SAZONADORES": [
        "Condimentos, Vinagres y Comida Instantánea",
        "Condimentos-Vinagres-y-Comida-Instantanea"
    ],
    "AJÍES, ADEREZOS, SALSAS Y OTROS": [
        "Salsas, Cremas y Condimentos",
        "Cremas, Salsas y Condimentos a Granel"
    ],
    
    # BOCADITOS
    "BOCADITOS Y ALIMENTOS DIVERSOS": [
        "Snacks y Piqueos", "Galletas, Snacks y Golosinas"
    ],
    
    # BEBIDAS CALIENTES
    "CAFÉ": ["Café e Infusiones"],
    "TÉ Y OTRAS HIERBAS PARA INFUSIÓN": ["Café e Infusiones"],
    "CACAO Y POLVO A BASE DE CHOCOLATE": ["Modificadores de Leche"],
    "PRODUCTO ACHOCOLATADO Y COMPLEMENTO ENRIQUECIDO": [
        "Modificadores de Leche",
        "Suplementos Nutricionales para Adultos"   
    ],
    
    # BEBIDAS FRÍAS
    "AGUAS MINERALES Y BEBIDAS GASEOSAS": ["Aguas", "Gaseosas"],
    "REFRESCOS Y JUGOS DE FRUTA": [
        "Jugos y Otras Bebidas", "Bebidas Funcionales",
        "Bebidas Regeneradoras"
    ],
    
    # CONSERVAS Y PREPARADOS
    "ALIMENTOS PROCESADOS E INSUFLADOS (POPEADOS)": ["Snacks y Piqueos"],
    "PREPARADOS": [
        "Comidas Instantáneas", 
        "Comidas Instantaneas",
        "Comida Congelada",   
        "Masas y Bocaditos",  
        "Enrollados"          
    ],
    "CONSERVAS": ["Conservas", "Alimentos en Conserva"]
}

# Crear diccionario inverso para búsqueda rápida
SUBCATEGORIA_TO_INEI = {}
for subclase_inei, subcats in MAPEO_INEI.items():
    for subcat in subcats:
        SUBCATEGORIA_TO_INEI[subcat.lower()] = subclase_inei

# ============================================================================
# FUNCIONES DE NORMALIZACIÓN 
# ============================================================================

def extraer_cantidad_unidad(nombre):
    if not isinstance(nombre, str):
        return None, None
    
    patrones_empaque = [
        r'(?:bolsa|paquete|caja|pack|frasco|sobre|bandeja)\s+(\d+(?:\.\d+)?)\s*(un|und|unidades?|unidad)\b',
        r'(?:bolsa|paquete|caja|pack|frasco|sobre|bandeja)\s+(?:de\s+)?(\d+(?:\.\d+)?)\b',
        r'(?:x|por)\s+(\d+)\s*(?:un|und|unidades?|unidad)\b',
    ]
    
    for patron in patrones_empaque:
        match = re.search(patron, nombre, re.IGNORECASE)
        if match:
            cantidad = float(match.group(1))
            return cantidad, 'un'
    
    patrones_estandar = [
        r'(\d+(?:\.\d+)?)\s*(ml|l|lt|lts|litros?|cc|g|gramos?|gramo|gr|kg|kilogramos?|kilogramo)\b',
        r'(\d+(?:\.\d+)?)(ml|l|lt|lts|cc|g|gr|kg)\b',
        r'de\s+(\d+(?:\.\d+)?)\s*(ml|l|lt|lts|litros?|cc|g|gramos?|gramo|gr|kg|kilogramos?|kilogramo)\b',
        r'x\s*(\d+(?:\.\d+)?)\s*(ml|l|lt|lts|litros?|cc|g|gramos?|gramo|gr|kg|kilogramos?|kilogramo)\b',
    ]
    
    for patron in patrones_estandar:
        match = re.search(patron, nombre, re.IGNORECASE)
        if match:
            cantidad = float(match.group(1))
            unidad_raw = match.group(2).lower()
            
            if unidad_raw in ['l', 'lt', 'lts', 'litros', 'litro']:
                return cantidad, 'L'
            elif unidad_raw in ['ml', 'mililitros', 'mililitro', 'cc']:
                return cantidad, 'ml'
            elif unidad_raw in ['g', 'gramos', 'gramo', 'gr']:
                return cantidad, 'g'
            elif unidad_raw in ['kg', 'kilogramos', 'kilogramo']:
                return cantidad, 'kg'
    
    patron_unidades = r'(\d+(?:\.\d+)?)\s*(un|und|unidades?|unidad|tabletas?)\b'
    match = re.search(patron_unidades, nombre, re.IGNORECASE)
    if match:
        cantidad = float(match.group(1))
        return cantidad, 'un'
    
    return None, None


def detectar_paquete_multiple(nombre):
    if not isinstance(nombre, str):
        return 1

    patrones_multipaquete = [
        r'(\d+)\s*pack\b',
        r'pack\s*(\d+)\b',
        r'(\d+)-pack\b',
        r'\b(six|twelve)pack\b',
    ]
    
    word_to_num = {'six': 6, 'twelve': 12}

    for patron in patrones_multipaquete:
        match = re.search(patron, nombre, re.IGNORECASE)
        if match:
            valor = match.group(1).lower()
            if valor in word_to_num:
                return word_to_num[valor]
            try:
                return int(valor)
            except ValueError:
                continue

    return 1


def extraer_marca(nombre):
    if not isinstance(nombre, str):
        return None
    
    marcas = re.findall(r'\b[A-Z]{2,}(?:\s+[A-Z]{2,})*\b', nombre)
    palabras_excluir = ['UN', 'ML', 'KG', 'BOLSA', 'PAQUETE', 'CAJA', 'UND']
    marcas_filtradas = [m for m in marcas if m not in palabras_excluir]
    
    return marcas_filtradas[0] if marcas_filtradas else None


def calcular_precio_unitario(precio_final, cantidad, unidad, multiplicador=1, nombre=None, usar_fallback=True):
    if not precio_final or pd.isna(precio_final) or precio_final <= 0:
        return None

    if nombre:
        nombre_limpio = nombre.lower()
        for prod in PRECIO_REAL_POR_KG:
            if prod in nombre_limpio:
                return precio_final

    if cantidad and cantidad > 0 and unidad:
        if not multiplicador or multiplicador <= 0:
            multiplicador = 1

        cantidad_total = cantidad * multiplicador
        
        if cantidad_total <= 0:
            if usar_fallback:
                return precio_final
            return None

        try:
            if unidad == 'ml':
                return (precio_final / cantidad_total) * 1000
            elif unidad == 'L':
                return precio_final / cantidad_total
            elif unidad == 'g':
                return (precio_final / cantidad_total) * 1000
            elif unidad == 'kg':
                return precio_final / cantidad_total
            elif unidad == 'un':
                return precio_final / cantidad_total
        except ZeroDivisionError:
            if usar_fallback:
                return precio_final
            return None

    if usar_fallback:
        return precio_final
    
    return None


def mapear_a_subclase_inei(subcategoria):
    """
    Mapea una subcategoría de supermercado a una subclase INEI
    """
    if not subcategoria:
        return "SIN CLASIFICAR"
    
    subcat_limpia = subcategoria.lower().strip()
    
    # Búsqueda exacta
    if subcat_limpia in SUBCATEGORIA_TO_INEI:
        return SUBCATEGORIA_TO_INEI[subcat_limpia]
    
    # Búsqueda por similitud parcial
    for key, valor in SUBCATEGORIA_TO_INEI.items():
        if key in subcat_limpia or subcat_limpia in key:
            return valor
    
    return "SIN CLASIFICAR"


# ============================================================================
# FUNCIÓN PRINCIPAL DE NORMALIZACIÓN
# ============================================================================

def normalizar_tienda(df, nombre_tienda):
    """
    Normaliza datos de una tienda específica
    """
    print(f"\n{'='*60}")
    print(f"NORMALIZANDO: {nombre_tienda.upper()}")
    print(f"{'='*60}")
    
    # 1. Filtros
    total_inicial = len(df)
    
    # Filtro: disponible
    df = df[df['disponible'] == True].copy()
    print(f"✓ Disponibles: {len(df)} ({total_inicial - len(df)} excluidos)")
    
    # Filtro: productos con + o pack
    antes = len(df)
    mask_pack = df['nombre'].str.contains(r'\+|pack', case=False, na=False)
    df = df[~mask_pack].copy()
    print(f"✓ Sin '+' o 'pack': {len(df)} ({antes - len(df)} excluidos)")
    
    # Filtro: categorías no deseadas
    categorias_excluir = [
        'Cuidado Personal y Salud', 'Limpieza', 'Mascotas',
        'Pollo Rostizado y Comidas Preparadas', 'Mercado Saludable',
        'Vinos, licores y cervezas', 'Vinos, Licores y Cervezas',
        'Cuidado del Bebé', 'Higiene, Salud y Belleza', 'Cervezas, Vinos y Licores',
        'Higiene-Salud-y-Belleza', 'Dermocosmetica', 'Dermocosmética'
    ]
    antes = len(df)
    df = df[~df['CATEGORIA'].isin(categorias_excluir)].copy()
    print(f"✓ Categorías válidas: {len(df)} ({antes - len(df)} excluidos)")
    
    # Filtro: subcategorías no deseadas
    subcategorias_excluir = [
        'Canastas Navideñas', 'Tablas y Piqueos', 'Tortillas y Masas'
    ]
    antes = len(df)
    df = df[~df['SUBCATEGORIA'].isin(subcategorias_excluir)].copy()
    print(f"✓ Subcategorías válidas: {len(df)} ({antes - len(df)} excluidos)")
    
    print(f"\n📊 Total a normalizar: {len(df)}")
    
    # 2. Normalización
    df[['cantidad_extraida', 'unidad_extraida']] = df['nombre'].apply(
        lambda x: pd.Series(extraer_cantidad_unidad(x))
    )
    
    df['multiplicador_paquete'] = df['nombre'].apply(detectar_paquete_multiple)
    df['multiplicador_paquete'] = df['multiplicador_paquete'].replace(0, 1)
    
    df['cantidad_total'] = df.apply(
        lambda row: row['cantidad_extraida'] * row['multiplicador_paquete'] 
        if pd.notna(row['cantidad_extraida']) and row['cantidad_extraida'] > 0 
        else None,
        axis=1
    )
    
    df['marca_extraida'] = df['nombre'].apply(extraer_marca)
    
    df['precio_por_unidad_estandar'] = df.apply(
        lambda row: calcular_precio_unitario(
            row['precio_final'],
            row['cantidad_extraida'],
            row['unidad_extraida'],
            row['multiplicador_paquete'],
            nombre=row['nombre'],
            usar_fallback=True
        ),
        axis=1
    )
    
    # 3. Mapeo a subclase INEI
    df['subclase_inei'] = df['SUBCATEGORIA'].apply(mapear_a_subclase_inei)
    
    con_precio = df['precio_por_unidad_estandar'].notna().sum()
    print(f"✓ Con precio unitario: {con_precio} ({(con_precio/len(df))*100:.1f}%)")
    
    return df


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("="*80)
    print("NORMALIZADOR MULTI-TIENDA CON MAPEO A SUBCLASES INEI")
    print("="*80)
    print(f"Fecha: {FECHA_ESPECIFICA}\n")
    
    todos_los_datos = []
    
    # Procesar cada tienda
    for tienda, ruta in RUTAS_TIENDAS.items():
        archivos = list(ruta.glob(f"CONSOLIDADO_{tienda}_*.xlsx"))
        
        if not archivos:
            print(f"  {tienda.upper()}: No se encontró archivo")
            continue
        
        archivo = archivos[0]
        df = pd.read_excel(archivo)
        print(f"✓ {tienda.upper()}: {len(df)} productos cargados")
        
        df_normalizado = normalizar_tienda(df, tienda)
        todos_los_datos.append(df_normalizado)
    
    if not todos_los_datos:
        print("\n No se encontraron datos para procesar")
        return
    
    # Consolidar
    print("\n" + "="*80)
    print("CONSOLIDANDO DATOS")
    print("="*80)
    
    df_final = pd.concat(todos_los_datos, ignore_index=True)
    
    # Eliminar duplicados por SKU
    antes = len(df_final)
    df_final = df_final.drop_duplicates(subset=['sku', 'tienda'], keep='first')
    print(f"✓ Duplicados eliminados: {antes - len(df_final)}")
    print(f"✓ Total productos: {len(df_final)}")
    
    # Estadísticas
    print("\n ESTADÍSTICAS POR TIENDA:")
    for tienda in df_final['tienda'].unique():
        count = (df_final['tienda'] == tienda).sum()
        print(f"   {tienda}: {count:,} productos")
    
    print("\n PRODUCTOS POR SUBCLASE INEI:")
    subclases = df_final['subclase_inei'].value_counts()
    for subclase, count in subclases.head(10).items():
        print(f"   {subclase}: {count:,}")
    
        # ========================================================================
    # VALIDACIÓN DE RANGOS DE PRECIO POR SUBCLASE INEI
    # ========================================================================

    # RANGOS COMPLETOS (35 SUBCLASES INEI)
    RANGOS_PRECIOS = {
        "ARROZ DE TODAS CLASES": (2, 12),
        "CEREALES EN GRANO": (3, 18),
        "HARINA FINA Y HARINA GRUESA": (2, 15),
        "CEREALES EN HOJUELA": (5, 20),

        "PRODUCTOS DE PANADERÍA Y PASTELERÍA": (6, 30),
        "PASTAS": (2, 10),

        "CARNE DE RES FRESCA, REFRIGERADA Y CONGELADA": (12, 45),
        "CARNE DE AVE FRESCA, REFRIGERADA Y CONGELADA": (6, 18),
        "MENUDENCIA FRESCA, REFRIGERADA Y CONGELADA": (4, 15),
        "EMBUTIDO Y CARNE SECA": (10, 45),
        "PREPARADOS Y PRODUCTOS CÁRNICOS": (12, 40),

        "PESCADO FRESCO, REFRIGERADO O CONGELADO": (10, 35),
        "MARISCOS": (12, 45),

        "LECHE": (2, 10),
        "YOGUR, CREMA Y DERIVADOS LÁCTEOS": (4, 18),
        "QUESOS": (10, 80),
        "HUEVOS": (0.3, 1),   # por unidad

        "ACEITES COMESTIBLES": (3, 30),  # por litro
        "GRASAS COMESTIBLES": (5, 40),

        "FRUTAS FRESCAS": (1, 12),
        "HORTALIZAS FRESCAS DE HOJAS O TALLO": (1, 12),
        "HORTALIZAS CULTIVADAS POR SU FRUTO": (1, 12),
        "HORTALIZAS DE RAÍZ OR BULBO Y HONGOS": (1, 12),

        "VERDURAS Y LEGUMBRES PROCESADAS": (5, 25),

        "AZÚCAR Y OTRAS PRESENTACIONES": (2, 8),
        "MERMELADAS, MIEL Y PREPARADOS DULCES": (5, 25),
        "CHOCOLATE Y PRODUCTOS DE CONFITERÍA": (4, 20),
        "HIELO COMESTIBLE, HELADOS Y SORBETES": (8, 30),

        "SAL, ESPECIES Y SAZONADORES": (2, 15),
        "AJÍES, ADEREZOS, SALSAS Y OTROS": (2, 15),

        "BOCADITOS Y ALIMENTOS DIVERSOS": (2, 25),

        "CAFÉ": (8, 45),
        "TÉ Y OTRAS HIERBAS PARA INFUSIÓN": (4, 15),
        "CACAO Y POLVO A BASE DE CHOCOLATE": (6, 28),
        "PRODUCTO ACHOCOLATADO Y COMPLEMENTO ENRIQUECIDO": (10, 60),

        "AGUAS MINERALES Y BEBIDAS GASEOSAS": (1, 8),
        "REFRESCOS Y JUGOS DE FRUTA": (2, 15),

        "ALIMENTOS PROCESADOS E INSUFLADOS (POPEADOS)": (2, 15),
        "PREPARADOS": (6, 35),
        "CONSERVAS": (3, 15),
    }

    def dentro_de_rango(row):
        sub = row["subclase_inei"]
        precio = row["precio_por_unidad_estandar"]

        # si la subclase no está en el diccionario → no filtrar
        if sub not in RANGOS_PRECIOS:
            return True

        # si no hay precio → eliminar
        if precio is None:
            return False

        minimo, maximo = RANGOS_PRECIOS[sub]
        return minimo <= precio <= maximo

    # aplicar filtro
    df_final = df_final[df_final.apply(dentro_de_rango, axis=1)]
    
 
    # Guardar
    timestamp = FECHA_ESPECIFICA.replace("-", "")
    archivo_salida = output_path / f"precios_normalizados_multitienda_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
        # Hoja principal
        df_final.to_excel(writer, sheet_name='Datos_Normalizados', index=False)
        
        # Hoja: Mapeo INEI
        mapeo_df = pd.DataFrame([
            {'subclase_inei': k, 'subcategorias_incluidas': ', '.join(v)}
            for k, v in MAPEO_INEI.items()
        ])
        mapeo_df.to_excel(writer, sheet_name='Mapeo_INEI', index=False)
        
        # Hoja: Estadísticas
        stats = pd.DataFrame([{
            'Total_productos': len(df_final),
            'Total_tiendas': df_final['tienda'].nunique(),
            'Con_precio_unitario': df_final['precio_por_unidad_estandar'].notna().sum(),
            'Subclases_INEI_identificadas': df_final['subclase_inei'].nunique()
        }])
        stats.to_excel(writer, sheet_name='Estadisticas', index=False)
    
    print(f"\n ARCHIVO GUARDADO:")
    print(f" {archivo_salida.name}")
    print(f" {output_path}")
    print("="*80)


if __name__ == "__main__":
    main()