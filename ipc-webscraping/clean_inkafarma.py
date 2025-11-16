"""
SCRIPT DE LIMPIEZA - Inkafarma
"""
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime

# CONFIG
current_dir = Path(__file__).parent.absolute()

FECHA_ESPECIFICA = "2025-11-16"  # Cambiar a None para procesar todas las fechas

# Rutas de entrada (por fecha)
input_base = current_dir / 'data' / 'raw' / 'inkafarma'
output_base = current_dir / 'data' / 'processed' / 'inkafarma'


# FUNCIONES DE EXTRACCIÓN

def extraer_nombre_comercial(nombre):
    """Extrae el nombre comercial incluyendo números que son parte del nombre"""
    if not isinstance(nombre, str):
        return None
    
    # LIMPIEZA PREVIA: corregir errores comunes
    nombre = re.sub(r'(\d+)\s*mgmg', r'\1mg', nombre, flags=re.IGNORECASE)
    nombre = re.sub(r',(\s*\d+\s*(?:mg|g|ml|mcg|ui|%|gr))', r' \1', nombre, flags=re.IGNORECASE)
    nombre = re.sub(r'(\d+)\s+\1\s+(mg|g|ml|mcg|ui|%|gr)', r'\1 \2', nombre, flags=re.IGNORECASE)
    
    # 1. Patrón para números grandes con espacios seguidos de unidad
    patron_numero_grande = r'\d+(?:\s+\d{3})+\s*(?:ui|UI|mg|g|ml|mcg|%|gr)\b'
    match_grande = re.search(patron_numero_grande, nombre, re.IGNORECASE)
    if match_grande:
        nombre_comercial = nombre[:match_grande.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 2. Patrón para concentraciones en paréntesis
    patron_parentesis = r'\([^)]*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)[^)]*\)'
    match_parentesis = re.search(patron_parentesis, nombre, re.IGNORECASE)
    if match_parentesis:
        nombre_comercial = nombre[:match_parentesis.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 3. Patrón para concentraciones compuestas múltiples (con +)
    patron_multiple = r'\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)(?:/\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr))?\s*\+\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)'
    match_multiple = re.search(patron_multiple, nombre, re.IGNORECASE)
    if match_multiple:
        nombre_comercial = nombre[:match_multiple.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 4. Patrón para concentraciones tipo: 250-62.5/5ml
    patron_guion_compuesto = r'\d+\s*-\s*\d+[.,]?\d*\s*[/-]\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)'
    match_guion = re.search(patron_guion_compuesto, nombre, re.IGNORECASE)
    if match_guion:
        texto_antes = nombre[:match_guion.start()]
        patron_ultimo_num = r'(\d+)\s*$'
        match_num = re.search(patron_ultimo_num, texto_antes)
        if match_num:
            nombre_comercial = texto_antes[:match_num.start()].strip()
        else:
            nombre_comercial = texto_antes.strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 5. Patrón especial para casos como "Telmilife A 80/10 80Mmg +"
    patron_slash_con_unidad = r'(\d+[.,]?\d*/\d+[.,]?\d*)\s+\d+[.,]?\d*\s*(?:M?mg|M?g|ml|mcg|ui|%|gr)'
    match_slash = re.search(patron_slash_con_unidad, nombre, re.IGNORECASE)
    if match_slash:
        nombre_comercial = nombre[:match_slash.end()].strip()
        final_pos = match_slash.start() + len(match_slash.group(1))
        nombre_comercial = nombre[:final_pos].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 6. Patrón para concentraciones con unidad seguida de /
    patron_con_unidad = r'\d+[.,]?\d*\s*(?:mg|g|mcg|ui|gr)\s*[/-]\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)?'
    match_con_unidad = re.search(patron_con_unidad, nombre, re.IGNORECASE)
    if match_con_unidad:
        nombre_comercial = nombre[:match_con_unidad.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 7. Patrón para porcentajes
    patron_porcentaje = r'\d+[.,]?\d*\s*%'
    match_porcentaje = re.search(patron_porcentaje, nombre, re.IGNORECASE)
    if match_porcentaje:
        nombre_comercial = nombre[:match_porcentaje.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 8. Patrón para concentración simple con unidad
    patron_simple = r'\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|gr)\b'
    match_simple = re.search(patron_simple, nombre, re.IGNORECASE)
    if match_simple:
        nombre_comercial = nombre[:match_simple.start()].strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 9. Patrón para detectar solo + al final
    patron_plus_solo = r'\s*\+\s*$'
    if re.search(patron_plus_solo, nombre):
        nombre_comercial = re.sub(patron_plus_solo, '', nombre).strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # 10. Patrón para detectar guion al final
    patron_guion_solo = r'\s*-\s*$'
    if re.search(patron_guion_solo, nombre):
        nombre_comercial = re.sub(patron_guion_solo, '', nombre).strip()
        nombre_comercial = re.sub(r'[/\-,+%\s]+$', '', nombre_comercial).strip()
        return nombre_comercial if nombre_comercial else None
    
    # Si no hay concentración, tomar las primeras 3 palabras
    palabras = nombre.split()
    if len(palabras) >= 3:
        resultado = ' '.join(palabras[:3])
        resultado = re.sub(r'[/\-,+%\s]+$', '', resultado).strip()
        return resultado
    
    resultado = re.sub(r'[/\-,+%\s]+$', '', nombre).strip()
    return resultado


def extraer_concentracion(nombre):
    """Extrae concentración completa y normaliza formato"""
    if not isinstance(nombre, str):
        return None, None, None
    
    # LIMPIEZA PREVIA
    nombre = re.sub(r'(\d+)\s*mgmg', r'\1mg', nombre, flags=re.IGNORECASE)
    nombre = re.sub(r',(\s*\d+\s*(?:mg|g|ml|mcg|ui|%|gr))', r' \1', nombre, flags=re.IGNORECASE)
    nombre = re.sub(r'(\d+)\s+\1\s+(mg|g|ml|mcg|ui|%|gr)', r'\1 \2', nombre, flags=re.IGNORECASE)
    # IMPORTANTE: Corregir punto como separador de miles antes de UI (ej: 10.000UI -> 10000UI)
    nombre = re.sub(r'(\d+)\.(\d{3})\s*(ui|UI)\b', r'\1\2 \3', nombre, flags=re.IGNORECASE)
    
    # 1. Números grandes con espacios (1 000 000 UI, 1200 000 UI)
    patron_numero_grande = r'(\d+(?:\s+\d{3})+)\s+(ui|UI|mg|g|ml|mcg|%|gr)\b'
    match_grande = re.search(patron_numero_grande, nombre, re.IGNORECASE)
    if match_grande:
        inicio_match = match_grande.start()
        if inicio_match > 0:
            texto_antes = nombre[:inicio_match].strip()
            if re.search(r'\d+\s*(?:mg|g|ml|mcg|ui|%|gr)$', texto_antes, re.IGNORECASE):
                pass
            else:
                numero_con_espacios = match_grande.group(1)
                cantidad_str = numero_con_espacios.replace(' ', '')
                cantidad = float(cantidad_str)
                unidad = match_grande.group(2).lower()
                concentracion_texto = f"{int(cantidad)} {unidad}"
                return concentracion_texto, cantidad, unidad
        else:
            numero_con_espacios = match_grande.group(1)
            cantidad_str = numero_con_espacios.replace(' ', '')
            cantidad = float(cantidad_str)
            unidad = match_grande.group(2).lower()
            concentracion_texto = f"{int(cantidad)} {unidad}"
            return concentracion_texto, cantidad, unidad
    
    # 2. Patrón especial para "número/número unidad + unidad"
    patron_slash_mas_concentracion = r'(\d+[.,]?\d*/\d+[.,]?\d*)\s+(\d+[.,]?\d*\s*(?:M?mg|M?g|ml|mcg|ui|%|gr)(?:\s*/\s*\d+[.,]?\d*\s*(?:M?mg|M?g|ml|mcg|ui|%|gr))?\s*\+\s*\d+[.,]?\d*\s*(?:M?mg|M?g|ml|mcg|ui|%|gr))'
    match_slash_especial = re.search(patron_slash_mas_concentracion, nombre, re.IGNORECASE)
    if match_slash_especial:
        concentracion_texto = match_slash_especial.group(2)
        concentracion_texto = re.sub(r'\s+', ' ', concentracion_texto)
        concentracion_texto = re.sub(r'\s*/\s*', '/', concentracion_texto)
        concentracion_texto = re.sub(r'\s*\+\s*', ' + ', concentracion_texto)
        concentracion_texto = re.sub(r'Mmg', 'mg', concentracion_texto, flags=re.IGNORECASE)
        
        patron_primer_num = r'(\d+[.,]?\d*)\s*(?:M?mg|M?g|ml|mcg|ui|%|gr)'
        match_num = re.search(patron_primer_num, concentracion_texto, re.IGNORECASE)
        if match_num:
            cantidad_str = match_num.group(1).replace(',', '.')
            cantidad = float(cantidad_str)
            match_unidad = re.search(r'(M?mg|M?g|ml|mcg|ui|%|gr)', concentracion_texto, re.IGNORECASE)
            if match_unidad:
                unidad = match_unidad.group(1).lower()
                unidad = re.sub(r'mmg', 'mg', unidad)
                return concentracion_texto, cantidad, unidad
    
    # 3. Concentraciones múltiples con +
    patron_multiple = r'(\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)(?:\s*/\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr))?(?:\s*\+\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)(?:\s*/\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr))?)*)'
    match_multiple = re.search(patron_multiple, nombre, re.IGNORECASE)
    if match_multiple:
        concentracion_texto = match_multiple.group(1)
        concentracion_texto = re.sub(r'\s+', ' ', concentracion_texto)
        concentracion_texto = re.sub(r'\s*/\s*', '/', concentracion_texto)
        concentracion_texto = re.sub(r'\s*\+\s*', ' + ', concentracion_texto)
        
        patron_primer_num = r'(\d+[.,]?\d*)\s*(mg|g|ml|mcg|ui|%|gr)'
        match_num = re.search(patron_primer_num, concentracion_texto, re.IGNORECASE)
        if match_num:
            cantidad_str = match_num.group(1).replace(',', '.')
            cantidad = float(cantidad_str)
            unidad = match_num.group(2).lower()
            return concentracion_texto, cantidad, unidad
    
    # 4. Concentraciones en paréntesis
    patron_parentesis = r'\(([^)]*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)[^)]*)\)'
    match_parentesis = re.search(patron_parentesis, nombre)
    if match_parentesis:
        concentracion_texto = match_parentesis.group(1)
        concentracion_texto = re.sub(r'\s+', ' ', concentracion_texto)
        concentracion_texto = re.sub(r'\s*\+\s*', ' + ', concentracion_texto)
        
        patron_primer_num = r'(\d+[.,]?\d*)\s*(mg|g|ml|mcg|ui|%|gr)'
        match_num = re.search(patron_primer_num, concentracion_texto, re.IGNORECASE)
        if match_num:
            cantidad_str = match_num.group(1).replace(',', '.')
            cantidad = float(cantidad_str)
            unidad = match_num.group(2).lower()
            return concentracion_texto, cantidad, unidad
    
    # 5. Concentración compuesta
    patron_compuesto = r'(\d+[.,]?\d*)\s*(?:mg|g|mcg|ui|%|gr)\s*[/-]\s*(\d+[.,]?\d*)\s*(mg|g|ml|mcg|ui|%|gr)?'
    match_compuesto = re.search(patron_compuesto, nombre, re.IGNORECASE)
    if match_compuesto:
        cantidad1_str = match_compuesto.group(1).replace(',', '.')
        cantidad1 = float(cantidad1_str)
        cantidad2_str = match_compuesto.group(2).replace(',', '.')
        cantidad2 = float(cantidad2_str)
        
        patron_unidades = r'(\d+[.,]?\d*)\s*(mg|g|mcg|ui|%|gr)\s*[/-]\s*(\d+[.,]?\d*)\s*(mg|g|ml|mcg|ui|%|gr)?'
        match_unidades = re.search(patron_unidades, nombre, re.IGNORECASE)
        if match_unidades:
            unidad1 = match_unidades.group(2).lower()
            unidad2 = match_unidades.group(4).lower() if match_unidades.group(4) else 'ml'
            
            concentracion_texto = f"{int(cantidad1) if cantidad1.is_integer() else cantidad1} {unidad1}/{int(cantidad2) if cantidad2.is_integer() else cantidad2} {unidad2}"
            return concentracion_texto, cantidad1, unidad1
    
    # 6. Concentración simple
    patron_simple = r'(\d+[.,]?\d*)\s*(mg|g|ml|mcg|ui|%|gr)\b'
    match_simple = re.search(patron_simple, nombre, re.IGNORECASE)
    if match_simple:
        cantidad_str = match_simple.group(1).replace(',', '.')
        cantidad = float(cantidad_str)
        unidad = match_simple.group(2).lower()
        concentracion_texto = f"{int(cantidad) if cantidad.is_integer() else cantidad} {unidad}"
        return concentracion_texto, cantidad, unidad
    
    return None, None, None


def extraer_forma_farmaceutica(nombre):
    """Extrae descripción: concentración completa + forma farmacéutica"""
    if not isinstance(nombre, str):
        return None
    
    # 1. Números grandes con espacios (debe capturar TODO desde el inicio del número)
    patron_numero_grande = r'(\d+(?:\s+\d{3})+\s*(?:ui|UI|mg|g|ml|mcg|%|gr)\b.*)'
    match_grande = re.search(patron_numero_grande, nombre, re.IGNORECASE)
    if match_grande:
        descripcion = match_grande.group(1).strip()
        return descripcion if descripcion else None
    
    # 2. Concentraciones múltiples con +
    patron_multiple = r'(\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)(?:\s*/\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr))?(?:\s*\+\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)(?:\s*/\s*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr))?)*.*)'
    match_multiple = re.search(patron_multiple, nombre, re.IGNORECASE)
    if match_multiple:
        descripcion = match_multiple.group(1).strip()
        descripcion = re.sub(r'\s+', ' ', descripcion)
        descripcion = re.sub(r'\s*/\s*', '/', descripcion)
        descripcion = re.sub(r'\s*\+\s*', ' + ', descripcion)
        return descripcion if descripcion else None
    
    # 3. Concentraciones en paréntesis
    patron_parentesis = r'(\([^)]*\d+[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)[^)]*\).*)'
    match_parentesis = re.search(patron_parentesis, nombre, re.IGNORECASE)
    if match_parentesis:
        descripcion = match_parentesis.group(1).strip()
        return descripcion if descripcion else None
    
    # 4. Concentraciones compuestas o simples
    patron_concentracion = r'(\d+[.,]?\d*\s*(?:mg|g|mcg|ui|%|gr)\s*[/-]?\s*\d*[.,]?\d*\s*(?:mg|g|ml|mcg|ui|%|gr)?.*)'
    match = re.search(patron_concentracion, nombre, re.IGNORECASE)
    if match:
        descripcion = match.group(1).strip()
        descripcion = re.sub(r'\s*[-]\s*', '/', descripcion)
        descripcion = re.sub(r'\s*/\s*', '/', descripcion)
        descripcion = re.sub(r'\s+', ' ', descripcion)
        return descripcion if descripcion else None
    
    return None


def extraer_presentacion_detallada(presentacion):
    """Extrae tipo de envase, cantidad y unidad de la presentación"""
    if not isinstance(presentacion, str):
        return None, None, None
    
    patron = r'([A-ZÁÉÍÓÚáéíóú]+)\s+(\d+(?:\.\d+)?)\s*(ML|ml|UN|un|GR|gr|G|g|L|l|UNID|unid|U\s*N|EA|SOBRES?|sobres?|DOSIS|dosis|DSS|dss)'
    match = re.search(patron, presentacion, re.IGNORECASE)
    if match:
        tipo_envase = match.group(1).upper()
        if 'BLIST' in tipo_envase or 'BLÍST' in tipo_envase:
            tipo_envase = 'BLISTER'
        cantidad = float(match.group(2))
        unidad_raw = match.group(3).lower().replace(' ', '')
        
        if unidad_raw in ['ml']:
            unidad = 'ml'
        elif unidad_raw in ['un', 'unid', 'u', 'n', 'un', 'ea']:
            unidad = 'un'
        elif unidad_raw in ['gr', 'g']:
            unidad = 'g'
        elif unidad_raw in ['l']:
            unidad = 'l'
        elif 'sobre' in unidad_raw:
            unidad = 'un'
        elif unidad_raw in ['dosis', 'dss']:
            unidad = 'dosis'
        else:
            unidad = unidad_raw
        
        return tipo_envase, cantidad, unidad
    
    return None, None, None


def calcular_precio_unitario(precio_final, cantidad_envase, tipo_envase, unidad_envase):
    """Calcula precio unitario: divide el precio final entre la cantidad del envase siempre que sea válida"""
    if pd.isna(precio_final):
        return None
    if pd.isna(cantidad_envase) or cantidad_envase == 0:
        return round(precio_final, 2)
    return round(precio_final / cantidad_envase, 2)

def crear_clave_matching(nombre_comercial, concentracion=None):
    """Crea clave única para comparar productos entre farmacias"""
    partes = []
    if nombre_comercial:
        nombre_limpio = re.sub(r'[^a-zA-Z0-9]', '', nombre_comercial.lower())
        partes.append(nombre_limpio)
    if concentracion:
        conc_limpio = re.sub(r'[^a-z0-9/]', '', concentracion.lower())
        partes.append(conc_limpio)
    return '_'.join(partes) if partes else None


# FUNCIÓN PRINCIPAL DE LIMPIEZA

def limpiar_dataframe(df):
    """Aplica todas las limpiezas al DataFrame"""
    
    # 1. Estandarizar nombres de columnas
    columnas_rename = {}
    if 'PRECIO LISTA' in df.columns:
        columnas_rename['PRECIO LISTA'] = 'PRECIO_LISTA'
    if 'PRECIO OFERTA' in df.columns:
        columnas_rename['PRECIO OFERTA'] = 'PRECIO_OFERTA'
    if 'PRECIO FINAL' in df.columns:
        columnas_rename['PRECIO FINAL'] = 'PRECIO_FINAL'
    if 'RECIO LIST' in df.columns:
        columnas_rename['RECIO LIST'] = 'PRECIO_LISTA'
    if columnas_rename:
        df.rename(columns=columnas_rename, inplace=True)
    
    # 2. Filtrar productos no deseados ANTES de procesar
    filas_antes = len(df)
    
    # Eliminar productos con "SUPER PACK" en presentación
    df = df[~df['PRESENTACION'].str.contains('SUPER PACK', case=False, na=False)]
    
    # Eliminar shampoo
    df = df[~df['NOMBRE'].str.contains('shampoo', case=False, na=False)]
    df = df[~df['PRESENTACION'].str.contains('shampoo', case=False, na=False)]
    
    filas_despues = len(df)
    filas_eliminadas = filas_antes - filas_despues
    if filas_eliminadas > 0:
        print(f"Productos filtrados: {filas_eliminadas}")
    
    # 3. Extraer campos
    df['NOMBRE_COMERCIAL'] = df['NOMBRE'].apply(extraer_nombre_comercial)
    df[['CONCENTRACION', 'CONCENTRACION_NUM', 'CONCENTRACION_UNIDAD']] = df['NOMBRE'].apply(lambda x: pd.Series(extraer_concentracion(x)))
    df['DESCRIPCION'] = df['NOMBRE'].apply(extraer_forma_farmaceutica)
    df[['TIPO_ENVASE', 'CANTIDAD_ENVASE', 'UNIDAD_ENVASE']] = df['PRESENTACION'].apply(lambda x: pd.Series(extraer_presentacion_detallada(x)))
    df['PRECIO_UNITARIO'] = df.apply(lambda row: calcular_precio_unitario(row['PRECIO_FINAL'], row['CANTIDAD_ENVASE'], row['TIPO_ENVASE'], row['UNIDAD_ENVASE']), axis=1)
    df['CLAVE_MATCHING'] = df.apply(lambda row: crear_clave_matching(nombre_comercial=row['NOMBRE_COMERCIAL'], concentracion=row['CONCENTRACION']), axis=1)
    
    # 4. Reordenar columnas
    columnas_ordenadas = ['CODIGO', 'CATEGORIA', 'SUBCATEGORIA', 'NOMBRE', 'NOMBRE_COMERCIAL', 'CONCENTRACION', 'CONCENTRACION_NUM', 'CONCENTRACION_UNIDAD', 'DESCRIPCION', 'LABORATORIO', 'PRESENTACION', 'TIPO_ENVASE', 'CANTIDAD_ENVASE', 'UNIDAD_ENVASE', 'PRECIO_LISTA', 'PRECIO_OFERTA', 'PRECIO_FINAL', 'PRECIO_UNITARIO', 'FECHA', 'CLAVE_MATCHING']
    columnas_finales = [col for col in columnas_ordenadas if col in df.columns]
    df = df[columnas_finales]
    return df


def cargar_archivos_por_fecha(fecha=None):
    """Carga archivos Excel de Inkafarma por fecha"""
    todos_los_datos = []
    if not input_base.exists():
        print(f"ERROR: No existe la carpeta {input_base}")
        return None
    if fecha:
        ruta_fecha = input_base / fecha
        if not ruta_fecha.exists():
            print(f"ERROR: No existe la carpeta de fecha {fecha}")
            print(f"Ruta esperada: {ruta_fecha}")
            return None
        archivos = list(ruta_fecha.glob("*.xlsx")) + list(ruta_fecha.glob("*.xls"))
        if not archivos:
            print(f"No se encontraron archivos Excel en {ruta_fecha}")
            return None
        for archivo in archivos:
            try:
                df = pd.read_excel(archivo)
                df['ARCHIVO_ORIGEN'] = archivo.name
                df['FECHA_PROCESAMIENTO'] = fecha
                todos_los_datos.append(df)
            except Exception as e:
                print(f"Error al leer {archivo.name}: {e}")
    else:
        carpetas_fecha = [d for d in input_base.iterdir() if d.is_dir()]
        if not carpetas_fecha:
            print(f"No se encontraron carpetas de fechas en {input_base}")
            return None
        for carpeta_fecha in sorted(carpetas_fecha):
            archivos = list(carpeta_fecha.glob("*.xlsx")) + list(carpeta_fecha.glob("*.xls"))
            for archivo in archivos:
                try:
                    df = pd.read_excel(archivo)
                    df['ARCHIVO_ORIGEN'] = archivo.name
                    df['FECHA_PROCESAMIENTO'] = carpeta_fecha.name
                    todos_los_datos.append(df)
                except Exception as e:
                    print(f"Error al leer {archivo.name}: {e}")
    if not todos_los_datos:
        print("No se pudieron cargar datos")
        return None
    df_completo = pd.concat(todos_los_datos, ignore_index=True)
    return df_completo


# MAIN

def main():
    print("LIMPIEZA DE DATOS - Inkafarma")
    if FECHA_ESPECIFICA:
        print(f"Procesando fecha: {FECHA_ESPECIFICA}")
    else:
        print(f"Procesando TODAS las fechas disponibles")
    
    df = cargar_archivos_por_fecha(fecha=FECHA_ESPECIFICA)
    if df is None:
        return
    
    df_limpio = limpiar_dataframe(df)
    
    print(f"\nProductos procesados: {len(df_limpio)}")
    print(f"Con nombre comercial: {df_limpio['NOMBRE_COMERCIAL'].notna().sum()}")
    print(f"Con concentración: {df_limpio['CONCENTRACION'].notna().sum()}")
    print(f"Con descripción: {df_limpio['DESCRIPCION'].notna().sum()}")
    print(f"Con presentación extraída: {df_limpio['CANTIDAD_ENVASE'].notna().sum()}")
    print(f"Con precio unitario: {df_limpio['PRECIO_UNITARIO'].notna().sum()}")
    
    if FECHA_ESPECIFICA:
        output_path = output_base / FECHA_ESPECIFICA
        fecha_str = FECHA_ESPECIFICA
    else:
        output_path = output_base / 'consolidado'
        fecha_str = 'todas'

    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / f'inkafarma_limpio_{fecha_str}.xlsx'
    df_limpio.to_excel(output_file, index=False)
    print(f"\nArchivo guardado")
    print("PROCESO COMPLETADO")


if __name__ == "__main__":
    main()