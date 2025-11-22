# -*- coding: utf-8 -*-
"""
TAILOY — Limpieza y filtro 
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple

import pandas as pd
from zipfile import BadZipFile

# =============================================================================
# CONFIGURACIÓN — SOLO EDITA ESTAS 3 RUTAS SI ES NECESARIO
# =============================================================================
RAW_ROOT: Path = Path("data/raw/tailoy")
OUT_ROOT: Path = Path("data/processed/tailoy")
OUT_CONSOLIDADO: Path = Path("data/consolidated")

# Patrón de nombre de archivo dentro de cada carpeta de fecha
RAW_FILE_PATTERN: str = "tailoy_consolidado*.xlsx"

# =============================================================================
# LISTA PERMITIDA (ALLOWED)
# =============================================================================
ALLOWED: Dict[str, Dict[str, Set[str]]] = {
    "escolar": {
        "cintas-y-pegamentos": {
            "gomas-siliconas-y-colas",
            "limpiatipos",
        },
        "cuadernos-y-blocks": {
            "cuadernos-cosidos",
            "cuadernos-grapados-a4",
        },
        "forros-y-etiquetas": {"forros"},
        "mochilas-cartucheras-y-loncheras": {
            "cartucheras",
            "loncheras",
            "maletas-y-mochilas",
        },
        "papeleria": {"cartulinas", "papel-bond"},
        "utiles": {
            "borradores",
            "colores",
            "compases",
            "correctores",
            "crayones-y-oleos",
            "escuadras",
            "lapiceros",
            "lapices",
            "plastilinas",
            "plumones-para-papel",
            "reglas",
            "resaltadores",
            "tajadores",
            "tijeras",
        },
    },
    "oficina": {
        "utiles": {
            "borradores",
            "correctores",
            "lapiceros",
            "lapices",
            "plumones-para-papel",
            "resaltadores",
            "tajadores",
        }
    },
    "universitario": {
        "cartucheras-mochilas-y-loncheras": {
            "cartucheras",
            "loncheras",
        },
        "cintas-y-pegamentos": {
            "gomas-siliconas-y-colas",
            "limpiatipos",
        },
        "otros": {
            "calculadoras",
            "limpiatipos",
        },
        "papeleria": {"papel-bond"},
        "utiles": {
            "borradores",
            "correctores",
            "lapiceros",
            "lapices",
            "plumones-para-papel",
            "resaltadores",
            "tajadores",
            "tijeras",
        },
    },
}

# =============================================================================
# POLÍTICAS DE UNIDADES BASE
# =============================================================================
PAPER_PRODS: Set[str] = {
    "papel-bond",
    "papel-copia",
    "papel-fotocopia",
    "papelografos",
    "papel-periodico",
}
IGNORE_HOJAS_FOR: Set[str] = {
    "blocks-de-escritura-y-libretas",
    "blocks-de-manualidades",
    "cuadernos-cosidos",
    "cuadernos-grapados-a4",
    "cuadernos-grapados-a5",
    "libretas",
    "sketchbooks",
}
LIQUID_PRODS: Set[str] = {
    "gomas-siliconas-y-colas",
    "temperas-y-acuarelas-escolares",
    "correctores",
}

# Familias donde “N colores” cuenta como N unidades (incluye lapiceros)
COLOR_UNITS_PRODS: Set[str] = {
    "plumones-para-pizarra",
    "plumones-para-papel",
    "plumones-indelebles",
    "resaltadores",
    "colores",
    "crayones-y-oleos",
    "lapiceros",
}

# =============================================================================
# UTILIDADES DE TEXTO / NORMALIZACIÓN
# =============================================================================
def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def _norm_token(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = s.replace("&", " y ").replace("/", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "-")
    s = _strip_accents(s)
    s = re.sub(r"-{2,}", "-", s)
    return s


def _resolve_column(df: pd.DataFrame, preferred: str, fallbacks: List[str]) -> str:
    if preferred in df.columns:
        return preferred
    for fb in fallbacks:
        if fb in df.columns:
            return fb
    raise KeyError(
        f"No se encontró la columna requerida '{preferred}'. Disponibles: {list(df.columns)}"
    )


def _is_allowed_triplet(cat: str, sub: str, prod: str) -> bool:
    return (cat in ALLOWED) and (sub in ALLOWED[cat]) and (prod in ALLOWED[cat][sub])


def _clean_text_for_parse(x: object) -> str:
    s = str(x or "")
    s = s.replace("\xa0", " ").replace("×", "x")
    return s


def _norm_text_simple(x: object) -> str:
    s = _strip_accents(str(x or "")).lower()
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# =============================================================================
# REGEX
# =============================================================================
PACK_WORD_RE = re.compile(
    r"\b(pack|paquete|combo|blister|set|kit|juego|caja|tubo|maletin|malet[ií]n|estuche|megapack)\b",
    re.IGNORECASE,
)
X_UND_RE = re.compile(
    r"[x×]\s*(\d{1,4})\s*(?:unidades?|unds?|uds?|u\b|pcs?|piezas?)\b",
    re.IGNORECASE,
)
NUM_UND_RE = re.compile(
    r"\b(\d{1,4})\s*(?:unidades?|unds?|uds?|u\b|pcs?|piezas?)\b",
    re.IGNORECASE,
)
HOJAS_RE = re.compile(r"\b(\d{1,5})\s*hojas?\b", re.IGNORECASE)

VOL_ML_RE = re.compile(r"\b(\d+(?:[.,]\d{1,3})?)\s*(ml|mL|cc|mililitros?)\b", re.IGNORECASE)
VOL_L_RE = re.compile(
    r"\b(\d+(?:[.,]\d{1,3})?)\s*(?:l\b|lt\b|litro?s?)\b",
    re.IGNORECASE,
)
VOL_OZ_RE = re.compile(
    r"\b(\d+(?:[.,]\d{1,3})?)\s*(oz|onza?s?)\b",
    re.IGNORECASE,
)
PACK_ML_RE = re.compile(
    r"\b(\d{1,4})\s*[x×]\s*(\d+(?:[.,]\d{1,3})?)\s*(ml|mL|cc|l|lt|litro?s?|oz|onza?s?)\b",
    re.IGNORECASE,
)

MASS_G_RE = re.compile(r"\b(\d+(?:[.,]\d{1,3})?)\s*(g|gr|gramos?)\b", re.IGNORECASE)
MASS_KG_RE = re.compile(
    r"\b(\d+(?:[.,]\d{1,3})?)\s*(kg|kilos?|kilogramos?)\b",
    re.IGNORECASE,
)
PACK_G_RE = re.compile(
    r"\b(\d{1,4})\s*[x×]\s*(\d+(?:[.,]\d{1,3})?)\s*(g|gr|gramos?|kg|kilos?|kilogramos?)\b",
    re.IGNORECASE,
)

X_COLORES_CAP_RE = re.compile(r"[x×]\s*(\d{1,4})\s*col(?:ores|\.?)\b", re.IGNORECASE)
NUM_COLORES_CAP_RE = re.compile(r"\b(\d{1,4})\s*col(?:ores|\.?)\b", re.IGNORECASE)
COLOR_SING_RE = re.compile(r"\bcolor\b", re.IGNORECASE)

REGLA_CM_RE = re.compile(r"\b(\d{1,3})\s*cm\b", re.IGNORECASE)

# =============================================================================
# PRECIOS (ROBUSTO)
# =============================================================================
def parse_price_to_float(val: object) -> Optional[float]:
    if val is None:
        return None
    s = str(val)
    s = s.replace("S/.", " ").replace("S/ ", " ").replace("S/", " ").replace("S ", " ").strip()
    s = re.sub(r"[^0-9\.,]", "", s)
    if not s:
        return None

    has_dot = "." in s
    has_com = "," in s

    try:
        if has_dot and has_com:
            last_dot = s.rfind(".")
            last_com = s.rfind(",")
            if last_com > last_dot:
                s_norm = s.replace(".", "").replace(",", ".")
            else:
                s_norm = s.replace(",", "")
            return float(s_norm)
        elif has_com and not has_dot:
            m = re.search(r",(\d+)$", s)
            tail = m.group(1) if m else ""
            if m and len(tail) <= 2:
                s_norm = s.replace(",", ".")
            else:
                s_norm = s.replace(",", "")
            return float(s_norm)
        else:
            return float(s)
    except ValueError:
        return None


# =============================================================================
# HELPERS
# =============================================================================
def _allow_colors_as_units(prod_slug: str) -> bool:
    return prod_slug in COLOR_UNITS_PRODS


def _oz_to_ml(x: float) -> float:
    return x * 29.5735


# =============================================================================
# DETECCIÓN PACKS / VOLUMEN / MASA / COLORES
# =============================================================================
def detect_color_units(text: str, prod_slug: str) -> Optional[int]:
    if not text or not _allow_colors_as_units(prod_slug):
        return None
    m = X_COLORES_CAP_RE.search(text)
    if m:
        return int(m.group(1))
    m2 = NUM_COLORES_CAP_RE.search(text)
    if m2:
        return int(m2.group(1))
    return None


def detect_pack_units(text: str, prod_slug: str, *, strict_und: bool = False) -> Optional[int]:
    """Si strict_und=True, NO acepta 'pack N' sin unidad."""
    if not text:
        return None

    # 1) Primero intento conteo por colores (si aplica a esa familia)
    cu = detect_color_units(text, prod_slug)
    if cu:
        return cu

    # 2) X N und / N und / N unidades / N piezas
    m_xu = X_UND_RE.search(text)
    if m_xu:
        return int(m_xu.group(1))

    m_numu = NUM_UND_RE.search(text)
    if m_numu:
        return int(m_numu.group(1))

    # 3) Si se permite, detectar 'pack N', 'set de N', etc. aunque no diga 'und'
    if not strict_und and PACK_WORD_RE.search(text):
        m_next = re.search(
            r"\b(pack|paquete|combo|blister|set|kit|juego|caja|tubo|megapack)\b[^0-9]{0,10}(\d{1,4})",
            text,
            re.IGNORECASE,
        )
        if m_next:
            return int(m_next.group(2))

    return None


def detect_volume_ml(text: str, prod_slug: str) -> Optional[float]:
    if not text or prod_slug not in LIQUID_PRODS:
        return None
    t = text
    m_ml = VOL_ML_RE.search(t)
    if m_ml:
        return float(m_ml.group(1).replace(",", "."))
    m_l = VOL_L_RE.search(t)
    if m_l:
        return float(m_l.group(1).replace(",", ".")) * 1000.0
    m_oz = VOL_OZ_RE.search(t)
    if m_oz:
        return _oz_to_ml(float(m_oz.group(1).replace(",", ".")))
    return None


def detect_pack_ml(text: str, prod_slug: str) -> Optional[Tuple[int, float]]:
    if not text or prod_slug not in LIQUID_PRODS:
        return None
    m = PACK_ML_RE.search(text)
    if not m:
        return None
    u = int(m.group(1))
    v = float(m.group(2).replace(",", "."))
    unit = m.group(3).lower()
    if unit.startswith("l"):
        v *= 1000.0
    if unit.startswith("oz") or unit.startswith("onza"):
        v = _oz_to_ml(v)
    return (u, v)


def detect_mass_g(text: str) -> Optional[float]:
    m_g = MASS_G_RE.search(text)
    if m_g:
        return float(m_g.group(1).replace(",", "."))
    m_kg = MASS_KG_RE.search(text)
    if m_kg:
        return float(m_kg.group(1).replace(",", ".")) * 1000.0
    return None


def detect_pack_g(text: str) -> Optional[Tuple[int, float]]:
    m = PACK_G_RE.search(text)
    if not m:
        return None
    u = int(m.group(1))
    v = float(m.group(2).replace(",", "."))
    unit = m.group(3).lower()
    if unit.startswith("kg"):
        v *= 1000.0
    return (u, v)


# =============================================================================
# FILTROS ESPECÍFICOS POR PRODUCTO (PASO 6)
# =============================================================================
def family_row_filter(prod_slug: str, nombre_norm: str) -> bool:
    # Borradores: eliminar filas cuyo nombre tenga "tajador"
    if prod_slug == "borradores":
        if "tajador" in nombre_norm:
            return False
        return True

    # Cartulinas
    if prod_slug == "cartulinas":
        if "cartulina" not in nombre_norm:
            return False
        if re.search(r"\b(\d+)\s*(g|gr|gramos?|kg|kilos?)\b", nombre_norm):
            return False
        return True

    # Cintas-adhesivas-y-masking-tape
    if prod_slug == "cintas-adhesivas-y-masking-tape":
        return True

    # Colores
    if prod_slug == "colores":
        embalaje = re.search(
            r"\b(maletin|malet[ií]n|estuche|kit|set|caja|tubo)\b", nombre_norm
        )
        conteo_unidades = (
            X_UND_RE.search(nombre_norm)
            or NUM_UND_RE.search(nombre_norm)
            or re.search(
                r"\b(\d{1,4})\s*(lapices|l[aá]pices|unidades?)\b", nombre_norm
            )
        )
        if embalaje and not conteo_unidades:
            return False

        m_units = re.search(
            r"\b(\d{1,4})\s*(lapices|l[aá]pices|unidades?|unds?|uds?|u|pcs?|piezas)\b",
            nombre_norm,
        )
        m_colors = re.search(r"\b(\d{1,4})\s*colores?\b", nombre_norm)
        # Doble conteo piezas vs colores → excluir por ambiguo
        if m_units and m_colors:
            return False
        return True

    # Compases
    if prod_slug == "compases":
        if re.search(r"\bpiezas?\b", nombre_norm) or re.search(r"\bset\b", nombre_norm):
            return False
        return True

    # Cuadernos-anillados
    if prod_slug == "cuadernos-anillados":
        if re.search(r"\ba5\b", nombre_norm):
            return False
        return True

    # Escuadras
    if prod_slug == "escuadras":
        if re.search(
            r"\b(transportador|curv[ií]grafo?s?|escal[ií]metro|trazador|plantilla|c[ií]rculos?)\b",
            nombre_norm,
        ):
            return False
        if "juego de escuadras" not in nombre_norm:
            return False
        ok_len = (
            re.search(r"\b20\s*cm\b", nombre_norm)
            or re.search(r"\b30\s*cm\b", nombre_norm)
            or re.search(r"\bflexible?s?\b", nombre_norm)
        )
        return bool(ok_len)

    # Forros
    if prod_slug == "forros":
        return "forro" in nombre_norm

    # Gomas-siliconas-y-colas
    if prod_slug == "gomas-siliconas-y-colas":
        if "pistola" in nombre_norm:
            return False
        return True

    # Lapiceros
    if prod_slug == "lapiceros":
        if re.search(r"\bl[aá]piz\b", nombre_norm) or "lapiz" in nombre_norm:
            return False
        if re.search(r"\bcolores?\b", nombre_norm) or re.search(r"\bcolor\b", nombre_norm):
            return False
        if "bicolor" in nombre_norm or re.search(r"\bhb\b", nombre_norm) or "grafito" in nombre_norm:
            return False
        return True

    # Lapices
    if prod_slug == "lapices":
        if "corrector" in nombre_norm:
            return False
        return True

    # ---- REGLA CORREGIDA PARA MALETAS-Y-MOCHILAS ----
    if prod_slug == "maletas-y-mochilas":
        has_mochila = "mochila" in nombre_norm
        has_lonchera = "lonchera" in nombre_norm
        has_cartuch = "cartuchera" in nombre_norm
        tipos_mlm = sum([has_mochila, has_lonchera, has_cartuch])

        # a) Mezcla de tipos (mochila/lonchera/cartuchera): excluir
        if tipos_mlm >= 2:
            return False

        # b) Pack/combo/set/kit/juego o conteos 'x N'/'N und' + menciona alguno de esos tipos: excluir
        has_pack_word = bool(PACK_WORD_RE.search(nombre_norm))
        has_qty_pack = bool(X_UND_RE.search(nombre_norm) or NUM_UND_RE.search(nombre_norm))
        if (has_pack_word or has_qty_pack) and (has_mochila or has_lonchera or has_cartuch):
            return False

        # c) Caso simple: conservar
        return True

    # Papel bond
    if prod_slug == "papel-bond":
        if not (re.search(r"\ba4\b", nombre_norm) or re.search(r"\ba5\b", nombre_norm)):
            return False
        if re.search(
            r"\b(a3|oficio|carta|legal|plotter|papel[oó]grafo|rollo|pliego)\b", nombre_norm
        ):
            return False
        if re.search(r"\b(a4\s*/\s*a3|a3\s*/\s*a4)\b", nombre_norm):
            return False
        return True

    # Plastilinas
    if prod_slug == "plastilinas":
        if re.search(r"\bplum[oó]n(?:es)?\b", nombre_norm):
            return False
        return bool(X_UND_RE.search(nombre_norm) or NUM_UND_RE.search(nombre_norm))

    # Plumones-para-papel
    if prod_slug == "plumones-para-papel":
        if not re.search(r"\bplum[oó]n(?:es)?\b", nombre_norm):
            return False
        if re.search(r"\brotulador(es)?\b", nombre_norm) or re.search(
            r"\bresaltador(es)?\b", nombre_norm
        ):
            return False
        if re.search(r"\bcolores?\b", nombre_norm) or re.search(r"\bl[aá]pice?s\b", nombre_norm):
            return False
        return True

    # Portaminas y minas
    if prod_slug == "portaminas-y-minas":
        return bool(
            re.search(r"\bportaminas\b", nombre_norm) or re.search(r"\bminas\b", nombre_norm)
        )

    # Reglas
    if prod_slug == "reglas":
        if "regla" not in nombre_norm:
            return False
        if not REGLA_CM_RE.search(nombre_norm):
            return False
        if re.search(
            r"\b(transportador|plantilla|curv[ií]grafos?|escal[ií]metro|trazador\s+de\s+c[íi]rculos|mapa)\b",
            nombre_norm,
        ):
            return False
        return True

    # Resaltadores
    if prod_slug == "resaltadores":
        if "stabilo" in nombre_norm:
            return False
        return "resaltador" in nombre_norm

    # Tajadores
    if prod_slug == "tajadores":
        if re.search(r"\ba5\b", nombre_norm):
            return False
        if re.search(r"\bkum\b", nombre_norm) or "smiling planet" in nombre_norm:
            return False
        if "hipopotamo" in nombre_norm or "hipopótamo" in nombre_norm:
            return False
        if "mariquita" in nombre_norm:
            return False
        return True

    # Por defecto, si no hay regla especial, se conserva
    return True


# =============================================================================
# UNIDAD ESTÁNDAR + NORMALIZACIÓN
# =============================================================================
def choose_unidad_estandar(prod_slug: str, nombre: str, qty_info: dict) -> str:
    txt = _norm_text_simple(nombre)
    has_ml = qty_info.get("has_ml", False)
    has_g = qty_info.get("has_g", False)
    is_sil_barra = bool(
        re.search(r"\bsilicona\s+en\s+barra\b", txt)
        or re.search(r"\bstick\s+de\s+silicona\b", txt)
    )
    is_goma_barra = bool(
        re.search(r"\b(goma|pegamento)\s+en\s+barra\b", txt) or re.search(r"\bstick\b", txt)
    )
    is_grapas = bool(re.search(r"\bgrapas\b", txt))

    # Casos específicos
    if prod_slug == "reglas":
        return "20cm"

    if prod_slug == "engrapadores-sacagrapas-y-grapas" and is_grapas:
        return "1000und"

    if prod_slug == "cintas-adhesivas-y-masking-tape":
        return "und"

    if prod_slug == "forros":
        return "und"

    # Papel
    if prod_slug in PAPER_PRODS:
        return "hoja" if HOJAS_RE.search(nombre) else "und"

    # Correctores (líquidos)
    if prod_slug == "correctores" and has_ml:
        return "10ml"

    # Gomas/siliconas/colas
    if prod_slug == "gomas-siliconas-y-colas":
        if is_sil_barra:
            return "und"
        if has_ml:
            return "100ml"
        if is_goma_barra and has_g:
            return "10g"
        if has_g:
            return "100g"
        return "und"

    # Cualquier producto que tenga volumen detectado: default a 100ml
    if (
        detect_pack_ml(_clean_text_for_parse(nombre), prod_slug)
        or detect_volume_ml(_clean_text_for_parse(nombre), prod_slug)
    ):
        return "100ml"

    # Default
    return "und"


def _tipo_portaminas_minas(nombre_norm: str) -> Optional[str]:
    is_porta = bool(re.search(r"\bportaminas\b", nombre_norm))
    is_minas = bool(re.search(r"\bminas\b", nombre_norm))
    is_kit_words = bool(
        re.search(r"\bkit\b", nombre_norm)
        or re.search(r"\b(con|incluye)\s+(minas|repuestos|borrador(?:es)?)\b", nombre_norm)
    )

    if is_porta and is_minas and is_kit_words:
        return "KIT"
    if is_minas and not is_porta:
        return "MINAS"
    if is_porta and not is_minas:
        return "PORTAMINAS"
    if is_porta and is_minas:
        return "KIT" if is_kit_words else "PORTAMINAS"
    return None


def build_quantity_fields(
    prod_slug: str, nombre: str
) -> Tuple[float, str, int, float, str, str, dict, Optional[str]]:
    txt_raw = (nombre or "").strip()
    txt = _clean_text_for_parse(txt_raw)
    tnorm = _norm_text_simple(txt)
    aux = {"regla_cm": None, "has_ml": False, "has_g": False, "cuad_hojas": 0}
    tipo_producto: Optional[str] = None

    # Escuadras: por diseño, 1 und
    if prod_slug == "escuadras":
        return 1.0, "und", 1, 1.0, "Unidad", "UND", aux, None

    # Papel (no ignorado)
    if prod_slug not in IGNORE_HOJAS_FOR and prod_slug in PAPER_PRODS:
        m_h = HOJAS_RE.search(txt)
        if m_h:
            hojas = int(m_h.group(1))
            return (
                float(hojas),
                "hojas",
                1,
                float(hojas),
                f"X {hojas} hojas",
                "HOJAS",
                aux,
                None,
            )

    # Reglas: extraer largo y posible pack
    if prod_slug == "reglas":
        mcm = REGLA_CM_RE.search(txt)
        if mcm:
            largo = float(mcm.group(1))
            aux["regla_cm"] = largo
            units = detect_pack_units(txt, prod_slug) or 1
            pres = "Unidad" if units == 1 else f"X {int(units)} und"
            return float(units), "und", 1, float(units), pres, "REGLA", aux, None

    # Packs de líquidos (ml)
    pm = detect_pack_ml(txt, prod_slug)
    if pm:
        u, v_ml = pm
        aux["has_ml"] = True
        pres = f"{u} x {v_ml:g} ml" if u > 1 else f"{v_ml:g} ml"
        return float(v_ml), "ml", int(u), float(u * v_ml), pres, "ML", aux, None

    # Volumen simple (ml, L, oz)
    vml = detect_volume_ml(txt, prod_slug)
    if vml is not None:
        units = detect_pack_units(txt, prod_slug) or 1
        aux["has_ml"] = True
        pres = f"{units} x {vml:g} ml" if units > 1 else f"{vml:g} ml"
        return float(vml), "ml", int(units), float(units * vml), pres, "ML", aux, None

    # Gomas/siliconas/colas por peso
    if prod_slug == "gomas-siliconas-y-colas":
        pg = detect_pack_g(txt)
        if pg:
            u, v_g = pg
            aux["has_g"] = True
            pres = f"{u} x {v_g:g} g" if u > 1 else f"{v_g:g} g"
            return float(v_g), "g", int(u), float(u * v_g), pres, "G", aux, None
        vg = detect_mass_g(txt)
        if vg is not None:
            units = detect_pack_units(txt, prod_slug) or 1
            aux["has_g"] = True
            pres = f"{units} x {vg:g} g" if units > 1 else f"{vg:g} g"
            return float(vg), "g", int(units), float(units * vg), pres, "G", aux, None
        # Silicona en barra
        if re.search(r"\bsilicona\s+en\s+barra\b", tnorm) or re.search(
            r"\bstick\s+de\siliciona\b", tnorm
        ):
            units = detect_pack_units(txt, prod_slug) or 1
            pres = "Unidad" if units == 1 else f"X {int(units)} und"
            return float(units), "und", 1, float(units), pres, "UND", aux, None

    # Portaminas y minas
    if prod_slug == "portaminas-y-minas":
        tipo_producto = _tipo_portaminas_minas(tnorm)
        units = detect_pack_units(txt, prod_slug) or 1
        pres = "Unidad" if units == 1 else f"X {int(units)} und"
        return float(units), "und", 1, float(units), pres, "UND", aux, tipo_producto

    # Plastilinas
    if prod_slug == "plastilinas":
        units = detect_pack_units(txt, prod_slug, strict_und=True)
        if units:
            pres = "Unidad" if units == 1 else f"X {int(units)} und"
            return float(units), "und", 1, float(units), pres, "UND", aux, None

    # Lápices
    if prod_slug == "lapices":
        units = detect_pack_units(txt, prod_slug) or 1
        pres = "Unidad" if units == 1 else f"X {int(units)} und"
        return float(units), "und", 1, float(units), pres, "UND", aux, None

    # Lapiceros con set de colores
    if prod_slug == "lapiceros":
        if COLOR_SING_RE.search(tnorm):
            pass
        cu = detect_color_units(tnorm, prod_slug)
        if cu:
            pres = f"Set {int(cu)} colores"
            return float(cu), "und", 1, float(cu), pres, "UND", aux, None

    # Packs normales (und)
    units = detect_pack_units(txt, prod_slug)
    if units:
        if _allow_colors_as_units(prod_slug) and (
            X_COLORES_CAP_RE.search(txt) or NUM_COLORES_CAP_RE.search(txt)
        ):
            pres = f"Set {int(units)} colores"
        else:
            pres = "Unidad" if int(units) == 1 else f"X {int(units)} und"
        return float(units), "und", 1, float(units), pres, "UND", aux, None

    # Default: 1 und
    return 1.0, "und", 1, 1.0, "Unidad", "UND", aux, None


def normalize_prices(
    precio_num: Optional[float],
    unidad_estandar: str,
    tipo_base: str,
    cantidad_total: float,
    aux: dict,
) -> Tuple[Optional[float], str]:
    if precio_num is None or precio_num <= 0:
        return (None, "Precio inválido")

    # Papel → por hoja
    if unidad_estandar == "hoja" and tipo_base in {"HOJAS"} and cantidad_total > 0:
        return (round(precio_num / cantidad_total, 6), "papel → por hoja")

    # Líquidos → por 100 ml
    if unidad_estandar == "100ml" and tipo_base == "ML" and cantidad_total > 0:
        return (round(precio_num / (cantidad_total / 100.0), 6), "líquidos → por 100 ml")

    # Corrector → por 10 ml
    if unidad_estandar == "10ml" and tipo_base == "ML" and cantidad_total > 0:
        return (round(precio_num / (cantidad_total / 10.0), 6), "corrector → por 10 ml")

    # Goma en barra → por 10 g
    if unidad_estandar == "10g" and tipo_base == "G" and cantidad_total > 0:
        return (round(precio_num / (cantidad_total / 10.0), 6), "goma en barra → por 10 g")

    # Adhesivo → por 100 g
    if unidad_estandar == "100g" and tipo_base == "G" and cantidad_total > 0:
        return (round(precio_num / (cantidad_total / 100.0), 6), "adhesivo → por 100 g")

    # Grapas → por 1000 und
    if unidad_estandar == "1000und" and cantidad_total > 0:
        return (
            round(precio_num * (1000.0 / cantidad_total), 6),
            "grapas → por 1000 und",
        )

    # Caso base: precio por unidad (o por cantidad_total)
    div = cantidad_total if cantidad_total > 0 else 1.0
    return (
        round(precio_num / div, 6),
        ("x und" if div > 1 else "Cantidad=1, sin conversión"),
    )


# =============================================================================
# DETECCIÓN DE MARCA
# =============================================================================
BRAND_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("Faber Castell", re.compile(r"\bfaber[\s-]?castell\b", re.IGNORECASE)),
    ("Artesco", re.compile(r"\bartesco\b", re.IGNORECASE)),
    ("Maped", re.compile(r"\bmaped\b", re.IGNORECASE)),
    ("Vinifan", re.compile(r"\bvinifan\b", re.IGNORECASE)),
    ("Crayola", re.compile(r"\bcrayola\b", re.IGNORECASE)),
    ("Staedtler", re.compile(r"\bstaedtler\b", re.IGNORECASE)),
    ("Pelikan", re.compile(r"\bpelikan\b", re.IGNORECASE)),
    ("BIC", re.compile(r"\bbic\b", re.IGNORECASE)),
    ("Pilot", re.compile(r"\bpilot\b", re.IGNORECASE)),
    ("Pentel", re.compile(r"\bpentel\b", re.IGNORECASE)),
    ("Stabilo", re.compile(r"\bstabilo\b", re.IGNORECASE)),
    ("Paper Mate", re.compile(r"\bpaper\s*mate\b|\bpapermate\b", re.IGNORECASE)),
    ("Sharpie", re.compile(r"\bsharpie\b", re.IGNORECASE)),
    ("Ove", re.compile(r"\bove\b", re.IGNORECASE)),
    ("Wex", re.compile(r"\bwex\b", re.IGNORECASE)),
    ("Deli", re.compile(r"\bdeli\b", re.IGNORECASE)),
    ("UHU", re.compile(r"\buhu\b", re.IGNORECASE)),
    ("Pritt", re.compile(r"\bpritt\b", re.IGNORECASE)),
    ("Kores", re.compile(r"\bkores\b", re.IGNORECASE)),
    ("Norma", re.compile(r"\bnorma\b", re.IGNORECASE)),
]


def extract_brand_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    t = _norm_text_simple(name)
    for canon, pat in BRAND_PATTERNS:
        if pat.search(t):
            return canon
    return None


def _brand_is_missing(x: object) -> bool:
    """True si la 'marca' está ausente (NaN, vacío, 'sin marca', 's/m'/'s-m')."""
    if x is None:
        return True
    s = _norm_text_simple(x)
    if s in {"", "nan", "none", "null"}:
        return True
    if s in {"sin marca", "s/m", "s-m"}:
        return True
    return False


# =============================================================================
# CLASIFICACIÓN DE PRODUCTO_ESPECIFICO
# =============================================================================
_MOCHILA_CHILD_KEYWORDS: List[str] = [
    "kids",
    "childrens",
    "childrens club",
    "nido",
    "mini morral",
    "mini mochila",
    "scool",
    "paw patrol",
    "marshall",
    "rubble",
    "skye",
    "liberty",
    "barbie",
    "frozen",
    "moana",
    "princesas",
    "disney",
    "stitch",
    "pokemon",
    "mandalorian",
    "bluey",
    "gabbys",
    "gabbys dollhouse",
    "dinosaurios",
    "unicornio",
    "sirena",
    "sirenita",
    "haditas",
    "galaxy",
    "arcoiris",
    "arco iris",
    "camiones",
    "cohetes",
    "bugs",
    "tiburones",
    "gradient",
    "glitter",
]

_MOCHILA_PRO_KEYWORDS: List[str] = [
    "laptop",
    "notebook",
    "case logic",
    "thule",
    "national geographic",
    "spinner",
    "subterra",
    "crossover",
    "enroute",
    "construct",
    "advantage",
    "propel",
    "notion",
    "austin",
]


def _contains_any(text: str, keywords: List[str]) -> bool:
    return any(kw in text for kw in keywords)


def classify_producto_especifico(
    cat_slug: str, sub_slug: str, prod_slug: str, nombre: str
) -> str:
    """
    Devuelve la subdivisión 'producto_especifico' según el nombre.
    - Cuadernos: cuaderno triple reglon / cuaderno doble reglon /
                 cuaderno cuadriculado / cuaderno rayado
    - Gomas-siliconas-y-colas: gomas / siliconas / colas
    - Maletas-y-mochilas: mochila niño / mochila escolar / mochila
    - Resto: igual a 'producto' (prod_slug)
    """
    nombre_norm = _norm_text_simple(nombre)
    prod = prod_slug  # valor por defecto: el mismo producto

    # --- CUADERNOS (cosidos / anillados / grapados A4) ---
    if prod_slug in {"cuadernos-cosidos", "cuadernos-anillados", "cuadernos-grapados-a4"}:
        # 1) Cuaderno triple renglón
        if re.search(r"triple\s+(raya|renglon)", nombre_norm) or re.search(
            r"\b3\s*(renglones?|rayas?)\b", nombre_norm
        ):
            return "cuaderno triple reglon"

        # 2) Cuaderno doble renglón
        if (
            "doble raya" in nombre_norm
            or "doblemax" in nombre_norm
            or re.search(r"\b2\s*(renglones?|rayas?)\b", nombre_norm)
            or (
                ("con sombra" in nombre_norm or "sin sombra" in nombre_norm)
                and "renglon" in nombre_norm
            )
        ):
            return "cuaderno doble reglon"

        # 3) Cuaderno cuadriculado
        if ("cuadriculado" in nombre_norm) or ("cuadrimax" in nombre_norm):
            return "cuaderno cuadriculado"

        # 4) Cuaderno rayado (incluye cuaderno de música)
        if ("rayado" in nombre_norm) or ("musica" in nombre_norm):
            return "cuaderno rayado"

        # 5) Fallback → cuaderno rayado
        return "cuaderno rayado"

    # --- GOMAS / SILICONAS / COLAS ---
    if prod_slug == "gomas-siliconas-y-colas":
        has_silicona = "silicona" in nombre_norm
        has_goma = "goma" in nombre_norm
        has_cola = "cola" in nombre_norm
        has_peg = bool(
            re.search(r"\bpegamento\b", nombre_norm)
            or re.search(r"\bpegatodo\b", nombre_norm)
            or re.search(r"\bpegapen\b", nombre_norm)
        )

        if has_silicona:
            return "siliconas"
        if has_cola or has_peg:
            return "colas"
        if has_goma:
            return "gomas"
        return prod

    # --- MALETAS Y MOCHILAS ---
    if prod_slug == "maletas-y-mochilas":
        child = _contains_any(nombre_norm, _MOCHILA_CHILD_KEYWORDS)
        pro = _contains_any(nombre_norm, _MOCHILA_PRO_KEYWORDS)

        has_tipo = any(
            kw in nombre_norm
            for kw in ["mochila", "morral", "maleta", "lonchera", "cartera", "neceser"]
        )

        if child:
            return "mochila niño"
        if pro:
            return "mochila"
        if has_tipo:
            return "mochila escolar"
        return "mochila"

    # --- RESTO: sin subdivisión especial ---
    return prod


# =============================================================================
# PIPELINE PARA UN SOLO ARCHIVO
# =============================================================================
def process_file(input_path: Path, output_dir: Path) -> Optional[pd.DataFrame]:
    """
    Aplica el pipeline de limpieza/normalización a un solo archivo Excel.
    Guarda la salida en `output_dir / (input_path.stem + "_clean_filt.xlsx")`.
    Además, devuelve el DataFrame final para poder consolidar.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # [1/11] Leer datos
    print(f"[1/11] Leyendo datos: {input_path.name}")
    try:
        df = pd.read_excel(input_path)
    except (BadZipFile, ValueError, OSError) as e:
        print(f"ERROR: No se pudo leer '{input_path.name}' como Excel ({type(e).__name__}): {e}")
        return None

    print(f"  Filas leídas: {len(df)}")

    # Columnas tolerantes
    col_categoria = _resolve_column(df, "categoria", ["Categoria", "CATEGORIA"])
    col_subcat = _resolve_column(
        df, "subcategoria", ["Subcategoria", "SUBCATEGORIA", "sub-categoria"]
    )
    col_productos = _resolve_column(
        df, "productos", ["producto", "Producto", "PRODUCTO", "Productos"]
    )
    col_codigo = _resolve_column(
        df, "codigo", ["Código", "CODIGO", "codigo_sku", "sku"]
    )
    col_nombre = _resolve_column(
        df,
        "nombre_producto",
        [
            "nombre",
            "Nombre",
            "producto_nombre",
            "ProductName",
            "name",
            "titulo",
            "titulo_producto",
            "descripcion",
        ],
    )
    col_precio = _resolve_column(
        df,
        "precio",
        ["Precio", "price", "precio_final", "precio_oferta", "precio_lista"],
    )
    col_marca = next((c for c in ["marca", "Marca", "brand", "Brand"] if c in df.columns), None)

    # [2/11] Sin 'codigo'
    print("\n[2/11] Eliminando filas sin 'codigo'...")
    mask_no_code = (
        df[col_codigo].isna()
        | (df[col_codigo].astype(str).str.strip() == "")
        | (
            df[col_codigo]
            .astype(str)
            .str.strip()
            .str.upper()
            .isin({"NAN", "NONE", "NULL"})
        )
    )
    removed_no_code = int(mask_no_code.sum())
    df = df.loc[~mask_no_code].copy()
    print(f"  Filas sin código eliminadas: {removed_no_code} | Restantes: {len(df)}")

    # [3/11] FILTRO ALLOWED
    print("\n[3/11] Aplicando filtro ALLOWED...")
    df["_cat"] = df[col_categoria].apply(_norm_token)
    df["_sub"] = df[col_subcat].apply(_norm_token)
    df["_prod"] = df[col_productos].apply(_norm_token)

    mask_allowed = df.apply(
        lambda r: _is_allowed_triplet(r["_cat"], r["_sub"], r["_prod"]), axis=1
    )
    kept_allowed = int(mask_allowed.sum())
    df = df.loc[mask_allowed].copy()
    print(
        f"  Filas conservadas (ALLOWED): {kept_allowed} | "
        f"Eliminadas (no permitidas): {len(mask_allowed) - kept_allowed}"
    )

    # [4/11] EXCLUSIONES genéricas
    print("\n[4/11] Excluyendo por nombre (témpera/transportador/marcador)...")
    excl_mask = df[col_nombre].apply(
        lambda x: bool(
            re.search(r"\btempera?s?\b", _norm_text_simple(x))
            or re.search(r"\btransportadores?\b", _norm_text_simple(x))
            or re.search(r"\bmarcadores?\b", _norm_text_simple(x))
        )
    )
    excluded_1 = int(excl_mask.sum())
    df = df.loc[~excl_mask].copy()
    print(f"  Filas excluidas por nombre: {excluded_1} | Restantes: {len(df)}")

    # [5/11] Excluir NOMBRE con '+'
    print("\n[5/11] Excluyendo filas con '+' en el nombre...")
    plus_mask = df[col_nombre].astype(str).str.contains(r"\+", regex=True, na=False)
    excluded_plus = int(plus_mask.sum())
    df = df.loc[~plus_mask].copy()
    print(f"  Filas excluidas por '+': {excluded_plus} | Restantes: {len(df)}")

    # [6/11] Filtros por producto (familia)
    print("\n[6/11] Aplicando filtros específicos por producto...")
    fam_mask = df.apply(
        lambda r: family_row_filter(
            _norm_token(r[col_productos]), _norm_text_simple(r[col_nombre])
        ),
        axis=1,
    )
    kept_fam = int(fam_mask.sum())
    df = df.loc[fam_mask].copy()
    print(
        f"  Filas conservadas por reglas de familia: {kept_fam} | "
        f"Eliminadas: {len(fam_mask) - kept_fam}"
    )

    # [7/11] Precio numérico (robusto)
    print("\n[7/11] Parseando precios (robusto)...")
    df["precio_num"] = df[col_precio].apply(parse_price_to_float)

    # [8/11] Cantidades + Normalización
    print("\n[8/11] Detectando cantidades y calculando valor estándar...")
    unidad_std_list: List[str] = []
    ce_list: List[float] = []
    ue_list: List[str] = []
    mp_list: List[int] = []
    ct_list: List[float] = []
    pres_list: List[str] = []
    precio_std_list: List[Optional[float]] = []
    nota_list: List[Optional[str]] = []

    for _, r in df.iterrows():
        prod_slug = r["_prod"]
        nombre = str(r.get(col_nombre, "") or "")
        precio_num = r.get("precio_num")

        ce, ue, mp, ct, pres, tipo_base, aux, tipo_prod = build_quantity_fields(
            prod_slug, nombre
        )
        unidad_std = choose_unidad_estandar(prod_slug, nombre, aux)
        precio_std, nota = normalize_prices(precio_num, unidad_std, tipo_base, ct, aux)

        # Limpieza de presentaciones triviales
        if pres.strip().lower() in {"x 1 und", "1 x 1 und", "1 und"}:
            pres = "Unidad"
        pres = re.sub(
            r"(x\s*1\s*und\s*){2,}", "Unidad", pres, flags=re.IGNORECASE
        )

        extra: List[str] = []
        if tipo_prod:
            extra.append(f"tipo_producto={tipo_prod}")
        if unidad_std == "1000und":
            extra.append("estandar=1000und")

        nota_full = (
            " | ".join(filter(None, [nota] + extra)) if nota else
            (" | ".join(extra) if extra else None)
        )

        unidad_std_list.append(unidad_std)
        ce_list.append(ce)
        ue_list.append(ue)
        mp_list.append(mp)
        ct_list.append(ct)
        pres_list.append(pres)
        precio_std_list.append(precio_std)
        nota_list.append(nota_full)

    # Resolver MARCA; si no hay columna, extraer desde nombre
    if col_marca:
        marca_series = df[col_marca]
    else:
        marca_series = df[col_nombre].apply(extract_brand_from_name)

    # Construcción parcial (incluye marca y precio_num; NO incluir 'precio')
    df_final = pd.DataFrame(
        {
            "categoria": df[col_categoria],
            "subcategoria": df[col_subcat],
            "producto": df[col_productos],
            "codigo": df[col_codigo],
            "nombre_producto": df[col_nombre],
            "marca": marca_series,
            "precio_num": df["precio_num"],
            "cantidad_extraida": ce_list,
            "unidad_extraida": ue_list,
            "multiplicador_paquete": mp_list,
            "cantidad_total": ct_list,
            "presentacion_detectada": pres_list,
            "unidad_estándar": unidad_std_list,
            "precio_por_unidad_estandar": precio_std_list,
            "nota": nota_list,
        }
    )

    # [9/11] producto_especifico + reglas extra + eliminar filas sin marca + excluir 'mochila'
    print("\n[9/11] Generando 'producto_especifico' y aplicando reglas adicionales...")

    df_final["producto_especifico"] = df_final.apply(
        lambda r: classify_producto_especifico(
            _norm_token(r["categoria"]),
            _norm_token(r["subcategoria"]),
            _norm_token(r["producto"]),
            str(r["nombre_producto"] or ""),
        ),
        axis=1,
    )

    # Normalizaciones auxiliares para reglas extra
    nombre_norm = df_final["nombre_producto"].apply(_norm_text_simple)
    prod_esp_norm = df_final["producto_especifico"].astype(str).str.strip().str.lower()
    if "marca" in df_final.columns:
        marca_norm = df_final["marca"].astype(str).str.strip().str.upper()
    else:
        marca_norm = pd.Series("", index=df_final.index)

    unidad_extra_norm = df_final["unidad_extraida"].astype(str).str.strip().str.lower()

    # Partimos conservando todo
    mask_keep = pd.Series(True, index=df_final.index)

    # colas — Galón en nombre_producto
    cond_colas_galon = (prod_esp_norm == "colas") & nombre_norm.str.contains("galon")
    mask_keep &= ~cond_colas_galon

    # colas — Debe contener 'cola' Y alguna referencia a 'g' o 'ml'
    cond_colas_no_cola = (prod_esp_norm == "colas") & ~nombre_norm.str.contains("cola")
    cond_colas_no_gml = (prod_esp_norm == "colas") & ~(
        nombre_norm.str.contains("g") | nombre_norm.str.contains("ml")
    )
    cond_colas_invalid = cond_colas_no_cola | cond_colas_no_gml
    mask_keep &= ~cond_colas_invalid

    # colas — unidad_extraida = 'und'
    cond_colas_und_unit = (prod_esp_norm == "colas") & (unidad_extra_norm == "und")
    mask_keep &= ~cond_colas_und_unit

    # colores — jumbo
    cond_colores_jumbo = (prod_esp_norm == "colores") & nombre_norm.str.contains("jumbo")
    mask_keep &= ~cond_colores_jumbo

    # colores — marca STABILO
    cond_colores_stabilo = (prod_esp_norm == "colores") & (marca_norm == "STABILO")
    mask_keep &= ~cond_colores_stabilo

    # compases — Profesional en nombre_producto
    cond_compases_prof = (prod_esp_norm == "compases") & nombre_norm.str.contains("profesional")
    mask_keep &= ~cond_compases_prof

    # compases — marca MAPED
    cond_compases_maped = (prod_esp_norm == "compases") & (marca_norm == "MAPED")
    mask_keep &= ~cond_compases_maped

    # crayones-y-oleos — pincel
    cond_cray_pincel = (prod_esp_norm == "crayones-y-oleos") & nombre_norm.str.contains("pincel")
    mask_keep &= ~cond_cray_pincel

    # cuaderno-rayado — "musica" en nombre_producto
    cond_cuad_musica = (prod_esp_norm == "cuaderno rayado") & nombre_norm.str.contains("musica")
    mask_keep &= ~cond_cuad_musica

    # escuadras — "45/60"
    cond_escu_4560 = (prod_esp_norm == "escuadras") & nombre_norm.str.contains("45/60")
    mask_keep &= ~cond_escu_4560

    # gomas — solo si hay "barra" en nombre_producto
    cond_gomas_sin_barra = (prod_esp_norm == "gomas") & ~nombre_norm.str.contains("barra")
    mask_keep &= ~cond_gomas_sin_barra

    # lapiceros — MOOVING
    cond_lapic_mooving = (prod_esp_norm == "lapiceros") & (marca_norm == "MOOVING")
    mask_keep &= ~cond_lapic_mooving

    # plastilinas — "premium"
    cond_plasti_premium = (prod_esp_norm == "plastilinas") & nombre_norm.str.contains("premium")
    mask_keep &= ~cond_plasti_premium

    # plumones-para-papel — eliminar si NO hay "und" en nombre_producto
    cond_plum_sin_und = (prod_esp_norm == "plumones-para-papel") & ~nombre_norm.str.contains("und")
    mask_keep &= ~cond_plum_sin_und

    # resaltadores — "plumon"
    cond_resalt_plumon = (prod_esp_norm == "resaltadores") & nombre_norm.str.contains("plumon")
    mask_keep &= ~cond_resalt_plumon

    # siliconas — eliminar todo
    cond_siliconas = (prod_esp_norm == "siliconas")
    mask_keep &= ~cond_siliconas

    # tijeras — "multiproposito"
    cond_tij_multi = (prod_esp_norm == "tijeras") & nombre_norm.str.contains("multiproposito")
    mask_keep &= ~cond_tij_multi

    # forros — "bolsa" en nombre_producto
    cond_forros_bolsa = (prod_esp_norm == "forros") & nombre_norm.str.contains("bolsa")
    mask_keep &= ~cond_forros_bolsa

    # Necesario para filtro premium de lapiceros (precio alto)
    precio_std_num = pd.to_numeric(
        df_final["precio_por_unidad_estandar"], errors="coerce"
    )

    # lapiceros — filtro económico/básico (premium por nombre o precio alto)
    cond_lapic_premium_name = (prod_esp_norm == "lapiceros") & nombre_norm.str.contains(
        r"frixion|pop\s+lol|rollerball|hi\s*tecpoint|g2",
        regex=True,
    )
    cond_lapic_premium_price = (
        (prod_esp_norm == "lapiceros")
        & precio_std_num.notna()
        & (precio_std_num > 7.5)
    )
    cond_lapic_premium = cond_lapic_premium_name | cond_lapic_premium_price
    mask_keep &= ~cond_lapic_premium

    # Aplicar todas las reglas extra
    removed_extra = int((~mask_keep).sum())
    df_final = df_final.loc[mask_keep].copy()
    print(
        f"  Filas removidas por reglas extra de producto_especifico: {removed_extra} | "
        f"Restantes: {len(df_final)}"
    )

    # Eliminar filas sin marca
    mask_missing_brand = df_final["marca"].apply(_brand_is_missing)
    removed_brandless = int(mask_missing_brand.sum())
    df_final = df_final.loc[~mask_missing_brand].copy()
    print(
        f"  Filas removidas por falta de marca: {removed_brandless} | Restantes: {len(df_final)}"
    )

    # Eliminar filas con producto_especifico == 'mochila'
    pe_norm = df_final["producto_especifico"].astype(str).str.strip().str.lower()
    mask_mochila = pe_norm.eq("mochila")
    removed_mochila = int(mask_mochila.sum())
    df_final = df_final.loc[~mask_mochila].copy()
    print(
        f"  Filas removidas producto_especifico='mochila': {removed_mochila} | Restantes: {len(df_final)}"
    )

    # [10/11] DEDUP por 'codigo' → conservar SOLO la PRIMERA aparición
    print("\n[10/11] Deduplicando por 'codigo' (conservar la primera)...")
    df_final["codigo_limpio"] = (
        df_final["codigo"].astype(str).str.strip().str.upper()
    )
    before = len(df_final)
    df_final = df_final.drop_duplicates(subset=["codigo_limpio"], keep="first").copy()
    after = len(df_final)
    print(f"  Duplicados removidos: {before - after} | Filas finales: {after}")

    # Reordenar columnas para que 'producto_especifico' quede junto a 'producto'
    cols_order = [
        "categoria",
        "subcategoria",
        "producto",
        "producto_especifico",
        "codigo",
        "nombre_producto",
        "marca",
        "precio_num",
        "cantidad_extraida",
        "unidad_extraida",
        "multiplicador_paquete",
        "cantidad_total",
        "presentacion_detectada",
        "unidad_estándar",
        "precio_por_unidad_estandar",
        "nota",
        "codigo_limpio",
    ]
    cols_order = [c for c in cols_order if c in df_final.columns]
    df_final = df_final[cols_order]

    # [11/11] Guardar (sin 'codigo_limpio')
    print("\n[11/11] Guardando archivo final...")
    out_path = output_dir / f"{input_path.stem}_clean_filt.xlsx"

    # Asegurar que la carpeta exista
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_final_sin_codigo = df_final.drop(columns=["codigo_limpio"], errors="ignore")
    df_final_sin_codigo.to_excel(out_path, index=False)

    print(f"Archivo final guardado: {out_path}")

    # Devuelvo el DF (con codigo_limpio, que ya no está en el Excel)
    return df_final_sin_codigo.copy()


# =============================================================================
# MAIN: RECORRE TODAS LAS CARPETAS DE FECHA
# =============================================================================
def main() -> None:
    print("=" * 80)
    print("PIPELINE TAILOY — LIMPIEZA + FILTRO (ALLOWED) — MULTI-FECHA")
    print("=" * 80)
    print(f"RAW_ROOT         : {RAW_ROOT}")
    print(f"OUT_ROOT         : {OUT_ROOT}")
    print(f"OUT_CONSOLIDADO  : {OUT_CONSOLIDADO}")
    print(f"PATRÓN           : {RAW_FILE_PATTERN}")
    print("-" * 80)

    if not RAW_ROOT.exists():
        print(f"ERROR: RAW_ROOT no existe: {RAW_ROOT}")
        return

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUT_CONSOLIDADO.mkdir(parents=True, exist_ok=True)

    date_dirs = sorted([p for p in RAW_ROOT.iterdir() if p.is_dir()])
    if not date_dirs:
        print("ADVERTENCIA: No se encontraron subcarpetas de fecha dentro de RAW_ROOT.")
        return

    total_files = 0
    processed_ok = 0
    consolidado_frames: List[pd.DataFrame] = []

    for date_dir in date_dirs:
        files = sorted(date_dir.glob(RAW_FILE_PATTERN))
        if not files:
            continue

        print("\n" + "=" * 80)
        print(f"Carpeta fecha: {date_dir.name}")
        print("=" * 80)

        output_dir = OUT_ROOT / date_dir.name
        output_dir.mkdir(parents=True, exist_ok=True)

        for input_path in files:
            total_files += 1
            print("\n" + "-" * 80)
            print(f"Procesando archivo: {input_path.name}")
            print("-" * 80)
            try:
                df_proc = process_file(input_path, output_dir)
                if df_proc is not None and not df_proc.empty:
                    df_proc = df_proc.copy()
                    df_proc["fecha_scraping"] = date_dir.name
                    consolidado_frames.append(df_proc)
                processed_ok += 1
            except Exception as e:
                print(f"ERROR procesando {input_path.name}: {e}")

    # Guardar archivo consolidado si hay algo
    if consolidado_frames:
        df_consol = pd.concat(consolidado_frames, ignore_index=True)
        consolidado_path = OUT_CONSOLIDADO / "consolidado_educacion.xlsx"
        df_consol.to_excel(consolidado_path, index=False)
        print("\n" + "=" * 80)
        print("ARCHIVO CONSOLIDADO GENERADO")
        print(f"  Ruta: {consolidado_path}")
        print("=" * 80)
    else:
        print("\nADVERTENCIA: No se generaron datos para el consolidado (sin filas procesadas).")

    print("\n" + "=" * 80)
    print("FIN PIPELINE TAILOY")
    print(f"Archivos encontrados : {total_files}")
    print(f"Archivos procesados  : {processed_ok}")
    print(f"Salida por fecha en  : {OUT_ROOT}")
    print(f"Consolidado en       : {OUT_CONSOLIDADO}")
    print("=" * 80)


if __name__ == "__main__":
    main()
