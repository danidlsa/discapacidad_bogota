"""
Módulo: transformaciones.py
---------------------------
Transformaciones (enriquecimiento + clasificación) para las capas descargadas.

Incluye:
- normalizar_texto: lowercase + sin acentos + espacios normalizados
- agregar_flags_discapacidad: crea columnas dicotómicas según keywords 
- clasificar_tematica: asigna temática por keywords con prioridad

"""

from __future__ import annotations

import re
import unicodedata
from typing import Sequence

import pandas as pd
from pathlib import Path


# ---------------------------------------------------------------------
# 1) Helper: normalización de texto 
# ---------------------------------------------------------------------

def normalizar_texto(valor) -> str:
    """
    Normaliza texto:
    - lowercase
    - quita tildes/diacríticos
    - colapsa espacios

    Maneja None/NaN devolviendo "".
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    s = str(valor).lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------------------------------------------------------------------
# 2) Enrich: flags de discapacidad 
# ---------------------------------------------------------------------

KW_DISCAPACIDAD = {

    "disc_visual": ["visual", "ceguera", "baja vision"],
    "disc_auditiva": ["auditiva", "sordera", "hipoacusia", "lengua de senas", "señas"],
    "disc_fisica": ["fisica", "motora", "movilidad", "paralisis", "amputacion"],
    "disc_intelectual": ["intelectual", "cognitiva", "aprendizaje"],
    "disc_psicosocial": ["psicosocial", "salud mental", "mental"],
    "disc_multiple": ["multiple", "múltiple"],
}

def agregar_flags_discapacidad(df: pd.DataFrame, columnas_texto: Sequence[str]) -> pd.DataFrame:
    """
    Crea flags dicotómicas (0/1) de discapacidad a partir de una o varias columnas de texto.

       """
    cols = [c for c in columnas_texto if c in df.columns]
    if not cols:
        return df

    # Texto combinado por fila
    texto = df[cols].astype(str).agg(" ".join, axis=1).map(normalizar_texto)

    for nombre_flag, lista_kw in KW_DISCAPACIDAD.items():
        patrones = [re.escape(normalizar_texto(k)) for k in lista_kw]
        if patrones:
            patron = re.compile("|".join(patrones))
            df[nombre_flag] = texto.map(lambda t: 1 if patron.search(t) else 0)
        else:
            df[nombre_flag] = 0

    return df


# ---------------------------------------------------------------------
# 3) Clasificación temática por keywords 
# ---------------------------------------------------------------------

KW_TEMATICA = {
    "Derechos humanos con enfoque diferencial": [
        "derechos humanos", "enfoque diferencial", "enfoque interseccional",
        "proteccion", "riesgo", "amenaza", "victimas", "trata de personas",
        "defensa de derechos", "acompanamiento juridico", "orientacion juridica",
        "medidas preventivas", "medidas de proteccion", "violencia",
        "reintegracion", "reincorporacion", "desmovilizacion",
        "fuerza publica", "danos directos", "danos indirectos", "lgbti", "lgbt"
    ],
    "Bienestar y autonomía": [
        "bienestar", "salud mental", "psicosocial",
        "dispositivos de asistencia", "asistencia personal", "ayudas tecnicas",
        "apoyos", "apoyo", "autonomia", "vida independiente"
    ],
    "Reconocimiento y apoyo a cuidadoras": [
        "cuidadoras", "cuidadores", "redes de cuidadoras", "redes de cuidado",
        "mujeres cuidadoras", "formacion de cuidadoras", "capacitacion para cuidadoras",
        "reconocimiento", "exaltacion",
        "trabajo de cuidado", "actividades de respiro", "respiro", "apoyo a cuidadoras"
    ],
} 

PRIORIDAD_TEMATICA = [
    "Reconocimiento y apoyo a cuidadoras",
    "Derechos humanos con enfoque diferencial",
    "Bienestar y autonomía",
]  

def _compilar_patrones_kw(diccionario_kw: dict) -> dict[str, re.Pattern]:
    """
    Compila los keywords en regex para búsqueda rápida.
    """
    patrones = {}
    for etiqueta, lista in diccionario_kw.items():
        partes = [re.escape(normalizar_texto(k)) for k in lista]
        patrones[etiqueta] = re.compile("|".join(partes)) if partes else re.compile(r"$^")
    return patrones


_PATRONES_TEMATICA = _compilar_patrones_kw(KW_TEMATICA)


def clasificar_tematica(
    texto,
    secretaria,
    *secretarias_objetivo,
    objetivo_por_defecto=("Gobierno",),
) -> str:
    """
    Si 'secretaria' está dentro de las secretarías objetivo:
        clasifica por keywords (KW_TEMATICA) siguiendo PRIORIDAD_TEMATICA.
    Si NO:
        devuelve el nombre de la secretaría como temática.
    """
    sec = str(secretaria) if secretaria is not None else ""
    sec_norm = normalizar_texto(sec)

    objetivos = secretarias_objetivo if secretarias_objetivo else objetivo_por_defecto
    objetivos_norm = {normalizar_texto(x) for x in objetivos}

    # si la secretaría no es objetivo, la temática es la propia secretaría
    if sec_norm and sec_norm not in objetivos_norm:
        return sec

    txt = normalizar_texto(texto)

    for etiqueta in PRIORIDAD_TEMATICA:
        patron = _PATRONES_TEMATICA.get(etiqueta)
        if patron and patron.search(txt):
            return etiqueta

    # default si no matchea nada
    return objetivos[0] if objetivos else ""


## Variables auxiliares para enriquecer capas específicas (SecGeneral y SDIS)


def ruta_apoyos(nombre_archivo: str) -> Path:
    """
    Asume que tus CSV de apoyo viven en:
    src/pipeline_datos/apoyos/<archivo>

    """
    return Path(__file__).resolve().parent / "apoyos" / nombre_archivo


def primero_no_vacio(serie: pd.Series):
    """Toma el primer valor no vacío/no NaN de una serie."""
    for v in serie:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if str(v).strip() == "":
            continue
        return v
    return pd.NA


def parsear_edades(valor):
    """
    Convierte 'Array de edades' a lista de enteros.
    Soporta strings con comas o espacios: '14, 15, 16' o '0 1 2 ...'
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return []
    nums = re.findall(r"\d+", str(valor))
    return [int(n) for n in nums]


def incorporar_extras_secgeneral(
    gdf,
    ruta_csv_extras: str | Path,
    col_llave_gdf: str = "ofer_nombr",
    col_llave_extras: str = "ofer_nombr",
):
    """
    Une a la capa de SecGeneral las variables:
    - tramo_edad_atendido
    - edades_array (string)
    - edades_lista (list[int]) opcional

    """
    extras = pd.read_csv(ruta_csv_extras, sep=";")

    # normalizamos llaves
    extras["_clave"] = extras[col_llave_extras].map(normalizar_texto)
    gdf["_clave"] = gdf[col_llave_gdf].map(normalizar_texto)

    # deduplicar: colapsar tomando el primer valor no vacío
    extras_agg = (
        extras.groupby("_clave", as_index=False)
        .agg({
            "Secretaría": primero_no_vacio,
            "Tramo de edad atendido": primero_no_vacio,
            "Array de edades": primero_no_vacio,
        })
    )

    # renombrar a snake_case
    extras_agg = extras_agg.rename(columns={
        "Tramo de edad atendido": "tramo_edad_atendido",
        "Array de edades": "edades_array",
        "Secretaría": "secretaria_extra",
    })

    out = gdf.merge(extras_agg, on="_clave", how="left")

    # opcional: lista de edades (útil para análisis)
    out["edades_lista"] = out["edades_array"].apply(parsear_edades)

    return out.drop(columns=["_clave"])


def incorporar_extras_sdis(
    gdf,
    ruta_csv_extras: str | Path,
    col_llave_gdf_preferida: str = "OSSUOpera",
):
    """
    Une a capas SDIS variables precalculadas:
    - tramo_edad_atendido
    - edades_array
    - tipo_discapacidad_extra
    - edades_lista (opcional)

    Estrategia de llave:
    - Si existe OSSUOpera en el gdf, unimos por OSSUOpera ↔ OSSUOpera (recomendado).  
    - Si NO existe OSSUOpera, intentamos OSSServic ↔ Modelo de operación (fallback). 
    """
    extras = pd.read_csv(ruta_csv_extras, sep=";")

    # elegimos llave del gdf
    if col_llave_gdf_preferida in gdf.columns:
        col_gdf = col_llave_gdf_preferida
        col_extras = "OSSUOpera"
    elif "OSSServic" in gdf.columns:
        col_gdf = "OSSServic"
        col_extras = "Modelo de operación"
    else:
        raise KeyError("No encuentro columna para unir SDIS. Esperaba 'OSSUOpera' o 'OSSServic' en el gdf.")

    extras["_clave"] = extras[col_extras].map(normalizar_texto)
    gdf["_clave"] = gdf[col_gdf].map(normalizar_texto)

    extras = extras.rename(columns={
        "Tramo de edad atendido": "tramo_edad_atendido",
        "Array de edades": "edades_array",
        "Tipo de discapacidad": "tipo_discapacidad_extra",
        "Secretaría": "secretaria_extra",
        "Descripcion": "descripcion_extra",  
    })

    cols_keep = [
        "_clave",
        "secretaria_extra",
        "descripcion_extra",       
        "tipo_discapacidad_extra",
        "tramo_edad_atendido",
        "edades_array",
    ]

# si hay duplicados por _clave, nos quedamos con el primero
    extras = extras[cols_keep].drop_duplicates("_clave")
    
    out = gdf.merge(extras, on="_clave", how="left")
    out["edades_lista"] = out["edades_array"].apply(parsear_edades)

    return out.drop(columns=["_clave"])


__all__ = [
    # helpers base
    "normalizar_texto",

    # enrich / classify
    "agregar_flags_discapacidad",
    "clasificar_tematica",

    # extras precalculados
    "incorporar_extras_sdis",
    "incorporar_extras_secgeneral",

    # diccionarios
    "KW_DISCAPACIDAD",
    "KW_TEMATICA",
    "PRIORIDAD_TEMATICA",
]