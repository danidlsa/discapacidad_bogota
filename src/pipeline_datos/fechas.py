"""
Módulo: fechas.py
-----------------
Actualiza la columna 'Fecha de actualización' de sourcelayers.csv con la
fecha de última actualización publicada por cada fuente.

Orden de consulta por capa:
1. Servicio ArcGIS REST: editingInfo.dataLastEditDate (o lastEditDate).
   Solo lo publican algunas capas (p. ej. FeatureServer de ArcGIS Online);
   los MapServer de SDIS y Catastro no lo exponen.
2. Portal Datos Abiertos Bogotá (CKAN): metadata_modified del dataset.
   Es la fecha de la ficha del catálogo, no necesariamente de los datos.
3. Si ninguna responde, se deja la fecha existente.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from .configuracion import COL_SOURCE_LAYER, COL_URL_SERVICIO_FEATURE
from .ingesta import extraer_primera_url


COL_FECHA = "Fecha de actualización"
COL_URL_DATOS_ABIERTOS = "URL Datos Abiertos de Bogotá"

API_CKAN = "https://datosabiertos.bogota.gov.co/api/3/action/package_show"
FORMATO_FECHA = "%d/%m/%Y"


# ---------------------------------------------------------------------
# Consulta de fechas
# ---------------------------------------------------------------------

def fecha_desde_arcgis(url_capa: str, sesion: requests.Session, timeout: int = 30):
    """Fecha de última edición de datos según el servicio ArcGIS, o None."""
    info = sesion.get(url_capa, params={"f": "json"}, timeout=timeout).json()
    edicion = info.get("editingInfo") or {}
    ms = edicion.get("dataLastEditDate") or edicion.get("lastEditDate")
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


def fecha_desde_datos_abiertos(url_dataset: str, sesion: requests.Session, timeout: int = 30):
    """metadata_modified del dataset en Datos Abiertos Bogotá, o None."""
    match = re.search(r"/dataset/([^/?#\s]+)", str(url_dataset))
    if not match:
        return None
    r = sesion.get(API_CKAN, params={"id": match.group(1)}, timeout=timeout).json()
    modificado = (r.get("result") or {}).get("metadata_modified")
    if not modificado:
        return None
    return datetime.fromisoformat(modificado).date()


# ---------------------------------------------------------------------
# API principal
# ---------------------------------------------------------------------

def actualizar_fechas_fuentes(ruta_csv: str | Path, escribir: bool = True) -> pd.DataFrame:
    """
    Consulta la fecha de cada fuente y, si escribir=True, reescribe el CSV.
    Devuelve un resumen por capa: fecha anterior, nueva y de dónde salió.
    """
    # dtype=str + keep_default_na=False: no convertir 'null' ni vacíos en NaN
    df = pd.read_csv(ruta_csv, sep=";", encoding="utf-8-sig", dtype=str, keep_default_na=False)

    sesion = requests.Session()
    sesion.headers.update({"User-Agent": "user-agent-data-fetch/1.0"})

    resumen = []

    for i, row in df.iterrows():
        anterior = row[COL_FECHA]
        nueva, origen = None, "sin cambios"

        url_servicio = extraer_primera_url(row.get(COL_URL_SERVICIO_FEATURE))
        url_dataset = row.get(COL_URL_DATOS_ABIERTOS)

        consultas = [
            ("servicio ArcGIS", fecha_desde_arcgis, url_servicio),
            ("Datos Abiertos", fecha_desde_datos_abiertos, url_dataset),
        ]
        for nombre, consulta, url in consultas:
            if not url:
                continue
            try:
                nueva = consulta(url, sesion)
            except Exception as e:
                print(f"AVISO: {row[COL_SOURCE_LAYER]} ({nombre}) -> {e}")
                continue
            if nueva:
                origen = nombre
                break

        if nueva:
            df.at[i, COL_FECHA] = nueva.strftime(FORMATO_FECHA)

        resumen.append({
            COL_SOURCE_LAYER: row[COL_SOURCE_LAYER],
            "fecha_anterior": anterior,
            "fecha_nueva": df.at[i, COL_FECHA],
            "origen": origen,
        })

    if escribir:
        df.to_csv(ruta_csv, sep=";", encoding="utf-8-sig", index=False)

    return pd.DataFrame(resumen)


__all__ = [
    "fecha_desde_arcgis",
    "fecha_desde_datos_abiertos",
    "actualizar_fechas_fuentes",
]
