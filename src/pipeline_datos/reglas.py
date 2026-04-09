"""
Reglas de transformación diferencial por capa / por entidad.

Este módulo:
- NO descarga datos.
- NO define transformaciones de bajo nivel.
- Solo decide qué transformaciones aplicar a cada capa.

Depende de:
- Un DataFrame de fuentes (sourcelayers.csv) con columnas:
    - source_layer
    - Entidad
- Funciones de transformaciones.py para incorporar variables extra:
    - incorporar_extras_sdis
    - incorporar_extras_secgeneral
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .configuracion import (
    ruta_apoyo,
    ARCHIVO_EXTRAS_SDIS,
    ARCHIVO_EXTRAS_SECGENERAL,
    COL_SOURCE_LAYER,
    COL_ENTIDAD,
    SOURCE_LAYER_SECGENERAL,
)
from .transformaciones import (
    incorporar_extras_sdis,
    incorporar_extras_secgeneral,
)


# ---------------------------------------------------------------------
# Identificación de entidades
# ---------------------------------------------------------------------

def es_capa_sdis(entidad: Any) -> bool:
    """
    Identifica si una capa corresponde a SDIS usando el campo 'Entidad'.
    """
    if entidad is None or (isinstance(entidad, float) and pd.isna(entidad)):
        return False
    s = str(entidad).lower()
    return ("integración social" in s) or ("integracion social" in s) or ("sdis" in s)


# ---------------------------------------------------------------------
# API principal
# ---------------------------------------------------------------------

def aplicar_reglas(
    capas: dict[str, Any],
    df_fuentes: pd.DataFrame,
    ruta_extras_sdis: str | None = None,
    ruta_extras_secgeneral: str | None = None,
    reglas_por_capa: dict[str, Callable[[Any], Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Aplica reglas de transformación sobre un dict {source_layer: gdf/df}.

    Parámetros:
    - capas: dict con resultados de ingesta (keys = source_layer)
    - df_fuentes: DataFrame de fuentes con columnas:
        - source_layer
        - Entidad
    - ruta_extras_sdis: ruta al CSV de extras SDIS
    - ruta_extras_secgeneral: ruta al CSV de extras SecGeneral
    - reglas_por_capa: dict opcional {source_layer: funcion} para reglas específicas adicionales

    Devuelve:
    - capas_transformadas
    - errores (si una regla falla, no rompe todo)
    """
    if ruta_extras_sdis is None:
        ruta_extras_sdis = str(ruta_apoyo(ARCHIVO_EXTRAS_SDIS))
    if ruta_extras_secgeneral is None:
        ruta_extras_secgeneral = str(ruta_apoyo(ARCHIVO_EXTRAS_SECGENERAL))

    reglas_por_capa = reglas_por_capa or {}

    # Mapa rápido: source_layer -> Entidad
    if COL_SOURCE_LAYER in df_fuentes.columns and COL_ENTIDAD in df_fuentes.columns:
        mapa_entidad = df_fuentes.set_index(COL_SOURCE_LAYER)[COL_ENTIDAD]
    else:
        mapa_entidad = pd.Series(dtype=object)

    salida: dict[str, Any] = {}
    errores: dict[str, Any] = {}

    for source_layer, obj in capas.items():
        try:
            # 0) Reglas específicas por capa (si se proveen)
            if source_layer in reglas_por_capa:
                obj = reglas_por_capa[source_layer](obj)

            # 1) Regla SecGeneral (por id de capa)
            if source_layer == SOURCE_LAYER_SECGENERAL:
                obj = incorporar_extras_secgeneral(
                    obj,
                    ruta_csv_extras=ruta_extras_secgeneral,
                    col_llave_gdf="ofer_nombr",
                    col_llave_extras="ofer_nombr",
                )

            # 2) Regla SDIS (por entidad)
            entidad = mapa_entidad.get(source_layer, None)
            if es_capa_sdis(entidad):
                obj = incorporar_extras_sdis(
                    obj,
                    ruta_csv_extras=ruta_extras_sdis,
                    col_llave_gdf_preferida="OSSUOpera",
                )

            salida[source_layer] = obj

        except Exception as e:
            errores[source_layer] = {"error": repr(e)}
            salida[source_layer] = obj  # no rompe el pipeline

    return salida, errores


__all__ = [
    "es_capa_sdis",
    "aplicar_reglas",
]