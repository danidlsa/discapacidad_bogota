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
    clasificar_tematica
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
# Filtro específico para Secretaría General
# ---------------------------------------------------------------------

def filtrar_secgeneral(obj: Any):
    """
    Filtrado específico para la capa de Secretaría General.

    - Elimina registros sin geometría o con geometría vacía
    - Excluye ofertas pertenecientes a secretarías que ya se procesan
      como capas propias
    """
    # Si no es un GeoDataFrame, no hacemos nada
    if not hasattr(obj, "geometry"):
        return obj

    # 1) Mantener solo geometría válida
    obj = obj[
        obj.geometry.notna() &
        (~obj.geometry.is_empty)
    ]

    # 2) Excluir secretarías ya cubiertas por otras capas
    if "sect_nombr" in obj.columns:
        excluir = {
            "Educación",
            "Mujer",
            "Integración Social",
            "Inclusión Social y Reconciliación",
        }
        obj = obj[~obj["sect_nombr"].isin(excluir)]

    return obj

def filtrar_sdmujer(obj):
    """
    Filtrado específico para la capa de la Secretaría de la Mujer.
    Solo conserva servicios dirigidos a cuidadoras y a sus familias/comunidad.
    """
    if "TIPO_SERVI" not in obj.columns:
        return obj

    valores_validos = {
        "Para las Cuidadoras",
        "Para familias de las cuidadoras y Comunidad",
    }

    return obj[obj["TIPO_SERVI"].isin(valores_validos)]

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

            entidad = mapa_entidad.get(source_layer, None)
            entidad_norm = str(entidad).lower() if entidad is not None else ""

            # 1) Secretaría General (capa consolidada)
            if source_layer == SOURCE_LAYER_SECGENERAL:
                obj = filtrar_secgeneral(obj)
                obj = incorporar_extras_secgeneral(
                    obj,
                    ruta_csv_extras=ruta_extras_secgeneral,
                    col_llave_gdf="ofer_nombr",
                    col_llave_extras="ofer_nombr",
                )

                
                # Secretaría General: clasificación temática
                if "ofer_descr" in obj.columns and "sect_nombr" in obj.columns:
                    obj["tematica_servicio"] = obj.apply(
                        lambda r: clasificar_tematica(
                            r["ofer_descr"],
                            r["sect_nombr"],
                            "Gobierno",
                            "Secretaría General",
                        ),
                        axis=1,
                    )
                else:
                    # fallback seguro (no debería ocurrir, pero evita vacíos)
                    obj["tematica_servicio"] = "Otra oferta para personas con discapacidad"


            # 2) SDIS
            elif es_capa_sdis(entidad):
                obj = incorporar_extras_sdis(
                    obj,
                    ruta_csv_extras=ruta_extras_sdis,
                    col_llave_gdf_preferida="OSSUOpera",
                )
                obj["tematica_servicio"] = "Cuidados y apoyos directos"

            # 3) Secretaría de la Mujer
            elif ("mujer" in entidad_norm) or ("sdmujer" in entidad_norm):
                obj = filtrar_sdmujer(obj)
                obj["tematica_servicio"] = "Cuidadoras"

            # 4) Educación
            elif "educación" in entidad_norm:
                obj["tematica_servicio"] = "Educación inclusiva"

            salida[source_layer] = obj

        except Exception as e:
            errores[source_layer] = {"error": repr(e)}
            salida[source_layer] = obj  # no rompe el pipeline

    return salida, errores


__all__ = [
    "es_capa_sdis",
    "filtrar_secgeneral",
    "aplicar_reglas",
]