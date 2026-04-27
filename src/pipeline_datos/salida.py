"""
Módulo: salida.py
-----------------
Guardar resultados del pipeline en la carpeta 'salidas/'.

Convención por defecto:
- GeoDataFrame -> GeoPackage (.gpkg)
- DataFrame    -> CSV (.csv)

Además:
- manifest.json con un resumen reproducible (filas/columnas, columnas, si tiene geometría).
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import Any

import pandas as pd

try:
    import geopandas as gpd
except Exception:
    gpd = None

import re
import unicodedata

# ---------------------------------------------------------------------
# Homogeneización de columnas (para que TODAS las salidas usen los mismos nombres)
# ---------------------------------------------------------------------

_MAPEO_COLUMNAS_ESTANDAR = {
    "direccion": ["OSSDirecc", "DIRECCION", "puan_direc"],
    "telefono": ["OSSTelefo", "TELEFONO"],
    "horario": ["OSSHAtenc", "HORARIO", "puan_horar"],
    "poblacion_atendida": ["OSSPBenef", "ofer_pobla"],
    "correo": ["OSSCorreo", "EMAIL"],
    "descripcion_oferta": ["descripcion_extra", "ofer_descr"],
    "tipo_discapacidad_str": ["tipo_discapacidad_extra", "DISCAPACID"],
    # (opcional) si quieres cubrir variantes por tildes/espacios, con la normalización abajo suele bastar
}

def _norm_colname(s: str) -> str:
    """Normaliza nombres de columna: lower, sin tildes/diacríticos, sin espacios extremos."""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s

def _primero_no_nulo(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Devuelve una serie con el primer valor no-nulo por fila entre varias columnas."""
    if len(cols) == 1:
        return df[cols[0]]
    # bfill en eje 1: rellena hacia la izquierda con el siguiente no nulo
    tmp = df[cols].copy()
    return tmp.bfill(axis=1).iloc[:, 0]

def _parse_edades_a_lista(valor):
    """Convierte un string tipo '0, 1, 2' o '0 1 2' a lista[int]."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return []
    if isinstance(valor, list):
        return valor
    nums = re.findall(r"\d+", str(valor))
    return [int(n) for n in nums]

def homogeneizar_columnas_estandar(obj: Any) -> Any:
    """
    Homogeneiza nombres de columnas en DF/GDF para salidas consistentes.
    - Renombra/combina alias -> nombre canónico
    - Resuelve edades_array / edades_lista si una falta
    """
    if not isinstance(obj, pd.DataFrame) and not _es_geodataframe(obj):
        return obj

    df = obj  # funciona igual para GeoDataFrame (es subclase de DataFrame)

    # índice normalizado de columnas existentes
    cols_actuales = list(df.columns)
    norm_to_actual = {}
    for c in cols_actuales:
        norm_to_actual[_norm_colname(c)] = c

    # 1) Variables canónicas principales
    for canon, variantes in _MAPEO_COLUMNAS_ESTANDAR.items():
        candidatos = []
        # incluir también si ya existe (aunque con otro case)
        for nombre in [canon] + variantes:
            n = _norm_colname(nombre)
            if n in norm_to_actual:
                candidatos.append(norm_to_actual[n])

        # quitar duplicados manteniendo orden
        vistos = set()
        candidatos = [c for c in candidatos if not (c in vistos or vistos.add(c))]

        if not candidatos:
            continue

        # crear/actualizar canónica con primero no nulo
        df[canon] = _primero_no_nulo(df, candidatos)

        # eliminar alias (pero no la canónica)
        for c in candidatos:
            if c != canon and c in df.columns:
                df.drop(columns=[c], inplace=True)

# 2) Edades: 
    posibles_array = ["edades_array", "Array de edades", "array de edades", "array_edades"]
    posibles_lista = ["edades_lista", "lista_edades", "edades"]

    col_array = None
    for p in posibles_array:
        k = _norm_colname(p)
        if k in norm_to_actual:
            col_array = norm_to_actual[k]
            break

    col_lista = None
    for p in posibles_lista:
        k = _norm_colname(p)
        if k in norm_to_actual:
            col_lista = norm_to_actual[k]
            break

    # Normalizar nombre si viene distinto
    if col_lista and col_lista != "edades_lista":
        df.rename(columns={col_lista: "edades_lista"}, inplace=True)

    # Si no existe edades_lista pero sí edades_array → derivarla
    if "edades_lista" not in df.columns and col_array:
        df["edades_lista"] = df[col_array].apply(_parse_edades_a_lista)

    # Eliminar cualquier versión de edades_array
    if col_array and col_array in df.columns:
        df.drop(columns=[col_array], inplace=True)

    return obj



# ---------------------------------------------------------------------
# Configuración por defecto
# ---------------------------------------------------------------------

def carpeta_salida_por_defecto() -> Path:
    """
    Carpeta de salida por defecto: <repo>/salidas/
    (asumiendo estructura: repo/src/pipeline_datos/salida.py)
    """
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "salidas"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _asegurar_carpeta(ruta: str | Path) -> Path:
    p = Path(ruta).expanduser().resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _es_geodataframe(obj: Any) -> bool:
    return gpd is not None and hasattr(obj, "geometry") and obj.__class__.__name__ == "GeoDataFrame"


def _sanear_para_json(obj: Any):
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    return obj


# ---------------------------------------------------------------------
# Guardar 1 capa
# ---------------------------------------------------------------------
def guardar_capa(obj: Any, ruta_archivo: str | Path) -> None:
    """
    Guarda un DataFrame/GeoDataFrame según extensión:
    - GeoDataFrame: .gpkg / .geojson / .parquet
    - DataFrame:    .csv / .tsv / .parquet
    """

    obj = homogeneizar_columnas_estandar(obj)

    ruta_archivo = Path(ruta_archivo)
    ruta_archivo.parent.mkdir(parents=True, exist_ok=True)

    ext = ruta_archivo.suffix.lower()

    if _es_geodataframe(obj):
        if ext == ".gpkg":
            obj.to_file(ruta_archivo, driver="GPKG")
        elif ext == ".geojson":
            obj.to_file(ruta_archivo, driver="GeoJSON")
        elif ext == ".parquet":
            obj.to_parquet(ruta_archivo)
        else:
            raise ValueError(f"Extensión no soportada para GeoDataFrame: {ext}")
        return

    if isinstance(obj, pd.DataFrame):
        if ext == ".parquet":
            obj.to_parquet(ruta_archivo, index=False)
        elif ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            obj.to_csv(ruta_archivo, index=False, sep=sep)
        else:
            raise ValueError(f"Extensión no soportada para DataFrame: {ext}")
        return

    raise TypeError(f"Tipo no soportado para guardar: {type(obj)}")

# ---------------------------------------------------------------------
# Guardar dict de capas
# ---------------------------------------------------------------------

def guardar_capas(
    capas: dict[str, Any],
    carpeta_salida: str | Path | None = None,
    formato_gdf: str = "gpkg",
    formato_df: str = "csv",
) -> dict[str, str]:
    """
    Guarda un dict {nombre_capa: df/gdf} en la carpeta indicada.
    Devuelve dict {nombre_capa: ruta_guardada}.
    """
    if carpeta_salida is None:
        carpeta_salida = carpeta_salida_por_defecto()

    outdir = _asegurar_carpeta(carpeta_salida)
    rutas: dict[str, str] = {}

    for nombre, obj in capas.items():
        if _es_geodataframe(obj):
            ext = formato_gdf
        elif isinstance(obj, pd.DataFrame):
            ext = formato_df
        else:
            continue

        ruta = outdir / f"{nombre}.{ext}"
        guardar_capa(obj, ruta)
        rutas[nombre] = str(ruta)

    return rutas


# ---------------------------------------------------------------------
# Manifest (reproducibilidad)
# ---------------------------------------------------------------------

def generar_manifest(
    capas: dict[str, Any],
    rutas_guardadas: dict[str, str] | None = None,
    notas: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Genera un manifest con metadatos simples para reproducibilidad.
    """
    manifest = {
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "rutas": rutas_guardadas or {},
        "notas": notas or {},
        "capas": {},
    }

    for nombre, obj in capas.items():
        info = {
            "tiene_geometria": bool(_es_geodataframe(obj)),
        }

        if hasattr(obj, "shape"):
            info["filas"] = int(obj.shape[0])
            info["columnas"] = int(obj.shape[1])

        if hasattr(obj, "columns"):
            info["nombres_columnas"] = [str(c) for c in obj.columns]

        manifest["capas"][nombre] = info

    return manifest


def guardar_manifest(
    manifest: dict[str, Any],
    carpeta_salida: str | Path | None = None,
    nombre_archivo: str = "manifest.json",
) -> str:
    """
    Guarda el manifest como JSON (por defecto en 'salidas/').
    """
    if carpeta_salida is None:
        carpeta_salida = carpeta_salida_por_defecto()

    outdir = _asegurar_carpeta(carpeta_salida)
    ruta = outdir / nombre_archivo

    with ruta.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=_sanear_para_json)

    return str(ruta)


__all__ = [
    "carpeta_salida_por_defecto",
    "guardar_capa",
    "guardar_capas",
    "generar_manifest",
    "guardar_manifest",
]
