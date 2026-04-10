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
