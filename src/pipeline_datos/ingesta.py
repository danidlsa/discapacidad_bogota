"""
Ingesta básica de datos desde ArcGIS REST a partir de un CSV de fuentes.

Este módulo apunta a:
- leer tabla de source layers
- limpiar la URL del servicio
- descargar capas via API ArcGIS
- devolver GeoDataFrame o DataFrame según haya geometría
"""

from __future__ import annotations

import re
import requests
import pandas as pd
import geopandas as gpd

from .configuracion import COL_ORIGEN_DESCARGA, COL_URL_DATOS_ABIERTOS

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def descargar_capa_arcgis_a_gdf(
    url_capa: str,
    where: str = "1=1",
    campos_salida: str = "*",
    sr_salida: int = 4326,
    tam_pagina: int | None = None,
    timeout: int = 120,
    sesion: requests.Session | None = None,
    devolver_df_si_no_hay_geom: bool = True,  # <- importante
):
    """
    Devuelve:
      - GeoDataFrame si existen geometrías
      - DataFrame si NO existen geometrías (tablas / geometría nula),
        si devolver_df_si_no_hay_geom=True
      - GeoDataFrame vacío si la capa está vacía
    """
    s = sesion or requests.Session()
    s.headers.update({"User-Agent": "user-agent-data-fetch/1.0"})

    # --- metadata de la capa
    info = s.get(
        url_capa,
        params={"f": "pjson"},
        timeout=timeout,
        verify=False,
    )
    info.raise_for_status()
    info = info.json()

    campo_oid = info.get("objectIdField") or "OBJECTID"
    max_rc = info.get("maxRecordCount") or 1000
    if tam_pagina is None:
        tam_pagina = int(max_rc)

    url_query = url_capa.rstrip("/") + "/query"

    features = []
    offset = 0

    while True:
        params = {
            "f": "geojson",  # mejor caso
            "where": where,
            "outFields": campos_salida,
            "returnGeometry": "true",
            "outSR": sr_salida,
            "resultOffset": offset,
            "resultRecordCount": tam_pagina,
            "orderByFields": campo_oid,
        }

        r = s.get(
            url_query,
            params=params,
            timeout=timeout,
            verify=False,
        )

        # fallback si f=geojson no es soportado
        if r.status_code >= 400:
            params["f"] = "json"
            r = s.get(
                url_query,
                params=params,
                timeout=timeout,
                verify=False,
            )

        r.raise_for_status()
        data = r.json()

        batch = data.get("features", [])
        if not batch:
            break

        # Caso A: ya son features GeoJSON
        if params["f"] == "geojson":
            features.extend(batch)

        # Caso B: ESRI JSON -> convertir a GeoJSON
        else:
            try:
                import arcgis2geojson
            except ImportError as e:
                raise RuntimeError(
                    "El servicio no devolvió GeoJSON y falta 'arcgis2geojson'. "
                    "Instala: pip install arcgis2geojson"
                ) from e

            esri_featureset = {
                "displayFieldName": data.get("displayFieldName"),
                "fieldAliases": data.get("fieldAliases"),
                "geometryType": data.get("geometryType"),
                "spatialReference": data.get("spatialReference"),
                "fields": data.get("fields"),
                "features": batch,
            }
            gj = arcgis2geojson.arcgis2geojson(esri_featureset)
            features.extend(gj.get("features", []))

        exceeded = data.get("exceededTransferLimit")
        if (exceeded is False) or (len(batch) < tam_pagina):
            break

        offset += tam_pagina

    # ---- construir salida de forma segura ----
    if not features:
        empty = gpd.GeoDataFrame(
            {"geometry": gpd.GeoSeries([], crs=f"EPSG:{sr_salida}")}
        )
        return empty

    feats_con_geom = [f for f in features if f.get("geometry") is not None]
    feats_sin_geom = [f for f in features if f.get("geometry") is None]

    if len(feats_con_geom) == 0:
        rows = [f.get("properties", {}) for f in feats_sin_geom]
        df = pd.DataFrame(rows)
        if devolver_df_si_no_hay_geom:
            return df

        empty = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries([None] * len(df)))
        return empty

    gdf = gpd.GeoDataFrame.from_features(feats_con_geom)

    if gdf.geometry is not None and gdf.geometry.notna().any():
        gdf = gdf.set_crs(f"EPSG:{sr_salida}", allow_override=True)

    gdf.attrs["descartados_sin_geometria"] = len(feats_sin_geom)

    return gdf


API_CKAN = "https://datosabiertos.bogota.gov.co/api/3/action/package_show"


def recurso_mas_reciente(
    url_dataset: str,
    sesion: requests.Session,
    formatos: tuple[str, ...] = ("gpkg", "geojson"),
    timeout: int = 60,
):
    """
    Busca en Datos Abiertos Bogotá (CKAN) el archivo descargable más reciente
    del dataset, entre los formatos indicados (en ese orden de preferencia si
    empatan en fecha). Devuelve el dict del recurso o None.
    """
    match = re.search(r"/dataset/([^/?#\s]+)", str(url_dataset))
    if not match:
        return None

    r = sesion.get(API_CKAN, params={"id": match.group(1)}, timeout=timeout)
    r.raise_for_status()
    recursos = (r.json().get("result") or {}).get("resources", [])

    candidatos = [
        x for x in recursos
        if str(x.get("format", "")).strip().lower() in formatos and x.get("url")
    ]
    if not candidatos:
        return None

    def _orden(x):
        fecha = x.get("last_modified") or x.get("created") or ""
        pref = -formatos.index(str(x.get("format")).strip().lower())
        return (fecha[:10], pref)

    return max(candidatos, key=_orden)


def descargar_archivo_datos_abiertos(
    url_dataset: str,
    sesion: requests.Session | None = None,
    sr_salida: int = 4326,
    timeout: int = 180,
):
    """
    Descarga el archivo más reciente (GPKG o GeoJSON) de un dataset de Datos
    Abiertos Bogotá y lo devuelve como GeoDataFrame en sr_salida.

    Se usa para capas cuyo servicio REST está desactualizado respecto del
    archivo publicado (p. ej. Colegios: el MapServer de Catastro seguía en la
    versión 12.25 cuando el portal ya tenía la 09.26).
    """
    import tempfile
    from pathlib import Path

    s = sesion or requests.Session()
    recurso = recurso_mas_reciente(url_dataset, s)
    if recurso is None:
        raise RuntimeError(f"No encontré archivos GPKG/GeoJSON en {url_dataset}")

    r = s.get(recurso["url"], timeout=timeout)
    r.raise_for_status()

    sufijo = "." + str(recurso.get("format")).strip().lower()
    with tempfile.TemporaryDirectory() as carpeta:
        ruta = Path(carpeta) / f"recurso{sufijo}"
        ruta.write_bytes(r.content)
        gdf = gpd.read_file(ruta)

    sin_geom = gdf.geometry.isna() | gdf.geometry.is_empty
    gdf = gdf[~sin_geom].copy()
    if gdf.crs is not None:
        gdf = gdf.to_crs(epsg=sr_salida)

    gdf.attrs["descartados_sin_geometria"] = int(sin_geom.sum())
    gdf.attrs["recurso"] = recurso.get("name")
    gdf.attrs["recurso_fecha"] = (recurso.get("last_modified") or recurso.get("created") or "")[:10]
    return gdf



def extraer_primera_url(valor):
    """Extrae la primera URL http(s) encontrada en un texto."""
    if pd.isna(valor):
        return None
    match = re.search(r"https?://[^\s]+", str(valor))
    return match.group(0) if match else None


def es_origen_datos_abiertos(valor) -> bool:
    """True si la columna de origen indica descargar el archivo de Datos Abiertos."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return False
    return "datos abiertos" in str(valor).strip().lower()



def cargar_fuentes_desde_csv(
    ruta_csv,
    columna_url="URL Servicio Feature",
    sep=";",
    encoding="utf-8",
):
    """
    Carga la tabla de fuentes desde CSV y extrae la URL limpia del servicio.
    """
    df = pd.read_csv(
        ruta_csv,
        sep=sep,
        encoding=encoding,
    )

    df["url_servicio_feature_clean"] = df[columna_url].apply(extraer_primera_url)

    # id_capa estable
    if "source_layer" in df.columns:
        df["id_capa"] = df["source_layer"]
    elif "Nombre de la capa" in df.columns:
        df["id_capa"] = df["Nombre de la capa"]
    else:
        df["id_capa"] = [f"capa_{i}" for i in range(len(df))]

    return df




def descargar_todas_las_capas(df_fuentes: pd.DataFrame) -> tuple[dict, dict]:
    """
    Descarga todas las capas indicadas en df_fuentes.
    Espera que existan:
      - source_layer
      - url_servicio_feature_clean
    y, opcionalmente, COL_ORIGEN_DESCARGA: "datos abiertos" descarga el archivo
    más reciente del dataset (COL_URL_DATOS_ABIERTOS) en vez del servicio REST.
    Devuelve (capas, errores).
    """
    sesion = requests.Session()
    sesion.headers.update({"User-Agent": "user-agent-data-fetch/1.0"})

    capas = {}
    errores = {}

    for _, row in df_fuentes.iterrows():
        key = row["source_layer"]
        url = row["url_servicio_feature_clean"]
        desde_archivo = es_origen_datos_abiertos(row.get(COL_ORIGEN_DESCARGA))
        if desde_archivo:
            url = row.get(COL_URL_DATOS_ABIERTOS)

        if not url or pd.isna(url):
            continue

        try:
            if desde_archivo:
                obj = descargar_archivo_datos_abiertos(url, sesion=sesion)
                print(f"   ({key}: archivo de Datos Abiertos '{obj.attrs.get('recurso')}', {obj.attrs.get('recurso_fecha')})")
            else:
                obj = descargar_capa_arcgis_a_gdf(url, sr_salida=4326, sesion=sesion)

            capas[key] = obj

            # log rápido
            if hasattr(obj, "geometry"):  # GeoDataFrame
                descartados = getattr(obj, "attrs", {}).get("descartados_sin_geometria", 0)
                print(f"OK: {key} -> {len(obj)} filas (descartados_sin_geometria={descartados})")
            else:  # DataFrame
                print(f"OK (sin geom): {key} -> {len(obj)} filas (pandas.DataFrame)")

        except Exception as e:
            errores[key] = {"url": url, "error": repr(e)}
            print(f"ERROR: {key} -> {e}")

    return capas, errores


