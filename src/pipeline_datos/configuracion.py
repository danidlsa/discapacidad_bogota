"""
Configuración del proyecto.

Este módulo centraliza:
- Rutas por defecto del repositorio (carpeta de apoyo y carpeta de salidas)
- Nombres de archivos de apoyo
- Nombres de columnas usadas en reglas y transformaciones

No contiene lógica de negocio: solo constantes y helpers de ruta.
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------
# Rutas base del repo
# ---------------------------------------------------------------------

# Estructura asumida:
#   repo/
#     src/pipeline_datos/configuracion.py
RAIZ_REPO = Path(__file__).resolve().parents[2]

# Tablas de apoyo dentro del paquete
CARPETA_APOYO = Path(__file__).resolve().parent / "apoyo"

# Salidas por defecto a nivel repo
CARPETA_SALIDAS = RAIZ_REPO / "salidas"


# ---------------------------------------------------------------------
# Archivos de apoyo (nombres)
# ---------------------------------------------------------------------

ARCHIVO_FUENTES = "sourcelayers.csv"                 
ARCHIVO_EXTRAS_SDIS = "sdis_extravar.csv"             
ARCHIVO_EXTRAS_SECGENERAL = "secgeneral_extravar.csv"   


# ---------------------------------------------------------------------
# Columnas esperadas (fuentes)
# ---------------------------------------------------------------------

COL_SOURCE_LAYER = "source_layer"            
COL_ENTIDAD = "Entidad"                     
COL_URL_SERVICIO_FEATURE = "URL Servicio Feature"    


# ---------------------------------------------------------------------
# Columnas esperadas (extras SDIS)
# ---------------------------------------------------------------------

COL_SDIS_LLAVE_PRIMARIA = "OSSUOpera"            
COL_SDIS_DESCRIPCION = "Descripcion"             
COL_SDIS_TIPO_DISCAPACIDAD = "Tipo de discapacidad"  
COL_SDIS_TRAMO_EDAD = "Tramo de edad atendido"   
COL_SDIS_ARRAY_EDADES = "Array de edades"        


# ---------------------------------------------------------------------
# Columnas esperadas (extras SecGeneral)
# ---------------------------------------------------------------------

COL_SECGENERAL_LLAVE = "ofer_nombr"                
COL_SECGENERAL_TRAMO_EDAD = "Tramo de edad atendido"    
COL_SECGENERAL_ARRAY_EDADES = "Array de edades"          


# ---------------------------------------------------------------------
# Identificadores (source_layer) relevantes
# ---------------------------------------------------------------------

SOURCE_LAYER_SECGENERAL = "oferta_discapacidad_secgeneral"    


# ---------------------------------------------------------------------
# Helpers de ruta
# ---------------------------------------------------------------------

def ruta_apoyo(nombre_archivo: str) -> Path:
    """Ruta absoluta a un archivo dentro de la carpeta de apoyo."""
    return CARPETA_APOYO / nombre_archivo


def ruta_salidas() -> Path:
    """Ruta absoluta a la carpeta de salidas por defecto."""
    return CARPETA_SALIDAS


__all__ = [
    "RAIZ_REPO",
    "CARPETA_APOYO",
    "CARPETA_SALIDAS",
    "ARCHIVO_FUENTES",
    "ARCHIVO_EXTRAS_SDIS",
    "ARCHIVO_EXTRAS_SECGENERAL",
    "COL_SOURCE_LAYER",
    "COL_ENTIDAD",
    "COL_URL_SERVICIO_FEATURE",
    "COL_SDIS_LLAVE_PRIMARIA",
    "COL_SDIS_DESCRIPCION",
    "COL_SDIS_TIPO_DISCAPACIDAD",
    "COL_SDIS_TRAMO_EDAD",
    "COL_SDIS_ARRAY_EDADES",
    "COL_SECGENERAL_LLAVE",
    "COL_SECGENERAL_TRAMO_EDAD",
    "COL_SECGENERAL_ARRAY_EDADES",
    "SOURCE_LAYER_SECGENERAL",
    "ruta_apoyo",
    "ruta_salidas",
]
