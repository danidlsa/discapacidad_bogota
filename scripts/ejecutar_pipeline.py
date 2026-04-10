#!/usr/bin/env python3
"""
Runner principal del pipeline de datos Discapacidad Bogotá

Ejecuta el pipeline completo de forma reproducible:
1. Lee la tabla de fuentes (sourcelayers.csv)
2. Descarga las capas desde ArcGIS / fuentes remotas
3. Aplica reglas y transformaciones diferenciales
4. Guarda resultados en la carpeta 'salidas/'

"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


# ---------------------------------------------------------------------
# Asegurar imports desde src/
# ---------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"

if SRC_PATH.as_posix() not in sys.path:
    sys.path.insert(0, SRC_PATH.as_posix())


# ---------------------------------------------------------------------
# Imports del pipeline
# ---------------------------------------------------------------------

from pipeline_datos.configuracion import (
    ruta_apoyo,
    ruta_salidas,
    ARCHIVO_FUENTES,
)

from pipeline_datos.ingesta import (
    cargar_fuentes_desde_csv,
    descargar_todas_las_capas,
)

from pipeline_datos.reglas import (
    aplicar_reglas,
)

from pipeline_datos.salida import (
    guardar_capas,
    generar_manifest,
    guardar_manifest,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def imprimir_resumen(titulo: str, items: dict):
    print(f"\n{titulo}")
    print("-" * len(titulo))
    for k, v in items.items():
        print(f"- {k}: {v}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    print("== Pipeline Discapacidad Bogotá ==")

    # --------------------------------------------------
    # 1. Leer fuentes
    # --------------------------------------------------

    ruta_fuentes = ruta_apoyo(ARCHIVO_FUENTES)

    print(f"\nLeyendo fuentes desde: {ruta_fuentes}")

    df_fuentes = cargar_fuentes_desde_csv(
        ruta_csv=ruta_fuentes,
        columna_url="URL Servicio Feature",
        sep=";",  # consistente con tus CSV finales
    )

    print(f"Fuentes cargadas: {len(df_fuentes)} filas")

    # --------------------------------------------------
    # 2. Ingesta
    # --------------------------------------------------

    print("\nDescargando capas...")

    capas, errores_ingesta = descargar_todas_las_capas(df_fuentes)

    imprimir_resumen("Resultado ingesta", {
        "capas descargadas": len(capas),
        "errores": len(errores_ingesta),
    })

    if errores_ingesta:
        print("\nErrores de ingesta:")
        for k, e in errores_ingesta.items():
            print(f"- {k}: {e}")

    # --------------------------------------------------
    # 3. Reglas / transformaciones
    # --------------------------------------------------

    print("\nAplicando reglas y transformaciones...")

    capas_transformadas, errores_reglas = aplicar_reglas(
        capas=capas,
        df_fuentes=df_fuentes,
    )

    imprimir_resumen("Resultado reglas", {
        "capas procesadas": len(capas_transformadas),
        "errores": len(errores_reglas),
    })

    if errores_reglas:
        print("\nErrores en reglas:")
        for k, e in errores_reglas.items():
            print(f"- {k}: {e}")

    # --------------------------------------------------
    # 4. Salidas
    # --------------------------------------------------

    carpeta_out = ruta_salidas()

    print(f"\nGuardando salidas en: {carpeta_out}")

    rutas_guardadas = guardar_capas(
        capas_transformadas,
        carpeta_salida=carpeta_out,
        formato_gdf="gpkg",
        formato_df="csv",
    )

    manifest = generar_manifest(
        capas_transformadas,
        rutas_guardadas=rutas_guardadas,
    )

    ruta_manifest = guardar_manifest(
        manifest,
        carpeta_salida=carpeta_out,
    )

    imprimir_resumen("Archivos generados", rutas_guardadas)
    print(f"- manifest: {ruta_manifest}")

    print("\n== Pipeline completado correctamente ==")


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()
