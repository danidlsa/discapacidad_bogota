#!/usr/bin/env python3
"""
Actualiza 'Fecha de actualización' en sourcelayers.csv con la fecha que
publica cada fuente (servicio ArcGIS o Datos Abiertos Bogotá).

Uso:
    python scripts/actualizar_fechas.py            # actualiza el CSV
    python scripts/actualizar_fechas.py --dry-run  # solo muestra los cambios
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"

if SRC_PATH.as_posix() not in sys.path:
    sys.path.insert(0, SRC_PATH.as_posix())

from pipeline_datos.configuracion import ruta_apoyo, ARCHIVO_FUENTES
from pipeline_datos.fechas import actualizar_fechas_fuentes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--dry-run", action="store_true", help="no escribe el CSV")
    args = parser.parse_args()

    ruta = ruta_apoyo(ARCHIVO_FUENTES)
    resumen = actualizar_fechas_fuentes(ruta, escribir=not args.dry_run)

    print(resumen.to_string(index=False))
    if args.dry_run:
        print("\n(dry-run: no se modificó el CSV)")
    else:
        print(f"\nCSV actualizado: {ruta}")


if __name__ == "__main__":
    main()
