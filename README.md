# Pipeline de datos – Mapa de Cuidados y apoyos para personas con discapacidad y personas cuidadoras en Bogotá
## PNUD

Este repositorio contiene un pipeline reproducible para la descarga, transformación y documentación de capas geoespaciales y tabulares relacionadas con la oferta institucional para personas con discapacidad en Bogotá.

El objetivo del pipeline es:
- descargar automáticamente capas desde servicios ArcGIS REST,
- aplicar transformaciones y reglas diferenciadas según la fuente de datos,
- generar salidas consistentes, documentadas y reproducibles.

---

## Estructura del repositorio

```
discapacidad_bogota/
├── src/
│   └── pipeline_datos/
│       ├── ingesta.py
│       ├── transformaciones.py
│       ├── reglas.py
│       ├── salida.py
│       └── configuracion.py
├── scripts/
│   └── ejecutar_pipeline.py
├── apoyo/
│   ├── sourcelayers.csv
│   ├── sdis_extravar.csv
│   └── secgeneral_extravar.csv
├── salidas/
└── README.md
```

---

## Flujo general del pipeline

El pipeline sigue cuatro etapas principales:

1. **Ingesta**
   - Lectura de la tabla de fuentes (`sourcelayers.csv`)
   - Descarga de capas desde servicios ArcGIS REST
   - Manejo tanto de capas espaciales como de tablas no espaciales

2. **Reglas**
   - Identificación de la entidad responsable de cada capa
   - Aplicación de reglas diferenciadas según el tipo de capa

3. **Transformaciones**
   - Limpieza y normalización de texto
   - Generación de nuevas variables
   - Cruce con tablas auxiliares precalculadas

4. **Salidas**
   - Exportación de capas procesadas
   - Generación de metadatos de ejecución

---

## Capas utilizadas

Las capas se definen en la tabla `sourcelayers.csv`. Se incluyen:

### Secretaría Distrital de Integración Social (SDIS)
- Centro de Atención Distrital para la Inclusión Social (CADIS)
- Centros Renacer
- Centros Crecer
- Centros Integrarte (atención interna y externa)

## Secretaría de Educación
- Matrícula Total en Colegios Oficiales. Bogotá D.C.

## Secretaría de la Mujer
- Servicios prestados en las Manzanas del Cuidado

### Secretaría General
- Oferta de Discapacidad Bogotá (capa intersectorial consolidada)

---

## Transformaciones aplicadas

Las transformaciones se implementan en `transformaciones.py` e incluyen:

- **Normalización de texto**
  - Conversión a minúsculas
  - Eliminación de tildes y caracteres especiales

- **Identificación de discapacidad**
  - Creación de variables binarias a partir de palabras clave

- **Incorporación de variables precalculadas**
  - Tramo de edad atendido
  - Array de edades
  - Tipo de discapacidad (solo para capas SDIS)
  - Descripción ampliada de servicios SDIS

---

## Reglas diferenciales por capa

Las reglas se implementan en `reglas.py` y determinan qué transformaciones recibe cada capa.

Las capas de Educación y Mujer no reciben tratamiento extra.

### Capas SDIS
- Unión con `sdis_extravar.csv`
- Incorporación de:
  - tramo de edad
  - array de edades
  - tipo de discapacidad
  - descripción del servicio
- Relleno de descripciones faltantes cuando corresponde

### Capa Secretaría General
- Filtrado de registros:
  - eliminación de geometrías nulas o vacías
  - exclusión de ofertas pertenecientes a entidades que ya se procesan como capas propias
- Unión con `secgeneral_extravar.csv`
- Incorporación de variables de edad
- Incorporación de tipo de discapacidad

---

## Salidas del pipeline

Los resultados se guardan en la carpeta `salidas/`:

- Capas espaciales: formato GeoPackage (`.gpkg`)
- Tablas no espaciales: formato CSV (`.csv`)
- Archivo `manifest.json` con información de ejecución:
  - fecha de la corrida
  - capas generadas
  - número de filas y columnas

---

## Ejecución

Desde la raíz del repositorio:

```bash
python scripts/ejecutar_pipeline.py
```

### Requisitos
- Python 3.11 o 3.12
- Uso de entorno virtual recomendado
