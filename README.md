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
│       ├── configuracion.py      # rutas y constantes
│       ├── fechas.py             # actualiza las fechas de sourcelayers.csv
│       ├── ingesta.py            # descarga desde ArcGIS REST
│       ├── reglas.py             # reglas por capa / entidad
│       ├── transformaciones.py   # normalización, indicadores, uniones con tablas de apoyo
│       ├── salida.py             # homogeneización de columnas y escritura
│       └── apoyo/
│           ├── sourcelayers.csv
│           ├── sdis_extravar.csv
│           └── secgeneral_extravar.csv
├── scripts/
│   ├── ejecutar_pipeline.py
│   └── actualizar_fechas.py
├── salidas/
├── requirements.txt
└── README.md
```

---

## Flujo general del pipeline

`scripts/ejecutar_pipeline.py` corre cinco etapas:

0. **Fechas de actualización**
   - Actualiza la columna `Fecha de actualización` de `sourcelayers.csv` con la fecha que publica cada fuente: la última edición de datos del servicio ArcGIS (`editingInfo`) o, si el servicio no la publica, la fecha de la ficha en Datos Abiertos Bogotá.
   - Si una consulta falla, se deja la fecha anterior y el pipeline sigue.

1. **Lectura de fuentes**
   - Lectura de la tabla de fuentes (`sourcelayers.csv`)

2. **Ingesta**
   - Descarga completa de cada capa desde su servicio ArcGIS REST, paginando según `maxRecordCount`, en EPSG:4326
   - Se descartan los registros sin geometría

3. **Reglas y transformaciones**
   - Filtros, uniones con tablas de apoyo y variables nuevas según la capa (ver abajo)

4. **Salidas**
   - Homogeneización de nombres de columnas y exportación de las capas procesadas
   - Generación de metadatos de ejecución (`manifest.json`)

---

## Capas utilizadas

Las capas se definen en la tabla `src/pipeline_datos/apoyo/sourcelayers.csv`. Se incluyen:

### Secretaría Distrital de Integración Social (SDIS)
- Centro de Atención Distrital para la Inclusión Social (CADIS)
- Centros Crecer
- Centros Integrarte (atención interna y externa)

### Secretaría de Educación (SED)
- Colegios. Bogotá D.C. (oficiales y no oficiales)

### Secretaría General
- Oferta de Discapacidad Bogotá (capa intersectorial consolidada)

### Secretaría de la Mujer (SDMujer)
- Servicios prestados en las Manzanas del Cuidado

---

## Transformaciones comunes

Se implementan en `transformaciones.py` y `salida.py`:

- **Normalización de texto** para comparar nombres y buscar palabras: minúsculas, sin tildes, espacios colapsados.

- **Indicadores de tipo de discapacidad** (SDIS, Colegios y Secretaría General): ocho variables binarias (`disc_visual`, `disc_auditiva`, `disc_fisica`, `disc_intelectual`, `disc_psicosocial`, `disc_multiple`, `disc_voz_habla`, `disc_otra`) a partir de palabras clave (`KW_DISCAPACIDAD`) buscadas en un campo de texto de cada fuente:

  | Capa | Campo de texto |
  |---|---|
  | SDIS | `OSSPBenef` (población beneficiaria) |
  | Colegios | `DISCAPACID` |
  | Secretaría General | `ofer_descr` |

  «sistémica» cuenta como múltiple; «TEA» y «autismo» activan intelectual y psicosocial; «otra» y «tea» solo cuentan como palabra completa.

- **Temática del servicio** (`tematica_servicio`) en todas las capas.

- **Nombres comunes de columnas** en las salidas (`direccion`, `telefono`, `horario`, `correo`, `poblacion_atendida`, `descripcion_oferta`, `tipo_discapacidad_str`), sin importar cómo se llamen en cada fuente.

---

## Reglas diferenciales por capa

Las reglas se implementan en `reglas.py` y determinan qué transformaciones recibe cada capa.

### Capas SDIS
- Las cuatro capas se unen en una sola, `cuidados_sdis`; `origen_sdis` indica la capa de origen
- Unión con `sdis_extravar.csv` (una fila por servicio) por el campo `ModServ`, no por el nombre de la sede
- Incorporación de:
  - tramo de edad
  - array de edades
  - descripción del servicio
- Tipo de discapacidad calculado desde el dato (`OSSPBenef`): indicadores `disc_*` y `tipo_discapacidad_str` (p. ej. «Intelectual, Múltiple»; «Sin especificar» si no menciona ninguno)
- Aviso si llega un servicio sin fila en el CSV
- `tematica_servicio`: «Cuidados y apoyos directos»
- La capa `centro-renacer` se retiró en octubre de 2026: el endpoint pasó a publicar comunidades de cuidado para habitantes de calle adultos

### Capa Educación (Colegios)
- Filtrado: solo colegios que reportan algún tipo de discapacidad atendida (`DISCAPACID` no vacío y distinto de «NINGUNA»)
- Indicadores `disc_*` desde `DISCAPACID`; `DISCAPACID` se publica como `tipo_discapacidad_str`
- `tematica_servicio`: «Educación inclusiva»

### Capa Secretaría General
- Filtrado de registros:
  - eliminación de geometrías nulas o vacías
  - exclusión de los sectores que ya se procesan como capas propias (Educación, Mujer, Integración Social, Inclusión Social y Reconciliación)
- Unión con `secgeneral_extravar.csv` (una fila por nombre de oferta) por `ofer_nombr`: tramo de edad y array de edades
- Indicadores `disc_*` desde la descripción de la oferta (`ofer_descr`)
- `tematica_servicio`: el nombre del sector; la oferta de Gobierno se clasifica por palabras clave de su descripción (`KW_TEMATICA`)

### Capa Secretaría de la Mujer (Manzanas del Cuidado)
- Filtrado por tipo de servicio (`TIPO_SERVI`): se conservan los códigos 1 (Para las Cuidadoras) y 3 (Para las Familias de las cuidadoras y comunidad). Se acepta el código o su descripción.
- Se excluye el código 2 (Para quienes requieren cuidado): es oferta general de cuidado, y lo que tiene de discapacidad (Centros Crecer) ya entra por SDIS
- `tematica_servicio`: «Cuidadoras»

---

## Salidas del pipeline

Los resultados se guardan en la carpeta `salidas/`:

- Capas espaciales: formato GeoPackage (`.gpkg`), una por tema: `cuidados_sdis`, `educacion`, `oferta_discapacidad_secgeneral`, `serviciosmanzanas`
- Tablas no espaciales (si las hubiera): formato CSV (`.csv`)
- Archivo `manifest.json` con información de ejecución:
  - fecha de la corrida
  - capas generadas
  - número de filas y columnas, y nombres de columnas

Las capas `.gpkg` se versionan en el repositorio; `manifest.json` se genera en cada corrida pero no se versiona (está en `.gitignore`).

---

## Ejecución

Desde la raíz del repositorio:

```bash
pip install -r requirements.txt
python scripts/ejecutar_pipeline.py
```

Para actualizar solo las fechas de `sourcelayers.csv`, sin correr el pipeline:

```bash
python scripts/actualizar_fechas.py            # actualiza el CSV
python scripts/actualizar_fechas.py --dry-run  # solo muestra los cambios
```

### Requisitos
- Python 3.11 o 3.12
- Uso de entorno virtual recomendado
