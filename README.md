# Actividad U5 – Sistema End-to-End (PySpark + Dashboard Streamlit + PoC Databricks)

**Materia:** Herramientas de Big Data 2025 B2  
**Universidad:** Universidad de La Sabana

## Autores
- ANDRES CAMILO LOPEZ CASTRO
- JUAN DIEGO ROJAS PEÑA
- ANDRES FERNANDO DIAZ MORENO

---

## 1. Objetivo de la actividad
Construir e implementar un sistema **End-to-End** que:

1) Procese datos (backend) con **PySpark** aplicando buenas prácticas/optimización.
2) Genere un dataset **curado** en **Parquet** para consumo eficiente.
3) Alimente un **Dashboard interactivo** (Streamlit) con filtros y visualizaciones.
4) Permita validación en **Databricks Free Edition** como PoC (con evidencia/screenshot).

---

## 2. Estructura del proyecto
> Nota: en el repositorio la carpeta puede llamarse `ejercicio 5/` o `ejercicio 5/`.  
> Si tu carpeta tiene espacios (ej: `ejercicio 5`), usa comillas en la terminal.

```bash
ejercicio 5/
├─ data/
│  ├─ raw/
│  │  ├─ emergencias_123_raw.csv
│  │  ├─ emergencias_123_24m.csv
│  │  ├─ emergencias_123_24m.parquet
│  │  └─ emergencias_123_24m_spark.parquet
│  ├─ curated/
│  │  └─ emergencias_123_curated.parquet
│  ├─ Actividad1/
│  ├─ Actividad2/
│  └─ .venv/
├─ scripts/
│  ├─ step1_prepare_24m.py
│  ├─ fix_parquet_for_spark.py
│  └─ pipeline_spark_curated.py
├─ requirements-dashboard.txt
└─ u4_actividad2_dashboard.py
```

---

## 3. Requisitos (local)
- macOS / Linux recomendado
- Python 3.10+ (en la PoC se usó Python 3.13)
- Java instalado (requerido para Spark / PySpark)
- Paquetes:
  - Dashboard: `streamlit`, `pandas`, `plotly`, `pyarrow`
  - Backend: `pyspark`

---

## 4. Preparación del entorno local

### 4.1 Activar el entorno virtual
> El venv está ubicado en `data/.venv/`.

```bash
cd "ejercicio 5"
source data/.venv/bin/activate
```

### 4.2 Instalar dependencias del dashboard
```bash
pip install -r requirements-dashboard.txt
```

### 4.3 Instalar dependencias del backend (Spark)
```bash
pip install pyspark pyarrow
```

---

## 5. Paso 1 — Preparación del dataset (24 meses o máximo disponible)

### 5.1 Crear carpetas raw/curated
```bash
mkdir -p data/raw data/curated
```

### 5.2 Mover el dataset inicial a raw
Ejemplo (si el CSV estaba en `data/Actividad1/`):

```bash
mv data/Actividad1/emergencias_123_12meses.csv data/raw/emergencias_123_raw.csv
```

### 5.3 Ejecutar el script de preparación
Este script:
- Normaliza nombres de columnas (incluye el caso `MOVIl` vs `MOVIL`)
- Consolida columnas duplicadas (si aparecen tras renombres)
- Convierte la columna de fecha a datetime
- Elimina fechas inválidas o incoherentes
- Filtra a **últimos 24 meses** (o el máximo disponible según el dataset fuente)
- Genera:
  - `data/raw/emergencias_123_24m.csv`
  - `data/raw/emergencias_123_24m.parquet`

Ejecutar:

```bash
python3 scripts/step1_prepare_24m.py
```

**Resultados obtenidos (ejecución real):**
- Rows: **109,042**
- Range: **2024-11-01 → 2025-11-01**
- Unique months: **12**
- Saved: `data/raw/emergencias_123_24m.csv` y `data/raw/emergencias_123_24m.parquet`

> Nota: el resultado quedó en **12 meses** porque el dataset fuente disponible cubre ese rango.  
> Para la actividad se considera válido como “máximo disponible”.

---

## 6. Paso 2 — Fix de Parquet para compatibilidad con Spark (TIMESTAMP NANOS)

### 6.1 Problema detectado
Al leer el Parquet generado por pandas/pyarrow con Spark, apareció el error:

`PARQUET_TYPE_ILLEGAL: Illegal Parquet type: TIMESTAMP(NANOS,false)`

### 6.2 Solución aplicada
Se reescribe el Parquet forzando timestamps a microsegundos (`us`) para compatibilidad con Spark.

Ejecutar:
```bash
python3 scripts/fix_parquet_for_spark.py
```

Salida:
- `data/raw/emergencias_123_24m_spark.parquet`

---

## 7. Paso 3 — Pipeline PySpark (backend) para generar dataset curado

### 7.1 Objetivo del pipeline
- Leer `data/raw/emergencias_123_24m_spark.parquet`
- Asegurar tipo de fecha
- Crear columnas derivadas: `anio`, `mes_num`, `mes`, `dia_semana`, `hora`
- Limpiar strings (`trim`) en categóricas
- Proyectar columnas útiles (select)
- Repartition básico para mejorar paralelismo
- Guardar Parquet curado en: `data/curated/emergencias_123_curated.parquet`

### 7.2 Ejecutar pipeline Spark (local)
```bash
python3 scripts/pipeline_spark_curated.py
```

**Salida esperada (ejecución real):**
- ✅ Pipeline Spark completado
- Registros finales: **109,042**
- Output (curated): `data/curated/emergencias_123_curated.parquet`

---

## 8. Paso 4 — Ejecutar dashboard alimentado por curated (End-to-End)

### 8.1 Ejecutar el dashboard con el curated
```bash
export EMERGENCIAS_PATH=./data/curated/emergencias_123_curated.parquet
streamlit run u4_actividad2_dashboard.py
```

Abrir el enlace que imprime Streamlit (ejemplo):
- `http://localhost:8504`

**Verificación:**
- En el sidebar aparece: `Dataset cargado: ./data/curated/emergencias_123_curated.parquet`
- Métricas visibles (registros, localidades, tipos, prioridades)
- Filtros funcionales (rango de fechas, localidad, tipo, prioridad, etc.)

> Nota: pueden aparecer warnings de Streamlit (deprecaciones como `use_container_width`); no afectan la ejecución.

---

## 9. Validación en Databricks (PoC) — Free Edition

### 9.1 ¿Databricks es pago?
Databricks tiene planes pagos, **pero** existe **Databricks Free Edition** (como el que se usó en la PoC).  
En Free Edition normalmente trabajarás con **Serverless** (no siempre necesitas crear “clusters” clásicos).

### 9.2 Subir datos a Databricks y crear tabla (método usado en la PoC)
Recomendación por compatibilidad: subir el CSV:
- `data/raw/emergencias_123_24m.csv`

En Databricks (UI):
1) **Data Ingestion** → *Upload data*
2) Upload del archivo CSV
3) **Create new table**
4) Schema: `default`
5) Table name sugerido: `emergencias_123_24_m`

### 9.3 Notebook en Databricks (pipeline equivalente, guardando como tabla)
Crear Notebook (Python) y ejecutar:

```python
from pyspark.sql import functions as F

# Tabla RAW creada desde la UI (CSV -> tabla)
df_raw = spark.table("default.emergencias_123_24_m")

fecha_col = "fecha_inicio_desplazamiento_movil"
df = df_raw.withColumn(fecha_col, F.to_timestamp(F.col(fecha_col)))

df = (df
      .withColumn("anio", F.year(F.col(fecha_col)))
      .withColumn("mes_num", F.month(F.col(fecha_col)))
      .withColumn("mes", F.date_format(F.col(fecha_col), "yyyy-MM"))
      .withColumn("dia_semana", F.date_format(F.col(fecha_col), "EEEE"))
      .withColumn("hora", F.hour(F.col(fecha_col))))

for c in ["localidad", "genero", "tipo_incidente", "prioridad_final"]:
    if c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))

candidate_cols = [
    "numero_incidente","fecha_inicio_desplazamiento_movil","codigo_localidad",
    "localidad","edad","unidad","genero","tipo_incidente","prioridad_final",
    "recepcion","anio","mes_num","mes","dia_semana","hora"
]
cols_finales = [c for c in candidate_cols if c in df.columns]
df_final = df.select(*cols_finales)

# Guardar como tabla CURATED en el metastore (método usado en la PoC)
(df_final.write
 .mode("overwrite")
 .saveAsTable("default.emergencias_123_curated"))

print("✅ Curated creado: default.emergencias_123_curated")
print("Registros curated:", df_final.count())
display(df_final.limit(5))
```

### 9.4 Validación (consulta SQL)
```python
spark.sql("SHOW TABLES IN default").show(truncate=False)
spark.sql("SELECT COUNT(*) AS n FROM default.emergencias_123_curated").show()
```

**Resultado esperado (PoC real):**
- Tablas visibles: `emergencias_123_24_m` y `emergencias_123_curated`
- Conteo: `109042`

---

## 10. Evidencias (screenshots)
Crea esta carpeta en el repo y sube allí las capturas:

```bash
mkdir -p docs/evidencias
```

Luego referencia aquí:

```md
![Dashboard Streamlit ejecutándose en local](docs/evidencias/streamlit_dashboard_local.png)
![Databricks - creación de tabla desde CSV](docs/evidencias/databricks_create_table.png)
![Databricks - lectura de tabla RAW (spark.table)](docs/evidencias/databricks_read_raw.png)
![Databricks - creación de tabla curated (saveAsTable)](docs/evidencias/databricks_curated_ok.png)
![Databricks - SHOW TABLES y COUNT curated](docs/evidencias/databricks_show_tables_count.png)
```

---

## 11. Justificación técnica (optimización Spark)
En el backend PySpark se priorizó:
- **Formato Parquet:** reduce I/O y acelera lectura vs CSV.
- **Proyección de columnas (select):** solo columnas necesarias para reducir memoria.
- **Repartition:** mejora paralelismo y reduce cuellos de botella.
- **Tabla curated en Databricks:** deja un artefacto consultable y reutilizable.
- **Trim/normalización:** mejora calidad de datos para filtros y agregaciones del dashboard.

---

## 12. Troubleshooting (errores comunes)

### 12.1 `zsh: command not found: streamlit`
Causa: no está activo el venv o Streamlit no está instalado.

```bash
source data/.venv/bin/activate
pip install -r requirements-dashboard.txt
streamlit run u4_actividad2_dashboard.py
```

### 12.2 Error Spark leyendo Parquet (TIMESTAMP NANOS)
Error típico:
`PARQUET_TYPE_ILLEGAL: ... TIMESTAMP(NANOS,false)`

Solución:
```bash
python3 scripts/fix_parquet_for_spark.py
python3 scripts/pipeline_spark_curated.py
```

### 12.3 “No encontré el dataset…” en el dashboard
```bash
export EMERGENCIAS_PATH=./data/curated/emergencias_123_curated.parquet
```

---

## 13. Checklist de entrega (U5)
- [x] Proyecto con estructura organizada (raw/curated/scripts).
- [x] Pipeline PySpark ejecuta end-to-end sin fallos.
- [x] Dataset curado generado: `data/curated/emergencias_123_curated.parquet`.
- [x] Dashboard Streamlit consume el dataset curado (EMERGENCIAS_PATH) y filtra correctamente.
- [x] PoC en Databricks Free Edition + evidencia (screenshots).
- [x] README incluye instrucciones de instalación/ejecución y justificación técnica.
