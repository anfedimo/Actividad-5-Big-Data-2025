# Actividad U5 – Sistema End-to-End (PySpark + Dashboard Streamlit)
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
4) Permita validación en **Databricks Free Edition** como PoC (evidencia/screenshot).

---

## 2. Estructura del proyecto
Estructura final recomendada:

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
│  ├─ Actividad1/                # (opcional) origen inicial del CSV
│  ├─ Actividad2/                # (opcional) notebook/archivos del dashboard
│  └─ .venv/                     # entorno virtual (venv) usado para ejecución local
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
- Python 3.10+ (se usó Python 3.13)
- Java instalado (requerido para Spark)
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

**Evidencia ejemplo (resultado real):**
- Rows: 109042
- Range: 2024-11-01 → 2025-11-01
- Unique months: 12
- Saved: `data/raw/emergencias_123_24m.csv` y `data/raw/emergencias_123_24m.parquet`

> Nota: el resultado quedó en **12 meses** porque el dataset fuente disponible cubre ese rango. Para la actividad, se considera válido como “máximo disponible”.

---

## 6. Paso 2 — Fix de Parquet para compatibilidad con Spark (TIMESTAMP NANOS)

### 6.1 Problema detectado
Al leer el Parquet generado por pandas/pyarrow con Spark, apareció el error:

`PARQUET_TYPE_ILLEGAL: Illegal Parquet type: TIMESTAMP(NANOS,false)`

### 6.2 Solución aplicada
Se reescribe el Parquet forzando timestamps a microsegundos (`us`) para compatibilidad Spark.

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
- Crear columnas derivadas:
    - `anio`, `mes_num`, `mes`, `dia_semana`, `hora`
- Limpiar strings (`trim`) en categóricas
- Proyectar columnas útiles (select)
- Repartition básico para mejorar paralelismo
- Guardar Parquet curado en:
    - `data/curated/emergencias_123_curated.parquet`

### 7.2 Ejecutar pipeline Spark (local)
```bash
python3 scripts/pipeline_spark_curated.py
```

**Salida esperada:**
- ✅ Pipeline Spark completado
- Registros finales: 109042
- Output (curated): `data/curated/emergencias_123_curated.parquet`

---

## 8. Paso 4 — Ejecutar dashboard alimentado por curated (End-to-End)

### 8.1 Ejecutar el dashboard con el curated
```bash
export EMERGENCIAS_PATH=./data/curated/emergencias_123_curated.parquet
streamlit run u4_actividad2_dashboard.py
```

Abrir el enlace que imprime Streamlit, por ejemplo:
- `http://localhost:8504`

**Verificación:**
- En el sidebar aparece: `Dataset cargado: ./data/curated/emergencias_123_curated.parquet`
- Métricas visibles (registros, localidades, tipos, prioridades)
- Filtros funcionales (rango de fechas, localidad, tipo, prioridad, etc.)

> Nota: pueden aparecer warnings de Streamlit (por deprecaciones como `use_container_width`); no afectan la ejecución.

---

## 9. Validación en Databricks (PoC) — Free Edition

### 9.1 Objetivo
Validar que el pipeline (o su equivalente) se ejecuta en Databricks y dejar evidencia (screenshot).

### 9.2 Subir datos a Databricks
Recomendación por compatibilidad: subir el CSV:
- `data/raw/emergencias_123_24m.csv`

En Databricks:
1) Home → **Upload data**
2) Upload el archivo CSV
3) Copiar la ruta (usualmente queda bajo `dbfs:/FileStore/...`)

### 9.3 Notebook en Databricks (pipeline equivalente)
Crear Notebook (Python) y ejecutar un pipeline equivalente (ejemplo):

```python
from pyspark.sql import functions as F

INPUT_PATH = "dbfs:/FileStore/tables/emergencias_123_24m.csv"
CURATED_PATH = "dbfs:/FileStore/tables/emergencias_123_curated"

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(INPUT_PATH))

fecha_col = "fecha_inicio_desplazamiento_movil"
df = df.withColumn(fecha_col, F.to_timestamp(F.col(fecha_col)))

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

(df_final.write
 .mode("overwrite")
 .partitionBy("anio","mes_num")
 .parquet(CURATED_PATH))

print("✅ Curated guardado en:", CURATED_PATH)
print("Registros:", df_final.count())
display(df_final.limit(5))
```

### 9.4 Evidencia requerida
Tomar screenshot donde se vea:
- El notebook ejecutado correctamente (sin errores)
- `✅ Curated guardado en: ...`
- El conteo de registros
- Un `display(df_final.limit(5))`

Guardar la evidencia en el repo, por ejemplo:
- `docs/evidencias/databricks_ok.png`

Y referenciarla aquí:

```md
![Ejecución exitosa en Databricks](docs/evidencias/databricks_ok.png)
```

---

## 10. Justificación técnica (optimización Spark)
En el backend PySpark se priorizó:
- **Formato Parquet:** reduce I/O y acelera lectura vs CSV.
- **Proyección de columnas (select):** solo columnas necesarias para reducir memoria.
- **Repartition:** mejora paralelismo y reduce cuellos de botella.
- **Particionamiento (Databricks):** `partitionBy(anio, mes_num)` para consultas/lecturas más eficientes.
- **Trim/normalización:** mejora calidad de datos para filtros y agregaciones del dashboard.

---

## 11. Troubleshooting (errores comunes)

### 11.1 `zsh: command not found: streamlit`
Causa: no está activo el venv o Streamlit no está instalado.

Solución:
```bash
source data/.venv/bin/activate
pip install -r requirements-dashboard.txt
streamlit run u4_actividad2_dashboard.py
```

### 11.2 Error Spark leyendo Parquet (TIMESTAMP NANOS)
Error típico:
`PARQUET_TYPE_ILLEGAL: ... TIMESTAMP(NANOS,false)`

Solución:
```bash
python3 scripts/fix_parquet_for_spark.py
python3 scripts/pipeline_spark_curated.py
```

### 11.3 “No encontré el dataset…” en el dashboard
Solución: exportar la variable de entorno o ubicar el archivo donde el dashboard lo espera:

```bash
export EMERGENCIAS_PATH=./data/curated/emergencias_123_curated.parquet
```

---

## 12. Checklist de entrega (U5)
- [ ] Proyecto con estructura organizada (raw/curated/scripts).
- [ ] Pipeline PySpark ejecuta end-to-end sin fallos.
- [ ] Dataset curado generado: `data/curated/emergencias_123_curated.parquet`.
- [ ] Dashboard Streamlit consume el dataset curado (EMERGENCIAS_PATH) y filtra correctamente.
- [ ] PoC en Databricks Free Edition + screenshot en el README.
- [ ] README incluye instrucciones de instalación/ejecución y justificación técnica.
