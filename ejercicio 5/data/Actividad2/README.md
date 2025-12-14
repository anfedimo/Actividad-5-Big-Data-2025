# Actividad 2 – Dashboard Interactivo y Análisis de Valor (U4)

Este proyecto contiene el dashboard interactivo y el análisis derivado de los datos
consolidados del servicio de emergencias 123 de Bogotá, obtenidos en la Actividad 1.

## Contenidos

1. Código del dashboard (`Dashboard.py`)
2. Gráficos interactivos:
   - Incidentes por mes
   - Evolución de incidentes de salud mental
   - Top 10 tipos de incidentes
   - Incidentes por localidad
   - Distribución por género
   - Heatmap día vs hora
   - Prioridad por tipo de incidente
3. Informe PDF con insights:
   `U4_Actividad2_Dashboard_Interactivo_Insights_de_Negocio.pdf`

## Requisitos

- Python 3
- pandas
- plotly
- seaborn
- numpy

## Ejecución del Dashboard

Ejecuta: streamlit run u4_actividad2_dashboard.py


## Dataset utilizado

Este dashboard se alimenta del dataset consolidado en la Actividad 1:

`emergencias_123_12meses.csv`

El archivo se genera automáticamente al ejecutar la Actividad 1.

## Ejecucion Local

```text
source data/.venv/bin/activate
pip install -r requirements-dashboard.txt
export EMERGENCIAS_PATH=./data/raw/emergencias_123_24m.parquet
streamlit run u4_actividad2_dashboard.py
```

## Notas

- Todas las visualizaciones están justificadas según las mejores prácticas de análisis visual.
- Incluye 5 insights siguiendo la estructura:
  Hallazgo | Evidencia | Impacto | Recomendación


