import pandas as pd

IN_PARQUET = "data/raw/emergencias_123_24m.parquet"
OUT_PARQUET = "data/raw/emergencias_123_24m_spark.parquet"

df = pd.read_parquet(IN_PARQUET)

# Asegura que la columna de fecha exista y sea datetime
col = "fecha_inicio_desplazamiento_movil"
if col in df.columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# Reescribe Parquet forzando timestamps a microsegundos (Spark-friendly)
df.to_parquet(
    OUT_PARQUET,
    engine="pyarrow",
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True,
)

print("✅ Saved:", OUT_PARQUET)
