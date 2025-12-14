import pandas as pd
from dateutil.relativedelta import relativedelta

IN_PATH = "data/raw/emergencias_123_raw.csv"
OUT_CSV = "data/raw/emergencias_123_24m.csv"
OUT_PARQUET = "data/raw/emergencias_123_24m.parquet"

df = pd.read_csv(IN_PATH, low_memory=False)

# Normaliza nombres (incluye el caso MOVIl vs MOVIL)
rename_map = {
    "FECHA_INICIO_DESPLAZAMIENTO_MOVIl": "fecha_inicio_desplazamiento_movil",
    "FECHA_INICIO_DESPLAZAMIENTO_MOVIL": "fecha_inicio_desplazamiento_movil",
}
df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

# Si quedaron columnas duplicadas, consolídalas (primero no nulo)
if df.columns.duplicated().any():
    dup = df.columns[df.columns.duplicated()].unique()
    for name in dup:
        cols = df.loc[:, df.columns == name]
        df[name] = cols.bfill(axis=1).iloc[:, 0]
    df = df.loc[:, ~df.columns.duplicated()]

# Parse fecha
col = "fecha_inicio_desplazamiento_movil"
df[col] = pd.to_datetime(df[col], errors="coerce")

# Limpieza básica de fechas imposibles (esto elimina tu “1910”)
df = df[df[col].notna()]
df = df[df[col] >= pd.Timestamp("2015-01-01")]  # ajusta si tu dataset es más antiguo

# Ventana últimos 24 meses (o lo máximo disponible)
max_dt = df[col].max()
start_dt = (max_dt - relativedelta(months=24))
df_24m = df[df[col] >= start_dt].copy()

# Resumen rápido (para evidencia)
months = df_24m[col].dt.to_period("M").nunique()
print("Rows:", len(df_24m))
print("Range:", df_24m[col].min(), "→", df_24m[col].max())
print("Unique months:", months)

# Guardar
df_24m.to_csv(OUT_CSV, index=False)
df_24m.to_parquet(OUT_PARQUET, index=False)
print("Saved:", OUT_CSV, "and", OUT_PARQUET)