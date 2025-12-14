# u4_actividad2_dashboard.py
# Dashboard interactivo (Streamlit) para el dataset de Emergencias 123 Bogotá.
# - Lee CSV o Parquet (preferible Parquet si vienes de Spark).
# - Permite filtros y genera visualizaciones interactivas (Plotly).
#
# Ejecución:
#   streamlit run u4_actividad2_dashboard.py
#
# Variables de entorno opcionales:
#   EMERGENCIAS_PATH=/ruta/al/dataset.(csv|parquet)

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st


DEFAULT_PATHS = [
    os.getenv("EMERGENCIAS_PATH", "").strip(),
    # Rutas “típicas”
    "data/emergencias_123_12meses.csv",
    "data/emergencias_123_12meses_normalizado.csv",
    "data/curated/emergencias_123_curated.parquet",
    "emergencias_123_12meses.csv",
    # La estructura de la  (Actividad1)
    "data/Actividad1/emergencias_123_12meses.csv",
    "data/Actividad1/emergencias_123_12meses_normalizado.csv",
    "Actividad1/emergencias_123_12meses.csv",
    "Actividad1/emergencias_123_12meses_normalizado.csv",
]


def _first_existing_path(paths: list[str]) -> str | None:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None


def _consolidate_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df

    dup_names = df.columns[df.columns.duplicated()].unique()
    for name in dup_names:
        cols = df.loc[:, df.columns == name]
        # primera no-nula de izquierda a derecha
        df[name] = cols.bfill(axis=1).iloc[:, 0]

    df = df.loc[:, ~df.columns.duplicated()]
    return df


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        df = pd.read_parquet(p)
    else:
        df = pd.read_csv(p, low_memory=False)

    # Normalizar nombres de columnas esperadas
    rename_map = {
        "NUMERO_INCIDENTE": "numero_incidente",
        "FECHA_INICIO_DESPLAZAMIENTO_MOVIl": "fecha_inicio_desplazamiento_movil",
        "FECHA_INICIO_DESPLAZAMIENTO_MOVIL": "fecha_inicio_desplazamiento_movil",
        "CODIGO_LOCALIDAD": "codigo_localidad",
        "LOCALIDAD": "localidad",
        "EDAD": "edad",
        "UNIDAD": "unidad",
        "GENERO": "genero",
        "TIPO_INCIDENTE": "tipo_incidente",
        "PRIORIDAD FINAL": "prioridad_final",
        "PRIORIDAD_FINAL": "prioridad_final",
        "RECEPCION": "recepcion",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Evita ValueError por columnas duplicadas tras el rename
    df = _consolidate_duplicate_columns(df)

    # Parse fechas
    if "fecha_inicio_desplazamiento_movil" in df.columns:
        col = df["fecha_inicio_desplazamiento_movil"]
        # (por seguridad) si aún llegara como DataFrame, tomar primera columna
        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]
        df["fecha_inicio_desplazamiento_movil"] = pd.to_datetime(col, errors="coerce")

    # Tipos
    if "edad" in df.columns:
        df["edad"] = pd.to_numeric(df["edad"], errors="coerce")

    # Campos derivados para gráficos
    if "fecha_inicio_desplazamiento_movil" in df.columns:
        dt = df["fecha_inicio_desplazamiento_movil"]
        df["anio"] = dt.dt.year
        df["mes_num"] = dt.dt.month
        df["mes"] = dt.dt.strftime("%Y-%m")
        df["dia_semana"] = dt.dt.day_name()
        df["hora"] = dt.dt.hour

    # Limpieza mínima
    for c in ["localidad", "genero", "tipo_incidente", "prioridad_final"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


def mental_health_mask(series: pd.Series) -> pd.Series:
    pattern = re.compile(r"(SALUD\s*MENTAL|MENTAL|PSIQUI|PSIC|SUICID|AUTOLES)", re.IGNORECASE)
    return series.fillna("").astype(str).str.contains(pattern)


def main() -> None:
    st.set_page_config(page_title="Emergencias 123 – Dashboard", layout="wide")
    st.title("Emergencias 123 Bogotá – Dashboard Interactivo")

    st.sidebar.header("Fuente de datos")
    path = _first_existing_path([p for p in DEFAULT_PATHS if p])

    uploaded = st.sidebar.file_uploader("O sube un archivo (CSV o Parquet)", type=["csv", "parquet"])
    if uploaded is not None:
        suffix = f".{uploaded.name.split('.')[-1]}" if "." in uploaded.name else ""
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(uploaded.getbuffer())
        tmp.close()
        path = tmp.name

    if not path:
        st.error(
            "No encontré el dataset.\n\n"
            "Opciones:\n"
            "1) Colócalo en `data/Actividad1/` o `data/`\n"
            "2) O define la variable de entorno `EMERGENCIAS_PATH`.\n\n"
            "Ejemplo:\n"
            "  export EMERGENCIAS_PATH=./data/Actividad1/emergencias_123_12meses.csv"
        )
        st.stop()

    df = load_data(path)

    st.sidebar.caption(f"📌 Dataset cargado: {path}")

    # Validación mínima de columnas
    required = ["fecha_inicio_desplazamiento_movil", "tipo_incidente", "localidad", "genero", "prioridad_final"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.warning(f"Faltan columnas esperadas: {missing}. El dashboard intentará funcionar igual.")

    st.sidebar.header("Filtros")

    # Rango fechas
    if "fecha_inicio_desplazamiento_movil" in df.columns:
        min_d = df["fecha_inicio_desplazamiento_movil"].min()
        max_d = df["fecha_inicio_desplazamiento_movil"].max()
        if pd.isna(min_d) or pd.isna(max_d):
            date_range = None
        else:
            date_range = st.sidebar.date_input(
                "Rango de fechas",
                value=(min_d.date(), max_d.date()),
                min_value=min_d.date(),
                max_value=max_d.date(),
            )
    else:
        date_range = None

    def _multiselect(col: str, label: str):
        if col in df.columns:
            opts = sorted([x for x in df[col].dropna().unique().tolist() if x and x != "nan"])
            return st.sidebar.multiselect(label, opts)
        return []

    sel_localidad = _multiselect("localidad", "Localidad")
    sel_tipo = _multiselect("tipo_incidente", "Tipo de incidente")
    sel_prioridad = _multiselect("prioridad_final", "Prioridad")
    sel_genero = _multiselect("genero", "Género")

    f = df.copy()

    if (
            date_range
            and isinstance(date_range, tuple)
            and len(date_range) == 2
            and "fecha_inicio_desplazamiento_movil" in f.columns
    ):
        start, end = date_range
        f = f[
            (f["fecha_inicio_desplazamiento_movil"].dt.date >= start)
            & (f["fecha_inicio_desplazamiento_movil"].dt.date <= end)
            ]

    if sel_localidad and "localidad" in f.columns:
        f = f[f["localidad"].isin(sel_localidad)]
    if sel_tipo and "tipo_incidente" in f.columns:
        f = f[f["tipo_incidente"].isin(sel_tipo)]
    if sel_prioridad and "prioridad_final" in f.columns:
        f = f[f["prioridad_final"].isin(sel_prioridad)]
    if sel_genero and "genero" in f.columns:
        f = f[f["genero"].isin(sel_genero)]

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Registros (filtrados)", f"{len(f):,}".replace(",", "."))
    if "localidad" in f.columns:
        k2.metric("Localidades únicas", int(f["localidad"].nunique()))
    if "tipo_incidente" in f.columns:
        k3.metric("Tipos únicos", int(f["tipo_incidente"].nunique()))
    if "prioridad_final" in f.columns:
        k4.metric("Prioridades únicas", int(f["prioridad_final"].nunique()))

    st.divider()

    colA, colB = st.columns(2)

    # 1) Incidentes por mes
    if "mes" in f.columns:
        g = f.groupby("mes", as_index=False).size().rename(columns={"size": "incidentes"}).sort_values("mes")
        fig = px.line(g, x="mes", y="incidentes", markers=True, title="Incidentes por mes")
        colA.plotly_chart(fig, use_container_width=True)

    # 2) Evolución salud mental (heurística)
    if "mes" in f.columns and "tipo_incidente" in f.columns:
        mh = f[mental_health_mask(f["tipo_incidente"])].copy()
        if len(mh) > 0:
            g = mh.groupby("mes", as_index=False).size().rename(columns={"size": "incidentes"}).sort_values("mes")
            fig = px.line(g, x="mes", y="incidentes", markers=True, title="Evolución de incidentes (salud mental / relacionados)")
            colB.plotly_chart(fig, use_container_width=True)
        else:
            colB.info("No se encontraron incidentes que coincidan con la heurística de salud mental (ajusta el patrón si aplica).")

    colC, colD = st.columns(2)

    # 3) Top 10 tipos de incidente
    if "tipo_incidente" in f.columns:
        g = f["tipo_incidente"].value_counts().head(10).reset_index()
        g.columns = ["tipo_incidente", "incidentes"]
        fig = px.bar(g, x="incidentes", y="tipo_incidente", orientation="h", title="Top 10 tipos de incidente")
        colC.plotly_chart(fig, use_container_width=True)

    # 4) Incidentes por localidad (Top 15)
    if "localidad" in f.columns:
        g = f["localidad"].value_counts().head(15).reset_index()
        g.columns = ["localidad", "incidentes"]
        fig = px.bar(g, x="incidentes", y="localidad", orientation="h", title="Incidentes por localidad (Top 15)")
        colD.plotly_chart(fig, use_container_width=True)

    colE, colF = st.columns(2)

    # 5) Distribución por género
    if "genero" in f.columns:
        g = f["genero"].value_counts().reset_index()
        g.columns = ["genero", "incidentes"]
        fig = px.pie(g, names="genero", values="incidentes", title="Distribución por género")
        colE.plotly_chart(fig, use_container_width=True)

    # 6) Prioridad por tipo (Top 10)
    if "prioridad_final" in f.columns and "tipo_incidente" in f.columns:
        top = f["tipo_incidente"].value_counts().head(10).index.tolist()
        g = (
            f[f["tipo_incidente"].isin(top)]
            .groupby(["tipo_incidente", "prioridad_final"], as_index=False)
            .size()
            .rename(columns={"size": "incidentes"})
        )
        fig = px.bar(
            g,
            x="incidentes",
            y="tipo_incidente",
            color="prioridad_final",
            orientation="h",
            title="Prioridad por tipo de incidente (Top 10)",
            barmode="stack",
        )
        colF.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 7) Heatmap día vs hora
    if "dia_semana" in f.columns and "hora" in f.columns:
        order_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        pivot = (
            f.groupby(["dia_semana", "hora"]).size().reset_index(name="incidentes")
            .pivot(index="dia_semana", columns="hora", values="incidentes")
            .fillna(0)
        )
        pivot = pivot.reindex([d for d in order_days if d in pivot.index] + [d for d in pivot.index if d not in order_days])
        fig = px.imshow(pivot, aspect="auto", title="Heatmap: día de la semana vs hora (conteo de incidentes)")
        st.plotly_chart(fig, use_container_width=True)

    st.caption(f"Dataset: {path}")


if __name__ == "__main__":
    main()
