from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    hour,
    date_format,
    trim,
)

# Paths de entrada/salida
INPUT_PATH = "data/raw/emergencias_123_24m_spark.parquet"
CURATED_PATH = "data/curated/emergencias_123_curated.parquet"

def main():
    # 🔹 Spark Session optimizada básica para un solo nodo/local
    spark = (
        SparkSession.builder
        .appName("Emergencias123-U5-Curated")
        .config("spark.sql.shuffle.partitions", "8")  # menos particiones para dataset “pequeño”
        .getOrCreate()
    )

    # 1) Leer dataset 24m (ya limpio desde el paso 1)
    df = spark.read.parquet(INPUT_PATH)

    # 2) Normalizar tipos y columnas clave para el dashboard
    # Aseguramos que la columna de fecha exista y tenga tipo timestamp/date
    fecha_col = "fecha_inicio_desplazamiento_movil"
    df = df.withColumn(fecha_col, col(fecha_col).cast("timestamp"))

    # Campos derivados (los mismos que usa el dashboard)
    df = (
        df
        .withColumn("anio", year(col(fecha_col)))
        .withColumn("mes_num", month(col(fecha_col)))
        .withColumn("mes", date_format(col(fecha_col), "yyyy-MM"))
        .withColumn("dia_semana", date_format(col(fecha_col), "EEEE"))
        .withColumn("hora", hour(col(fecha_col)))
    )

    # Limpieza mínima de strings en columnas categóricas
    for c in ["localidad", "genero", "tipo_incidente", "prioridad_final"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    # 3) Proyección: nos quedamos solo con columnas útiles para análisis/dashboard
    candidate_cols = [
        "numero_incidente",
        "fecha_inicio_desplazamiento_movil",
        "codigo_localidad",
        "localidad",
        "edad",
        "unidad",
        "genero",
        "tipo_incidente",
        "prioridad_final",
        "recepcion",
        "anio",
        "mes_num",
        "mes",
        "dia_semana",
        "hora",
    ]
    cols_finales = [c for c in candidate_cols if c in df.columns]
    df_final = df.select(*cols_finales)

    # 4) Reparticionamiento (optimización sencilla)
    #    Usamos anio/mes_num para distribuir los datos de forma más uniforme.
    if "anio" in df_final.columns and "mes_num" in df_final.columns:
        df_final = df_final.repartition("anio", "mes_num")
    else:
        df_final = df_final.repartition(8)

    # 5) Guardar como dataset Parquet “curado”
    (
        df_final.write
        .mode("overwrite")
        .parquet(CURATED_PATH)
    )

    # 6) Log básico para tu reporte / README
    total = df_final.count()
    print("✅ Pipeline Spark completado")
    print(f"   Registros finales: {total}")
    print(f"   Output (curated): {CURATED_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
