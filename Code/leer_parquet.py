import polars as pl

# 1. Le decimos a Polars que el límite de filas es "infinito" (-1)
pl.Config.set_tbl_rows(-1)

# 2. Leemos el archivo
df_guardado = pl.read_parquet("evolucion_volumenes.parquet")

# 2. Imprimimos la tabla en la terminal
print(df_guardado)