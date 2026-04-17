import asyncio
from pathlib import Path
import re
import time

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import dash_ag_grid as dag

# ============================================================
# Links / Configuración
# ============================================================
version = "1.01 (Adaptado 5G)"
data_file = "cu-lan-ho-live.log"

# Vamos a fijar 4 UEs (0, 1, 2, 3) que son los que salen en tu simulador.
# Esto emula perfectamente las "n_cols" del profesor.
n_ues = 4 

# Patrón Regex para la Fase 1
patron = re.compile(r"\[SDAP\s+\].*ue=(\d+).*DL: TX PDU.*pdu_len=(\d+)")

# ============================================================
# Shared asyncio queue
# ============================================================
queue = asyncio.Queue()

# ============================================================
# Shared DataFrame
# ============================================================
# Creamos el DF vacío con una columna para cada UE (UE 0, UE 1, UE 2, UE 3)
agg_df = pl.DataFrame(
    schema={f"UE {i}": pl.Int64 for i in range(n_ues)}
)

# Vector global para almacenar la última lectura
vector_list = [0] * n_ues

# ============================================================
# asyncio producer:
# ============================================================
async def tail_file_producer(path: str, data_queue: asyncio.Queue):
    file = Path(path)
    print("Esperando a:", data_file)

    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r") as f:
        print("Abierto:", data_file)
        f.seek(0, 2) # Solo procesar líneas nuevas
        
        # Buffer de suma de bytes
        sumas_segundo = [0] * n_ues
        t_inicio = time.time()

        while True:
            line = f.readline()

            if line:
                match = patron.search(line)
                if match:
                    ue_id = int(match.group(1))
                    bytes_len = int(match.group(2))
                    
                    # Sumamos los bytes si el UE está dentro de los 4 esperados
                    if ue_id < n_ues:
                        sumas_segundo[ue_id] += bytes_len

            else:
                # Si no hay línea:
                await asyncio.sleep(0.1)

            # --- CADA 1 SEGUNDO AGREGAMOS Y METEMOS EN LA QUEUE ---
            t_actual = time.time()
            if t_actual - t_inicio >= 1.0:
                # Si ha habido datos o es todo 0, lo enviamos igual
                vector = tuple(sumas_segundo)
                
                print("▶ Valores agregados añadidos a la queue:", vector)
                
                # Añadimos a la queue
                await data_queue.put(vector)
                
                # Reseteamos el vector a 0 para el siguiente segundo
                sumas_segundo = [0] * n_ues
                t_inicio = t_actual

# ============================================================
# asyncio consumer:
# ============================================================
async def consumer(data_queue: asyncio.Queue):
    global agg_df, vector_list

    while True:
        vector_tuple = await data_queue.get()

        vector_list = list(vector_tuple)
        print("◀ Valores consumidos:", vector_list)

        # Crea una nueva fila con el vector y la concatena al DataFrame global
        new_row = pl.DataFrame([vector_list], schema=agg_df.columns, orient="row")
        agg_df = pl.concat([agg_df, new_row], how="vertical")

        data_queue.task_done()

# ============================================================
# asyncio Saver (Guarda en Parquet cada 30s)
# ============================================================
async def parquet_saver():
    # Nombre del archivo donde se guardará el histórico
    archivo_parquet = "evolucion_volumenes.parquet"
    
    while True:

        await asyncio.sleep(30)
        
        # si la tabla global tiene datos, la guarda
        if len(agg_df) > 0:
            agg_df.write_parquet(archivo_parquet)
            print(f"[BACKUP PARQUET] Archivo actualizado: {len(agg_df)} segundos de simulación guardados.")

# ============================================================
# Dash app
# ============================================================
app = Dash(__name__)

app.layout = html.Div([
    html.H3("Evolución de volúmenes agregados para cada UE (DL SDAP)"),
    # Grid idéntico al del profesor
    dag.AgGrid(
        id='live-adgrid',
        style={"height": 100, "width": "100%"},
        columnSize='autoSize',
        columnDefs=[{"field": i, "type": "rightAligned"} for i in agg_df.columns],
        rowData=[{j: vector_list[i] for i, j in enumerate(agg_df.columns)}]
    ),
    # Gráfica
    dcc.Graph(id="live-graph"),
    # Refresco de 1 segundo (1000ms)
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])

@app.callback(
    Output("live-graph", "figure"),
    Output("live-adgrid", "rowData"),
    Input("interval", "n_intervals")
)
def update_data(n):
    fig = go.Figure()
    
    for col in agg_df.columns:
        fig.add_trace(go.Scatter(
            y=agg_df[col].to_list(),
            mode='lines+markers',
            name=col
        ))

    fig.update_layout(
        xaxis_title="Segundos de simulación",
        yaxis_title="Bytes Totales Descargados",
        # ¡CLAVE 1! Quitamos el transition_duration para que el navegador no se atasque
        margin=dict(t=30, b=0, l=0, r=0) # Un pequeño ajuste para que se vea más limpia
    )

    # ¡CLAVE 2! La forma más directa y segura de Polars a Dash
    if len(agg_df) > 0:
        # tail(1) coge la última fila, y to_dicts() la convierte automáticamente 
        # al formato exacto que necesita Dash [{'UE 0': X, 'UE 1': Y...}]
        # Así es matemáticamente imposible que se desordenen o se queden pillados.
        row_data = agg_df.tail(1).to_dicts()
    else:
        row_data = [{col: 0 for col in agg_df.columns}]

    return fig, row_data

# ============================================================
# Main
# ============================================================
async def main():
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
    print("Live data Producer-Consumer 5G :: " + version)
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■\n")

    asyncio.create_task(tail_file_producer(data_file, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(parquet_saver()) # para que añada el parquet a las tareas 

    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8050,
        debug=False,
    )

if __name__ == '__main__':
    asyncio.run(main())