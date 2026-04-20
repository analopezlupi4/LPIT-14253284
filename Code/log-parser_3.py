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
version = "2.0 (Fase 1 + Fase 2)"
data_file = "cu-lan-ho-live.log"  
n_ues = 4
 
# --- [NUEVO] Patrones Regex ---
# Fase 1: Volumen DL
patron_sdap = re.compile(r"\[SDAP\s+\].*ue=(\d+).*DL: TX PDU.*pdu_len=(\d+)")
# Fase 2: Información de Red (Extrae UE, PLMN, PCI y RNTI)
patron_info = re.compile(r"ue=(\d+).*?plmn=(\d+).*?pci=(\d+).*?rnti=(0x[0-9a-fA-F]+)")
 
# ============================================================
# Shared asyncio queue & Data
# ============================================================
queue = asyncio.Queue()
 
agg_df = pl.DataFrame(
    schema={f"UE {i}": pl.Int64 for i in range(n_ues)}
)
 
vector_list = [0] * n_ues
 
# --- [NUEVO] Diccionario para guardar la info de red de cada UE ---
info_ues = {i: {"PLMN": "-", "PCI": "-", "RNTI": "-"} for i in range(n_ues)}
 
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
        f.seek(0, 2) 
        sumas_segundo = [0] * n_ues
        t_inicio = time.time()
 
        while True:
            line = f.readline()
 
            if line:
                # 1. Búsqueda de datos SDAP (Fase 1)
                match_sdap = patron_sdap.search(line)
                if match_sdap:
                    ue_id = int(match_sdap.group(1))
                    bytes_len = int(match_sdap.group(2))
                    if ue_id < n_ues:
                        sumas_segundo[ue_id] += bytes_len
 
                # 2. --- [NUEVO] Búsqueda de Información de Red (Fase 2) ---
                match_info = patron_info.search(line)
                if match_info:
                    ue_id = int(match_info.group(1))
                    if ue_id < n_ues:
                        # Guardamos los valores directamente en nuestro diccionario global
                        info_ues[ue_id]["PLMN"] = match_info.group(2)
                        info_ues[ue_id]["PCI"]  = match_info.group(3)
                        info_ues[ue_id]["RNTI"] = match_info.group(4)
                        print(f"[*] Detectado UE {ue_id} -> PLMN:{info_ues[ue_id]['PLMN']} | PCI:{info_ues[ue_id]['PCI']} | RNTI:{info_ues[ue_id]['RNTI']}")
 
            else:
                await asyncio.sleep(0.1)
 
            # --- CADA 1 SEGUNDO AGREGAMOS ---
            t_actual = time.time()
            if t_actual - t_inicio >= 1.0:
                vector = tuple(sumas_segundo)
                await data_queue.put(vector)
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
        new_row = pl.DataFrame([vector_list], schema=agg_df.columns, orient="row")
        agg_df = pl.concat([agg_df, new_row], how="vertical")
 
        data_queue.task_done()
 
# ============================================================
# asyncio Saver
# ============================================================
async def parquet_saver():
    archivo_parquet = "evolucion_volumenes.parquet"
    while True:
        await asyncio.sleep(30)
        if len(agg_df) > 0:
            agg_df.write_parquet(archivo_parquet)
            print(f"[BACKUP] Archivo actualizado: {len(agg_df)} segundos.")
 
# ============================================================
# Dash app
# ============================================================
app = Dash(__name__)
 
app.layout = html.Div([
    html.H3("Monitorización 5G y Parámetros de Red (Fase 1 y 2)"),
    # --- [NUEVO] Grid Transpuesto para mostrar las propiedades limpiamente ---
    dag.AgGrid(
        id='live-adgrid',
        style={"height": 200, "width": "100%"}, # Altura aumentada
        columnSize='autoSize',
        columnDefs=[
            {"field": "UE", "headerName": "ID Usuario"},
            {"field": "Bytes", "headerName": "Volumen (Último Seg)", "type": "rightAligned"},
            {"field": "PLMN"},
            {"field": "PCI"},
            {"field": "RNTI"}
        ],
        rowData=[]
    ),
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])
 
@app.callback(
    Output("live-graph", "figure"),
    Output("live-adgrid", "rowData"),
    Input("interval", "n_intervals")
)
def update_data(n):
    # 1. Dibujar gráfica
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
        margin=dict(t=30, b=0, l=0, r=0)
    )
 
    # 2. --- [NUEVO] Ensamblar datos de la tabla cruzando 'vector_list' y 'info_ues' ---
    row_data = []
    for i in range(n_ues):
        row_data.append({
            "UE": f"UE {i}",
            "Bytes": vector_list[i],
            "PLMN": info_ues[i]["PLMN"],
            "PCI": info_ues[i]["PCI"],
            "RNTI": info_ues[i]["RNTI"]
        })
 
    return fig, row_data
 
# ============================================================
# Main
# ============================================================
async def main():
    print("Iniciando...")
    asyncio.create_task(tail_file_producer(data_file, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(parquet_saver())
 
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8051,
        debug=False,
    )
 
if __name__ == '__main__':
    asyncio.run(main())