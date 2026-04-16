import time
import re
import polars as pl
import threading
import os
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# --- 1. RUTA CORRECTA (EL ARCHIVO LIVE) ---
# Según tu simulador, el archivo vivo es este:
ruta_log = 'cu-lan-ho-live.log' 

# Regex mejorado
patron = re.compile(r"^(\S+)\s+\[SDAP\s+\].*ue=(\d+).*DL: TX PDU.*pdu_len=(\d+)")

historial_velocidades = []

def procesar_log_en_vivo():
    global historial_velocidades
    buffer_datos = []
    t_inicio = time.time()

    print(f"Buscando el archivo vivo: {ruta_log}...")
    
    # Esperamos a que el simulador cree el archivo si no existe todavía
    while not os.path.exists(ruta_log):
        print("Esperando a que el simulador empiece a escribir...")
        time.sleep(2)

    with open(ruta_log, "r") as archivo:
        # IMPORTANTE: Empezamos desde el principio del archivo vivo
        while True:
            linea = archivo.readline()
            if linea:
                match = patron.search(linea)
                if match:
                    buffer_datos.append({
                        "hora_log": match.group(1),
                        "ue": match.group(2),
                        "bytes": int(match.group(3))
                    })
            else:
                time.sleep(0.1)

            t_actual = time.time()
            if t_actual - t_inicio >= 1.0:
                if len(buffer_datos) > 0:
                    df = pl.DataFrame(buffer_datos)
                    # Corregimos el formato a %.f para quitar el aviso amarillo
                    df = df.with_columns(
                        pl.col("hora_log").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                          .dt.truncate("1s").alias("segundo")
                    )
                    
                    df_vel = df.group_by(["ue", "segundo"]).agg(
                        pl.col("bytes").sum().alias("bytes_totales")
                    ).sort("segundo")
                    
                    for fila in df_vel.to_dicts():
                        historial_velocidades.append(fila)
                    
                    print(f"[{time.strftime('%H:%M:%S')}] Procesado 1s de datos reales.")
                    buffer_datos = []
                t_inicio = t_actual

# --- 2. DASH ---
app = Dash(__name__)
app.layout = html.Div([
    html.H2("Monitorización 5G en Tiempo Real - Fase 1"),
    dcc.Graph(id='grafico-vivo'),
    dcc.Interval(id='reloj', interval=1000, n_intervals=0)
])

@app.callback(Output('grafico-vivo', 'figure'), Input('reloj', 'n_intervals'))
def actualizar_grafica(n):
    if not historial_velocidades:
        return px.line(title="Esperando datos del simulador...")
    
    df_plot = pl.DataFrame(historial_velocidades)
    # Mostramos solo los últimos 60 segundos para que no se colapse la web
    # df_plot = df_plot.tail(100) 
    
    fig = px.line(df_plot, x="segundo", y="bytes_totales", color="ue", 
                  markers=True, title="Tráfico Downlink (SDAP)")
    fig.update_layout(transition_duration=500)
    return fig

if __name__ == '__main__':
    threading.Thread(target=procesar_log_en_vivo, daemon=True).start()
    app.run(host="127.0.0.1", port=8050, debug=True, use_reloader=False)