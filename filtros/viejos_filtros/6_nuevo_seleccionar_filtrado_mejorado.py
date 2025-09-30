import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from datetime import datetime, timedelta
import os

def seleccionar_archivo(mensaje):
    root = Tk()
    root.withdraw()
    return askopenfilename(title=mensaje, filetypes=[("CSV files", "*.csv")])

def filtrar_y_generar_archivo():
    archivo_base_path = seleccionar_archivo("Selecciona la base de datos para aplicar los filtros:")
    if not archivo_base_path:
        print("No se puede proceder sin seleccionar un archivo.")
        return

    try:
        # 1. Lectura y limpieza de 'dni'
        df = pd.read_csv(archivo_base_path, delimiter=';', encoding='utf-8', low_memory=False)
        df['dni'] = pd.to_numeric(df['dni'], errors='coerce')
        df = df.dropna(subset=['dni'])
        df['dni'] = df['dni'].astype(int)

        #2. Filtrar por contrato
        df = df[df['contrato'].isin(['Contrato CPP', 'Activa (Prepago)'])]

        #3. Convertir fecha_portout
        df['fecha_portacion'] = pd.to_datetime(df['fecha_portacion'], dayfirst=True, errors='coerce')

        #4. Filtrado por fecha condicional
        fecha_limite = datetime.now() - timedelta(days=30)
        df = df[df['fecha_portacion'] < fecha_limite]

        mask_cpp = (
            (df['contrato'] == 'Contrato CPP') &
            (df['fecha_portacion'].dt.year.isin([2025, 2024, 2023])) &
            (df['tipo_portacion'] == 'Port Out')
        )
        mask_prepagos = (
            (df['contrato'] == 'Activa (Prepago)') &
            (df['fecha_portacion'].dt.year.isin([2025])) &
            (df['tipo_portacion'] == 'Port Out')
        )
        df = df[mask_cpp | mask_prepagos]

        #5. Rango de DNI
        df = df[(df['dni'] >= 10000000) & (df['dni'] <= 99999999)]

        # 6. Filtrar cantidad_lineas
        df['cantidad_de_lineas'] = pd.to_numeric(df['cantidad_de_lineas'], errors='coerce')
        df = df.dropna(subset=['cantidad_de_lineas'])
        df = df[(df['cantidad_de_lineas'] >= 1) & (df['cantidad_de_lineas'] <= 7)]

        # 7. Filtrar compañia
        df = df[df['compania'] == 'Claro']

        # 8. Filtrar línea por rango o prefijos
        df['linea'] = pd.to_numeric(df['linea'], errors='coerce')
        df = df.dropna(subset=['linea'])
        # Prefijos a incluir
        prefijos = ('342', '341', '351', '387', '381')
        mask_prefijos = df['linea'].astype(int).astype(str).str.startswith(prefijos)
        mask_rango   = df['linea'] < 3000000000
        df = df[mask_rango | mask_prefijos]

        # 9. Seleccionar columnas y guardar
        columnas = [
            'linea',
            'nombre_completo',
            'dni',
            'cantidad_de_lineas',
            'otras_lineas',
            'compania',
            'contrato',
            'fecha_portacion'
        ]
        cols_existentes = [c for c in columnas if c in df.columns]
        df_salida = df[cols_existentes]

        output_path = os.path.splitext(archivo_base_path)[0] + "_seleccionado.csv"
        df_salida.to_csv(output_path, sep=';', index=False, encoding='utf-8')
        print(f"Archivo filtrado guardado en '{output_path}'")

    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")

if __name__ == "__main__":
    filtrar_y_generar_archivo()
