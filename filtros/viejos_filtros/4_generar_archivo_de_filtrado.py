import pandas as pd
from datetime import datetime, timedelta
import pytz

# Definir la zona horaria de Argentina
ARGENTINA_TZ = pytz.timezone('America/Argentina/Buenos_Aires')

def filter_by_days(dataframe, days):
    current_date = datetime.now(ARGENTINA_TZ).date()
    threshold_date = current_date - timedelta(days=days)
    return dataframe[dataframe['Date'] >= threshold_date]

def main():
    # Leer el archivo "Iris_unificado_por_fecha.txt"
    df = pd.read_csv('3_Iris_unificado_por_fecha.txt', sep='&', header=None, names=['ID', 'Date', 'Status', 'ModDate'], low_memory=False)
    
    # Convertir la columna ModDate a tipo datetime
    df['ModDate'] = pd.to_datetime(df['ModDate'], format='%Y-%m-%d').dt.date
    # Convertir la columna Date a tipo datetime
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors="coerce").dt.date
    
    # Quitar el decimal de la primera columna (ID)
    df['ID'] = df['ID'].astype(str).str.split('.').str[0]
    
    # Preguntar al usuario la cantidad de días para filtrar
    days = int(input("Ingrese la cantidad de días para filtrar: "))
    
    # Filtrar los registros basados en la cantidad de días
    filtered_df = filter_by_days(df, days)
    
    # Preguntar al usuario por el estado a filtrar
    print("Seleccione el estado a filtrar:")
    print("1. Port In")
    print("2. Port Out")
    print("3. Ambos (Port In y Port Out)")
    estado_opcion = input("Ingrese el número de la opción: ").strip()
    
    if estado_opcion == '1':
        final_df = filtered_df[filtered_df['Status'] == "Port In"]
    elif estado_opcion == '2':
        final_df = filtered_df[filtered_df['Status'] == "Port Out"]
    elif estado_opcion == '3':
        final_df = filtered_df[filtered_df['Status'].isin(["Port In", "Port Out"])]
    else:
        print("Opción inválida")
        return
    
    # Eliminar duplicados manteniendo el registro más reciente basado en la columna 'ID'
    final_df = final_df.sort_values(by=['ID', 'ModDate'], ascending=[True, False])
    final_df = final_df.drop_duplicates(subset=['ID'], keep='first')
    final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime("%d/%m/%Y")
    
    # Guardar los datos filtrados en formato CSV en "4_Iris_por_fecha_y_estado.csv"
    final_df.to_csv('4_Iris_por_fecha_y_estado.csv', sep=';', index=False, header=False)

if __name__ == "__main__":
    main()
