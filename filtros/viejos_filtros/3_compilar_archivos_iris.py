import os
import pandas as pd
from datetime import datetime

def get_file_modification_date(file_path):
    return datetime.fromtimestamp(os.path.getmtime(file_path))

def process_files():
    # Obtener la ruta de la carpeta actual
    folder_path = os.getcwd()
    real_folder_path = folder_path + '\\archivos_encontrados_por_contenido'
    
    # Inicializar una lista para almacenar los datos
    all_data = []
    
    # Recorrer todos los archivos en la carpeta
    for file_name in os.listdir(real_folder_path):
        if file_name.endswith(".txt"):
            file_path = os.path.join(real_folder_path, file_name)
            mod_date = get_file_modification_date(file_path).date()  # Obtener solo la fecha
            try:
                # Leer el archivo, descartando posibles columnas adicionales
                data = pd.read_csv(file_path, sep='&', header=None, usecols=[0, 1, 2], low_memory=False)
                data['ModDate'] = mod_date
                all_data.append(data)
            except ValueError as e:
                print(f"El archivo {file_path} no tiene al menos tres columnas. Saltando este archivo. Error: {e}")
    
    if all_data:
        # Combinar todos los datos en un solo DataFrame
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data.columns = ['ID', 'Date', 'Status', 'ModDate']
        
        # Ordenar los datos por ID y ModDate
        combined_data = combined_data.sort_values(by=['ID', 'ModDate'], ascending=[True, False])
        
        # Eliminar duplicados manteniendo el más reciente
        unique_data = combined_data.drop_duplicates(subset=['ID'], keep='first')
        
        # Quitar el decimal de la primera columna (ID)
        unique_data['ID'] = unique_data['ID'].astype(str).str.split('.').str[0]
        
        # Guardar el archivo con los registros únicos
        unique_data[['ID', 'Date', 'Status']].to_csv('3_Iris_unificado.txt', sep='&', index=False, header=False)
        
        # Guardar el archivo con la fecha de modificación incluida
        unique_data.to_csv('3_Iris_unificado_por_fecha.txt', sep='&', index=False, header=False, columns=['ID', 'Date', 'Status', 'ModDate'])
    else:
        print("No se encontraron datos válidos para procesar.")

if __name__ == "__main__":
    process_files()
