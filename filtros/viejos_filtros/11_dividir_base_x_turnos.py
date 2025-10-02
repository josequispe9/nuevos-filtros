import pandas as pd
import os
import random
from datetime import datetime, timedelta

# Función para crear las carpetas de salida si no existen
def crear_carpetas():
    if not os.path.exists("bases_llamador_ok"):
        os.makedirs("bases_llamador_ok")

# Función para pedir el archivo a cruzar
def pedir_archivo_a_cruzar():
    # Pregunta al usuario por el nombre del archivo
    archivo_a_cruzar = input("Por favor, ingrese el nombre del archivo a cruzar (con extensión .csv): ")

    # Verificar si el archivo existe
    if os.path.exists(archivo_a_cruzar):
        return archivo_a_cruzar
    else:
        print("Archivo no encontrado")
        return None

# Función para detectar si la columna es 'DNI' o 'CUIT'
def detectar_columna(df):
    if 'DNI' in df.columns:
        return 'DNI'
    elif 'CUIT' in df.columns:
        return 'CUIT'
    else:
        print("No se encontró la columna 'DNI' ni 'CUIT'.")
        return None

# Función para ordenar aleatoriamente y reducir a dos líneas por clave (DNI o CUIT)
def reducir_a_dos_lineas_ordenado(df, clave):
    df_shuffled = df.sample(frac=1, random_state=random.randint(1, 1000)).reset_index(drop=True)
    df_filtrado = df_shuffled.groupby(clave).head(2)  # Mantener solo dos filas por clave (DNI o CUIT)
    return df_filtrado

# Función para dividir aleatoriamente en dos partes iguales TM y TT
def dividir_en_tm_tt(df, clave, nombre_archivo):
    df_shuffled = df.sample(frac=1, random_state=random.randint(1, 1000)).reset_index(drop=True)

    # Verificar si el número de registros es impar y eliminar uno si es el caso
    if len(df_shuffled) % 2 != 0:
        df_shuffled = df_shuffled[:-1]

    # Dividir el dataframe en dos partes iguales
    mitad = len(df_shuffled) // 2
    df_tm = df_shuffled[:mitad]
    df_tt = df_shuffled[mitad:]

    # Obtener la fecha actual para los nombres de archivo
    fecha_actual = datetime.now().strftime('%d-%m-%Y')

    # Crear nombres de archivo con la clave (DNI o CUIT) y la fecha
    archivo_tm = f"TM_base_{clave}_{fecha_actual}.csv"
    archivo_tt = f"TT_base_{clave}_{fecha_actual}.csv"
    
    name = df_tm['BBDD'].iloc[0]
    name_modificado = name.replace('TM', 'TT')
    df_tt['BBDD'] = name_modificado

    # Guardar los archivos en la carpeta raíz
    df_tm.to_csv('bases_llamador_ok/' + archivo_tm, sep=';', index=False, encoding='utf-8')
    df_tt.to_csv('bases_llamador_ok/' + archivo_tt, sep=';', index=False, encoding='utf-8')

    print(f"Archivos guardados: {archivo_tm}, {archivo_tt}")

# Función principal para dividir los archivos de DNI o CUIT
def dividir_archivos_dni_o_cuit():
    # Crear las carpetas de salida
    crear_carpetas()

    # Pedir el archivo a cruzar
    archivo_a_cruzar = pedir_archivo_a_cruzar()

    if archivo_a_cruzar:
        # Cargar el archivo CSV
        df = pd.read_csv(archivo_a_cruzar, delimiter=';', encoding='utf-8')

        # Detectar si la columna es 'DNI' o 'CUIT'
        clave = detectar_columna(df)

        if clave:
            # Ordenar aleatoriamente y reducir a dos líneas por cada clave (DNI o CUIT)
            df_reducido = reducir_a_dos_lineas_ordenado(df, clave)

            # Dividir el archivo en dos partes iguales (TM y TT)
            dividir_en_tm_tt(df_reducido, clave, archivo_a_cruzar)
        else:
            print("No se puede proceder sin una columna válida ('DNI' o 'CUIT').")
    else:
        print("No se puede proceder sin un archivo válido.")

# Ejecutar el proceso
if __name__ == "__main__":
    dividir_archivos_dni_o_cuit()
