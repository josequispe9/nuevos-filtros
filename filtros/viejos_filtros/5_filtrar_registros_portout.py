import pandas as pd
import os
from tkinter import Tk, messagebox
from tkinter.filedialog import askopenfilename
import tkinter.simpledialog as simpledialog

# Función para seleccionar archivo
def seleccionar_archivo(mensaje):
    root = Tk()
    root.withdraw()  # Esconde la ventana de tkinter
    ruta_archivo = askopenfilename(title=mensaje, filetypes=[("CSV files", "*.csv")])
    root.destroy()
    return ruta_archivo

# Función para mostrar las primeras filas y preguntar si hay encabezados
def verificar_encabezados(df):
    root = Tk()
    root.withdraw()
    
    # Mostrar las primeras 5 filas
    primeras_filas = df.head(5).to_string()
    mensaje = f"Primeras filas del archivo:\n\n{primeras_filas}\n\n¿La primera fila contiene títulos de columnas?"
    
    respuesta = messagebox.askyesno("Verificar encabezados", mensaje)
    root.destroy()
    return respuesta

# Función para seleccionar columna por índice
def seleccionar_columna_por_indice(df, mensaje):
    root = Tk()
    root.withdraw()
    
    # Mostrar las columnas con sus índices
    columnas_texto = "Columnas disponibles:\n"
    for i, col in enumerate(df.columns):
        # Mostrar algunos valores de ejemplo
        valores_ejemplo = df[col].dropna().head(3).tolist()
        valores_str = ", ".join(str(v)[:20] for v in valores_ejemplo)  # Limitar longitud
        columnas_texto += f"\n{i}: Columna {i} - Ejemplos: {valores_str}"
    
    mensaje_completo = f"{mensaje}\n\n{columnas_texto}\n\nIngrese el número de la columna:"
    
    while True:
        respuesta = simpledialog.askstring("Seleccionar columna", mensaje_completo)
        if respuesta is None:
            root.destroy()
            return None
        
        try:
            indice = int(respuesta)
            if 0 <= indice < len(df.columns):
                root.destroy()
                return indice
            else:
                messagebox.showerror("Error", f"Número fuera de rango. Debe ser entre 0 y {len(df.columns)-1}")
        except ValueError:
            messagebox.showerror("Error", "Por favor ingrese un número válido.")

# Preguntar por la base de datos a cruzar
print("=== SCRIPT DE CRUCE DE DATOS ===\n")
print("Seleccionando archivo principal...")
documento_finalizado_path = seleccionar_archivo("Selecciona la base de datos a cruzar (archivo principal):")

if not documento_finalizado_path:
    print("No se seleccionó ningún archivo. Saliendo...")
    exit()

archivo_unificado_path = '4_Iris_por_fecha_y_estado.csv'

# Verificar que el archivo unificado existe
if not os.path.exists(archivo_unificado_path):
    print(f"Error: No se encuentra el archivo '{archivo_unificado_path}'")
    exit()

try:
    # Leer el archivo principal inicialmente sin encabezados
    print(f"\nLeyendo archivo principal: {documento_finalizado_path}")
    df_temp = pd.read_csv(documento_finalizado_path, delimiter=';', encoding='latin-1', header=None, low_memory=False)
    
    # Verificar si tiene encabezados
    tiene_encabezados = verificar_encabezados(df_temp)
    
    if tiene_encabezados:
        # Releer con la primera fila como encabezados
        df_documento_finalizado = pd.read_csv(documento_finalizado_path, delimiter=';', encoding='latin-1', header=0, low_memory=False)
        print("✓ Archivo leído con encabezados")
    else:
        # Usar el dataframe sin encabezados
        df_documento_finalizado = df_temp
        # Renombrar columnas con números
        df_documento_finalizado.columns = [f'columna_{i}' for i in range(len(df_documento_finalizado.columns))]
        print("✓ Archivo leído sin encabezados")
    
    df_documento_finalizado["linea"] = df_documento_finalizado["linea"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    print(f"Total de registros: {len(df_documento_finalizado)}")
    print(f"Total de columnas: {len(df_documento_finalizado.columns)}")
    
    # Seleccionar la columna que contiene los números de teléfono
    print("\nSeleccionando columna con números de teléfono...")
    indice_columna = seleccionar_columna_por_indice(df_documento_finalizado, 
                                                    "Seleccione la columna que contiene los números de teléfono/líneas:")
    
    if indice_columna is None:
        print("No se seleccionó ninguna columna. Saliendo...")
        exit()
    
    columna_telefono = df_documento_finalizado.columns[indice_columna]
    print(f"✓ Columna seleccionada: {columna_telefono} (índice {indice_columna})")
    
    # Leer el archivo unificado
    print(f"\nLeyendo archivo de portaciones: {archivo_unificado_path}")
    df_archivo_unificado = pd.read_csv(archivo_unificado_path, delimiter=';', header=None, low_memory=False)
    
    # Verificar si el archivo unificado tiene encabezados
    if df_archivo_unificado.iloc[0].astype(str).str.contains('[a-zA-Z]').any():
        print("Detectados posibles encabezados en archivo de portaciones, eliminando primera fila...")
        df_archivo_unificado = df_archivo_unificado.iloc[1:]
    
    # Asignar nombres a las columnas del archivo unificado
    if len(df_archivo_unificado.columns) >= 4:
        df_archivo_unificado.columns = ['numero_telefono', 'fecha_portacion', 'tipo_portacion', 'fecha_filtrado']
    else:
        print(f"Error: El archivo de portaciones debe tener al menos 4 columnas, encontradas: {len(df_archivo_unificado.columns)}")
        exit()
    
    # Convertir a string y limpiar espacios
    df_archivo_unificado['numero_telefono'] = df_archivo_unificado['numero_telefono'].astype(str).str.strip()
    df_documento_finalizado[columna_telefono] = df_documento_finalizado[columna_telefono].astype(str).str.strip()
    
    # Eliminar filas donde numero_telefono sea 'nan' o vacío
    df_archivo_unificado = df_archivo_unificado[
        (df_archivo_unificado['numero_telefono'] != 'nan') & 
        (df_archivo_unificado['numero_telefono'] != '') &
        (df_archivo_unificado['numero_telefono'].notna())
    ]
    
    # Filtrar solo Port Out y Port In
    df_archivo_unificado = df_archivo_unificado[
        df_archivo_unificado['tipo_portacion'].isin(['Port Out', 'Port In'])
    ]
    print(f"✓ Registros Port In/Out encontrados: {len(df_archivo_unificado)}")
    
    # Convertir fecha_filtrado a datetime
    df_archivo_unificado['fecha_filtrado'] = pd.to_datetime(
        df_archivo_unificado['fecha_filtrado'], 
        format='%Y-%m-%d', 
        errors='coerce'
    )
    
    # Ordenar por número y fecha (más reciente primero)
    df_archivo_unificado = df_archivo_unificado.sort_values(
        by=['numero_telefono', 'fecha_filtrado'], 
        ascending=[True, False]
    )
    
    # Eliminar duplicados, quedándose con el más reciente
    df_archivo_unificado = df_archivo_unificado.drop_duplicates(
        subset='numero_telefono', 
        keep='first'
    )
    print(f"✓ Números únicos para cruzar: {len(df_archivo_unificado)}")
    
    # Realizar el cruce
    print("\nRealizando cruce de datos...")
    df_merged = pd.merge(
        df_documento_finalizado, 
        df_archivo_unificado,
        left_on=columna_telefono, 
        right_on='numero_telefono', 
        how='left'
    )
    
    # Mantener columnas originales y agregar las nuevas al final
    nuevas_columnas = ['fecha_portacion', 'tipo_portacion', 'fecha_filtrado']
    columnas_originales = df_documento_finalizado.columns.tolist()
    columnas_finales = columnas_originales + nuevas_columnas
    
    # Reordenar columnas
    df_merged = df_merged[columnas_finales]
    
    # Separar registros con y sin coincidencia
    mask_con_datos = df_merged[nuevas_columnas].notna().any(axis=1)
    df_con_coincidencia = df_merged[mask_con_datos]
    df_sin_coincidencia = df_merged[~mask_con_datos]
    
    print(f"\n=== RESULTADOS DEL CRUCE ===")
    print(f"✓ Registros con portación encontrada: {len(df_con_coincidencia)}")
    print(f"✓ Registros sin portación encontrada: {len(df_sin_coincidencia)}")
    print(f"✓ Total procesado: {len(df_merged)}")
    
    # Generar nombres de archivos de salida
    base_name = os.path.splitext(os.path.basename(documento_finalizado_path))[0]
    output_path_final = f'{base_name}_con_portacion.csv'
    output_path_sin_resultado = f'{base_name}_sin_portacion.csv'
    
    # Guardar resultados
    print("\nGuardando archivos...")
    df_con_coincidencia.to_csv(output_path_final, index=False, sep=';', encoding='utf-8-sig')
    df_sin_coincidencia.to_csv(output_path_sin_resultado, index=False, sep=';', encoding='utf-8-sig')
    
    print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE!")
    print(f"\nArchivos generados:")
    print(f"  📄 Con portación: '{output_path_final}' ({len(df_con_coincidencia)} registros)")
    print(f"  📄 Sin portación: '{output_path_sin_resultado}' ({len(df_sin_coincidencia)} registros)")
    
    # Mostrar algunas estadísticas adicionales
    if len(df_con_coincidencia) > 0:
        port_in = len(df_con_coincidencia[df_con_coincidencia['tipo_portacion'] == 'Port In'])
        port_out = len(df_con_coincidencia[df_con_coincidencia['tipo_portacion'] == 'Port Out'])
        print(f"\nDetalle de portaciones encontradas:")
        print(f"  - Port In: {port_in}")
        print(f"  - Port Out: {port_out}")
    
except Exception as e:
    print(f"\n❌ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

input("\nPresione Enter para salir...")