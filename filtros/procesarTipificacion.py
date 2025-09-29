import pandas as pd
import re
  

def normalize_phone(phone):
    """
    Normaliza números de teléfono argentinos eliminando 0, 90 y 15
    para obtener números de 10 dígitos
    """
    if pd.isna(phone):
        return None
    
    phone_str = str(phone).strip()
    
    # Eliminar espacios, guiones y paréntesis
    phone_clean = re.sub(r'[\s\-\(\)]', '', phone_str)
    
    # Solo procesar si contiene solo dígitos
    if not phone_clean.isdigit():
        return None
    
    # Eliminar prefijo 0 o 90 al inicio
    if phone_clean.startswith('90'):
        phone_clean = phone_clean[2:]
    elif phone_clean.startswith('0'):
        phone_clean = phone_clean[1:]
    
    # Eliminar 15 después del código de área (posiciones 2-4 típicamente)
    # Buscar patrón de código de área + 15
    if len(phone_clean) > 4:
        # Códigos de área de 2 dígitos + 15
        if phone_clean[2:4] == '15':
            phone_clean = phone_clean[:2] + phone_clean[4:]
        # Códigos de área de 3 dígitos + 15  
        elif len(phone_clean) > 5 and phone_clean[3:5] == '15':
            phone_clean = phone_clean[:3] + phone_clean[5:]
        # Códigos de área de 4 dígitos + 15
        elif len(phone_clean) > 6 and phone_clean[4:6] == '15':
            phone_clean = phone_clean[:4] + phone_clean[6:]
    
    # Retornar solo si tiene exactamente 10 dígitos
    if len(phone_clean) == 10:
        return phone_clean
    
    return None

def process_csv_chunks(input_file, output_file, chunk_size=10000):
    """
    Procesa el CSV por chunks y genera archivo de salida
    """
    # Tipificaciones válidas
    valid_tipifications = {
        "Cliente moroso (Supera umbral)",
        "Edificio sin Disp de Caja", 
        "Venta",
        "Ya tiene MVS"
    }
    
    # Crear archivo de salida
    output_data = []
    
    try:
        # Procesar CSV por chunks
        chunk_iter = pd.read_csv(
            input_file, 
            sep=';', 
            chunksize=chunk_size,
            dtype=str,  # Leer todo como string para preservar números
            low_memory=False
        )
        
        for chunk_num, chunk in enumerate(chunk_iter, 1):
            print(f"Procesando chunk {chunk_num} con {len(chunk)} filas...")
            
            # Verificar que existan las columnas necesarias
            if 'Cliente' not in chunk.columns or 'Tipificación' not in chunk.columns:
                raise ValueError("El CSV debe contener las columnas 'Cliente' y 'Tipificación'")
            
            # Filtrar por tipificaciones válidas
            filtered_chunk = chunk[chunk['Tipificación'].isin(valid_tipifications)].copy()
            
            if len(filtered_chunk) > 0:
                # Normalizar números de teléfono
                filtered_chunk['Cliente_Normalizado'] = filtered_chunk['Cliente'].apply(normalize_phone)
                
                # Eliminar filas con números inválidos
                valid_phones = filtered_chunk.dropna(subset=['Cliente_Normalizado'])
                
                # Agregar a resultados
                if len(valid_phones) > 0:
                    result_chunk = valid_phones[['Cliente_Normalizado', 'Tipificación']].copy()
                    result_chunk.columns = ['Cliente', 'Tipificación']
                    output_data.append(result_chunk)
                    
                print(f"  - {len(valid_phones)} números válidos encontrados")
            else:
                print(f"  - No se encontraron tipificaciones válidas")
        
        # Combinar todos los chunks procesados
        if output_data:
            final_df = pd.concat(output_data, ignore_index=True)
            
            # Eliminar duplicados manteniendo el primer registro
            final_df = final_df.drop_duplicates(subset=['Cliente'], keep='first')
            
            # Guardar archivo de salida
            final_df.to_csv(output_file, sep=';', index=False)
            
            print(f"\nProcesamiento completado:")
            print(f"- Total de registros válidos: {len(final_df)}")
            print(f"- Archivo guardado: {output_file}")
            
            # Mostrar estadísticas por tipificación
            print("\nDistribución por tipificación:")
            tipif_counts = final_df['Tipificación'].value_counts()
            for tipif, count in tipif_counts.items():
                print(f"  - {tipif}: {count}")
                
        else:
            print("No se encontraron registros válidos para procesar")
            
    except Exception as e:
        print(f"Error al procesar el archivo: {str(e)}")
        raise

# Ejemplo de uso
if __name__ == "__main__":
    import tkinter as tk
    from tkinter import filedialog, messagebox
    from pathlib import Path
    
    # Crear ventana oculta para diálogos
    root = tk.Tk()
    root.withdraw()  # Ocultar ventana principal
    
    # Seleccionar archivo de entrada
    print("Selecciona el archivo CSV de entrada...")
    input_filename = filedialog.askopenfilename(
        title="Seleccionar archivo CSV de entrada",
        filetypes=[("Archivos CSV", "*.csv"), ("Todos los archivos", "*.*")]
    )
    
    if not input_filename:
        print("No se seleccionó ningún archivo")
        exit()
    
    # Generar nombre del archivo de salida en el directorio del script
    input_path = Path(input_filename)
    script_dir = Path(__file__).parent
    output_filename = script_dir / f"{input_path.stem}_procesado.csv"
    
    root.destroy()  # Cerrar tkinter
    
    print(f"\nArchivo de entrada: {input_filename}")
    print(f"Archivo de salida: {output_filename}")
    
    # Procesar archivo
    try:
        process_csv_chunks(input_filename, output_filename, chunk_size=10000)
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")
        messagebox.showerror("Error", f"Error durante el procesamiento: {str(e)}")
        
        
        
### MODIFICACIONES A REALIZAR EN EL CODIGO
""" 
# Guardar en un archivo txt con la columna linea, fecha (fecha del reporte), fecha_agregacion
# el formato del nombre del archivo debe ser el nombre de la categoria filtrada + fecha del reporte
# la fecha del reporte esta en la columna df['Inicio'] es un tipo object con el formato '24/9/2025  11:10:59' deberas transformarlo a datetime conservando solo la fecha
cliente_moroso_path = '..data/raw/repo_Cliente_Moroso/' 
edificio_sin_disp_path = '..data/raw/repo_Edificio_sin_disp/'
venta_path = '..data/raw/repo_Venta/'
ya_tiene_mvs_path = '..data/raw/repo_ya_tiene_MVS/'
linea_en_reparacion_path = '..data/raw/linea_en_reparacion/'
linea_fuera_de_servicio_path =  '..data/raw/linea_fuera_de_servicio/'

# Ademas deberas ir sumando esos valores a este archivo en el mismo formato linea, fecha (fecha del reporte), fecha_agregacion
tipificaciones_consolidadas = '../data/processed/Tipificaciones-consolidadas.txt'

# Filtrar con df[df['Tipificación']]
valid_tipifications = {
        "Cliente moroso (Supera umbral)",
        "Edificio sin Disp de Caja", 
        "Venta",
        "Ya tiene MVS"
    }
#Filtrar con df[df['Causa Terminación']]
valid_causa_terminacion = {
        "Se discó un número que no corresponde a un abonado en servicio",
        "La Linea se encuentra en reparación"
    }
""" 