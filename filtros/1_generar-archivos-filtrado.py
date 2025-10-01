import pandas as pd
import re
import os
from datetime import datetime
from pathlib import Path

"""Usar parquet

# Leer CSV, forzando todo como str
df = pd.read_csv("mi_archivo.csv", sep=";", dtype=str, encoding="utf-8")

# Guardar en Parquet, manteniendo todo como str
df = df.astype(str)  # asegura que todas las columnas sean string
df.to_parquet("mi_archivo.parquet", engine="pyarrow", compression="snappy", index=False)

"""

# ==================== CONFIGURACIÓN DE PATHS ====================
# El script está en filtros/, los datos están en data/ (un nivel arriba)
BASE_DIR = Path(__file__).parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'
DATA_PROCESSED = BASE_DIR / 'data' / 'processed'

REPORTES_DIR = DATA_RAW / 'reportes'
IRIS_DIR = DATA_RAW / 'extraerEstado'

TIPIFICACIONES_OUTPUT = DATA_PROCESSED / 'Tipificaciones-consolidadas.parquet'
IRIS_OUTPUT = DATA_PROCESSED / 'Iris-consolidado.parquet'


# ==================== FUNCIONES AUXILIARES ====================
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


def get_unprocessed_files(directory, extension=''):
    """
    Obtiene lista de archivos que NO tienen '-p' antes de la extensión
    """
    if not directory.exists():
        print(f"⚠ Advertencia: El directorio {directory} no existe")
        return []
    
    files = []
    for file in directory.glob(f'*{extension}'):
        # Verificar que no tenga '-p' antes de la extensión
        if not file.stem.endswith('-p'):
            files.append(file)
    return files


def mark_as_processed(file_path):
    """
    Renombra el archivo agregando '-p' antes de la extensión
    """
    file_path = Path(file_path)
    new_name = file_path.stem + '-p' + file_path.suffix
    new_path = file_path.parent / new_name
    file_path.rename(new_path)
    print(f"✓ Archivo marcado como procesado: {new_path.name}")


def load_existing_consolidated(file_path, key_column):
    """
    Carga archivo consolidado existente, retorna DataFrame vacío si no existe o está vacío
    """
    if file_path.exists():
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            # Verificar si el DataFrame está vacío o no tiene columnas
            if df.empty or len(df.columns) == 0:
                print(f"⚠ El archivo consolidado existe pero está vacío, se creará nuevo")
                return pd.DataFrame()
            
            df['fecha'] = pd.to_datetime(df['fecha'])
            print(f"✓ Cargado consolidado existente: {len(df)} registros")
            return df
        except Exception as e:
            print(f"⚠ Error al cargar consolidado ({str(e)}), se creará nuevo")
            return pd.DataFrame()
    else:
        print(f"⚠ No existe consolidado previo, se creará nuevo")
        return pd.DataFrame()

def update_consolidated(df_existing, df_new, key_column):
    """
    Actualiza el consolidado: reemplaza si fecha es mayor, sino agrega nuevos
    """
    if df_existing.empty:
        return df_new
    
    # Convertir fechas a datetime para comparación
    df_existing['fecha'] = pd.to_datetime(df_existing['fecha'])
    df_new['fecha'] = pd.to_datetime(df_new['fecha'])
    
    # IMPORTANTE: Eliminar duplicados del existente, quedándose con el más reciente
    df_existing = df_existing.sort_values(by='fecha', ascending=False)
    df_existing = df_existing.drop_duplicates(subset=key_column, keep='first')
    
    # Crear diccionario con los registros existentes
    existing_dict = df_existing.set_index(key_column).to_dict('index')
    
    # Procesar nuevos registros
    updated_count = 0
    added_count = 0
    skipped_count = 0
    
    for idx, row in df_new.iterrows():
        key = row[key_column]
        
        # Saltar si la clave es nula
        if pd.isna(key):
            skipped_count += 1
            continue
        
        # Crear diccionario de la fila SIN incluir la columna clave
        row_dict = row.to_dict()
        row_dict.pop(key_column, None)  # Eliminar la columna clave del diccionario
        
        if key in existing_dict:
            # Si existe, comparar fechas
            existing_date = pd.to_datetime(existing_dict[key]['fecha'])
            new_date = pd.to_datetime(row['fecha'])
            
            if new_date > existing_date:
                # Actualizar con datos más recientes
                existing_dict[key] = row_dict
                updated_count += 1
        else:
            # Agregar nuevo registro
            existing_dict[key] = row_dict
            added_count += 1
    
    print(f"  → {updated_count} registros actualizados")
    print(f"  → {added_count} registros nuevos agregados")
    if skipped_count > 0:
        print(f"  → {skipped_count} registros omitidos (clave nula)")
    
    # Convertir de vuelta a DataFrame
    df_result = pd.DataFrame.from_dict(existing_dict, orient='index')
    df_result = df_result.reset_index()
    df_result.rename(columns={'index': key_column}, inplace=True)
    
    return df_result

# ==================== CARGA DE DATOS ====================
print("\n" + "="*60)
print("PROCESANDO ARCHIVOS DE REPORTES")
print("="*60)

# Obtener archivos sin procesar de reportes
reportes_files = get_unprocessed_files(REPORTES_DIR, '.csv')
print(f"\nArchivos de reportes a procesar: {len(reportes_files)}")

df_reporte_all = pd.DataFrame()

for file in reportes_files:
    print(f"\n→ Procesando: {file.name}")
    
    try:
        df_temp = pd.read_csv(file, sep=';', encoding='utf-8', dtype=str)
        
        # LIMPIEZA DE DATOS (Reportes)
        valid_tipifications = {
            "Cliente moroso (Supera umbral)",
            "Edificio sin Disp de Caja", 
            "Venta",
            "Ya tiene MVS"
        }
        valid_causa_termincacion = {
            "Se discó un número que no corresponde a un abonado en servicio",
            "La Linea se encuentra en reparación"
        }
        
        df_temp = df_temp[["Inicio", "Cliente", "Tipificación", "Causa Terminación", "TalkingTime", "Sentido"]]
        df_temp = df_temp[(df_temp['Tipificación'].isin(valid_tipifications)) | (df_temp['Causa Terminación'].isin(valid_causa_termincacion))]
        df_temp['TalkingTime'] = pd.to_numeric(df_temp['TalkingTime'], errors='coerce').astype('Int64')
        
        df_temp['Inicio'] = pd.to_datetime(df_temp['Inicio'], format='%d/%m/%Y %H:%M:%S')
        df_temp = df_temp.sort_values(by='Inicio', ascending=False)
        df_temp = df_temp.drop_duplicates(subset='Cliente', keep='first')
        
        df_temp['Cliente'] = df_temp['Cliente'].apply(normalize_phone)
        df_temp['Tipificacion'] = df_temp.apply(
            lambda row: row['Causa Terminación'] if row['Tipificación'] == 'No Disp.' else row['Tipificación'],
            axis=1
        )
        df_temp['fecha'] = df_temp['Inicio'].dt.date
        df_temp['fecha_consulta'] = datetime.today().date()
        
        # ✅ COLUMNAS A CONSERVAR
        df_temp = df_temp[['Cliente', 'Tipificacion', 'fecha', 'fecha_consulta']]
        
        # Eliminar filas con Cliente nulo (normalize_phone puede retornar None)
        df_temp = df_temp.dropna(subset=['Cliente'])
        
        print(f"  Registros válidos extraídos: {len(df_temp)}")
        
        # Acumular en el DataFrame general
        df_reporte_all = pd.concat([df_reporte_all, df_temp], ignore_index=True)
        
        # Marcar archivo como procesado
        mark_as_processed(file)
        
    except Exception as e:
        print(f"  ✗ Error procesando {file.name}: {str(e)}")
        continue
    
    

print("\n" + "="*60)
print("PROCESANDO ARCHIVOS DE IRIS")
print("="*60)

# Obtener archivos sin procesar de Iris
iris_files = get_unprocessed_files(IRIS_DIR, '.txt')
print(f"\nArchivos de Iris a procesar: {len(iris_files)}")

df_iris_all = pd.DataFrame()

for file in iris_files:
    print(f"\n→ Procesando: {file.name}")
    
    try:
        # Leer el archivo SIN asumir número fijo de columnas
        # on_bad_lines='skip' descarta automáticamente filas problemáticas
        df_temp = pd.read_csv(
            file, 
            sep='&', 
            encoding='utf-8', 
            dtype=str,
            header=None,  # Sin encabezados
            on_bad_lines='skip'  # Saltar líneas con problemas
        )
        
        initial_count = len(df_temp)
        
        # Filtrar solo filas que tengan exactamente 3 columnas
        df_temp = df_temp[df_temp.apply(lambda row: row.notna().sum() == 3, axis=1)]
        
        # Si después del filtro no quedan filas, saltar archivo
        if df_temp.empty:
            print(f"  ✗ No se encontraron filas válidas con 3 columnas")
            continue
        
        # Tomar solo las primeras 3 columnas
        df_temp = df_temp.iloc[:, :3]
        df_temp.columns = ['linea', 'fecha', 'estado']
        
        corrupted_rows = initial_count - len(df_temp)
        if corrupted_rows > 0:
            print(f"  ⚠ Se descartaron {corrupted_rows} filas corruptas/incompletas")
        
        # LIMPIEZA DE DATOS (Iris)
        df_temp['fecha'] = pd.to_datetime(df_temp['fecha'], format='%d/%m/%Y', errors='coerce')
        
        # Eliminar filas donde la fecha no se pudo convertir
        df_temp = df_temp.dropna(subset=['fecha'])
        
        # Validación adicional: verificar que 'linea' contenga solo dígitos
        df_temp = df_temp[df_temp['linea'].str.match(r'^\d+$', na=False)]
        
        df_temp = df_temp.sort_values(by='fecha', ascending=False)
        df_temp = df_temp.drop_duplicates(subset='linea', keep='first')
        df_temp['fecha_consulta'] = datetime.today().date()
        
        print(f"  Registros válidos extraídos: {len(df_temp)}")
        
        # Acumular en el DataFrame general
        df_iris_all = pd.concat([df_iris_all, df_temp], ignore_index=True)
        
        # Marcar archivo como procesado
        mark_as_processed(file)
        
    except Exception as e:
        print(f"  ✗ Error procesando {file.name}: {str(e)}")
        continue

# ==================== EXPORTACIÓN DE DATOS ====================
print("\n" + "="*60)
print("CONSOLIDANDO Y EXPORTANDO DATOS")
print("="*60)

# Crear directorio de salida si no existe
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Consolidar Tipificaciones
if not df_reporte_all.empty:
    print("\n→ Consolidando Tipificaciones...")
    df_tipificaciones_existing = load_existing_consolidated(TIPIFICACIONES_OUTPUT, 'Cliente')
    df_tipificaciones_final = update_consolidated(df_tipificaciones_existing, df_reporte_all, 'Cliente')
    df_tipificaciones_final = df_tipificaciones_final.astype(str)
    df_tipificaciones_final.to_parquet(TIPIFICACIONES_OUTPUT, engine='pyarrow', compression='snappy', index=False)
    print(f"✓ Guardado: {TIPIFICACIONES_OUTPUT}")
    print(f"  Total registros: {len(df_tipificaciones_final)}")
else:
    print("\n⚠ No hay nuevos datos de reportes para consolidar")

# Consolidar Iris
if not df_iris_all.empty:
    print("\n→ Consolidando Iris...")
    df_iris_existing = load_existing_consolidated(IRIS_OUTPUT, 'linea')
    df_iris_final = update_consolidated(df_iris_existing, df_iris_all, 'linea')
    df_iris_final = df_iris_final.astype(str)
    df_iris_final.to_parquet(IRIS_OUTPUT, engine='pyarrow', compression='snappy', index=False)
    print(f"✓ Guardado: {IRIS_OUTPUT}")
    print(f"  Total registros: {len(df_iris_final)}")
else:
    print("\n⚠ No hay nuevos datos de Iris para consolidar")

print("\n" + "="*60)
print("PROCESO COMPLETADO")
print("="*60 + "\n")



"""
import pandas as pd
import re
import os
from datetime import datetime


# CARGA DE DATOS
data_path =  'data/raw/'
df_reporte = pd.read_csv('../' + data_path + 'reportes/092625.csv', sep=';', encoding='utf-8', dtype=str)
df_iris = pd.read_csv('../' + data_path + 'extraerEstado/data-30_IrisBotExtraerEstado.txt', sep='&', encoding='utf-8', dtype=str)

# FUNCIONES AUXILIARES  
def normalize_phone(phone):
    ""
    Normaliza números de teléfono argentinos eliminando 0, 90 y 15
    para obtener números de 10 dígitos
    ""
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


# LIMPIEZA DE DATOS

df_iris.columns = ['linea', 'fecha', 'estado'] # colocar encabezados a las columnas
df_iris['fecha'] = pd.to_datetime(df_iris['fecha'], format='%d/%m/%Y') # convertimos a fecha
df_iris = df_iris.dropna(subset=['linea', 'fecha', 'estado']) # borramos nulos
df_iris = df_iris.sort_values(by='fecha', ascending=False) # ordenar fechas
df_iris = df_iris.drop_duplicates(subset='linea', keep='first') # borrar duplicados por numero
df_iris['fecha_consulta'] = datetime.today().date()


valid_tipifications = {
        "Cliente moroso (Supera umbral)",
        "Edificio sin Disp de Caja", 
        "Venta",
        "Ya tiene MVS"
    }
valid_causa_termincacion = {
        "Se discó un número que no corresponde a un abonado en servicio",
        "La Linea se encuentra en reparación"
    }

df_reporte = df_reporte[["Inicio", "Cliente", "Tipificación", "Causa Terminación", "TalkingTime", "Sentido"]] # seleccionar columnas importantes
df_reporte = df_reporte[(df_reporte['Tipificación'].isin(valid_tipifications) ) | (df_reporte['Causa Terminación'].isin(valid_causa_termincacion))] # seleccionar tipificaciones importantes
df_reporte['TalkingTime'] = pd.to_numeric(df_reporte['TalkingTime'], errors='coerce').astype('Int64') # convertir TalkingTime a entero

df_reporte['Inicio'] = pd.to_datetime(df_reporte['Inicio'], format='%d/%m/%Y %H:%M:%S') # Convertir a fecha
df_reporte = df_reporte.sort_values(by='Inicio', ascending=False) # Ordenar por mas reciente a mas antiguo
df_reporte = df_reporte.drop_duplicates(subset='Cliente', keep='first') # Borrar quedandote con el mas reciente

df_reporte['Cliente'] = df_reporte['Cliente'].apply(normalize_phone) # Formatear numeros
df_reporte['Tipificacion'] = df_reporte.apply(
    lambda row: row['Causa Terminación'] if row['Tipificación'] == 'No Disp.' else row['Tipificación'],
    axis=1
)
df_reporte['fecha'] = df_reporte['Inicio'].dt.date # solo conservar la fecha
df_reporte['fecha_consulta'] = datetime.today().date() # fecha de la modificacion
df_reporte = df_reporte[['Cliente', 'Tipificacion', 'fecha', 'fecha_consulta']] # columnas a conservar



# EXPORTACION DE DATOS

df_reporte.to_csv('data\processed\Tipificaciones-consolidadas.csv', sep=';', encoding='utf-8', index=False)
df_iris.to_csv('data\processed\Iris-consolidado.csv', sep=';', encoding='utf-8', index=False)
"""