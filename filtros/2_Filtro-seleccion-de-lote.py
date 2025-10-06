import pandas as pd
import os
from datetime import datetime, timedelta
from pathlib import Path


# ==================== CONFIGURACIÓN DE PATHS ====================
BASE_DIR = Path(__file__).parent.parent
DATA_ROOT = BASE_DIR / 'data' 
BASE_OUTPUT = DATA_ROOT / 'bases' / 'base.csv'

BASE_MAIN_DIR = DATA_ROOT / 'base_2024_2025_actualizada.parquet'
NO_LLAME_DIR = DATA_ROOT / 'Registro_No_Llame.parquet'
LINEAS_FILTRADAS = DATA_ROOT / 'lineas_filtradas_150.parquet'
IRIS_CONSOLIDADO = DATA_ROOT / 'processed/Iris-consolidado.parquet'

ayer = datetime.now() - timedelta(days=1)
fecha_str = ayer.strftime("%m%d%y")  # formato mmddyy, ej: 100225

REPORTE_DIA_ANTERIOR = DATA_ROOT / f'raw/reportes/{fecha_str}-p.csv'


# ==================== CARGAR DATAFRAMES ====================

df_base = pd.read_parquet(BASE_MAIN_DIR, engine="pyarrow")
df_lineas_filtradas = pd.read_parquet(LINEAS_FILTRADAS, engine="pyarrow")
df_registro_no_llame = pd.read_parquet(NO_LLAME_DIR, engine="pyarrow")

# Cargar Iris con manejo de errores
try:
    df_iris_consolidado = pd.read_parquet(IRIS_CONSOLIDADO, engine="pyarrow")
except Exception as e:
    print(f"⚠️ Error al cargar Iris-consolidado.parquet: {e}")
    print("Regenerando archivo desde el script 1_generar-archivos-filtrado.py...")
    import subprocess
    import sys
    script_path = BASE_DIR / 'filtros' / '1_generar-archivos-filtrado.py'
    subprocess.run([sys.executable, str(script_path)], check=True)
    df_iris_consolidado = pd.read_parquet(IRIS_CONSOLIDADO, engine="pyarrow")

df_reporte_dia_anterior = pd.read_csv(REPORTE_DIA_ANTERIOR, sep=';', encoding='utf-8', dtype=str)

# ==================== INFO DE DATAFRAMES ====================

"""
df_base.info(verbose=True, show_counts=True)

formato:
<class 'pandas.core.frame.DataFrame'>
Index: 5182930 entries, 0 to 6496111
Data columns (total 9 columns):
 #   Column              Non-Null Count    Dtype 
---  ------              --------------    ----- 
 0   linea               5182930 non-null  object
 1   nombre_completo     5182930 non-null  object
 2   tipo_doc            5182930 non-null  object
 3   dni                 5182930 non-null  object
 4   compania            5182930 non-null  object
 5   contrato            5182930 non-null  object
 6   fecha_portout       5182930 non-null  object
 7   cantidad_de_lineas  5182930 non-null  object
 8   otras_lineas        5182930 non-null  object
dtypes: object(9)
memory usage: 395.4+ MB

"""
# ------------
"""
df_lineas_filtradas.info(verbose=True, show_counts=True)

formato:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1049287 entries, 0 to 1049286
Data columns (total 1 columns):
 #   Column  Non-Null Count    Dtype 
---  ------  --------------    ----- 
 0   linea   1049287 non-null  object
dtypes: object(1)
memory usage: 8.0+ MB
"""
# -------------
"""
df_registro_no_llame.info(verbose=True, show_counts=True)

formato:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3075996 entries, 0 to 3075995
Data columns (total 1 columns):
 #   Column  Non-Null Count    Dtype 
---  ------  --------------    ----- 
 0   linea   3075996 non-null  object
dtypes: object(1)
memory usage: 23.5+ MB
"""


# ---------------------
# LIMPIEZA DEL df_base
# ---------------------

# Asegurar que las columnas importantes sean string y eliminar espacios en blanco
cols_to_strip = ['linea', 'dni', 'fecha_portout']
for col in cols_to_strip:
    df_base[col] = df_base[col].astype(str).str.strip()

df_base["linea"] = df_base["linea"].str.replace(r'\.0$', '', regex=True) # Formatear la columna 'linea' eliminando '.0' si existiera al final
df_base = df_base[(df_base['linea'] != '') & (df_base['dni'] != '') & (df_base['fecha_portout'] != '')] # Eliminar filas donde 'linea', 'dni' o 'fecha_portout' estén vacías
df_base = df_base.drop_duplicates(subset=['linea'], keep='first') # Eliminar duplicados en ['linea', 'dni']


# ---------------------
# CONTAR Y FILTRAR REGISTROS NO DESEADOS
# ---------------------

# Cantidad de registros que coinciden en cada parquet
eliminados_lineas_filtradas = df_base['linea'].isin(df_lineas_filtradas['linea']).sum()
eliminados_no_llame = df_base['linea'].isin(df_registro_no_llame['linea']).sum()

print(f"Registros eliminados por df_lineas_filtradas: {eliminados_lineas_filtradas}")
print(f"Registros eliminados por df_registro_no_llame: {eliminados_no_llame}")

# Crear un conjunto con todas las líneas que se deben eliminar
lineas_a_eliminar = set(df_lineas_filtradas['linea']).union(set(df_registro_no_llame['linea']))
df_base = df_base[~df_base['linea'].isin(lineas_a_eliminar)]

print(f"Cantidad de registros después de la limpieza y filtrado: {df_base.shape[0]}")



# ---------------------
# APLICAR FILTROS ADICIONALES - para seleccion de LOTE
# ---------------------

# 1. Filtrar por DNI válido (numérico y rango argentino)
print("\n=== FILTRO 1: DNI ===")
df_base['dni'] = pd.to_numeric(df_base['dni'], errors='coerce')
registros_antes = df_base.shape[0]
df_base = df_base.dropna(subset=['dni'])
df_base['dni'] = df_base['dni'].astype(int)
df_base = df_base[(df_base['dni'] >= 10000000) & (df_base['dni'] <= 99999999)]
print(f"Eliminados por DNI inválido: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")

# 2. Filtrar por tipo de contrato
print("\n=== FILTRO 2: CONTRATO ===")
registros_antes = df_base.shape[0]
df_base = df_base[df_base['contrato'].isin(['Contrato CPP', 'Activa (Prepago)'])]
print(f"Eliminados por tipo de contrato: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")

# 3. Convertir y filtrar por fecha de portación
print("\n=== FILTRO 3: FECHA PORTACIÓN ===")
df_base['fecha_portout'] = pd.to_datetime(df_base['fecha_portout'], errors='coerce')
registros_antes = df_base.shape[0]
df_base = df_base.dropna(subset=['fecha_portout'])
print(f"Eliminados por fecha inválida/nula: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")
# Filtrar portaciones mayores a 30 días
fecha_limite = datetime.now() - timedelta(days=30)
df_base = df_base[df_base['fecha_portout'] < fecha_limite]
print(f"Eliminados por fecha inválida o reciente (<30 días): {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")


# 4. Filtros complejos por contrato + año
print("\n=== FILTRO 4: CONTRATO + AÑO ===")
registros_antes = df_base.shape[0]
mask_cpp = (
    (df_base['contrato'] == 'Contrato CPP') &
    (df_base['fecha_portout'].dt.year.isin([2025, 2024, 2023]))
)
mask_prepagos = (
    (df_base['contrato'] == 'Activa (Prepago)') &
    (df_base['fecha_portout'].dt.year.isin([2025, 2024]))
)
df_base = df_base[mask_cpp | mask_prepagos]
print(f"Eliminados por reglas de contrato+año: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")

# 5. Filtrar cantidad de líneas (1 a 7)
print("\n=== FILTRO 5: CANTIDAD DE LÍNEAS ===")
df_base['cantidad_de_lineas'] = pd.to_numeric(df_base['cantidad_de_lineas'], errors='coerce')
registros_antes = df_base.shape[0]
df_base = df_base.dropna(subset=['cantidad_de_lineas'])
df_base = df_base[(df_base['cantidad_de_lineas'] >= 1) & (df_base['cantidad_de_lineas'] <= 7)]
print(f"Eliminados por cantidad de líneas: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")


# 6. Filtrar por compañía (Claro)  -- NO FILTRAR SOLO POR CLARO
print("\n=== FILTRO 6: COMPAÑÍA ===")
registros_antes = df_base.shape[0]
df_base = df_base[df_base['compania'] == 'Claro']
print(f"Eliminados por compañía diferente a Claro: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")



# 7. Filtrar línea por rango o prefijos
print("\n=== FILTRO 7: RANGO/PREFIJOS DE LÍNEA ===")
df_base['linea_num'] = pd.to_numeric(df_base['linea'], errors='coerce')
registros_antes = df_base.shape[0]
df_base = df_base.dropna(subset=['linea_num'])

# Prefijos argentinos a incluir
prefijos = ('342', '341', '351', '387', '381')
mask_prefijos = df_base['linea_num'].astype(int).astype(str).str.startswith(prefijos)
mask_rango = df_base['linea_num'] < 3000000000
df_base = df_base[mask_rango | mask_prefijos]

# Eliminar columna temporal
df_base = df_base.drop(columns=['linea_num'])
print(f"Eliminados por rango/prefijo de línea: {registros_antes - df_base.shape[0]}")
print(f"Registros restantes: {df_base.shape[0]}")

# ---------------------
# RESUMEN FINAL
# ---------------------
print("\n" + "="*60)
print("RESUMEN DE FILTROS APLICADOS")
print("="*60)
print(f"✅ Registros finales: {df_base.shape[0]}")
print(f"✅ Columnas: {df_base.columns.tolist()}")
print("\nDatos listos para procesar")



# ---------------------
# ACTUALIZAR ESTADOS CON IRIS CONSOLIDADO
# ---------------------
"""
df_base.head(2)

linea	nombre_completo	tipo_doc	dni	compania	contrato	fecha_portout	cantidad_de_lineas	otras_lineas
1120122233	GUERRINA DOMINGO LUIS	Documento Nacional Identidad	21903576	Claro	Contrato CPP	2024-09-17	3	1126401407, 1154280257
1120122769	MARTINEZ MARIA CRISTINA	Documento Nacional Identidad	12647091	Claro	Contrato CPP	2025-08-26	1	nan
"""
"""
df_base.info()

<class 'pandas.core.frame.DataFrame'>
Index: 783363 entries, 255 to 6461328
Data columns (total 9 columns):
 #   Column              Non-Null Count   Dtype         
---  ------              --------------   -----         
 0   linea               783363 non-null  object        
 1   nombre_completo     783363 non-null  object        
 2   tipo_doc            783363 non-null  object        
 3   dni                 783363 non-null  int64         
 4   compania            783363 non-null  object        
 5   contrato            783363 non-null  object        
 6   fecha_portout       783363 non-null  datetime64[ns]
 7   cantidad_de_lineas  783363 non-null  int64         
 8   otras_lineas        783363 non-null  object        
dtypes: datetime64[ns](1), int64(2), object(6)
memory usage: 59.8+ MB
"""
"""
df_iris_consolidado.head(2)

linea	fecha	estado	fecha_consulta
2966217123	2025-09-30	Port In	2025-09-30
1161933139	2025-09-30	Port In	2025-09-30
"""

"""
df_iris_consolidado.info()

<class 'pandas.core.frame.DataFrame'>
RangeIndex: 5469332 entries, 0 to 5469331
Data columns (total 4 columns):
 #   Column          Dtype 
---  ------          ----- 
 0   linea           object
 1   fecha           object
 2   estado          object
 3   fecha_consulta  object
dtypes: object(4)
memory usage: 166.9+ MB
"""

# --- Asegurar tipos ---
df_base['linea'] = df_base['linea'].astype(str)
df_iris_consolidado['linea'] = df_iris_consolidado['linea'].astype(str)
df_iris_consolidado['fecha'] = pd.to_datetime(df_iris_consolidado['fecha'], errors='coerce')
df_iris_consolidado['fecha_consulta'] = pd.to_datetime(df_iris_consolidado['fecha_consulta'], errors='coerce')

# Asegurar que las líneas sean string
df_base['linea'] = df_base['linea'].astype(str)
df_iris_consolidado['linea'] = df_iris_consolidado['linea'].astype(str)

# Filtrar df_iris solo con 'Port Out'
df_iris_portout = df_iris_consolidado[df_iris_consolidado['estado'] == 'Port Out']

# Crear un set de líneas con Port Out para búsqueda rápida
lineas_portout = set(df_iris_portout['linea'])

# Filtrar df_base
df_filtered = df_base[
    (~df_base['linea'].isin(df_iris_consolidado['linea'])) |  # no está en iris
    (df_base['linea'].isin(lineas_portout))                 # está y es Port Out
].copy()

print(f"Registros originales: {df_base.shape[0]}")
print(f"Registros después del filtro: {df_filtered.shape[0]}")



# ---------------------
# QUITAR LOTE LLAMADO AYER
# ---------------------



cant_original = df_reporte_dia_anterior.shape[0]
df_reporte_dia_anterior = df_reporte_dia_anterior[df_reporte_dia_anterior['Cliente'].notna() & (df_reporte_dia_anterior['Cliente'].str.strip() != '')].copy()
print(f"Registros nulos/vacíos eliminados el reporte: {cant_original - df_reporte_dia_anterior.shape[0]}")


def procesar_numero(numero):
    """Procesa un número telefónico según las reglas especificadas"""
    if not isinstance(numero, str):
        numero = str(numero)
        
    numero = numero.strip()
    
    if numero.startswith("0"):
        numero = numero[1:]
    if numero.startswith("90"):
        numero = numero[2:]
    if numero.startswith("11"):
        if len(numero) > 3 and numero[2:4] == "15":  # Si "15" sigue a la característica
            numero = "11" + numero[4:]
    else:
        # Procesar otras características (3 o 4 dígitos)
        if len(numero) >= 5:
            caracteristica = numero[:3]
            if numero[3:5] == "15":  # Característica de 3 dígitos
                numero = caracteristica + numero[5:]
            elif len(numero) > 4 and numero[:4].isdigit() and numero[4:6] == "15":  # Característica de 4 dígitos
                caracteristica = numero[:4]
                numero = caracteristica + numero[6:]
    
    return numero

df_reporte_dia_anterior['Cliente'] = df_reporte_dia_anterior['Cliente'].apply(procesar_numero)
cant_original = df_base.shape[0]
df_base = df_base[~df_base['linea'].isin(df_reporte_dia_anterior['Cliente'])].copy()
print(f"Registros eliminados de df_base: {cant_original - df_base.shape[0]}")
print(f"Registros restantes en df_base: {df_base.shape[0]}")
 
df_base.to_csv(BASE_OUTPUT, sep=';', encoding='utf-8', index=False)