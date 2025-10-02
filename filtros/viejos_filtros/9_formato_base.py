import pandas as pd
from datetime import datetime

# 📌 Lista de códigos de área de 4 dígitos
codigos_4_digitos = {
    "2202", "2221", "2223", "2224", "2225", "2226", "2227", "2229", "2241", "2242", "2243", "2244",
    "2245", "2246", "2252", "2254", "2255", "2257", "2261", "2262", "2264", "2265", "2266", "2267",
    "2268", "2271", "2272", "2273", "2274", "2281", "2283", "2284", "2285", "2286", "2291", "2292",
    "2296", "2297", "2302", "2314", "2316", "2317", "2320", "2323", "2324", "2325", "2326", "2331",
    "2333", "2334", "2335", "2336", "2337", "2338", "2342", "2343", "2344", "2345", "2346", "2352",
    "2353", "2354", "2355", "2356", "2357", "2358", "2392", "2393", "2394", "2395", "2396", "2473",
    "2474", "2475", "2477", "2478", "2622", "2624", "2625", "2626", "2646", "2647", "2648", "2651",
    "2652", "2655", "2656", "2657", "2658", "2901", "2902", "2903", "2920", "2921", "2922", "2923",
    "2924", "2925", "2926", "2927", "2928", "2929", "2931", "2932", "2933", "2934", "2935", "2936",
    "2940", "2942", "2945", "2946", "2948", "2952", "2953", "2954", "2962", "2963", "2964", "2966",
    "2972", "2982", "2983", "3327", "3329", "3382", "3385", "3387", "3388", "3400", "3401", "3402",
    "3404", "3405", "3406", "3407", "3408", "3409", "3435", "3436", "3437", "3438", "3442", "3444",
    "3445", "3446", "3447", "3454", "3455", "3456", "3458", "3460", "3462", "3463", "3464", "3465",
    "3466", "3467", "3468", "3469", "3471", "3472", "3476", "3482", "3483", "3487", "3489", "3491",
    "3492", "3493", "3496", "3497", "3498", "3521", "3522", "3524", "3525", "3532", "3533", "3537",
    "3541", "3542", "3543", "3544", "3546", "3547", "3548", "3549", "3562", "3563", "3564", "3571",
    "3572", "3573", "3574", "3575", "3576", "3582", "3583", "3584", "3585", "3711", "3715", "3716",
    "3718", "3721", "3725", "3731", "3734", "3735", "3741", "3743", "3751", "3754", "3755", "3756",
    "3757", "3758", "3772", "3773", "3774", "3775", "3777", "3781", "3782", "3786", "3821", "3825",
    "3826", "3827", "3832", "3835", "3837", "3838", "3841", "3843", "3844", "3845", "3846", "3854",
    "3855", "3856", "3857", "3858", "3861", "3862", "3863", "3865", "3867", "3868", "3869", "3873",
    "3876", "3877", "3878", "3885", "3886", "3887", "3888", "3891", "3892", "3894"
}

archivo_base = input("Ingrese la ruta del archivo BASE a formatear: ").strip()

# 📌 Cargar el archivo CSV original y eliminar duplicados
df = pd.read_csv(archivo_base, delimiter=';', dtype=str)

df = df.drop_duplicates(subset=['linea'])  # Elimina líneas duplicadas

# Función para extraer ANI1
def extraer_ani1(linea):
    if isinstance(linea, str):
        linea = linea.strip()
        if linea.startswith("11"):
            return "11"
        if linea[:4] in codigos_4_digitos:
            return linea[:4]
        return linea[:3]
    return ""


# Función para agregar "15" después del ANI1 en Linea1
def agregar_15_a_linea(numero):
    if isinstance(numero, str):
        numero = numero.strip()
        ani1 = extraer_ani1(numero)
        return ani1 + "15" + numero[len(ani1):]
    return ""

# Función para generar la columna BBDD
def generar_nombre_bbdd(dni):
    if isinstance(dni, str) and dni.strip().isdigit():
        dni = dni.strip()
        hoy = datetime.today().strftime('%Y%m%d')
        hora_actual = datetime.now().hour
        turno = "TM" if hora_actual < 13 else "TT" #QUE ES ESTO?? ajjajs usar el codigo de abajo
        tipo_id = "MIXTA"
        return f"{hoy}_Mza_{tipo_id}_{turno}"
    return ""

"""
# USAR ESTE CODIGO PARA QUE LA MITAD DE LA COLUMA DEL df_final SEA TT Y LA OTRA MITAD TM
def generar_nombre_bbdd(dni, indice, total):
    if isinstance(dni, str) and dni.strip().isdigit():
        dni = dni.strip()
        hoy = datetime.today().strftime('%Y%m%d')
        tipo_id = "MIXTA"
        # Mitad TM, mitad TT
        turno = "TM" if indice < total // 2 else "TT"
        return f"{hoy}_Mza_{tipo_id}_{turno}"
    return ""

total_filas = len(df_final)
df_final['BBDD'] = [
    generar_nombre_bbdd(dni, i, total_filas) 
    for i, dni in enumerate(df_final['DNI'])
]
    
"""

#  Aplicar transformaciones
df['ANI1'] = df['linea'].apply(extraer_ani1)
df['Linea1'] = df['linea'].apply(agregar_15_a_linea)
df['Linea2'] = df['linea']

#  Renombrar columnas y asignar valores
df_final = df.rename(columns={
    'nombre_completo': 'Nombre del Cliente',
    'dni': 'DNI',
    'compania': 'OperadorActual',
    'contrato': 'PlanActual',
    'cantidad_de_lineas': 'CP',
    'otras_lineas': 'Domicilio',
    'fecha_portacion': 'Localidad'
})

#  Generar la columna BBDD
df_final['BBDD'] = df_final['DNI'].apply(generar_nombre_bbdd) # Usar el codigo anterior para reemplazar este

#  Agregar columnas vacías para completar el formato
columnas_faltantes = ['email', 'Provincia', 'Generico']
for col in columnas_faltantes:
    df_final[col] = ''

#  Reordenar columnas y guardar
df_final = df_final[['Nombre del Cliente', 'DNI', 'ANI1', 'Linea1', 'Linea2', 'PlanActual', 'OperadorActual',
                     'Domicilio', 'CP', 'Localidad', 'email', 'Provincia', 'BBDD', 'Generico']]
nombre_archivo = generar_nombre_bbdd(df_final['DNI'].iloc[0]) + '.csv'
df_final.to_csv(nombre_archivo, sep=';', index=False)

print(f"✅ Conversión completada. Archivo guardado como '{nombre_archivo}'.")
