import os
import pandas as pd

archivo_base = input("Ingrese la ruta del archivo BASE a rellenar: ").strip()
archivoCuit = "base_cuit.csv"


def llenarCuit():
    df_base = pd.read_csv(archivo_base, delimiter=";", encoding='utf-8')
    df_cuit = pd.read_csv(archivoCuit, delimiter=",", encoding="utf-8")
    
    chunk_size = 100000  
    chunks = []
    total_rows = df_base.shape[0]
        
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        
        df_chunk = df_base.iloc[start:end].copy()
        df_chunk = df_chunk.merge(df_cuit[['DNI', 'CUIT']], on='DNI', how='left')
        
        # Crear columna email basada en si tiene CUIT
        df_chunk['email'] = df_chunk['CUIT'].notnull().map({True: 'BASE CUIT', False: ""})
        df_chunk['Provincia'] = df_chunk['CUIT'].fillna("").astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
        df_chunk.rename(columns={'CUIT': 'CUIT_base'}, inplace=True)
           
        df_chunk = df_chunk.fillna("")
        
        chunks.append(df_chunk)

    df_enriquecido = pd.concat(chunks, ignore_index=True)
    
    #df_enriquecido = df_enriquecido.iloc[:, 1:-1]    
    df_enriquecido.to_csv('CON_CUIT_'+archivo_base, sep=";", encoding='utf-8')
    
    return df_enriquecido

if __name__ == "__main__":
    llenarCuit()