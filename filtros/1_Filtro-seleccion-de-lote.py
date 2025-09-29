import pandas as pd
import re
import os


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



