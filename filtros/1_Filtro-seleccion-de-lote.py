import pandas as pd
import os
from pathlib import Path

# ==================== CONFIGURACIÓN DE PATHS ====================
BASE_DIR = Path(__file__).parent.parent
DATA_ROOT = BASE_DIR / 'data' 
BASE_OUTPUT = DATA_ROOT / 'bases'

BASE_MAIN_DIR = DATA_ROOT / 'base_2024_2025_actualizada.csv'
NO_LLAME_DIR = DATA_ROOT / 'Registro_No_Llame.csv'


df = pd.read_csv(BASE_MAIN_DIR, sep=';', encoding='utf-8', dtype=str)


