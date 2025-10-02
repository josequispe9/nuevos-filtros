# Proyecto de Filtro de Datos para Campañas de Telemarketing

## Descripción

Este proyecto procesa y filtra grandes volúmenes de datos de clientes para campañas de telemarketing. El objetivo es consolidar, limpiar, normalizar y seleccionar los registros más relevantes para maximizar la eficiencia de las campañas.

El proceso se divide en tres scripts principales que deben ejecutarse en orden:

1.  **Generación de archivos de filtrado** (`1_generar-archivos-filtrado.py`): Consolida y limpia datos brutos de reportes y del sistema Iris, generando archivos Parquet consolidados.
2.  **Filtro y selección de lote** (`2_Filtro-seleccion-de-lote.py`): Aplica reglas de negocio complejas para seleccionar un lote de clientes potenciales, eliminando registros no deseados.
3.  **Formato de base** (`3_Formato-base.py`): Genera el archivo CSV final con el formato requerido para las campañas, incluyendo enriquecimiento con datos CUIT.

## Estructura de Carpetas

```
.
├── data/
│   ├── bases/                          # Bases finales generadas
│   ├── processed/                      # Archivos consolidados (.parquet)
│   │   ├── Tipificaciones-consolidadas.parquet
│   │   └── Iris-consolidado.parquet
│   ├── raw/                           # Datos crudos de entrada
│   │   ├── reportes/                  # Reportes de llamadas (CSV)
│   │   └── extraerEstado/             # Estados Iris (TXT)
│   ├── base_2024_2025_actualizada.parquet
│   ├── Registro_No_Llame.parquet
│   ├── lineas_filtradas_150.parquet
│   └── base_cuit.csv
├── filtros/
│   ├── 1_generar-archivos-filtrado.py
│   ├── 2_Filtro-seleccion-de-lote.py
│   └── 3_Formato-base.py
├── .gitignore
└── requirements.txt
```

## Requisitos

- Python 3.7 o superior

## Instalación

Instalar las dependencias desde el archivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

**Dependencias:**
- `pandas`: Manipulación y análisis de datos
- `pyarrow`: Lectura/escritura de archivos Parquet

## Uso

Los scripts deben ejecutarse en orden. Se recomienda ejecutarlos desde la raíz del proyecto:

### 1. Generar Archivos de Filtrado

**Descripción:**
Consolida reportes de llamadas y estados Iris, creando archivos `.parquet` actualizados incrementalmente.

**Entrada:**
- `data/raw/reportes/*.csv` - Reportes de llamadas con tipificaciones
- `data/raw/extraerEstado/*.txt` - Estados de líneas desde sistema Iris

**Salida:**
- `data/processed/Tipificaciones-consolidadas.parquet`
- `data/processed/Iris-consolidado.parquet`

**Características:**
- Normaliza números telefónicos argentinos (elimina 0, 90, 15)
- Filtra solo tipificaciones relevantes
- Actualización incremental: conserva registros históricos, actualiza con datos más recientes
- Marca archivos procesados con sufijo `-p` para evitar reprocesamiento

**Ejecución:**
```bash
python filtros/1_generar-archivos-filtrado.py
```

---

### 2. Filtro y Selección de Lote

**Descripción:**
Aplica reglas de negocio para seleccionar clientes potenciales, eliminando registros no deseados.

**Entrada:**
- `data/base_2024_2025_actualizada.parquet` - Base principal de clientes
- `data/Registro_No_Llame.parquet` - Lista de exclusión
- `data/lineas_filtradas_150.parquet` - Líneas previamente filtradas
- `data/processed/Iris-consolidado.parquet` - Estados de líneas
- `data/raw/reportes/[fecha]-p.csv` - Reporte del día anterior

**Salida:**
- `data/bases/base.csv` - Base filtrada lista para formato final

**Filtros aplicados:**
1. **DNI válido**: Solo DNI argentinos (8 dígitos, rango 10M-99M)
2. **Tipo de contrato**: CPP o Prepago
3. **Fecha portación**: Mayor a 30 días
4. **Contratos + Año**:
   - CPP: 2023, 2024, 2025
   - Prepago: 2024, 2025
5. **Cantidad de líneas**: Entre 1 y 7
6. **Compañía**: Solo Claro
7. **Prefijos/Rangos**: Líneas argentinas válidas
8. **Estado Iris**: Excluye Port In (conserva Port Out y líneas no registradas)
9. **Llamadas recientes**: Excluye lote del día anterior

**Ejecución:**
```bash
python filtros/2_Filtro-seleccion-de-lote.py
```

---

### 3. Formato de Base

**Descripción:**
Genera el archivo CSV final con el formato requerido para las campañas de telemarketing.

**Entrada:**
- `data/bases/base.csv` - Base filtrada del script anterior
- `data/base_cuit.csv` - Base de CUIT para enriquecimiento

**Salida:**
- `data/bases/BASE_FINAL_[YYYYMMDD]_MIXTA.csv`

**Transformaciones:**
- Extrae código de área (ANI1)
- Genera líneas con formato de 15
- Enriquece con datos CUIT (si existe)
- Limita duplicados a máximo 2 registros por DNI/CUIT
- Divide la base en dos turnos: TM (mañana) y TT (tarde)

**Columnas finales:**
- `Nombre del Cliente`, `DNI`, `ANI1`, `Linea1`, `Linea2`
- `PlanActual`, `OperadorActual`, `Domicilio`, `CP`, `Localidad`
- `email` (indica "BASE CUIT" si tiene CUIT)
- `Provincia` (contiene CUIT si existe)
- `BBDD` (nombre de base + turno), `Generico`

**Ejecución:**
```bash
python filtros/3_Formato-base.py
```
