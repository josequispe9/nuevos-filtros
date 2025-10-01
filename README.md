# Proyecto de Filtro de Datos para Campañas de Telemarketing

## Descripción

Este proyecto está diseñado para procesar y filtrar grandes volúmenes de datos de clientes para campañas de telemarketing. El objetivo es limpiar, normalizar y seleccionar los registros más relevantes para maximizar la eficiencia de las campañas.

El proceso se divide en tres scripts principales que deben ser ejecutados en orden:

1.  **Generación de archivos de filtrado:** Consolida y limpia los datos brutos de reportes y del sistema Iris.
2.  **Filtro y selección de lote:** Aplica una serie de reglas de negocio para seleccionar un lote de clientes potenciales.
3.  **Formato de base:** Genera un archivo CSV final con la base de clientes filtrada.

## Estructura de Carpetas

```
.
├── data/
│   ├── bases/
│   ├── processed/
│   └── raw/
│       ├── reportes/
│       └── extraerEstado/
├── filtros/
│   ├── 1_generar-archivos-filtrado.py
│   ├── 2_Filtro-seleccion-de-lote.py
│   └── 3_Formato-base.py
├── .gitignore
└── requirements.txt
```

## Instalación

Para ejecutar los scripts de este proyecto, necesitas tener Python 3 instalado. Luego, puedes instalar las dependencias desde el archivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

Las dependencias necesarias son:

*   `pandas`
*   `pyarrow`

## Uso

Los scripts deben ser ejecutados en el siguiente orden desde la carpeta `filtros`:

### 1. Generar Archivos de Filtrado

Este script lee los datos crudos de `data/raw/reportes` y `data/raw/extraerEstado`, los procesa, y crea dos archivos consolidados en formato Parquet en la carpeta `data/processed`:

*   `Tipificaciones-consolidadas.parquet`
*   `Iris-consolidado.parquet`

Para ejecutarlo:

```bash
python filtros/1_generar-archivos-filtrado.py
```

### 2. Filtro y Selección de Lote

Este script carga la base de datos principal y aplica una serie de filtros para seleccionar un lote de clientes. Utiliza los archivos generados en el paso anterior, así como una lista de "no llamar".

Para ejecutarlo:

```bash
python filtros/2_Filtro-seleccion-de-lote.py
```

### 3. Formato de Base

Este script genera el archivo final `base.csv` con los datos de los clientes listos para ser utilizados en la campaña.

Para ejecutarlo:

```bash
python filtros/3_Formato-base.py
```
