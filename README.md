# PEU-INEI-PROYECTO INTEGRADOR-IPC

Proyecto Integrador sobre Cálculo del índice de precios al consumidor utilizando datos de la web para Lima Metropolitana, utilizando las categorías establecidas en la Encuesta Nacional de Presupuestos Nacionales (ENAPREF), específicamente para **5 divisiones** de productos: **Alimentos y Bebidas no alcohólicas**, **Salud**, **Educación**, **Transporte** y **Alojamiento, Agua, Electricidad, Gas y Otros Combustibles**

## Índice
- [Comenzando](#Comenzando) 
- [Autores](#Autores) 

## Comenzando
Estas instrucciones te permitirán obtener una copia del proyecto en funcionamiento en tu máquina local para propósitos de desarrollo y pruebas.

### Pre-requisitos 
- Anaconda [Descargar](https://www.anaconda.com/download)

### Instalación 
Para poder ejecutar los códigos, primero se debe crear un entorno realizando los siguientes pasos:

1. Entrar a la consola de Anaconda (Anaconda Prompt (Anaconda)).

2. Dentro de la consola de Anaconda, crear el entorno con el siguiente comando:
    ```bash
    conda create -n ENV_NAME python=3.12.3
    ```

3. Una vez creado el entorno, acceder a él usando el siguiente comando:
    ```bash
    conda activate ENV_NAME
    ```

4. Para desactivar el entorno, ejecutar el siguiente comando:
    ```bash
    conda deactivate
    ```

### Ejecución 
Para ejecutar cualquier código de extracción de datos, sigue los siguientes pasos:

1. Clona el repositorio en una carpeta usando el siguiente comando:
    ```bash
    git clone https://github.com/LuisF3381/PEU-CD-Grupo-1-Proyecto-Integrador.git
    ```

2. Una vez clonado el proyecto de GitHub, dirígete a la carpeta que contiene los códigos de los scrapers:
    ```bash
    cd PEU_INEI_Proyecto_Integrador_IPC/ipc-webscraping
    ```

3. Desde la consola de Anaconda, activa nuevamente el entorno:
    ```bash
    conda activate EN

4. Selecciona uno de los siguientes códigos según la página web a scrapear:

| **Website**   | **Ruta Código**                                        |
| ------------- | ------------------------------------------------------ |
| **PlazaVea**  | [scraper_plazavea.py](ipc-webscraping/scraper_plazavea.py) |
| **Metro**     | [scraper_metro.py](ipc-webscraping/scraper_metro.py)   |
| **Wong**      | [scraper_wong.py](ipc-webscraping/scraper_wong.py)     |
| **Vivanda**   | [scraper_vivanda.py](ipc-webscraping/scraper_vivanda.py) |
| **MiFarma**   | [Scraper_MIFARMA.py](ipc-webscraping/Scraper_MIFARMA.py) |
| **Inkafarma** | [Scraper_INKAFARMA.py](ipc-webscraping/Scraper_INKAFARMA.py) |
| **TaiLoy**    | [Tailoy_scraping.py](ipc-webscraping/Tailoy_scraping.py) |
| **Osinergmin**| [scraper_osinergmin.py](ipc-webscraping/scraper_osinergmin) |
| **Urbania**   | [scraper_urbania.py](ipc-webscraping/scraper_urbania) |

5. Una vez elegido el código a ejecutar, corre el siguiente comando:
    ```bash
    python RUTA_CODIGO
    ```

6. Cabe aclarar que la información se guarda en la carpeta `raw` [Carpeta](ipc-webscraping/data/raw). Posteriormente, se aplica la limpieza a la base de datos, y se guarda en la carpeta `processed` [Carpeta](ipc-webscraping/data/processed). Luego, se consolidan todas las fechas scrapeadas y el resultado se guarda en la carpeta `consolidated` [Carpeta](ipc-webscraping/data/consolidated). Finalmente, se realiza el cálculo para obtener el IPC por categorías y se guarda en la carpeta `base100` [Carpeta](ipc-webscraping/data/base100).

## Autores
- Jherson Cueva - [https://github.com/jhersoncueva]
- Luis Pérez - [https://github.com/luisenrique17500-pixel]
- Mateo Manrique - [https://github.com/MATEO-PROG-001]
- Ginno Castro - [https://github.com/GinnoJC]
- Diana Hidalgo - [https://github.com/dianahe2003]
