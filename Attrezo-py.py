
# Librerías de extracción de datos
# -----------------------------------------------------------------------
from bs4 import BeautifulSoup
import requests

# Tratamiento de datos
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np
from tqdm import tqdm

import aiohttp
import asyncio

async def fetch_async_url(url, session_objetivo):
    """
    Envía una solicitud HTTP asíncrona al URL especificado y retorna el contenido HTML de la respuesta.

    Args:
        url (str): La URL a la que se realizará la solicitud.
        session_objetivo (aiohttp.ClientSession): Sesión asíncrona activa para gestionar las solicitudes HTTP.

    Returns:
        str: El contenido HTML de la respuesta si la solicitud es exitosa.
        None: Si la solicitud falla o el código de estado no es 200.
    """
    async with session_objetivo.get(url) as response:
        if response.status != 200:
            print("Se ha producido un error")
            
        print(f"Status code: {response.status}")
        return await response.text()


async def sopa_atrezzo(session, page):
    """
    Realiza una solicitud a la página web del catálogo de Atrezzo Vázquez y obtiene el contenido HTML en forma de `BeautifulSoup`.

    Args:
        session (aiohttp.ClientSession): Sesión asíncrona activa para gestionar las solicitudes HTTP.
        page (int): Número de página del catálogo a solicitar.

    Returns:
        BeautifulSoup: Objeto de `BeautifulSoup` que contiene el HTML parseado de la página web.
    """
    url_atrezzo = f"https://atrezzovazquez.es/shop.php?search_type=-1&search_terms=&limit=48&page={page}"
    html_content = await fetch_async_url(session_objetivo=session,url = url_atrezzo)
    if html_content:
        return BeautifulSoup(html_content, "html.parser")


def llenar_diccionario(sopa_atrezzo):
    """
    Extrae información sobre los productos del contenido HTML proporcionado por la página de Atrezzo Vázquez, 
    incluyendo nombre, categoría, descripción, sección y URL de la imagen.

    Args:
        sopa_atrezzo (BeautifulSoup): Objeto de `BeautifulSoup` que contiene el contenido HTML parseado.

    Returns:
        pandas.DataFrame: DataFrame que contiene los detalles de los productos, con las columnas:
                          - 'nombre'
                          - 'categoría'
                          - 'descripción'
                          - 'sección'
                          - 'url'
    """
    lista_prod_cat = sopa_atrezzo.findAll("div", {"class": "product-slide-entry shift-image"})
    nombres_productos = []
    categorias_productos = []

    for producto in lista_prod_cat:
        try:
            nombre = producto.findAll("a", {"class": "title"})[0].getText()
            nombres_productos.append(nombre)
        except:
            nombres_productos.append(np.nan)
        
        try:
            categoria = producto.findAll("a", {"class": "tag"})[0].getText()
            categorias_productos.append(categoria)
        except:
            categorias_productos.append(np.nan)
    
    lista_secciones = sopa_atrezzo.findAll("div", {"class": "cat-sec-box"})
    secciones_productos = []
    
    for seccion in lista_secciones:
        if len(seccion.getText()) > 1:  # hay resultados
            secciones_productos.append(seccion.getText())
        else:
            secciones_productos.append(np.nan)
    
    lista_descripciones = sopa_atrezzo.findAll("p")
    descripciones_productos = []
    
    for descripcion in lista_descripciones:
        descripciones_productos.append(descripcion.getText())
    descripciones_productos = descripciones_productos[:48]
    
    lista_imagenes = sopa_atrezzo.findAll("div", {"class": "product-image"})
    imagenes_productos = []
    
    for imagen in lista_imagenes:
        try:
            link = imagen.findAll("img")[0].get("src")
            imagenes_productos.append(link)
        except:
            imagenes_productos.append(np.nan)
    
    df_atrezzo = pd.DataFrame({
        "nombre": nombres_productos,
        "categoría": categorias_productos,
        "descripción": descripciones_productos,
        "sección": secciones_productos,
        "url": imagenes_productos
    })
    return df_atrezzo


def concat_df(df_list):
    """
    Concatena una lista de DataFrames en un único DataFrame.

    Args:
        df_list (list): Lista de DataFrames a concatenar.

    Returns:
        pandas.DataFrame: DataFrame concatenado que incluye todos los datos de los DataFrames en la lista.
    """
    df_completo = pd.concat(df_list, ignore_index=True)
    return df_completo


async def main(paginas):
    """
    Función principal que gestiona la extracción de datos de múltiples páginas del catálogo de Atrezzo Vázquez
    de manera asíncrona, procesa los datos y los concatena en un DataFrame final.

    Args:
        paginas (int): Número de páginas del catálogo a procesar.

    Returns:
        pandas.DataFrame: DataFrame final que contiene todos los datos de las páginas procesadas.
    """
    async with aiohttp.ClientSession() as session:
        tareas = []
        for pagina in range(1, paginas):
            tareas.append(sopa_atrezzo(session, pagina))
        
        sopas = await asyncio.gather(*tareas)  # Lanza todas las solicitudes asíncronas
        
        df_list = []
        for sopa in sopas:
            try:
                df_atrezzo = llenar_diccionario(sopa)
                df_list.append(df_atrezzo)
            except:
                print("No se ha añadido la página")
        
        df_completo = concat_df(df_list)
        return df_completo