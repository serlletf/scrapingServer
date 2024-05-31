from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import re
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB
uri = "mongodb+srv://fuentesserllet4:lB8vGdvT5NvJa5pZ@clusterscrape.49uafag.mongodb.net/?retryWrites=true&w=majority&appName=ClusterScrape"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Configuración de opciones del navegador
opts = Options()
opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
opts.add_argument("--headless")

# XPaths para los elementos a extraer
xpaths = {
    "name": '//p[@class="Text_text__cB7NM Shelf_nameProduct__CXI5M Text_text--left__1v2Xw Text_text--flex__F7yuI Text_text--regular__KSs6J Text_text--md__H7JI_ Text_text--black__zYYxI Text_text__cursor--pointer__WZsQE Text_text--none__zez2n"]',
    "price": '//p[@class="Text_text__cB7NM Text_text--left__1v2Xw Text_text--flex__F7yuI Text_text--medium__rIScp Text_text--lg__GZWsa Text_text--primary__OoK0C Text_text__cursor--auto__cMaN1 Text_text--none__zez2n"]',
    "format": '//p[@class="Text_text__cB7NM Text_text--left__1v2Xw Text_text--flex__F7yuI Text_text--medium__rIScp Text_text--xs__Snd0F Text_text--gray-light__DxcpX Text_text__cursor--auto__cMaN1 Text_text--none__zez2n"]'
}

# Inicialización del driver de Chrome
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def scraping(url):
    driver.get(url)
    sleep(3)
    
    productos = {key: [element.text for element in driver.find_elements(By.XPATH, xpath)] for key, xpath in xpaths.items()}
    categoria = driver.title
    
    return productos, categoria

def clean_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def transform_to_mongodb(productos, categoria):
    data = [{"nombre": nombre, "precio": precio, "formato": formato} 
            for nombre, precio, formato in zip(productos["name"], productos["price"], productos["format"])]
    
    db = client['scraping']
    collection = db[clean_filename(categoria)]
    
    # Crear un índice único en el campo 'nombre' si no existe
    collection.create_index("nombre", unique=True)
    
    # Insertar datos en MongoDB sin duplicados
    for item in data:
        try:
            collection.insert_one(item)
        except:
            print(f"El producto {item['nombre']} ya existe en la colección {categoria}")

def categorias():
    urls = [
        'https://www.unimarc.cl/category/lacteos-huevos-y-refrigerados',
        'https://www.unimarc.cl/category/carnes',
        'https://www.unimarc.cl/category/frutas-y-verduras',
        'https://www.unimarc.cl/category/limpieza'
    ]
    
    for url in urls:
        productos, categoria = scraping(url)
        transform_to_mongodb(productos, categoria)

# Llamada a la función para extraer y guardar los datos
categorias()

# Cerrar el navegador al finalizar
driver.quit()