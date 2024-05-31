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
client = MongoClient(uri, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)

# Configuración de opciones del navegador
opts = Options()
opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
opts.add_argument("--headless")

# XPaths para los elementos a extraer
xpaths = {
    "name": '//a[@class="product-card-name"]',
    "price": '//span[@class="prices-main-price"]',
    "brand": '//a[@class="product-card-brand"]'
}

# Inicialización del driver de Chrome
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def scraping(url):
    driver.get(url)
    sleep(3)

    productos = {}
    for key, xpath in xpaths.items():
        values = []
        for attempt in range(3):
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                values = [element.text for element in elements]
                break
            except Exception as e:
                if attempt < 2:
                    sleep(1)
                else:
                    raise e
        productos[key] = values

    categoria = driver.title
    return productos, categoria

def clean_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def extract_format(name):
    # Expresión regular para extraer el formato del nombre del producto
    match = re.search(r'\d+\s?[a-zA-Z]+', name)
    if match:
        return name.replace(match.group(), '').strip(), match.group().strip()
    else:
        return name, ''

def transform_to_mongodb(productos, categoria):
    data = []
    for nombre, precio, marca in zip(productos["name"], productos["price"], productos["brand"]):
        nombre, formato = extract_format(nombre)
        data.append({"nombre": nombre, "precio": precio, "marca": marca, "formato": formato})
    
    db = client['scraping']
    collection = db[clean_filename(categoria)]
    
    # Crear un índice único en el campo 'nombre' si no existe
    collection.create_index("nombre", unique=True)
    
    # Insertar o actualizar datos en MongoDB
    for item in data:
        collection.update_one(
            {"nombre": item['nombre']},
            {"$set": item},
            upsert=True
        )

def categorias():
    urls = [
        'https://www.santaisabel.cl/lacteos',
        'https://www.jumbo.cl/lacteos',
    ]
    
    for url in urls:
        productos, categoria = scraping(url)
        transform_to_mongodb(productos, categoria)

# Llamada a la función para extraer y guardar los datos
categorias()

# Cerrar el navegador al finalizar
driver.quit()