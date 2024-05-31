from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import re
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging

# Configuración de logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configuración de MongoDB
MONGO_URI = "mongodb+srv://fuentesserllet4:lB8vGdvT5NvJa5pZ@clusterscrape.49uafag.mongodb.net/?retryWrites=true&w=majority&appName=ClusterScrape"
DB_NAME = 'scraping'

# XPaths para los elementos a extraer
XPATHS = {
    "name": '//p[@class="CardName__CardNameStyles-sc-147zxke-0 bWeSzf prod__name"]',
    "price": '//p[@class="CardBasePrice__CardBasePriceStyles-sc-1dlx87w-0 bhSKFL base__price"]'
}

def init_mongodb(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[DB_NAME]
    return db

def init_webdriver():
    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
    opts.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

def clean_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def extract_format(name):
    match = re.search(r'\d+\s?[a-zA-Z]+', name)
    if match:
        return name.replace(match.group(), '').strip(), match.group().strip()
    else:
        return name, ''

def scraping(driver, url):
    logger.debug(f"Navegando a {url}")
    driver.get(url)
    sleep(12)  # Ajustar según el rendimiento de la página y la conexión

    productos = {}
    for key, xpath in XPATHS.items():
        values = []
        for attempt in range(2):
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                values = [element.text for element in elements]
                break
            except Exception as e:
                logger.warning(f"Error al intentar extraer {key}, intento {attempt + 1}: {e}")
                sleep(1)
        productos[key] = values

    categoria = driver.title
    logger.debug(f"Productos extraídos: {productos}")
    logger.debug(f"Categoría extraída: {categoria}")
    return productos, categoria

def transform_to_mongodb(db, productos, categoria):
    data = []
    for nombre, precio in zip(productos["name"], productos["price"]):
        nombre, formato = extract_format(nombre)
        data.append({"nombre": nombre, "precio": precio, "formato": formato})
    
    collection_name = clean_filename(categoria)
    collection = db[collection_name]

    logger.debug(f"Insertando datos en la colección '{collection_name}'")
    collection.create_index("nombre", unique=True)

    for item in data:
        logger.debug(f"Insertando/Actualizando: {item}")
        collection.update_one(
            {"nombre": item['nombre']},
            {"$set": item},
            upsert=True
        )

    logger.info(f"Datos insertados/actualizados en la colección '{collection_name}'")

def categorias(driver, db):
    urls = [
        'https://www.acuenta.cl/ca/frescos-y-lacteos/07',
    ]
    
    for url in urls:
        try:
            productos, categoria = scraping(driver, url)
            transform_to_mongodb(db, productos, categoria)
        except Exception as e:
            logger.error(f"Error al procesar la URL {url}: {e}")

def main():
    db = init_mongodb(MONGO_URI)
    driver = init_webdriver()

    try:
        categorias(driver, db)
    finally:
        driver.quit()
        logger.info("Navegador cerrado correctamente")

if __name__ == "__main__":
    main()
