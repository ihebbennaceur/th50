import json
import psycopg2
from concurrent.futures import ThreadPoolExecutor
import threading

with open("json_name.txt", 'r', encoding='utf-8') as text_file:
    input_string = text_file.read().strip()

fn = input_string
# fn="fproducts_success_2025-01-29_18-58-32.json"
# Charger le fichier JSON existant
with open(fn, 'r') as file:
    data = json.load(file)

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="new_db3",
    user="new_user",
    password="nazir",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

# Charger les correspondances de la base de données
cursor.execute("SELECT original_url, cloud_url FROM mimages")
url_mapping = {row[0]: row[1] for row in cursor.fetchall()}

# Verrou pour la liste des liens non trouvés
not_found_lock = threading.Lock()
not_found_links = []

# Fonction pour remplacer les liens et collecter les non trouvés
def replace_urls(url_list):
    new_urls = []
    local_not_found = []
    for url in url_list:
        if url in url_mapping:
            new_urls.append(url_mapping[url])
        else:
            local_not_found.append(url)
            new_urls.append("")  # Remplacer par une chaîne vide si l'URL n'est pas trouvée
    
    # Ajouter les liens non trouvés à la liste globale de manière thread-safe
    with not_found_lock:
        not_found_links.extend(local_not_found)
    
    return list(set(new_urls))  # Éviter les doublons en utilisant un set

# Fonction pour traiter un produit
def process_product(product):
    if 'descImg' in product:
        product['descImg']['RU'] = replace_urls(product['descImg']['RU'])
    if 'mainImages' in product:
        product['mainImages']['RU'] = replace_urls(product['mainImages']['RU'])
    if 'sku' in product:
        for sku in product['sku']:
            if 'skuImage' in sku:
                original_url = sku['skuImage']['RU']
                if original_url not in url_mapping:
                    sku['skuImage']['RU'] = ""  # Supprime l'URL si elle n'est pas trouvée
                else:
                    sku['skuImage']['RU'] = url_mapping[original_url]
    return product

# Traiter les produits en parallèle
with ThreadPoolExecutor() as executor:
    data = list(executor.map(process_product, data))

# Sauvegarder le fichier JSON modifié dans le même fichier
with open(fn ,'w') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)

# Sauvegarder les liens non trouvés dans un fichier
with open('notfoundlink.txt', 'w') as file:
    for link in not_found_links:
        file.write(link + '\n')

# Fermer la connexion à la base de données
cursor.close()
conn.close()