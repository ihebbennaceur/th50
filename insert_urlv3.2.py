import json
import psycopg2

with open("json_name.txt", 'r', encoding='utf-8') as text_file:
    input_string = text_file.read().strip()

fn = input_string

# fn="fproducts_success_2025-01-29_18-58-32.json"

# Charger le fichier JSON existant
print("Chargement du fichier JSON...")
with open(fn, 'r') as file:
    data = json.load(file)
print("Fichier JSON chargé avec succès.")

# Connexion à la base de données PostgreSQL
print("Connexion à la base de données PostgreSQL...")
conn = psycopg2.connect(
    dbname="new_db3",
    user="new_user",
    password="nazir",
    host="localhost",
    port=5432
)
cursor = conn.cursor()
print("Connexion réussie.")

# Charger les correspondances de la base de données (cloud_url, cat, category)
print("Chargement des correspondances de la base de données...")
cursor.execute("SELECT cloud_url, cat, category FROM mimages")
url_mapping = {row[0]: (row[1] if row[1] else "-1", row[2]) for row in cursor.fetchall()}  # Mapping cloud_url -> (cat, category)
print(f"{len(url_mapping)} correspondances chargées.")

# Liste des catégories autorisées
allowed_categories = {"0", "-1", "13", "6", "7", "8", "10", "4"}

# Liste pour stocker les liens non trouvés
not_found_links = set()

# Fonction pour remplacer les liens et collecter les non trouvés
def replace_urls(url_list):
    new_urls = set()  # Utiliser un set pour éviter les doublons
    for url in url_list:
        if url in url_mapping:
            cat, category = url_mapping[url]
            if category == 'main' and cat not in allowed_categories:
                print(f"URL {url} supprimée (catégorie {cat} non autorisée).")
            else:
                new_urls.add(url)  # Ajouter l'URL seulement si elle est valide
                print(f"URL {url} conservée.")
        else:
            not_found_links.add(url)
            print(f"URL {url} non trouvée dans la base de données.")
    
    return list(new_urls)  # Convertir en liste après suppression des doublons et des vides

# Remplacement des URLs dans les produits
print("Mise à jour des images dans les produits...")
for product in data:
    product_id = product.get('id', 'inconnu')
    
    if 'descImg' in product:
        print(f"Traitement des images 'descImg' pour le produit {product_id}")
        product['descImg']['RU'] = replace_urls(product['descImg']['RU'])
    
    if 'mainImages' in product:
        print(f"Traitement des images 'mainImages' pour le produit {product_id}")
        product['mainImages']['RU'] = replace_urls(product['mainImages']['RU'])

# Sauvegarder le fichier JSON modifié
print("Sauvegarde du fichier JSON modifié...")
with open(fn, 'w') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)
print("Fichier JSON sauvegardé avec succès.")

# Sauvegarder les liens non trouvés dans un fichier
print(f"Enregistrement des {len(not_found_links)} liens non trouvés...")
with open('notfoundlink.txt', 'w') as file:
    for link in not_found_links:
        file.write(link + '\n')
print("Liens non trouvés enregistrés dans 'notfoundlink.txt'.")

# Fermer la connexion à la base de données
cursor.close()
conn.close()
print("Connexion à la base de données fermée.")
