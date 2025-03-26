import json
import os
from concurrent.futures import ThreadPoolExecutor
import threading

with open("json_name.txt", 'r', encoding='utf-8') as text_file:
    input_string = text_file.read().strip()

fn = input_string
# fn="fproducts_success_2025-01-29_18-58-32.json"

# Récupérer le chemin du fichier JSON depuis la variable d'environnement
json_file_path = fn
print(f"Chemin du fichier JSON récupéré : {json_file_path}")

# Fonction pour charger le fichier JSON
def load_json_file(file_path):
    print(f"Chargement du fichier JSON : {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# # Fonction pour traiter un seul produit
# def process_product(product):
#     product_id = product.get('id', 'inconnu')
#     print(f"Traitement du produit : {product_id}")
    
#     main_image_urls = set(product["mainImages"].get("RU", []))
#     #print(f"URLs des images principales avant mise à jour : {main_image_urls}")
    
#     for sku in product["sku"]:
#         sku_image_url = sku["skuImage"].get("RU", "")
#         if sku_image_url and sku_image_url not in main_image_urls:
#             #print(f"URL SKU {sku_image_url} ajoutée aux images principales.")
#             main_image_urls.add(sku_image_url)
    
#     # Mettre à jour mainImages avec les nouvelles URLs
#     product["mainImages"]["RU"] = list(main_image_urls)
#     print(f"URLs des images principales après mise à jour : {main_image_urls}")
    
#     return product
def process_product(product):
    product_id=product.get('id','inconnu')
    print(f"working on product :  {product_id}")

    main_image_urls=product["mainImages"].get("RU",[])

    for sku in product["sku"]:
        sku_image_url=sku["skuImage"].get("RU","")
        if sku_image_url and sku_image_url not in main_image_urls:
            main_image_urls.append(sku_image_url)

    product["mainImages"]["RU"] = main_image_urls
    print(f"URls of image main after update :  {main_image_urls}")   

    return product     


# Fonction principale pour traiter tous les produits en parallèle
def append_sku_images(data):
    print("Mise à jour des images en parallèle...")
    
    # Déterminer le nombre optimal de workers
    num_workers = min(30, (os.cpu_count() or 1) * 4)
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Traiter tous les produits en parallèle
        updated_data = list(executor.map(process_product, data))
    
    return updated_data

# Vérifier si le chemin du fichier JSON est valide
if json_file_path:
    # Charger les données JSON depuis le fichier
    try:
        data = load_json_file(json_file_path)
        print(f"{len(data)} produits trouvés dans le fichier JSON.")

        # Appliquer la fonction pour mettre à jour les images
        updated_data = append_sku_images(data)
        
        # Affichage des données mises à jour
        print("Données mises à jour :")
        print(json.dumps(updated_data[:1], ensure_ascii=False, indent=4))  # Affiche seulement le premier produit pour l'exemple

    except FileNotFoundError:
        print(f"Le fichier {json_file_path} n'a pas été trouvé.")
    except json.JSONDecodeError:
        print("Erreur lors de la lecture du fichier JSON.")
else:
    print("Le chemin du fichier JSON n'est pas défini dans le fichier .env.")