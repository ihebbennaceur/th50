import json
import psycopg2

with open("json_name.txt", 'r', encoding='utf-8') as text_file:
    input_string = text_file.read().strip()

fn = input_string

# fn="fproducts_success_2025-01-29_18-58-32.json"
# Configuration
JSON_FILE = fn
DB_CONFIG = {
    "dbname": "new_db3",
    "user": "new_user",
    "password": "nazir",
    "host": "localhost",
    "port": 5432
}

def get_url_mapping():
    """Récupère le mapping product_id -> urls depuis la base avec diagnostic"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 1. Compter le nombre total d'entrées
    cursor.execute("SELECT COUNT(*) FROM mimages WHERE cat IN ('4', '13')")
    total = cursor.fetchone()[0]
    print(f"Total entries in DB: {total}")
    
    # 2. Récupérer les URLs avec leur product_id
    query = """
    SELECT product_id, cloud_url 
    FROM mimages 
    WHERE cat IN ('4', '13') 
    AND cloud_url IS NOT NULL
    """
    cursor.execute(query)
    
    url_map = {}
    sample_urls = []
    
    for product_id, cloud_url in cursor.fetchall():
        if product_id not in url_map:
            url_map[product_id] = []
        url_map[product_id].append(cloud_url)
        
        # Garder quelques exemples pour le debug
        if len(sample_urls) < 5:
            sample_urls.append((product_id, cloud_url))
    
    print(f"Exemples d'URLs trouvées: {sample_urls}")
    cursor.close()
    conn.close()
    return url_map

def update_json_file():
    """Met à jour le fichier JSON avec diagnostic complet"""
    # 1. Charger les URLs depuis la base
    url_map = get_url_mapping()
    print(f"\nProduct_ids uniques dans la base: {len(url_map)}")
    
    # 2. Charger le JSON
    with open(JSON_FILE, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        print(f"Produits dans le JSON: {len(data)}")
        
        # 3. Préparer le diagnostic
        matched_ids = set()
        updated = 0
        
        # 4. Vérifier les 5 premiers productIds du JSON
        print("\nExemples de productIds dans le JSON:")
        for i, product in enumerate(data[:5]):
            pid = product.get('productId', 'N/A')
            print(f"  {i+1}. {pid}")
        
        # 5. Parcourir tous les produits
        for product in data:
            product_id = product.get('productId')
            if not product_id:
                continue
                
            if product_id in url_map:
                matched_ids.add(product_id)
                
                # Initialiser sizeTableUrl si besoin
                if 'sizeTableUrl' not in product:
                    product['sizeTableUrl'] = []
                
                # Filtrer les URLs existantes
                existing = set(product['sizeTableUrl'])
                new_urls = [url for url in url_map[product_id] if url not in existing]
                
                # Ajouter les nouvelles URLs
                if new_urls:
                    product['sizeTableUrl'].extend(new_urls)
                    updated += 1
        
        # 6. Diagnostic final
        print(f"\nProduct_ids correspondants trouvés: {len(matched_ids)}")
        print(f"Produits mis à jour: {updated}")
        
        # 7. Réécrire le fichier
        f.seek(0)
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.truncate()

if __name__ == "__main__":
    update_json_file()
    print("\nOpération terminée")