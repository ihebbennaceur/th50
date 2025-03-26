import json

fn="fproducts_success_2025-01-29_18-58-32.json"

def remove_duplicate_urls_from_main(json_file):
    """
    Supprime les URLs de mainImages.RU si elles existent dans sizeTableUrl
    Affiche les URLs supprimées et les correspondances trouvées
    Modifie directement le fichier JSON original
    """
    with open(json_file, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        updated = 0
        total_duplicates = 0
        sample_removals = []  # Pour stocker quelques exemples de suppression
        
        print("\nAnalyse en cours...\n")
        
        for product in data:
            # Vérifier que les champs existent
            if ('mainImages' in product and 'RU' in product['mainImages'] and
                'sizeTableUrl' in product):
                
                main_urls = product['mainImages']['RU']
                size_table_urls = set(product['sizeTableUrl'])
                
                # URLs à supprimer (doublons)
                urls_to_remove = [url for url in main_urls if url in size_table_urls]
                
                if urls_to_remove:
                    # Garder quelques exemples pour affichage
                    if len(sample_removals) < 5:
                        sample_removals.append({
                            'productId': product.get('productId', 'N/A'),
                            'removed_urls': urls_to_remove
                        })
                    
                    total_duplicates += len(urls_to_remove)
                    
                    # Filtrer pour garder seulement les URLs uniques à mainImages
                    new_main_urls = [url for url in main_urls if url not in size_table_urls]
                    product['mainImages']['RU'] = new_main_urls
                    updated += 1
        
        # Affichage des résultats
        print(f"Produits analysés: {len(data)}")
        print(f"Produits modifiés: {updated}")
        print(f"URLs supprimées (total): {total_duplicates}\n")
        
        print("Exemples de modifications :")
        for example in sample_removals:
            print(f"\nProductID: {example['productId']}")
            for url in example['removed_urls']:
                print(f" - Supprimé: {url}")
        
        # Réécrire le fichier
        f.seek(0)
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.truncate()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python clean_main_images.py fichier.json")    
        sys.exit(1)
    
    input_file = sys.argv[1]
    print(f"\nNettoyage des doublons dans {input_file}...")
    remove_duplicate_urls_from_main(input_file)
    print("\nOpération terminée")