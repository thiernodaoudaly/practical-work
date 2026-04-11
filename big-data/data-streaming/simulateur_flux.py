import os
import time
from datetime import datetime, timedelta
import random
import csv

# Répertoire de destination
data_dir = "./data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Identifiants de capteurs simulés
sensor_ids = ["101", "102", "103"]

# Nombre de fichiers à générer
n_fichiers = 10

print("Simulation de flux en cours...")

for i in range(n_fichiers):
    now = datetime.utcnow()

    # Génère 3 lignes de données aléatoires
    lignes = []
    for sid in sensor_ids:
        temp = round(random.uniform(20.0, 30.0), 2)
        lignes.append([now.isoformat(), sid, temp])

    # Nom de fichier unique basé sur le temps
    fichier_path = os.path.join(data_dir, f"sensor_data_{i}_{int(time.time())}.csv")

    # Écriture du fichier CSV
    with open(fichier_path, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "sensor_id", "temperature"])  
        writer.writerows(lignes)

    print(f"✅ Fichier écrit : {fichier_path}")
    time.sleep(10)  # attendre 10 secondes
