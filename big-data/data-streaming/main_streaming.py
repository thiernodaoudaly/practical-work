from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import signal

# Création de la session Spark
spark = SparkSession.builder \
    .appName("TP_Data_Streaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Définition du schéma pour les données de capteurs
sensor_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("temperature", FloatType(), True)
])

streaming_dir = "./data" 
checkpoint_path = "./checkpoints"

if not os.path.exists(checkpoint_path):
    os.makedirs(checkpoint_path)

# --------------------------------------------------------------------------------
# Partie 1 : Mise en place du flux

# Ici, on lit le fichier CSV en mode streaming (même s'il existe déjà en entier) 
# et on affiche chaque événement dans la console au fur et à mesure.
# C'est la base du streaming: lire des données en continu.
# --------------------------------------------------------------------------------

def partie1_chargement_du_flux():
    print("Partie 1 : Mise en place du flux")
    
    # 1. Charger les données en mode streaming
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(sensor_schema) \
        .load(streaming_dir) 
    
    # 2. Afficher les événements à mesure qu'ils arrivent
    query = df_stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # On laisse le query s'exécuter pendant 10 secondes pour voir quelques événements
    try:
        query.awaitTermination(10000)  # 10 secondes
    finally:
        query.stop()
        print("Partie 1 terminée")

# --------------------------------------------------------------------------------
# Partie 2 : Traitement par fenêtre fixe (Tumbling Window)

# Cette partie regroupe les événements dans des fenêtres de 1 minute (sans chevauchement) 
# et calcule la température moyenne par capteur pour chaque fenêtre. Les fenêtres fixes sont
# comme des "tranches de temps" distinctes.

# --------------------------------------------------------------------------------

def partie2_fenetre_fixe():
    print("Partie 2 : Traitement par fenêtre fixe (Tumbling Window)")
    
    # Charger les données en streaming
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(sensor_schema) \
        .load(streaming_dir)  
    
    # 3. Regrouper les événements dans des fenêtres fixes de 1 minute
    # 4. Calculer pour chaque capteur la température moyenne par fenêtre
    windowed_avg = df_stream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("sensor_id")
        ) \
        .agg(avg("temperature").alias("avg_temperature"))
    
    # Formater la sortie pour qu'elle soit plus lisible
    query = windowed_avg \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            round(col("avg_temperature"), 2).alias("avg_temperature")
        ) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_path + "/partie2") \
        .start()
    
    try:
        query.awaitTermination(15000)  
    finally:
        query.stop()
        print("Partie 2 terminée")

# --------------------------------------------------------------------------------
# Partie 3 : Traitement par fenêtre glissante (Sliding Window)
# --------------------------------------------------------------------------------

def partie3_fenetre_glissante():
    print("Partie 3 : Traitement par fenêtre glissante (Sliding Window)")
    
    # Charger les données en streaming
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(sensor_schema) \
        .load(streaming_dir)  
    
    # 5. Créer des fenêtres de 2 minutes glissant toutes les 30 secondes
    # 6. Calculer la température moyenne par capteur
    sliding_avg = df_stream \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "2 minutes", "30 seconds"),
            col("sensor_id")
        ) \
        .agg(avg("temperature").alias("avg_temperature"))
    
    # On formate la sortie pour qu'elle soit plus lisible
    query = sliding_avg \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            round(col("avg_temperature"), 2).alias("avg_temperature")
        ) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_path + "/partie3") \
        .start()
    
    try:
        query.awaitTermination(20000) 
    finally:
        query.stop()
        print("Partie 3 terminée")

# --------------------------------------------------------------------------------
# Partie 4 : Requête continue
# --------------------------------------------------------------------------------

def partie4_requete_continue():
    print("Partie 4 : Requête continue")
    
    # Charger les données en streaming
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(sensor_schema) \
        .load(streaming_dir) 
    
    # 7. Écrire une requête continue affichant toutes les 30 secondes
    # - Nombre d'événements reçus
    # - Température maximale par capteur
    
    # Calculer le nombre d'événements reçus et la température maximale
    count_and_max = df_stream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "30 seconds", "30 seconds"),
            col("sensor_id")
        ) \
        .agg(
            count("*").alias("event_count"),
            max("temperature").alias("max_temperature")
        )
    
    # Formater la sortie
    query = count_and_max \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("event_count"),
            round(col("max_temperature"), 2).alias("max_temperature")
        ) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="30 seconds") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_path + "/partie4") \
        .start()
    
    try:
        query.awaitTermination(60000) 
    finally:
        query.stop()
        print("Partie 4 terminée")

# --------------------------------------------------------------------------------
# Partie 5 : Jointure de flux
# Cette partie avancée montre comment joindre deux flux de données (capteurs et statuts) 
# en se basant sur l'identifiant du capteur et une fenêtre temporelle (±30 secondes).
# --------------------------------------------------------------------------------

def partie5_jointure_flux():
    print("Partie 5 : Jointure de flux")
    
    # Pour cette partie, on crée un second répertoire pour le flux de statuts
    status_dir = "./status"
    if not os.path.exists(status_dir):
        os.makedirs(status_dir)
    
    # On doit aussi créer manuellement un fichier CSV de statuts dans ce répertoire
    # avec les colonnes: timestamp, sensor_id, status
    
    # On charge les données des capteurs en streaming
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(sensor_schema) \
        .load(streaming_dir)  
    
    # 8. Simuler un deuxième flux de statuts de capteurs (OK ou ALERTE)
    # Définition du schéma pour les données de statut
    status_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("sensor_id", IntegerType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Charger le flux de statuts
    status_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(status_schema) \
        .load(status_dir) 
    
    # 9. Pour simuler simplement, nous allons traiter séparément les flux
    # et montrer un exemple conceptuel de la jointure
    
    # Traitement des données de capteurs
    sensor_query = df_stream \
        .select(
            col("timestamp"),
            col("sensor_id"),
            col("temperature")
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_path + "/partie5_sensors") \
        .start()
    
    # Dans un cas réel, on utilisera une jointure comme:
    """
    joined_stream = df_stream
        .withWatermark("timestamp", "1 minute")
        .join(
            status_stream.withWatermark("timestamp", "1 minute"),
            expr('''
            df_stream.sensor_id = status_stream.sensor_id AND
            df_stream.timestamp >= status_stream.timestamp - INTERVAL 30 SECONDS AND
            df_stream.timestamp <= status_stream.timestamp + INTERVAL 30 SECONDS
            '''),
            "inner"
        )
    """
    
    try:
        # Exécuter pendant 20 secondes
        sensor_query.awaitTermination(20000) 
    finally:
        sensor_query.stop()
        print("Partie 5 terminée")

# --------------------------------------------------------------------------------
# Exécution du TP
# --------------------------------------------------------------------------------

if __name__ == "__main__":
    print("Exécution du TP sur le Data Streaming et les Requêtes continues")
    
    # partie1_chargement_du_flux()
    # partie2_fenetre_fixe()
    # partie3_fenetre_glissante()
    # partie4_requete_continue()
    partie5_jointure_flux()
    
    spark.stop()

def handle_ctrl_c(sig, frame):
    print('\nCtrl+C pressé. Arrêt du programme...')
    sys.exit(0)


signal.signal(signal.SIGINT, handle_ctrl_c)