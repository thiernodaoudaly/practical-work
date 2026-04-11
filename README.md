# TPs Académiques — EPT (DIC1 & DIC2)

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-1.x-013243?style=flat-square&logo=numpy&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.3.0-66CCFF?style=flat-square&logo=apachehadoop&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-Streaming-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-notebook-F37626?style=flat-square&logo=jupyter&logoColor=white)

Ce dépôt regroupe quelques travaux pratiques de Big Data et ML réalisés à l'**École Polytechnique de Thiès (EPT)** dans le cadre du cursus Génie Informatique et Télécommunications (GIT).

## Compétences acquises

### Machine Learning

| Compétence | TP |
|---|---|
| Génération et visualisation de données d'entraînement avec bruit gaussien | ML — TP1 |
| Construction de la matrice de design et représentation vectorielle | ML — TP1 |
| Implémentation from scratch de la régression linéaire | ML — TP1 |
| Résolution analytique par l'équation normale `θ = (XᵀX)⁻¹ Xᵀy` | ML — TP1 |
| Comparaison de modèles et sélection du meilleur paramètre θ | ML — TP1 |
| Vectorisation NumPy vs boucles Python — benchmark de temps de calcul | ML — TP1 |
| Implémentation de la fonction de coût MSE | ML — TP2 |
| Visualisation de la surface 3D et des contours de la fonction de coût | ML — TP2 |
| Standardisation / déstandardisation des données | ML — TP2 |
| Descente de gradient batch, SGD et mini-batch — implémentation et comparaison | ML — TP2 |
| Régression polynomiale | ML — TP2 |

### Big Data

| Compétence | TP |
|---|---|
| Traitement de flux en temps réel avec Spark Structured Streaming | BD — TP1 |
| Fenêtres temporelles fixes (Tumbling) et glissantes (Sliding) | BD — TP1 |
| Jointure de deux flux de données sur fenêtre temporelle | BD — TP1 |
| Simulation de flux IoT avec génération de fichiers CSV | BD — TP1 |
| Paradigme MapReduce — implémentation mapper/reducer en Python | BD — TP2 |
| Analyse de logs Apache sur cluster Hadoop 3.3.0 | BD — TP2 |

## Structure du dépôt

```
practical-work/
├── machine-learning/
│   ├── tp1.ipynb    # Régression linéaire + Vectorisation
│   └── tp2.ipynb    # Descente de gradient
│
└── big-data/
    ├── data-streaming/
    │   ├── main_streaming.py           # 5 exercices Spark Streaming
    │   ├── simulateur_flux.py          # Générateur de données IoT
    │   ├── data/
    │      ├── sensor_data.csv
    │      └── status_stream.csv
    └── map-reduce/
        └── tp_map_reduce.ipynb        # MapReduce sur logs Apache
```

## Machine Learning — DIC1 

### TP1 — Régression Linéaire & Vectorisation

**Exercice 1 : Régression linéaire, équation normale**

Construction d'un pipeline de régression linéaire from scratch sur données synthétiques :

- Génération d'un intervalle `[xMin, xMax]` aléatoire, vecteur d'entraînement `x` (20 points)
- Calcul de `F = 4 + 3x` puis ajout de bruit gaussien `Y = F + N(0, 3)`
- Construction de la matrice de design `X = [1 | x]`, comparaison de trois modèles θ₁, θ₂, θ₃
- Solution optimale par **équation normale** : `θ̂ = (XᵀX)⁻¹ Xᵀy`

**Exercice 2 : Importance de la vectorisation**

- Produit matriciel par boucle Python vs `np.dot`
- Benchmark sur matrices carrées de taille 10 à 10⁴ — confirmation de la complexité O(n²)
- La vectorisation NumPy est plusieurs ordres de grandeur plus rapide

### TP2 — Descente de Gradient

**Fonction de coût et visualisation :**

- MSE : `J(θ) = (1/2m) Σ(θᵀxᵢ - yᵢ)²`
- Surface 3D et courbes de niveau de J en fonction de θ₀ et θ₁
- Standardisation `Xn = (X - μ) / σ` pour conditionner la surface de coût

**3 variantes de gradient descent :**

| Variante | Mise à jour | Avantage | Inconvénient |
|---|---|---|---|
| Batch GD | Tout le dataset | Convergence stable | Lent sur grands datasets |
| SGD | 1 exemple à la fois | Rapide, adaptatif | Trajectoire bruitée |
| Mini-batch GD | k exemples | Compromis vitesse/stabilité | Hyperparamètre k à régler |

**Régression polynomiale** : ajout de features `x², x³, ...` pour modéliser des relations non linéaires dans un cadre linéaire en θ.

## Big Data — DIC2

### TP1 — Data Streaming avec Spark Structured Streaming

Traitement de flux en temps réel sur données de capteurs IoT (température).

| Partie | Concept | Description |
|---|---|---|
| 1 | Mise en place du flux | Lecture CSV streaming, affichage console en append |
| 2 | Tumbling Window | Fenêtre fixe 1 min — température moyenne par capteur |
| 3 | Sliding Window | Fenêtre glissante avec chevauchement |
| 4 | Requête continue | Traitement événement par événement |
| 5 | Jointure de flux | Join capteurs + statuts sur sensor_id ± 30s |

```bash
python simulateur_flux.py   # génère les données
python main_streaming.py    # lance le traitement
```

**Prérequis :** `pip install pyspark`

### TP2 — MapReduce avec Hadoop

Implémentation du paradigme MapReduce en Python sur Hadoop 3.3.0 (Google Colab), dataset : logs Apache `access_log`.

| # | Question | Approche |
|---|---|---|
| 1 | Visites par fichier du site | `mapper1.py` (regex GET/POST) + `reducer1.py` |
| 2 | Visites par adresse IP | `mapper2.py` (extraction IP) + `reducer2.py` |
| 3 | Fichier le plus populaire | `mapper3.py` + `reducer3.py` (max global) |

**Environnement :** Google Colab — Hadoop installé directement en cellule, dataset depuis Google Drive.
