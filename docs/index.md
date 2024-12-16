
# Project structure

```bash
ViperGraph/
│
├── src/
│   ├── vipergraph/
│   │   ├── __init__.py                   # Point d'entrée du package
│   │   ├── core/                         # Composants essentiels
│   │   │   ├── graph.py                  # Gestion principale du graphe
│   │   │   ├── node.py                   # Classe Node
│   │   │   ├── edge.py                   # Classe Edge
│   │   │   ├── transaction.py            # Gestion des transactions
│   │   │   └── schema.py                 # Gestion du schéma
│   │   ├── storage/                      # Gestion du stockage
│   │   │   ├── binary_storage.py         # Sauvegarde/chargement binaire
│   │   │   ├── backup_manager.py         # Gestion des sauvegardes
│   │   │   └── persistence.py            # Interface pour adapter différents types de stockage
│   │   ├── temporal/                     # Gestion temporelle
│   │   │   ├── temporal_manager.py       # Gestion des snapshots
│   │   │   ├── prediction.py             # Prédictions basées sur l'évolution
│   │   │   └── trends.py                 # Analyse des tendances
│   │   ├── analytics/                    # Analyses avancées
│   │   │   ├── graph_analytics.py        # Calculs globaux (communautés, centralité)
│   │   │   ├── metrics.py                # Calcul des métriques de performances
│   │   │   ├── anomaly_detection.py      # Détection d'anomalies dans le graphe
│   │   │   └── recommendations.py        # Recommandations et modèles connexes
│   │   ├── gnn/                          # GNN pour tâches spécifiques
│   │   │   ├── gnn_manager.py            # Gestion des modèles GNN
│   │   │   ├── models.py                 # Définition des modèles (GCN, GAT)
│   │   │   └── datasets.py               # Préparation des données pour GNN
│   │   ├── utils/                        # Outils généraux
│   │   │   ├── logging.py                # Configuration et gestion des logs
│   │   │   ├── validation.py             # Vérifications et validations des entrées
│   │   │   ├── serialization.py          # Sérialisation/désérialisation JSON, binaire
│   │   │   └── helpers.py                # Fonctions utilitaires
│   │   ├── api/                          # API pour intégration externe
│   │   │   ├── cli.py                    # Interface CLI
│   │   │   ├── rest_api.py               # API REST pour manipuler le graphe
│   │   │   └── grpc_service.py           # Service gRPC pour performances
│   │   └── tests/                        # Tests intégrés au package
│       │   ├── test_graph.py             # Tests pour graph.py
│       │   ├── test_transaction.py       # Tests pour transaction.py
│       │   ├── test_gnn.py               # Tests pour les modèles GNN
│       │   └── ...
│
├── examples/                             # Exemples d’utilisation
│   ├── create_graph_demo.py              # Exemple de création de graphe
│   ├── temporal_analysis_demo.py         # Démo sur les snapshots
│   ├── gnn_anomaly_demo.py               # Exemple d'utilisation GNN pour anomalies
│   └── advanced_patterns_demo.py         # Exemple d'analyse avancée des motifs
│
├── docs/                                 # Documentation Sphinx
│   ├── conf.py                           # Configuration de Sphinx
│   ├── index.rst                         # Sommaire principal
│   ├── tutorials/                        # Tutoriels
│   │   ├── getting_started.rst           # Démarrage rapide
│   │   ├── examples.rst                  # Guide d’exemples
│   │   └── advanced_features.rst         # Fonctionnalités avancées
│   └── api/                              # Documentation API auto-générée
│
├── benchmarks/                           # Benchmarks de performances
│   ├── graph_performance.py              # Benchmarks sur les graphes
│   ├── storage_comparison.py             # Comparaison des formats de stockage
│   └── gnn_training.py                   # Benchmarks sur l'entraînement GNN
│
├── scripts/                              # Scripts utilitaires
│   ├── data_loader.py                    # Chargement de données externes
│   ├── migrate_storage.py                # Migration entre différents stockages
│   └── manage_backups.py                 # Gestion des sauvegardes automatisée
│
├── tests/                                # Tests unitaires et intégration
│   ├── unit/                             # Tests unitaires
│   │   ├── test_graph.py
│   │   ├── test_storage.py
│   │   └── test_gnn.py
│   ├── integration/                      # Tests d'intégration
│   │   ├── test_api.py
│   │   └── test_temporal.py
│   └── performance/                      # Tests de performance
│       ├── test_large_graphs.py
│       └── test_gnn_scalability.py
│
├── pyproject.toml                        # Configurations Poetry
├── requirements.txt                      # Dépendances (pour non-Poetry users)
├── README.md                             # Présentation du projet
├── LICENSE                               # Licence
├── .gitignore                            # Fichiers ignorés par git
├── setup.py                              # Setup optionnel pour compatibilité setuptools
└── Makefile                              # Commandes de gestion simplifiée
```