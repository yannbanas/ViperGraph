import datetime
import time
from rich.console import Console
from rich.table import Table
from typing import Any, Dict, List, Set, Tuple
from collections import defaultdict
from node import Node
from edge import Edge
from vipergraph.core.isolation_level import IsolationLevel
from vipergraph.core.transaction import Transaction

console = Console()

class GraphDB:
    """Base de données graphe avec fonctionnalités avancées"""
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.edge_types: Set[str] = set()
        self.schema = Schema()
        self.lock = threading.RLock()
        self.index_manager = IndexManager(self)
        self.stored_procedures: Dict[str, StoredProcedure] = {}
        self.transaction_manager = TransactionManager(self)
        self.backup_manager = BackupManager(self)
        self.performance_monitor = PerformanceMonitor()
        self.schema_version_manager = SchemaVersionManager()
        self.schema_rollback_manager = SchemaRollbackManager(self)
        self.analytics = GraphAnalytics(self)
        self.pattern_matcher = PatternMatcher(self)
        self.temporal_manager = TemporalManager(self)
        self.hypergraph_manager = HypergraphManager(self)
        self.cypher_converter = CypherConverter(self)
        self.gnn_manager = GNNManager(self)
        # Cache multi-niveaux
        self.cache = SmartCache()
        self.binary_storage = BinaryStorageManager("data/binary_storage")
        
        # Statistiques et monitoring
        self.statistics = {
            "nodes_count": 0,
            "edges_count": 0,
            "labels_count": defaultdict(int),
            "edge_types_count": defaultdict(int),
            "last_updated": datetime.now().isoformat(),
            "query_stats": {
                "total_queries": 0,
                "cached_hits": 0,
                "avg_execution_time": 0
            },
            "cache_stats": {
                "hits": 0,
                "misses": 0,
                "memory_usage": 0
            }
        }
        
        # Initialisation
        self.setup_initial_indexes()
        print("GraphDB initialized with advanced features")

    def export_to_cypher(self, file_path: str = None) -> str:
        """
        Exporte le graphe en requêtes Cypher.
        
        Args:
            file_path: Chemin du fichier où sauvegarder les requêtes (optionnel)
            
        Returns:
            str: Les requêtes Cypher générées
        """
        cypher_queries = self.cypher_converter.export_to_cypher()
        
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(cypher_queries)
                
        return cypher_queries
    
    def import_from_cypher(self, source: Union[str, Path]) -> Dict[str, Any]:
        """
        Importe des données depuis des requêtes Cypher.
        
        Args:
            source: Chemin du fichier Cypher ou string contenant les requêtes
            
        Returns:
            Dict contenant les statistiques d'import
        """
        if isinstance(source, (str, Path)) and os.path.exists(str(source)):
            with open(str(source), 'r', encoding='utf-8') as f:
                cypher_queries = f.read()
        else:
            cypher_queries = source
            
        return self.cypher_converter.import_from_cypher(cypher_queries)
    
    def import_from_memgraph(self, host: str = 'localhost', port: int = 7687,
                           username: str = None, password: str = None) -> Dict[str, Any]:
        """
        Importe des données depuis une instance Memgraph.
        
        Args:
            host: Hôte Memgraph
            port: Port Memgraph
            username: Nom d'utilisateur (optionnel)
            password: Mot de passe (optionnel)
            
        Returns:
            Dict contenant les statistiques d'import
        """
        return self.cypher_converter.import_from_memgraph(host, port, username, password)
    
    # Méthodes pour Pattern Matcher
    def learn_graph_patterns(self) -> Dict[str, Any]:
        """Apprend et retourne les motifs récurrents dans le graphe"""
        return self.pattern_matcher.learn_patterns()

    def find_similar_patterns(self, seed_pattern: Dict[str, Any], 
                            similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Trouve des motifs similaires à un motif donné"""
        return self.pattern_matcher.find_similar_patterns(seed_pattern, similarity_threshold)

    # Méthodes pour Gestion Temporelle
    def create_temporal_snapshot(self, timestamp: datetime = None) -> str:
        """Crée un snapshot temporel du graphe"""
        return self.temporal_manager.create_snapshot(timestamp)

    def predict_graph_evolution(self, window: timedelta = None) -> Dict[str, Any]:
        """Prédit l'évolution future du graphe"""
        return self.temporal_manager.predict_evolution(window)

    def reconstruct_historical_state(self, timestamp: datetime) -> Dict[str, Any]:
        """Reconstruit l'état historique du graphe"""
        return self.temporal_manager.reconstruct_state(timestamp)

    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyse les motifs temporels dans l'évolution du graphe"""
        return self.temporal_manager.analyze_temporal_patterns()

    # Méthodes pour Hypergraphes
    def create_hyperedge(self, nodes: List[str], edge_type: str, 
                        properties: Dict[str, Any] = None) -> str:
        """Crée une hyperarête connectant plusieurs nœuds"""
        return self.hypergraph_manager.create_hyperedge(nodes, edge_type, properties)

    def get_hyperedge(self, hyperedge_id: str) -> Dict[str, Any]:
        """Récupère une hyperarête par son ID"""
        return self.hypergraph_manager.get_hyperedge(hyperedge_id)

    def get_node_hyperedges(self, node_id: str) -> List[Dict[str, Any]]:
        """Récupère toutes les hyperarêtes connectées à un nœud"""
        return self.hypergraph_manager.get_node_hyperedges(node_id)

    def analyze_hypergraph(self) -> Dict[str, Any]:
        """Analyse la structure du hypergraphe"""
        return self.hypergraph_manager.analyze_hypergraph()

    def add_to_hyperedge(self, hyperedge_id: str, node_id: str):
        """Ajoute un nœud à une hyperarête existante"""
        self.hypergraph_manager.add_node_to_hyperedge(hyperedge_id, node_id)

    def remove_from_hyperedge(self, hyperedge_id: str, node_id: str):
        """Retire un nœud d'une hyperarête"""
        self.hypergraph_manager.remove_node_from_hyperedge(hyperedge_id, node_id)

    # Méthode utilitaire pour l'analyse globale
    def analyze_graph_structure(self) -> Dict[str, Any]:
        """Analyse complète de la structure du graphe incluant les nouvelles fonctionnalités"""
        analysis = {
            'basic_metrics': {
                'nodes': len(self.nodes),
                'edges': self.statistics['edges_count'],
                'hyperedges': len(self.hypergraph_manager.hyperedges)
            },
            'patterns': self.pattern_matcher.learn_patterns(),
            'temporal': self.temporal_manager.analyze_temporal_patterns(),
            'hypergraph': self.hypergraph_manager.analyze_hypergraph(),
            'timestamp': datetime.now().isoformat()
        }

        # Ajoute des métriques avancées
        analysis.update({
            'advanced_metrics': {
                'pattern_complexity': self._calculate_pattern_complexity(),
                'temporal_stability': self._calculate_temporal_stability(),
                'structural_cohesion': self._calculate_structural_cohesion()
            }
        })

        return analysis

    def _calculate_pattern_complexity(self) -> float:
        """Calcule la complexité des motifs dans le graphe"""
        patterns = self.pattern_matcher.patterns
        if not patterns:
            return 0.0

        complexity_scores = []
        for pattern_info in patterns.values():
            size_score = pattern_info['size'] / 10  # Normalise la taille
            frequency_score = pattern_info['frequency'] / max(1, len(self.nodes))
            complexity_scores.append(size_score * frequency_score)

        return sum(complexity_scores) / len(complexity_scores)

    def _calculate_temporal_stability(self) -> float:
        """Calcule la stabilité temporelle du graphe"""
        stability_metrics = self.temporal_manager._calculate_stability_metrics()
        return stability_metrics['overall_stability']

    def _calculate_structural_cohesion(self) -> float:
        """Calcule la cohésion structurelle du graphe"""
        # Combine les métriques de cohésion du graphe standard et du hypergraphe
        hypergraph_analysis = self.hypergraph_manager.analyze_hypergraph()
        
        # Calcule la densité du graphe standard
        total_possible_edges = len(self.nodes) * (len(self.nodes) - 1) / 2
        graph_density = self.statistics['edges_count'] / max(1, total_possible_edges)
        
        # Calcule la densité du hypergraphe
        hypergraph_density = len(self.hypergraph_manager.hyperedges) / max(1, len(self.nodes))
        
        # Combine les métriques (moyenne pondérée)
        return (0.7 * graph_density + 0.3 * hypergraph_density)

    def export_graph_analysis(self, file_path: str):
        """Exporte une analyse complète du graphe vers un fichier JSON"""
        analysis = self.analyze_graph_structure()
        
        # Convertit l'analyse en format JSON-compatible
        json_analysis = self._convert_to_json_serializable(analysis)
        
        # Ajoute des métadonnées
        json_analysis['metadata'] = {
            'export_timestamp': datetime.now().isoformat(),
            'graph_version': self.schema.version,
            'analysis_version': '1.0'
        }
        
        # Sauvegarde l'analyse
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_analysis, f, indent=2, ensure_ascii=False)

    def _convert_to_json_serializable(self, obj):
        """Convertit récursivement un objet en format JSON-compatible"""
        if isinstance(obj, dict):
            return {key: self._convert_to_json_serializable(value) for key, value in obj.items()}
            
        elif isinstance(obj, (list, tuple)):
            return [self._convert_to_json_serializable(item) for item in list(obj)]
            
        elif isinstance(obj, set):
            return [self._convert_to_json_serializable(item) for item in list(obj)]
            
        elif isinstance(obj, defaultdict):
            return {key: self._convert_to_json_serializable(value) for key, value in dict(obj).items()}
            
        elif isinstance(obj, datetime):
            return obj.isoformat()
            
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
            
        elif hasattr(obj, '__dict__'):
            return self._convert_to_json_serializable(obj.__dict__)
            
        # Pour les types qui ne peuvent pas être convertis directement en JSON
        return str(obj)

    def import_temporal_data(self, data: Dict[str, Any]) -> bool:
        """Importe des données temporelles dans le graphe"""
        try:
            if 'snapshots' in data:
                for snapshot in data['snapshots']:
                    self.temporal_manager.temporal_snapshots[snapshot['id']] = snapshot
                    
            if 'evolution_models' in data:
                self.temporal_manager.evolution_models.update(data['evolution_models'])
                
            return True
        except Exception as e:
            print(f"Error importing temporal data: {e}")
            return False
        
    def detect_communities(self, algorithm: str = "louvain") -> Dict[str, Set[str]]:
        return self.analytics.detect_communities(algorithm)

    def calculate_centrality(self, metric: str = "betweenness") -> Dict[str, float]:
        return self.analytics.calculate_centrality(metric)

    def identify_influencers(self, method: str = "combined", threshold: float = 0.8) -> List[str]:
        return self.analytics.identify_influencers(method, threshold)
    
    def setup_initial_indexes(self):
        """Configure les index initiaux"""
        self.index_manager.create_index("created_at")
        self.index_manager.create_index("updated_at")
        print("Initial indexes created")

    def create_node(self, labels: Set[str], properties: Dict[str, Any] = None) -> Node:
        """Crée un nouveau nœud avec ID automatique"""
        with self.lock:
            node_id = str(uuid4())
            return self._create_node_internal(node_id, labels, properties or {})

    def _create_node_internal(self, node_id: str, labels: Set[str], properties: Dict[str, Any]) -> Node:
        """Création interne d'un nœud avec validation"""
        with self.lock:
            # Validation du schéma
            for label in labels:
                if not self.schema.validate_node(label, properties, self):
                    raise ValueError(f"Node validation failed for label {label}")

            # Vérifie si le nœud existe
            if node_id in self.nodes:
                raise ValueError(f"Node {node_id} already exists")

            # Crée le nœud
            start_time = time.time()
            node = Node(node_id, labels, properties)
            self.nodes[node_id] = node

            # Met à jour les statistiques
            self.statistics["nodes_count"] += 1
            for label in labels:
                self.statistics["labels_count"][label] += 1

            # Met à jour les index
            self.index_manager.update_indexes(node, {}, properties)

            # Mesure les performances
            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_creation", duration)

            # Met en cache
            self.cache.put(f"node:{node_id}", node)

            print(f"Created node: {node}")
            return node

    def create_edge(self, source_id: str, target_id: str, 
                   edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Crée une nouvelle relation"""
        return self._create_edge_internal(source_id, target_id, edge_type, properties)

    def _create_edge_internal(self, source_id: str, target_id: str, 
                            edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Création interne d'une relation avec validation"""
        with self.lock:
            source = self.nodes.get(source_id)
            target = self.nodes.get(target_id)
            
            if not (source and target):
                raise ValueError("Source or target node not found")

            start_time = time.time()
            
            # Crée la relation
            edge = Edge(source, target, edge_type, properties or {})
            
            # Met à jour les connexions
            source.outgoing_edges[edge_type].add(edge)
            target.incoming_edges[edge_type].add(edge)
            self.edge_types.add(edge_type)

            # Met à jour les statistiques
            self.statistics["edges_count"] += 1
            self.statistics["edge_types_count"][edge_type] += 1

            # Mesure les performances
            duration = time.time() - start_time
            self.performance_monitor.record_metric("edge_creation", duration)

            print(f"Created edge: {edge}")
            return edge

    def create_relationship(self, source_id: str, target_id: str, 
                       edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Alias pour create_edge pour une meilleure lisibilité"""
        return self.create_edge(source_id, target_id, edge_type, properties)
    
    def _update_node_internal(self, node_id: str, properties: Dict[str, Any]) -> Node:
        """Mise à jour interne d'un nœud"""
        with self.lock:
            node = self.nodes.get(node_id)
            if not node:
                raise ValueError(f"Node {node_id} not found")

            start_time = time.time()
            
            # Sauvegarde les anciennes propriétés
            old_properties = node._properties.copy()
            
            # Met à jour les propriétés
            for key, value in properties.items():
                node.set_property(key, value)
            
            # Met à jour les index
            self.index_manager.update_indexes(node, old_properties, node._properties)
            
            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_update", duration)
            
            return node

    def _delete_node_internal(self, node_id: str, force: bool = False):
        """Suppression interne d'un nœud"""
        with self.lock:
            node = self.nodes.get(node_id)
            if not node:
                raise ValueError(f"Node {node_id} not found")

            # Vérifie les relations
            has_edges = bool(node.incoming_edges or node.outgoing_edges)
            if has_edges and not force:
                raise ValueError("Node has relationships. Use force=True to delete anyway")

            start_time = time.time()

            # Supprime les relations si force=True
            if force:
                for edges in list(node.incoming_edges.values()):
                    for edge in list(edges):
                        self._delete_edge(edge)
                for edges in list(node.outgoing_edges.values()):
                    for edge in list(edges):
                        self._delete_edge(edge)

            # Met à jour les statistiques
            self.statistics["nodes_count"] -= 1
            for label in node.labels:
                self.statistics["labels_count"][label] -= 1

            # Met à jour les index
            self.index_manager.update_indexes(node, node._properties, {})

            # Supprime le nœud
            del self.nodes[node_id]

            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_deletion", duration)

            # Invalide le cache
            self.cache.put(f"node:{node_id}", None)
            
            print(f"Deleted node: {node_id}")

    def _merge_node_internal(self, labels: Set[str], match_properties: Dict[str, Any],
                           create_properties: Dict[str, Any] = None) -> Tuple[Node, bool]:
        """Fusion interne d'un nœud (création si n'existe pas)"""
        with self.lock:
            # Recherche les nœuds existants
            existing_nodes = self.find_nodes(labels, match_properties)
            
            if existing_nodes:
                # Met à jour le nœud existant si nécessaire
                node = existing_nodes[0]
                if create_properties:
                    self._update_node_internal(node.node_id, create_properties)
                return node, False
            else:
                # Crée un nouveau nœud
                properties = {**match_properties, **(create_properties or {})}
                node = self.create_node(labels, properties)
                return node, True

    def execute_stored_procedure(self, name: str, **kwargs) -> Any:
        """
        Exécute une procédure stockée avec les paramètres donnés.
        
        Args:
            name: Nom de la procédure stockée à exécuter
            **kwargs: Arguments à passer à la procédure
        
        Returns:
            Le résultat de l'exécution de la procédure
        
        Raises:
            ValueError: Si la procédure n'existe pas
        """
        if name not in self.stored_procedures:
            raise ValueError(f"Stored procedure '{name}' not found")
            
        procedure = self.stored_procedures[name]
        
        start_time = time.time()
        try:
            result = procedure.execute(self, **kwargs)
            duration = time.time() - start_time
            
            # Enregistrement des métriques de performance
            self.performance_monitor.record_metric(
                f"stored_procedure_{name}_execution_time", 
                duration
            )
            
            print(f"Executed stored procedure {name} in {duration:.3f} seconds")
            return result
            
        except Exception as e:
            print(f"Error executing stored procedure {name}: {e}")
            raise

    def save_to_binary(self):
        """Sauvegarde le graphe au format binaire"""
        self.binary_storage.save_graph(self)

    def load_from_binary(self):
        """Charge le graphe depuis le format binaire"""
        self.binary_storage.load_graph(self)
        
    def backup(self) -> str:
        """
        Crée une sauvegarde complète de la base de données.
        
        Returns:
            str: Chemin du fichier de sauvegarde créé
        """
        try:
            start_time = time.time()
            
            # Obtient un verrou global pour assurer la cohérence
            with self.lock:
                backup_file = self.backup_manager.create_backup()
                
            duration = time.time() - start_time
            self.performance_monitor.record_metric("backup_duration", duration)
            
            print(f"Created backup: {backup_file} in {duration:.2f} seconds")
            return backup_file
            
        except Exception as e:
            print(f"Error during backup: {e}")
            raise

    def restore(self, backup_file: str):
        """
        Restaure la base de données depuis une sauvegarde.
        
        Args:
            backup_file: Chemin vers le fichier de sauvegarde
        """
        try:
            start_time = time.time()
            
            # Vérifie que le fichier existe
            if not os.path.exists(backup_file):
                raise ValueError(f"Backup file not found: {backup_file}")
                
            # Restaure depuis la sauvegarde
            self.backup_manager.restore_from_backup(backup_file)
            
            duration = time.time() - start_time
            self.performance_monitor.record_metric("restore_duration", duration)
            
            print(f"Restored from backup: {backup_file} in {duration:.2f} seconds")
            
        except Exception as e:
            print(f"Error during restore: {e}")
            raise

    def list_backups(self) -> List[str]:
        """
        Liste tous les fichiers de backup disponibles.
        
        Returns:
            List[str]: Liste des chemins des fichiers de backup
        """
        return self.backup_manager.list_backups()

    def get_backup_info(self, backup_file: str) -> Dict[str, Any]:
        """
        Obtient les informations sur une sauvegarde.
        
        Args:
            backup_file: Nom du fichier de sauvegarde
        
        Returns:
            Dict contenant les métadonnées de la sauvegarde
        """
        return self.backup_manager.get_backup_info(backup_file)

    def cleanup_old_backups(self, max_age_days: int = 30):
        """
        Nettoie les anciennes sauvegardes.
        
        Args:
            max_age_days: Âge maximum en jours des sauvegardes à conserver
        """
        self.backup_manager.cleanup_old_backups(max_age_days)
        
    def get_version(self, key: str) -> int:
        """Obtient la version d'une ressource"""
        if key.startswith("node:"):
            node_id = key.split(":", 1)[1]
            node = self.nodes.get(node_id)
            return node._version if node else -1
        return 0

    def optimize(self) -> Dict[str, Any]:
        """
        Optimise la base de données en effectuant diverses opérations de maintenance.
        
        Returns:
            Dict contenant les résultats des optimisations effectuées
        """
        start_time = time.time()
        results = {
            "optimizations": [],
            "improvements": {},
            "errors": []
        }

        try:
            with self.lock:
                # 1. Nettoyage des nœuds et relations marqués comme supprimés
                deleted_nodes = self._cleanup_deleted_nodes()
                results["optimizations"].append("deleted_nodes_cleanup")
                results["improvements"]["deleted_nodes_removed"] = deleted_nodes

                # 2. Optimisation des index
                index_stats = self._optimize_indexes()
                results["optimizations"].append("index_optimization")
                results["improvements"]["index_stats"] = index_stats

                # 3. Nettoyage et optimisation du cache
                cache_stats = self._optimize_cache()
                results["optimizations"].append("cache_optimization")
                results["improvements"]["cache_stats"] = cache_stats

                # 4. Compactage des structures de données
                compact_stats = self._compact_data_structures()
                results["optimizations"].append("data_compaction")
                results["improvements"]["compaction_stats"] = compact_stats

                # 5. Réorganisation des relations pour une meilleure performance
                edge_stats = self._optimize_edge_storage()
                results["optimizations"].append("edge_optimization")
                results["improvements"]["edge_stats"] = edge_stats

            duration = time.time() - start_time
            results["duration"] = duration
            results["timestamp"] = datetime.now().isoformat()

            # Enregistre les métriques de performance
            self.performance_monitor.record_metric("optimization_duration", duration)
            print(f"Database optimization completed in {duration:.2f} seconds")

            return results

        except Exception as e:
            print(f"Error during optimization: {e}")
            results["errors"].append(str(e))
            return results

    def _cleanup_deleted_nodes(self) -> int:
        """Nettoie les nœuds et relations marqués comme supprimés"""
        deleted_count = 0
        nodes_to_remove = []

        # Identifie les nœuds à supprimer
        for node_id, node in self.nodes.items():
            if node._deleted:
                nodes_to_remove.append(node_id)
                deleted_count += 1

        # Supprime les nœuds
        for node_id in nodes_to_remove:
            del self.nodes[node_id]

        return deleted_count

    def _optimize_indexes(self) -> Dict[str, Any]:
        """Optimise les index de la base de données"""
        stats = {
            "rebuilt_indexes": 0,
            "cleaned_entries": 0,
            "index_sizes": {}
        }

        # Reconstruit les index
        index_stats = self.index_manager.get_index_stats()
        for prop_name, index_info in index_stats.items():
            old_size = index_info.get("entries", 0)
            self.index_manager.create_index(prop_name)  # Recrée l'index
            new_stats = self.index_manager.get_index_stats()
            new_size = new_stats[prop_name]["entries"] if prop_name in new_stats else 0
            
            stats["rebuilt_indexes"] += 1
            stats["cleaned_entries"] += max(0, old_size - new_size)
            stats["index_sizes"][prop_name] = new_size

        return stats

    def _optimize_cache(self) -> Dict[str, Any]:
        """Optimise le cache"""
        stats = {
            "before": self.cache.get_stats(),
        }
        
        # Force un nettoyage complet du cache
        self.cache.clear()
        
        stats["after"] = self.cache.get_stats()
        stats["cleared_entries"] = stats["before"].get("entries", 0)

        return stats

    def _compact_data_structures(self) -> Dict[str, Any]:
        """Compacte les structures de données internes"""
        stats = {
            "nodes_before": len(self.nodes),
            "edge_types_before": len(self.edge_types)
        }

        # Compacte les structures
        self.nodes = {k: v for k, v in self.nodes.items() if not v._deleted}
        self.edge_types = {et for et in self.edge_types}

        stats["nodes_after"] = len(self.nodes)
        stats["edge_types_after"] = len(self.edge_types)
        stats["space_saved"] = stats["nodes_before"] - stats["nodes_after"]

        return stats

    def _optimize_edge_storage(self) -> Dict[str, Any]:
        """Optimise le stockage des relations"""
        stats = {
            "reorganized_edges": 0,
            "cleaned_references": 0
        }

        # Nettoie les références invalides dans les relations
        for node in self.nodes.values():
            # Optimise les relations sortantes
            for edge_type in list(node.outgoing_edges.keys()):
                valid_edges = {edge for edge in node.outgoing_edges[edge_type] if edge.is_valid()}
                stats["cleaned_references"] += len(node.outgoing_edges[edge_type]) - len(valid_edges)
                node.outgoing_edges[edge_type] = valid_edges
                stats["reorganized_edges"] += len(valid_edges)

            # Optimise les relations entrantes
            for edge_type in list(node.incoming_edges.keys()):
                valid_edges = {edge for edge in node.incoming_edges[edge_type] if edge.is_valid()}
                stats["cleaned_references"] += len(node.incoming_edges[edge_type]) - len(valid_edges)
                node.incoming_edges[edge_type] = valid_edges
                stats["reorganized_edges"] += len(valid_edges)

        return stats

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Génère un rapport détaillé des performances de la base de données.

        Returns:
            Dict contenant les métriques de performance diverses
        """
        report = {
            "performance_metrics": {
                "general_stats": self.get_statistics(),
                "cache": {
                    "stats": self.cache.get_stats(),
                    "configuration": {
                        "memory_cache_size": self.cache.memory_cache.capacity,
                        "off_heap_cache_size": self.cache.off_heap_cache.capacity,
                    }
                },
                "transactions": {
                    "stats": self.transaction_manager.transaction_log.get_transaction_history(),
                    "active_count": len(self.transaction_manager.active_transactions)
                },
                "operations": {
                    "node_operations": self.performance_monitor.detailed_metrics["node_operations"],
                    "edge_operations": self.performance_monitor.detailed_metrics["edge_operations"]
                },
                "query_performance": {
                    "execution_times": self.performance_monitor.detailed_metrics["query_execution"],
                    "cached_query_ratio": self.statistics["query_stats"]["cached_hits"] / 
                                        max(1, self.statistics["query_stats"]["total_queries"])
                },
                "memory_usage": {
                    "nodes_count": len(self.nodes),
                    "edge_types_count": len(self.edge_types),
                    "cache_memory_usage": self.statistics["cache_stats"]["memory_usage"]
                },
                "index_performance": self.index_manager.get_index_stats()
            },
            "timestamp": datetime.now().isoformat(),
            "uptime": str(datetime.now() - self.performance_monitor.start_time),
            "version": "1.0"
        }

        # Ajout des métriques détaillées du moniteur de performance
        report["performance_metrics"].update(
            self.performance_monitor.get_detailed_statistics()
        )

        print("Generated performance report")
        return report

    def health_check(self) -> Dict[str, Any]:
        """
        Effectue une vérification complète de l'état de santé de la base de données.
        
        Returns:
            Dict contenant les indicateurs de santé et les alertes éventuelles
        """
        start_time = time.time()
        health_status = {
            "status": "healthy",  # Par défaut, sera mis à jour si des problèmes sont trouvés
            "timestamp": datetime.now().isoformat(),
            "checks": {},
            "alerts": [],
            "metrics": {}
        }

        try:
            # Vérification de la cohérence des nœuds et relations
            node_check = self._check_nodes_consistency()
            health_status["checks"]["nodes"] = node_check

            # Vérification des index
            index_check = self._check_indexes()
            health_status["checks"]["indexes"] = index_check

            # Vérification du cache
            cache_check = self._check_cache_health()
            health_status["checks"]["cache"] = cache_check

            # Vérification des transactions
            tx_check = self._check_transactions()
            health_status["checks"]["transactions"] = tx_check

            # Métriques de performance
            health_status["metrics"] = {
                "response_times": {
                    "avg": self.performance_monitor.get_statistics("response_time").get("avg", 0),
                    "max": self.performance_monitor.get_statistics("response_time").get("max", 0)
                },
                "memory_usage": {
                    "nodes": len(self.nodes),
                    "cache": self.cache.get_stats()
                },
                "operations": {
                    "success_rate": self._calculate_success_rate()
                }
            }

            # Détermination du statut global
            if any(check.get("status") == "critical" for check in health_status["checks"].values()):
                health_status["status"] = "critical"
            elif any(check.get("status") == "warning" for check in health_status["checks"].values()):
                health_status["status"] = "warning"

            # Durée de la vérification
            health_status["duration"] = time.time() - start_time

        except Exception as e:
            health_status["status"] = "error"
            health_status["error"] = str(e)
            print(f"Error during health check: {e}")

        return health_status

    def _check_nodes_consistency(self) -> Dict[str, Any]:
        """Vérifie la cohérence des nœuds et des relations"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }
        
        try:
            # Vérifie les références circulaires
            circular_refs = []
            visited = set()
            
            for node in self.nodes.values():
                if not node._deleted and node.node_id not in visited:
                    path = []
                    if self._check_circular_references(node, visited, path):
                        circular_refs.append(path)

            # Vérifie les relations orphelines
            orphaned_edges = []
            for node in self.nodes.values():
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        if not edge.is_valid():
                            orphaned_edges.append(edge.id)

            result["details"] = {
                "total_nodes": len(self.nodes),
                "total_edges": sum(len(edges) for node in self.nodes.values() 
                                for edges in node.outgoing_edges.values()),
                "circular_references": len(circular_refs),
                "orphaned_edges": len(orphaned_edges)
            }

            if circular_refs:
                result["status"] = "warning"
                result["issues"].append({
                    "type": "circular_reference",
                    "count": len(circular_refs),
                    "details": circular_refs[:5]  # Limite aux 5 premiers pour éviter un rapport trop long
                })

            if orphaned_edges:
                result["status"] = "warning"
                result["issues"].append({
                    "type": "orphaned_edges",
                    "count": len(orphaned_edges),
                    "edge_ids": orphaned_edges[:5]
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_indexes(self) -> Dict[str, Any]:
        """Vérifie l'état des index"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            index_stats = self.index_manager.get_index_stats()
            result["details"] = index_stats

            # Vérifie la cohérence des index
            for prop, stats in index_stats.items():
                if stats["entries"] == 0:
                    result["issues"].append({
                        "type": "empty_index",
                        "property": prop
                    })

            if result["issues"]:
                result["status"] = "warning"

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_cache_health(self) -> Dict[str, Any]:
        """Vérifie l'état du cache"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            cache_stats = self.cache.get_stats()
            result["details"] = cache_stats

            # Vérifie le taux de hit du cache
            hit_rate = cache_stats.get("hits", 0) / max(1, cache_stats.get("hits", 0) + cache_stats.get("misses", 0))
            if hit_rate < 0.5:  # Seuil d'avertissement
                result["status"] = "warning"
                result["issues"].append({
                    "type": "low_cache_hit_rate",
                    "hit_rate": hit_rate
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_transactions(self) -> Dict[str, Any]:
        """Vérifie l'état des transactions"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            active_tx = len(self.transaction_manager.active_transactions)
            result["details"]["active_transactions"] = active_tx

            if active_tx > 100:  # Seuil d'avertissement
                result["status"] = "warning"
                result["issues"].append({
                    "type": "high_active_transactions",
                    "count": active_tx
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _calculate_success_rate(self) -> float:
        """Calcule le taux de succès des opérations"""
        total_ops = sum(self.performance_monitor.detailed_metrics["node_operations"].values())
        if total_ops == 0:
            return 1.0

        failed_ops = (self.performance_monitor.detailed_metrics["node_operations"].get("failed", 0) +
                    self.performance_monitor.detailed_metrics["edge_operations"].get("failed", 0))
        
        return (total_ops - failed_ops) / total_ops

    def _check_circular_references(self, node: Node, visited: Set[str], path: List[str]) -> bool:
        """Vérifie les références circulaires dans le graphe"""
        if node.node_id in path:
            path.append(node.node_id)
            return True
            
        if node.node_id in visited:
            return False

        visited.add(node.node_id)
        path.append(node.node_id)

        for edges in node.outgoing_edges.values():
            for edge in edges:
                target = edge.target()
                if target and self._check_circular_references(target, visited, path):
                    return True

        path.pop()
        return False

    def find_nodes(self, labels: Set[str] = None, properties: Dict[str, Any] = None) -> List[Node]:
        """Recherche des nœuds par labels et/ou propriétés"""
        with self.lock:
            results = []
            for node in self.nodes.values():
                if not node._deleted:
                    # Vérifie les labels
                    if labels and not (node.labels & labels):
                        continue

                    # Vérifie les propriétés
                    if properties:
                        match = True
                        for key, value in properties.items():
                            if node.get_property(key) != value:
                                match = False
                                break
                        if not match:
                            continue

                    results.append(node)
            return results
        
    def register_stored_procedure(self, name: str, code: str, params: List[str] = None, 
                                description: str = "", tags: List[str] = None) -> StoredProcedure:
        """Enregistre une nouvelle procédure stockée"""
        procedure = StoredProcedure(name, params or [], code, description, tags)
        self.stored_procedures[name] = procedure
        print(f"Registered stored procedure: {name}")
        return procedure
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> Transaction:
        """Démarre une nouvelle transaction"""
        return self.transaction_manager.begin_transaction(isolation_level)

    def _create_node_with_transaction(self, labels: Set[str], properties: Dict[str, Any] = None, tx: Transaction = None) -> Node:
        """Crée un nœud dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._create_node_with_transaction(labels, properties, new_tx)
        
        node = self.create_node(labels, properties)
        tx.add_operation("create_node", node=node)
        return node

    def _create_edge_with_transaction(self, source_id: str, target_id: str, edge_type: str, 
                                    properties: Dict[str, Any] = None, tx: Transaction = None) -> Edge:
        """Crée une relation dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._create_edge_with_transaction(source_id, target_id, edge_type, properties, new_tx)
        
        edge = self.create_edge(source_id, target_id, edge_type, properties)
        tx.add_operation("create_edge", edge=edge)
        return edge

    def _update_node_with_transaction(self, node_id: str, properties: Dict[str, Any], tx: Transaction = None) -> Node:
        """Met à jour un nœud dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._update_node_with_transaction(node_id, properties, new_tx)
        
        node = self._update_node_internal(node_id, properties)
        tx.add_operation("update_node", node=node, properties=properties)
        return node
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retourne les statistiques complètes"""
        with self.lock:
            stats = {
                **self.statistics,
                "index_stats": self.index_manager.get_index_stats(),
                "cache_stats": self.cache.get_stats(),
                "performance": self.performance_monitor.get_detailed_statistics()
            }
            return stats

    def _delete_edge(self, edge: Edge):
        """Supprime une relation"""
        if not edge.is_valid():
            return

        source = edge.source()
        target = edge.target()
        
        if source:
            source.outgoing_edges[edge.edge_type].discard(edge)
        if target:
            target.incoming_edges[edge.edge_type].discard(edge)
            
        # Met à jour les statistiques
        self.statistics["edges_count"] -= 1
        self.statistics["edge_types_count"][edge.edge_type] -= 1
    
    def compress_history(self, older_than_days: int = 30, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Compresse les données historiques plus anciennes qu'un certain nombre de jours.
        
        Args:
            older_than_days: Âge en jours au-delà duquel compresser les données
            batch_size: Nombre de nœuds à traiter par lot pour éviter la surcharge mémoire
        
        Returns:
            Dict contenant les statistiques de compression
        """
        stats = {
            "compressed_entries": 0,
            "space_saved_bytes": 0,
            "nodes_processed": 0,
            "timestamp": datetime.now().isoformat()
        }
        
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        
        try:
            with self.lock:
                # Traitement par lots pour gérer les grandes bases de données
                node_batches = [list(self.nodes.values())[i:i + batch_size] 
                              for i in range(0, len(self.nodes), batch_size)]
                
                for batch in node_batches:
                    for node in batch:
                        # Compression de l'historique des propriétés
                        original_size = len(node._property_history)
                        node._property_history = [
                            entry for entry in node._property_history
                            if datetime.fromisoformat(entry["timestamp"]) > cutoff_date
                        ]
                        compressed_size = len(node._property_history)
                        stats["compressed_entries"] += original_size - compressed_size
                        
                        # Compression de l'historique des labels
                        original_label_size = len(node._label_history)
                        node._label_history = [
                            entry for entry in node._label_history
                            if datetime.fromisoformat(entry["timestamp"]) > cutoff_date
                        ]
                        compressed_label_size = len(node._label_history)
                        stats["compressed_entries"] += original_label_size - compressed_label_size
                        
                        stats["nodes_processed"] += 1
                
                # Enregistre les métriques
                self.performance_monitor.record_metric(
                    "history_compression",
                    stats["compressed_entries"]
                )
                
                print(f"Compressed history: {stats['compressed_entries']} entries removed")
                
        except Exception as e:
            print(f"Error during history compression: {e}")
            stats["error"] = str(e)
            
        return stats

    def validate_import_data(self, data: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
        """
        Valide les données avant leur import dans la base de données.
        
        Args:
            data: Dictionnaire contenant les données à importer
            
        Returns:
            Tuple contenant:
            - bool: True si valide, False sinon
            - List[str]: Liste des erreurs trouvées
            - Dict[str, Any]: Statistiques de validation
        """
        errors = []
        warnings = []
        stats = {
            "nodes_checked": 0,
            "edges_checked": 0,
            "schema_violations": 0
        }
        
        try:
            # 1. Vérification de la structure de base
            required_keys = {"nodes", "edges", "schema", "metadata"}
            if not all(key in data for key in required_keys):
                missing = required_keys - set(data.keys())
                errors.append(f"Missing required sections: {missing}")
                return False, errors, stats
            
            # 2. Validation du schéma
            if "schema" in data:
                for label, properties in data["schema"].get("node_labels", {}).items():
                    for prop_name, prop_def in properties.items():
                        if "type" not in prop_def:
                            errors.append(f"Missing type definition for {label}.{prop_name}")
                        else:
                            try:
                                PropertyType[prop_def["type"]]
                            except KeyError:
                                errors.append(f"Invalid property type for {label}.{prop_name}: {prop_def['type']}")
            
            # 3. Validation des nœuds
            for node_id, node_data in data.get("nodes", {}).items():
                stats["nodes_checked"] += 1
                
                if "labels" not in node_data:
                    errors.append(f"Missing labels for node {node_id}")
                
                if "properties" not in node_data:
                    errors.append(f"Missing properties for node {node_id}")
                else:
                    # Vérifie si les propriétés correspondent au schéma
                    for label in node_data.get("labels", []):
                        schema_props = data["schema"]["node_labels"].get(label, {})
                        for prop_name, prop_def in schema_props.items():
                            if prop_def.get("required", False) and prop_name not in node_data["properties"]:
                                errors.append(f"Missing required property {prop_name} for node {node_id}")
                                stats["schema_violations"] += 1
            
            # 4. Validation des relations
            for edge_data in data.get("edges", []):
                stats["edges_checked"] += 1
                
                if "source_id" not in edge_data or "target_id" not in edge_data:
                    errors.append(f"Missing source or target for edge {edge_data.get('id', 'unknown')}")
                
                if "type" not in edge_data:
                    errors.append(f"Missing type for edge {edge_data.get('id', 'unknown')}")
                
                # Vérifie si les nœuds source et cible existent
                if edge_data.get("source_id") not in data["nodes"]:
                    errors.append(f"Source node {edge_data.get('source_id')} not found for edge {edge_data.get('id', 'unknown')}")
                
                if edge_data.get("target_id") not in data["nodes"]:
                    errors.append(f"Target node {edge_data.get('target_id')} not found for edge {edge_data.get('id', 'unknown')}")
            
            if len(warnings) > 0:
                print(f"Import validation warnings: {warnings}")
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            print(f"Error during import validation: {e}")
        
        return len(errors) == 0, errors, stats

    def merge_graph(self, other_graph: 'GraphDB', 
                   conflict_strategy: str = "skip",
                   batch_size: int = 1000) -> Dict[str, Any]:
        """
        Fusionne un autre graphe dans celui-ci.
        
        Args:
            other_graph: Le graphe à fusionner
            conflict_strategy: Stratégie en cas de conflit ('skip', 'overwrite', 'rename')
            batch_size: Nombre d'éléments à traiter par lot
            
        Returns:
            Dict contenant les statistiques de fusion
        """
        stats = {
            "nodes_merged": 0,
            "edges_merged": 0,
            "conflicts_resolved": 0,
            "errors": [],
            "warnings": []
        }
        
        try:
            with self.lock:
                # 1. Fusion du schéma
                self._merge_schema(other_graph.schema, stats)
                
                # 2. Fusion des nœuds par lots
                node_batches = [list(other_graph.nodes.items())[i:i + batch_size] 
                              for i in range(0, len(other_graph.nodes), batch_size)]
                
                for batch in node_batches:
                    for node_id, node in batch:
                        try:
                            self._merge_node(node_id, node, conflict_strategy, stats)
                        except Exception as e:
                            stats["errors"].append(f"Error merging node {node_id}: {str(e)}")
                
                # 3. Fusion des relations par lots
                for node in other_graph.nodes.values():
                    for edge_type, edges in node.outgoing_edges.items():
                        try:
                            self._merge_edges(edges, conflict_strategy, stats)
                        except Exception as e:
                            stats["errors"].append(f"Error merging edges: {str(e)}")
                
                # 4. Mise à jour des index
                self.index_manager.rebuild_indexes()
                
                print(f"Graph merge completed: {stats['nodes_merged']} nodes, "
                          f"{stats['edges_merged']} edges merged")
                
        except Exception as e:
            stats["errors"].append(f"Merge error: {str(e)}")
            print(f"Error during graph merge: {e}")
        
        return stats

    def verify_data_integrity(self) -> Dict[str, Any]:
        """Vérifie l'intégrité complète des données avec validation exhaustive des contraintes."""
        console.print("[yellow]Starting data integrity verification...[/yellow]")
        
        with self.lock:
            results = {
                "status": "ok",
                "errors_found": [],
                "warnings": [],
                "statistics": {
                    "nodes_checked": 0,
                    "edges_checked": 0,
                    "indexes_checked": 0,
                    "issues_found": 0,
                    "repairs_made": 0
                }
            }

            try:
                # Affichage de l'aperçu initial
                console.print(f"\n[bold cyan]Database Overview:[/bold cyan]")
                console.print(f"Total nodes: {len(self.nodes)}")
                console.print(f"Total schemas: {len(self.schema.node_labels)}")
                
                # Affichage des schémas et contraintes
                console.print("\n[bold cyan]Schema Definitions and Constraints:[/bold cyan]")
                for label in self.schema.node_labels:
                    constraints = self.schema.get_constraints(label)
                    required_fields = {c.property for c in constraints if c.type == "required"}
                    unique_fields = {c.property for c in constraints if c.type == "unique"}
                    
                    console.print(f"\nSchema for {label}:")
                    console.print(f"Required fields: {list(required_fields)}")
                    console.print(f"Unique fields: {list(unique_fields)}")

                # 1. Vérification des nœuds
                console.print("\n[bold cyan]Node Validation:[/bold cyan]")
                
                for node_id, node in self.nodes.items():
                    results["statistics"]["nodes_checked"] += 1
                    
                    if node._deleted:
                        continue

                    node_name = node.get_property('name', 'unnamed')
                    console.print(f"\n[bold]Checking node: {node_name} ({node_id})[/bold]")
                    console.print(f"Labels: {node.labels}")
                    console.print(f"Properties: {node._properties}")

                    for label in node.labels:
                        schema = self.schema.get_node_definition(label)
                        if not schema:
                            warning = f"No schema defined for label {label}"
                            console.print(f"[yellow]Warning: {warning}[/yellow]")
                            results["warnings"].append(warning)
                            continue

                        console.print(f"\nValidating against schema for {label}")
                        
                        # 1.1 Vérification des contraintes required
                        constraints = self.schema.get_constraints(label)
                        required_fields = {c.property for c in constraints if c.type == "required"}
                        
                        for field_name in required_fields:
                            is_missing = (
                                field_name not in node._properties or
                                node._properties[field_name] is None or
                                node._properties[field_name] == ""
                            )
                            if is_missing:
                                error = f"Missing required field: {field_name}"
                                console.print(f"[red]Error: {error}[/red]")
                                results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                results["statistics"]["issues_found"] += 1

                        # 1.2 Vérification des contraintes unique
                        unique_fields = {c.property for c in constraints if c.type == "unique"}
                        for field_name in unique_fields:
                            value = node.get_property(field_name)
                            if value is not None:
                                similar_nodes = [
                                    n for n in self.nodes.values()
                                    if (label in n.labels and
                                        n.node_id != node_id and
                                        n.get_property(field_name) == value and
                                        not n._deleted)
                                ]
                                if similar_nodes:
                                    duplicate_names = [n.get_property('name', 'unnamed') for n in similar_nodes]
                                    error = f"Unique constraint violation on {field_name}: value '{value}' also used by {duplicate_names}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                    results["statistics"]["issues_found"] += 1

                        # 1.3 Vérification des types de données
                        for field_name, prop_def in schema.items():
                            if field_name in node._properties and node._properties[field_name] is not None:
                                value = node._properties[field_name]
                                expected_type = prop_def.type.value
                                if not isinstance(value, expected_type):
                                    error = f"Invalid type for {field_name}: got {type(value).__name__}, expected {prop_def.type.name}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                    results["statistics"]["issues_found"] += 1

                # 2. Vérification des relations
                console.print("\n[bold cyan]Relationship Validation:[/bold cyan]")
                for node in self.nodes.values():
                    for edge_type, edges in node.outgoing_edges.items():
                        for edge in edges:
                            results["statistics"]["edges_checked"] += 1
                            
                            if edge.is_valid():
                                source_name = edge.source().get_property('name', 'unnamed')
                                target_name = edge.target().get_property('name', 'unnamed')
                                
                                console.print(f"\nChecking relationship: {source_name} -[{edge_type}]-> {target_name}")

                                # 2.1 Vérifie si le type existe
                                if edge_type not in self.schema.edge_types:
                                    error = f"Undefined relationship type: {edge_type}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Edge {edge.id}: {error}")
                                    results["statistics"]["issues_found"] += 1
                                    continue

                                # 2.2 Vérifie les propriétés
                                edge_schema = self.schema.get_edge_definition(edge_type)
                                if edge_schema:
                                    for field_name, prop_def in edge_schema.items():
                                        if prop_def.required and field_name not in edge._properties:
                                            error = f"Missing required property: {field_name}"
                                            console.print(f"[red]Error: {error}[/red]")
                                            results["errors_found"].append(f"Edge {edge.id}: {error}")
                                            results["statistics"]["issues_found"] += 1
                                            continue

                                        if field_name in edge._properties:
                                            value = edge._properties[field_name]
                                            if value is not None and not isinstance(value, prop_def.type.value):
                                                error = f"Invalid type for {field_name}"
                                                console.print(f"[red]Error: {error}[/red]")
                                                results["errors_found"].append(f"Edge {edge.id}: {error}")
                                                results["statistics"]["issues_found"] += 1

                # Mise à jour du statut final et affichage des résultats
                if results["errors_found"]:
                    results["status"] = "error"
                    console.print(f"\n[red]Found {len(results['errors_found'])} errors[/red]")
                    for error in results["errors_found"]:
                        console.print(f"[red] • {error}[/red]")
                elif results["warnings"]:
                    console.print(f"\n[yellow]Found {len(results['warnings'])} warnings[/yellow]")
                    for warning in results["warnings"]:
                        console.print(f"[yellow] • {warning}[/yellow]")
                else:
                    console.print("\n[green]All verifications passed successfully[/green]")

                # Affichage des statistiques
                console.print("\n[bold cyan]Verification Statistics:[/bold cyan]")
                stats_table = Table(show_header=True, header_style="bold magenta")
                stats_table.add_column("Metric", style="cyan")
                stats_table.add_column("Value", style="green")
                for key, value in results["statistics"].items():
                    stats_table.add_row(key, str(value))
                console.print(stats_table)

            except Exception as e:
                results["status"] = "error"
                error = f"Verification error: {str(e)}"
                console.print(f"[red]Error: {error}[/red]")
                results["errors_found"].append(error)
                results["statistics"]["issues_found"] += 1

            return results
    
    def _apply_schema_changes(self, node: Node, new_schema: Dict[str, Any], results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Applique les changements de schéma à un nœud spécifique et retourne les changements appliqués.
        
        Args:
            node: Le nœud à modifier
            new_schema: Le nouveau schéma à appliquer
            results: Dictionnaire des résultats pour les statistiques
            
        Returns:
            Liste des changements appliqués
        """
        changes_applied = []
        
        try:
            for label in node.labels:
                if label in new_schema:
                    new_properties = new_schema[label]
                    
                    # Applique les modifications de propriétés existantes
                    for prop_name, prop_def in new_properties.items():
                        current_value = node.get_property(prop_name)
                        
                        # Gestion des valeurs manquantes pour les propriétés requises
                        if current_value is None and prop_def.required:
                            node.set_property(prop_name, prop_def.default)
                            changes_applied.append({
                                "type": "add_property",
                                "node_id": node.node_id,
                                "property": prop_name,
                                "value": prop_def.default
                            })
                            continue
                            
                        # Si la propriété existe, vérifie le type
                        if current_value is not None:
                            try:
                                # Conversion du type si nécessaire
                                if isinstance(current_value, str) and prop_def.type == PropertyType.INTEGER:
                                    new_value = int(float(current_value))
                                    node.set_property(prop_name, new_value)
                                    changes_applied.append({
                                        "type": "convert_type",
                                        "node_id": node.node_id,
                                        "property": prop_name,
                                        "old_value": current_value,
                                        "new_value": new_value
                                    })
                                elif isinstance(current_value, str) and prop_def.type == PropertyType.FLOAT:
                                    new_value = float(current_value)
                                    node.set_property(prop_name, new_value)
                                    changes_applied.append({
                                        "type": "convert_type",
                                        "node_id": node.node_id,
                                        "property": prop_name,
                                        "old_value": current_value,
                                        "new_value": new_value
                                    })
                                # Ajouter d'autres conversions si nécessaire
                            
                            except (ValueError, TypeError) as e:
                                results["errors"].append(
                                    f"Failed to convert property {prop_name} for node {node.node_id}: {str(e)}"
                                )
            
            return changes_applied
            
        except Exception as e:
            results["errors"].append(f"Error applying changes to node {node.node_id}: {str(e)}")
            return changes_applied
        
    def _migrate_property(self, node: Node, prop_name: str, 
                         prop_def: PropertyDefinition) -> bool:
        """Migre une propriété selon la nouvelle définition"""
        current_value = node.get_property(prop_name)
        
        # Si la propriété n'existe pas et est requise
        if current_value is None and prop_def.required:
            node.set_property(prop_name, prop_def.default)
            return True
            
        # Si le type doit être converti
        if current_value is not None:
            try:
                new_value = self._convert_property_value(current_value, prop_def.type)
                if new_value != current_value:
                    node.set_property(prop_name, new_value)
                    return True
            except ValueError as e:
                print(f"Could not convert property {prop_name} for node {node.node_id}: {e}")
                
        return False

    def _convert_property_value(self, value: Any, target_type: PropertyType) -> Any:
        """Convertit une valeur vers le nouveau type"""
        if target_type == PropertyType.STRING:
            return str(value)
        elif target_type == PropertyType.INTEGER:
            return int(float(value))
        elif target_type == PropertyType.FLOAT:
            return float(value)
        elif target_type == PropertyType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'y')
            return bool(value)
        elif target_type == PropertyType.DATETIME:
            if isinstance(value, str):
                return datetime.fromisoformat(value)
            raise ValueError(f"Cannot convert {type(value)} to datetime")
        elif target_type == PropertyType.LIST:
            if isinstance(value, (list, tuple, set)):
                return list(value)
            return [value]
        
        return value

    def _update_schema_definition(self, new_schema: Dict[str, Any], 
                                results: Dict[str, Any]):
        """Met à jour la définition du schéma"""
        # Sauvegarde l'ancien schéma
        old_schema = copy.deepcopy(self.schema.node_labels)
        
        try:
            # Met à jour les définitions de nœuds
            for label, properties in new_schema.items():
                self.schema.add_node_definition(label, properties)
                
            # Met à jour les contraintes
            self._update_schema_constraints(new_schema, old_schema, results)
            
        except Exception as e:
            # Restaure l'ancien schéma en cas d'erreur
            self.schema.node_labels = old_schema
            raise ValueError(f"Failed to update schema: {e}")

    def _update_schema_constraints(self, new_schema: Dict[str, Any], 
                                 old_schema: Dict[str, Any],
                                 results: Dict[str, Any]):
        """Met à jour les contraintes du schéma"""
        for label, props in new_schema.items():
            # Ajoute les nouvelles contraintes
            for prop_name, prop_def in props.items():
                if prop_def.required:
                    constraint = SchemaConstraint("required", prop_name)
                    self.schema.add_constraint(label, constraint)
                    results["statistics"]["constraints_added"] += 1
                
            # Vérifie les anciennes contraintes à supprimer
            old_props = old_schema.get(label, {})
            for prop_name, prop_def in old_props.items():
                if prop_name not in props and prop_def.required:
                    # Supprime la contrainte
                    self.schema.constraints[label] = [
                        c for c in self.schema.get_constraints(label)
                        if not (c.type == "required" and c.property == prop_name)
                    ]
                    results["statistics"]["constraints_removed"] += 1

    def _merge_schema(self, other_schema: Schema, stats: Dict[str, Any]):
        """Fusionne le schéma d'un autre graphe"""
        try:
            # Fusion des définitions de nœuds
            for label, properties in other_schema.node_labels.items():
                if label not in self.schema.node_labels:
                    self.schema.add_node_definition(label, properties)
                else:
                    # Fusionne les propriétés
                    for prop_name, prop_def in properties.items():
                        if prop_name not in self.schema.node_labels[label]:
                            self.schema.node_labels[label][prop_name] = prop_def
            
            # Fusion des contraintes
            for label, constraints in other_schema.constraints.items():
                for constraint in constraints:
                    exists = any(
                        c.type == constraint.type and c.property == constraint.property
                        for c in self.schema.get_constraints(label)
                    )
                    if not exists:
                        self.schema.add_constraint(label, constraint)
                        
            return True
        except Exception as e:
            stats["errors"].append(f"Schema merge error: {str(e)}")
            return False

    def _merge_node(self, node_id: str, node: 'Node', conflict_strategy: str, 
                    stats: Dict[str, Any]) -> bool:
        """Fusionne un nœud dans le graphe"""
        try:
            if node_id in self.nodes:
                if conflict_strategy == "skip":
                    stats["warnings"].append(f"Skipped existing node: {node_id}")
                    return False
                elif conflict_strategy == "overwrite":
                    existing_node = self.nodes[node_id]
                    # Met à jour les propriétés
                    for key, value in node._properties.items():
                        existing_node.set_property(key, value)
                    # Met à jour les labels
                    for label in node.labels - existing_node.labels:
                        existing_node.add_label(label)
                    stats["conflicts_resolved"] += 1
                    return True
                elif conflict_strategy == "rename":
                    new_id = f"{node_id}_{str(uuid4())[:8]}"
                    self.nodes[new_id] = node
                    stats["conflicts_resolved"] += 1
                    return True
            else:
                self.nodes[node_id] = node
                stats["nodes_merged"] += 1
                return True
                
        except Exception as e:
            stats["errors"].append(f"Error merging node {node_id}: {str(e)}")
            return False

    def _merge_edges(self, edges: Set['Edge'], conflict_strategy: str, 
                    stats: Dict[str, Any]) -> int:
        """Fusionne un ensemble de relations dans le graphe"""
        merged_count = 0
        for edge in edges:
            try:
                if not edge.is_valid():
                    continue
                    
                source = self.nodes.get(edge.source().node_id)
                target = self.nodes.get(edge.target().node_id)
                
                if source and target:
                    # Vérifie si une relation similaire existe déjà
                    existing = False
                    for existing_edge in source.outgoing_edges.get(edge.edge_type, set()):
                        if (existing_edge.target().node_id == target.node_id and 
                            existing_edge._properties == edge._properties):
                            existing = True
                            break
                    
                    if not existing:
                        new_edge = self.create_edge(
                            source.node_id,
                            target.node_id,
                            edge.edge_type,
                            edge._properties
                        )
                        merged_count += 1
                        stats["edges_merged"] += 1
                    
            except Exception as e:
                stats["errors"].append(f"Error merging edge: {str(e)}")
                
        return merged_count
