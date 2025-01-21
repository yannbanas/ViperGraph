from uuid import uuid4
import torch
import torch_geometric
from torch_geometric.nn import GCNConv, SAGEConv, GATConv
from torch_geometric.data import Data, Batch
import torch.nn.functional as F
from torch_geometric.nn import GATConv
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import warnings
warnings.filterwarnings("ignore", message=".*An issue occurred while importing.*")

class BaseGNN(torch.nn.Module):
    """Classe de base pour tous les modèles GNN"""
    def train_model(self, data: Data, num_epochs: int = 100):
        self.train(mode=True)
        optimizer = torch.optim.Adam(self.parameters(), lr=0.01)
        
        for epoch in range(num_epochs):
            optimizer.zero_grad()
            out = self(data)
            loss = self._compute_loss(out, data)
            
            # Ajout de retain_graph=True
            loss.backward(retain_graph=True)
            
            optimizer.step()
            
            if epoch % 10 == 0:
                print(f'Epoch {epoch}: Loss {loss.item():.4f}')
                
    def _compute_loss(self, pred: torch.Tensor, data: Data) -> torch.Tensor:
        raise NotImplementedError

class GNNManager:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.models = {}
        self.embeddings = {}
        self.node_features = {}
        self.edge_features = {}
        
    def generate_node_embeddings(self, node_id: str) -> torch.Tensor:
        """Génère ou récupère l'embedding d'un nœud"""
        if node_id in self.embeddings:
            return self.embeddings[node_id]
            
        node = self.graph.nodes[node_id]
        features = self._extract_node_features(node)
        self.node_features[node_id] = features
        
        # Génère l'embedding initial
        embedding = self._compute_initial_embedding(features)
        self.embeddings[node_id] = embedding
        return embedding
        
    def _prepare_training_data(self, task_type: str) -> Data:
        """Prépare les données d'entraînement pour les modèles GNN"""
        # Collecte tous les nœuds et leurs features
        x = []
        node_mapping = {}
        
        for idx, node_id in enumerate(self.graph.nodes):
            node_mapping[node_id] = idx
            features = self.generate_node_embeddings(node_id)
            x.append(features)
        
        x = torch.stack(x)
        
        # Crée la liste des arêtes
        edge_index = []
        edge_attr = []
        
        for node_id, node in self.graph.nodes.items():
            src_idx = node_mapping[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target_id = edge.target().node_id
                        target_idx = node_mapping[target_id]
                        edge_index.append([src_idx, target_idx])
                        
                        # Ajoute les attributs d'arête selon le type de tâche
                        if task_type == "weighted_pathfinding":
                            weight = edge.get_property("weight", 1.0)
                            edge_attr.append([weight])
        
        edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
        
        # Prépare les données spécifiques à la tâche
        data = Data(x=x, edge_index=edge_index)
        
        if task_type == "community_evolution":
            data.communities = self._prepare_community_data()
        elif task_type == "weighted_pathfinding":
            data.edge_attr = torch.tensor(edge_attr, dtype=torch.float)
        elif task_type == "anomaly_detection":
            data.normal_patterns = self._extract_normal_patterns()
        
        return data

    def _prepare_community_data(self) -> torch.Tensor:
        """Prépare les données de communauté"""
        communities = {}
        for node in self.graph.nodes.values():
            community = node.get_property("community")
            if community:
                if community not in communities:
                    communities[community] = []
                communities[community].append(node.node_id)
        
        # Crée un tenseur one-hot pour les communautés
        num_communities = len(communities)
        community_tensor = torch.zeros((len(self.graph.nodes), num_communities))
        
        for i, (_, members) in enumerate(communities.items()):
            for node_id in members:
                node_idx = list(self.graph.nodes.keys()).index(node_id)
                community_tensor[node_idx, i] = 1
        
        return community_tensor

    def _extract_normal_patterns(self) -> torch.Tensor:
        """Extrait les motifs normaux pour la détection d'anomalies"""
        patterns = []
        
        # Calcule des statistiques de base pour chaque nœud
        for node in self.graph.nodes.values():
            pattern = [
                len(node.outgoing_edges),  # Degré sortant
                sum(len(edges) for edges in node.incoming_edges.values()),  # Degré entrant
                self.graph.analytics.clustering_coefficient(node.node_id)  # Coefficient de clustering
            ]
            patterns.append(pattern)
        
        return torch.tensor(patterns, dtype=torch.float)

    def train_gnn_model(self, task_type: str, config: Dict[str, Any]) -> str:
        """Entraîne un modèle GNN pour une tâche spécifique"""
        # Prépare les données d'entraînement
        data = self._prepare_training_data(task_type)
        
        # Définit d'abord input_dim à partir des données
        config['input_dim'] = data.x.size(1)
        
        # Ajuste ensuite la configuration selon le type de tâche
        if task_type == "community_evolution":
            num_communities = data.communities.size(1)
            config['output_dim'] = num_communities
        elif task_type == "weighted_pathfinding":
            config['output_dim'] = 1
        elif task_type == "anomaly_detection":
            config['output_dim'] = config['input_dim']  # Maintenant input_dim existe toujours
        
        # Vérifie que les dimensions minimales sont définies
        if 'hidden_dim' not in config:
            config['hidden_dim'] = config['input_dim'] // 2  # Dimension cachée par défaut

        print(f"Model configuration: input_dim={config['input_dim']}, "
            f"hidden_dim={config['hidden_dim']}, output_dim={config['output_dim']}")
        
        # Initialise le modèle selon la tâche
        if task_type == "community_evolution":
            model = CommunityEvolutionGNN(config)
        elif task_type == "weighted_pathfinding":
            model = PathfindingGNN(config)
        elif task_type == "anomaly_detection":
            model = AnomalyDetectionGNN(config)
        
        # Entraîne le modèle
        model.train_model(data)
        
        # Stocke le modèle avec son type comme clé
        self.models[task_type] = model
        
        return task_type  # Retourne le type comme ID
                
    def _compute_loss(self, pred: torch.Tensor, data: Data) -> torch.Tensor:
        """Calcule la loss selon le type de modèle"""
        # Implémenté différemment dans chaque sous-classe
        raise NotImplementedError
      
    def predict_community_evolution(self, community_ids: List[str], 
                                  time_steps: int) -> List[Dict[str, Any]]:
        """Prédit l'évolution des communautés"""
        model = self.models.get("community_evolution")
        if not model:
            raise ValueError("No trained community evolution model found")
            
        # Prépare les données des communautés
        community_data = self._prepare_community_data(community_ids)
        
        # Fait la prédiction
        predictions = model.predict_evolution(community_data, time_steps)
        return predictions
        
    def optimize_path(self, start_id: str, end_id: str) -> List[str]:
        """Trouve le chemin optimal avec GNN"""
        model = self.models.get("pathfinding")
        if not model:
            raise ValueError("No trained pathfinding model found")
            
        # Prépare le graphe avec les embeddings
        graph_data = self._prepare_graph_data()
        
        # Optimise le chemin
        path = model.find_path(graph_data, start_id, end_id)
        return path
        
    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Détecte les anomalies structurelles"""
        model = self.models.get("anomaly_detection")
        if not model:
            raise ValueError("No trained anomaly detection model found")
            
        # Prépare les données du graphe
        graph_data = self._prepare_graph_data()
        
        # Détecte les anomalies
        anomalies = model.detect(graph_data)
        return anomalies

    def _prepare_graph_data(self) -> Data:
        """Prépare les données du graphe pour l'inférence"""
        # Collecte tous les nœuds et leurs features
        x = []
        node_mapping = {}
        
        for idx, node_id in enumerate(self.graph.nodes):
            node_mapping[node_id] = idx
            features = self.generate_node_embeddings(node_id)
            x.append(features)
        
        x = torch.stack(x)
        
        # Crée la liste des arêtes
        edge_index = []
        edge_attr = []
        
        for node_id, node in self.graph.nodes.items():
            src_idx = node_mapping[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target_id = edge.target().node_id
                        target_idx = node_mapping[target_id]
                        edge_index.append([src_idx, target_idx])
                        # Ajoute les attributs d'arête
                        weight = edge.get_property("weight", 1.0)
                        edge_attr.append([weight])
        
        edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
        edge_attr = torch.tensor(edge_attr, dtype=torch.float)
        
        # Crée l'objet Data
        data = Data(
            x=x, 
            edge_index=edge_index,
            edge_attr=edge_attr,
            node_mapping=node_mapping  # Utile pour retrouver les IDs des nœuds
        )
        
        # Ajoute les motifs normaux pour la détection d'anomalies
        data.normal_patterns = self._extract_normal_patterns()
        
        return data
   
    def _extract_node_features(self, node: 'Node') -> torch.Tensor:
        """Extrait les caractéristiques d'un nœud de manière plus robuste"""
        features = []
        
        # Caractéristiques structurelles
        out_degree = sum(len(edges) for edges in node.outgoing_edges.values())
        in_degree = sum(len(edges) for edges in node.incoming_edges.values())
        unusual_connections = len(node.outgoing_edges.get('UNUSUAL_CONNECTION', set()))
        
        # Caractéristiques numériques normalisées
        activity_score = node.get_property('activity_score', 0) / 200.0  # Normalisation
        connections = node.get_property('connections', 0) / 100.0  # Normalisation
        
        features.extend([
            out_degree / 50.0,  # Normalisation
            in_degree / 50.0,   # Normalisation
            unusual_connections / 20.0,  # Normalisation
            activity_score,
            connections
        ])
        
        # Pad jusqu'à 64 dimensions avec des zéros
        features.extend([0.0] * (64 - len(features)))
        
        return torch.tensor(features, dtype=torch.float)
        
    def _encode_categorical(self, value: str) -> List[float]:
        """Encode une valeur catégorielle"""
        # Utilise un hachage simple pour créer un vecteur binaire
        hash_val = hash(value)
        binary = format(abs(hash_val), '032b')
        return [float(b) for b in binary[:8]]  # Utilise les 8 premiers bits
        
    def _compute_initial_embedding(self, features: torch.Tensor) -> torch.Tensor:
        """Calcule l'embedding initial des caractéristiques"""
        projection_dim = 64
        if not hasattr(self, 'projection'):
            self.projection = torch.nn.Linear(features.shape[0], projection_dim)
        
        # Redimensionne pour correspondre aux attentes de la normalisation
        features = features.unsqueeze(0)  # Ajoute une dimension batch
        embedding = self.projection(features)
        normalized = torch.nn.functional.normalize(embedding, p=2, dim=-1)
        return normalized.squeeze(0)  # Retire la dimension batch

class CommunityEvolutionGNN(BaseGNN):
    def __init__(self, config: dict):
        super().__init__()
        # Architecture améliorée avec batch normalization
        self.conv1 = GCNConv(config['input_dim'], config['hidden_dim'])
        self.conv2 = GCNConv(config['hidden_dim'], config['hidden_dim'])
        self.conv3 = GCNConv(config['hidden_dim'], config['hidden_dim'])
        
        self.batch_norm1 = torch.nn.BatchNorm1d(config['hidden_dim'])
        self.batch_norm2 = torch.nn.BatchNorm1d(config['hidden_dim'])
        self.batch_norm3 = torch.nn.BatchNorm1d(config['hidden_dim'])
        
        # Prédicteurs d'événements avec dropout
        self.split_predictor = torch.nn.Sequential(
            torch.nn.Linear(config['hidden_dim'], config['hidden_dim'] // 2),
            torch.nn.ReLU(),
            torch.nn.Dropout(0.3),
            torch.nn.Linear(config['hidden_dim'] // 2, 1),
            torch.nn.Sigmoid()
        )
        
        self.merge_predictor = torch.nn.Sequential(
            torch.nn.Linear(config['hidden_dim'], config['hidden_dim'] // 2),
            torch.nn.ReLU(),
            torch.nn.Dropout(0.3),
            torch.nn.Linear(config['hidden_dim'] // 2, 1),
            torch.nn.Sigmoid()
        )
        
        self.dissolve_predictor = torch.nn.Sequential(
            torch.nn.Linear(config['hidden_dim'], config['hidden_dim'] // 2),
            torch.nn.ReLU(),
            torch.nn.Dropout(0.3),
            torch.nn.Linear(config['hidden_dim'] // 2, 1),
            torch.nn.Sigmoid()
        )
        
        self.community_predictor = torch.nn.Sequential(
            torch.nn.Linear(config['hidden_dim'], config['hidden_dim']),
            torch.nn.ReLU(),
            torch.nn.Dropout(0.3),
            torch.nn.Linear(config['hidden_dim'], config['output_dim'])
        )
        
    def forward(self, data: Data) -> tuple:
        x, edge_index = data.x, data.edge_index
        
        # Couches de convolution avec normalisation et skip connections
        h1 = self.batch_norm1(torch.relu(self.conv1(x, edge_index)))
        h2 = self.batch_norm2(torch.relu(self.conv2(h1, edge_index))) + h1
        h3 = self.batch_norm3(torch.relu(self.conv3(h2, edge_index))) + h2
        
        # Prédictions
        splits = self.split_predictor(h3)
        merges = self.merge_predictor(h3)
        dissolves = self.dissolve_predictor(h3)
        communities = self.community_predictor(h3)
        
        return communities, splits, merges, dissolves

    def predict_evolution(self, data: Data, time_steps: int) -> List[Dict[str, Any]]:
        self.eval()
        predictions = []
        current_data = data
        
        with torch.no_grad():
            for step in range(time_steps):
                communities, splits, merges, dissolves = self(current_data)
                
                # Seuils dynamiques avec limitation du nombre d'événements
                split_threshold = self._calculate_dynamic_threshold(splits, 0.85)
                merge_threshold = self._calculate_dynamic_threshold(merges, 0.85)
                dissolve_threshold = self._calculate_dynamic_threshold(dissolves, 0.90)
                
                # Collecte les événements prédits
                step_predictions = self._collect_events(
                    current_data, communities, splits, merges, dissolves,
                    split_threshold, merge_threshold, dissolve_threshold
                )
                
                predictions.append(step_predictions)
                
                # Met à jour l'état pour le prochain pas
                current_data = self._update_state(current_data, step_predictions, communities)
                
        return predictions
    
    def _calculate_dynamic_threshold(self, scores: torch.Tensor, base_percentile: float) -> float:
        """Calcule un seuil dynamique avec ajustement selon la distribution"""
        if scores.numel() == 0:
            return 0.5
        flat_scores = scores.flatten()
        threshold = torch.quantile(flat_scores, base_percentile)
        
        # Ajuste le seuil selon la distribution
        mean_score = torch.mean(flat_scores)
        std_score = torch.std(flat_scores)
        if std_score < 0.1:  # Distribution trop serrée
            threshold = mean_score + std_score
            
        return threshold.item()
    
    def _collect_events(self, data: Data, communities, splits, merges, 
                       dissolves, split_thresh, merge_thresh, dissolve_thresh) -> dict:
        """Collecte les événements prédits avec limitations"""
        events = {
            'splits': [],
            'merges': [],
            'dissolves': []
        }
        
        current_communities = torch.argmax(data.communities, dim=1)
        num_communities = len(torch.unique(current_communities))
        
        # Limite le nombre d'événements selon la taille du graphe
        max_splits = max(1, num_communities // 4)
        max_merges = max(1, num_communities // 4)
        max_dissolves = max(1, num_communities // 5)
        
        # Collecte les divisions
        split_candidates = (splits.squeeze() > split_thresh).nonzero().flatten()
        for idx in split_candidates[:max_splits]:
            comm_id = current_communities[idx].item()
            events['splits'].append({
                'original': comm_id,
                'new': communities.size(1),
                'size': (current_communities == comm_id).sum().item() // 2
            })
        
        # Collecte les fusions
        processed = set()
        merge_candidates = (merges.squeeze() > merge_thresh).nonzero().flatten()
        for idx in merge_candidates:
            comm_id = current_communities[idx].item()
            if comm_id not in processed and len(events['merges']) < max_merges:
                target_id = self._find_merge_target(communities[idx], comm_id)
                if target_id is not None:
                    events['merges'].append({
                        'source': comm_id,
                        'target': target_id,
                        'size': (current_communities == comm_id).sum().item()
                    })
                    processed.update([comm_id, target_id])
        
        # Collecte les dissolutions
        dissolve_candidates = (dissolves.squeeze() > dissolve_thresh).nonzero().flatten()
        for idx in dissolve_candidates[:max_dissolves]:
            comm_id = current_communities[idx].item()
            if comm_id not in processed:
                events['dissolves'].append({
                    'dissolved': comm_id,
                    'size': (current_communities == comm_id).sum().item()
                })
                processed.add(comm_id)
        
        return events
    
    def _find_merge_target(self, community_scores: torch.Tensor, source_id: int) -> Optional[int]:
        """Trouve la meilleure communauté cible pour une fusion"""
        scores = community_scores.clone()
        scores[source_id] = -float('inf')
        target_id = torch.argmax(scores).item()
        
        # Vérifie si la fusion est pertinente
        if scores[target_id] > 0.5:
            return target_id
        return None
    
    def _update_state(self, data: Data, events: Dict, new_communities: torch.Tensor) -> Data:
        """Met à jour l'état du graphe avec les changements prédits"""
        current_comms = torch.argmax(data.communities, dim=1)
        num_communities = new_communities.size(1)
        
        new_data = Data(
            x=data.x,
            edge_index=data.edge_index,
            communities=F.softmax(new_communities, dim=1)
        )
        
        # Applique les changements dans l'ordre : fusions -> divisions -> dissolutions
        for merge in events['merges']:
            mask = (current_comms == merge['source'])
            target_id = min(merge['target'], num_communities - 1)  # Assure un indice valide
            new_data.communities[mask] = F.one_hot(
                torch.tensor(target_id),
                num_classes=num_communities
            ).float()
        
        # Pour les divisions, utilise l'indice de la dernière communauté existante
        used_community_indices = set(current_comms.unique().tolist())
        available_indices = set(range(num_communities)) - used_community_indices
        
        for split in events['splits']:
            mask = (current_comms == split['original'])
            split_mask = mask & (torch.rand_like(mask.float()) > 0.5)
            
            # Trouve un nouvel indice de communauté disponible
            new_community_id = (
                min(available_indices) if available_indices 
                else min(num_communities - 1, split['original'] + 1)
            )
            
            new_data.communities[split_mask] = F.one_hot(
                torch.tensor(new_community_id),
                num_classes=num_communities
            ).float()
            
            # Met à jour les indices disponibles
            if new_community_id in available_indices:
                available_indices.remove(new_community_id)
        
        for dissolve in events['dissolves']:
            mask = (current_comms == dissolve['dissolved'])
            # Crée une distribution sur toutes les communautés sauf celle dissoute
            distribution = torch.ones_like(new_data.communities)
            distribution[:, dissolve['dissolved']] = 0
            
            # Normalise la distribution
            distribution = F.normalize(distribution, p=1, dim=1)
            new_data.communities[mask] = distribution[mask]
        
        return new_data

    def _compute_loss(self, pred: Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor],
                    data: Data) -> torch.Tensor:
        """Calcule la loss avec pondération des composantes"""
        communities, splits, merges, dissolves = pred
        
        # Loss des communautés avec focal loss pour gérer le déséquilibre
        ce_loss = F.cross_entropy(communities, torch.argmax(data.communities, dim=1))
        probs = F.softmax(communities, dim=1)
        focal_weight = (1 - probs) ** 2
        community_loss = (ce_loss * focal_weight).mean()
        
        # Loss des événements avec pondération
        split_loss = F.binary_cross_entropy(splits.squeeze(), data.splits.squeeze())
        merge_loss = F.binary_cross_entropy(merges.squeeze(), data.merges.squeeze())
        dissolve_loss = F.binary_cross_entropy(dissolves.squeeze(), data.dissolves.squeeze())
        
        # Pondération des composantes
        total_loss = (
            0.4 * community_loss +
            0.2 * split_loss +
            0.2 * merge_loss +
            0.2 * dissolve_loss
        )
        
        return total_loss

class PathfindingGNN(BaseGNN):
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.conv1 = GATConv(config['input_dim'], config['hidden_dim'])
        self.conv2 = GATConv(config['hidden_dim'], config['hidden_dim'])
        self.edge_predictor = torch.nn.Linear(config['hidden_dim'] * 2, 1)
        
    def _compute_loss(self, pred: torch.Tensor, data: Data) -> torch.Tensor:
        return torch.nn.functional.mse_loss(pred, data.edge_attr)
    
    def forward(self, data: Data) -> torch.Tensor:
        x, edge_index = data.x, data.edge_index
        
        # Mise à jour des nœuds
        h = torch.relu(self.conv1(x, edge_index))
        h = self.conv2(h, edge_index)
        
        # Prédiction des poids des arêtes
        edge_features = self._get_edge_features(h, edge_index)
        edge_weights = self.edge_predictor(edge_features)
        
        return edge_weights
        
    def _get_edge_features(self, node_embeddings: torch.Tensor, 
                          edge_index: torch.Tensor) -> torch.Tensor:
        """Calcule les caractéristiques des arêtes"""
        # Concatène les embeddings des nœuds source et cible
        src, dst = edge_index
        edge_features = torch.cat([node_embeddings[src], node_embeddings[dst]], dim=1)
        return edge_features
        
    def find_path(self, graph_data: Data, start_id: str, end_id: str) -> List[str]:
        """Trouve le chemin optimal"""
        self.eval()
        with torch.no_grad():
            # Prédit les poids des arêtes
            edge_weights = self(graph_data)
            
            # Utilise A* avec les poids prédits
            path = self._astar_search(graph_data, edge_weights, start_id, end_id)
            return path
            
    def _astar_search(self, graph_data: Data, edge_weights: torch.Tensor,
                      start_id: str, end_id: str) -> List[str]:
        """Implémente A* avec les poids prédits"""
        # Implémenter A* ici
        return []

class AnomalyDetectionGNN(torch.nn.Module):
    def __init__(self, config: dict):
        super().__init__()
        
        # Architecture basée sur GAT avec plusieurs têtes d'attention
        self.gat1 = GATConv(config['input_dim'], 128, heads=8, dropout=0.2)
        self.gat2 = GATConv(128 * 8, 64, heads=4, dropout=0.2)
        self.gat3 = GATConv(64 * 4, 32, heads=2, dropout=0.2)
        
        # Normalisation et dropout entre les couches
        self.norm1 = torch.nn.LayerNorm(128 * 8)
        self.norm2 = torch.nn.LayerNorm(64 * 4)
        self.norm3 = torch.nn.LayerNorm(32 * 2)
        
        # MLP pour le score d'anomalie
        self.anomaly_scorer = torch.nn.Sequential(
            torch.nn.Linear(32 * 2, 64),
            torch.nn.ReLU(),
            torch.nn.LayerNorm(64),
            torch.nn.Dropout(0.2),
            torch.nn.Linear(64, 32),
            torch.nn.ReLU(),
            torch.nn.LayerNorm(32),
            torch.nn.Linear(32, 1),
            torch.nn.Sigmoid()
        )
        
    def forward(self, data: Data) -> torch.Tensor:
        x, edge_index = data.x, data.edge_index
        
        # Couches GAT avec skip connections
        h1 = F.relu(self.gat1(x, edge_index))
        h1 = self.norm1(h1)
        
        h2 = F.relu(self.gat2(h1, edge_index))
        h2 = self.norm2(h2)
        h2 = h2 + h1[:, :h2.size(1)]  # Skip connection
        
        h3 = F.relu(self.gat3(h2, edge_index))
        h3 = self.norm3(h3)
        h3 = h3 + h2[:, :h3.size(1)]  # Skip connection
        
        # Score d'anomalie
        scores = self.anomaly_scorer(h3)
        
        return scores

    def train_model(self, data: Data, epochs: int = 100, lr: float = 0.001):
        optimizer = torch.optim.Adam(self.parameters(), lr=lr)
        
        self.train()
        for epoch in range(epochs):
            optimizer.zero_grad()
            
            # Forward pass
            anomaly_scores = self(data)
            
            # Calcul de la perte
            loss = self._compute_loss(anomaly_scores, data)
            
            # Backward pass
            loss.backward()
            optimizer.step()
            
            if (epoch + 1) % 10 == 0:
                print(f'Epoch {epoch+1:02d}, Loss: {loss.item():.4f}')

    def _compute_loss(self, anomaly_scores: torch.Tensor, data: Data) -> torch.Tensor:
        # Perte basée sur les labels d'anomalie avec pondération
        anomaly_labels = data.anomaly_labels
        
        # Poids plus élevé pour les faux positifs
        weights = torch.ones_like(anomaly_labels)
        weights[anomaly_labels == 0] = 2.0  # Pénalise plus les faux positifs
        
        loss = F.binary_cross_entropy(
            anomaly_scores.squeeze(),
            anomaly_labels.float(),
            weight=weights
        )
        
        return loss
    
    def _compute_structural_loss(self, h: torch.Tensor, 
                               edge_index: torch.Tensor) -> torch.Tensor:
        """Calcule la perte structurelle basée sur l'homophilie"""
        # Calcule la similarité entre nœuds connectés
        src, dst = edge_index
        sim_connected = torch.nn.functional.cosine_similarity(h[src], h[dst])
        
        # Les nœuds connectés devraient être similaires
        return torch.mean(1 - sim_connected)
        
    def _compute_contrast_loss(self, h: torch.Tensor, 
                             anomaly_scores: torch.Tensor) -> torch.Tensor:
        """Perte contrastive pour séparer les anomalies"""
        # Normalise les embeddings
        h_norm = torch.nn.functional.normalize(h, dim=1)
        
        # Matrice de similarité
        sim_matrix = torch.mm(h_norm, h_norm.t())
        
        # Les anomalies devraient être différentes des points normaux
        weights = (anomaly_scores - anomaly_scores.t()).abs()
        contrast_loss = (sim_matrix * weights).mean()
        
        return contrast_loss
        
    def detect(self, graph_data: Data) -> List[Dict[str, Any]]:
        """Détecte les anomalies dans le graphe"""
        self.eval()
        anomalies = []
        
        with torch.no_grad():
            # Forward pass
            x_reconstructed, h, anomaly_scores = self(graph_data)
            
            # Calcule les scores d'anomalie combinés
            reconstruction_errors = torch.norm(x_reconstructed - graph_data.x, dim=1)
            structural_scores = self._compute_structural_scores(h, graph_data.edge_index)
            
            # Combine les scores
            combined_scores = (
                0.4 * anomaly_scores.squeeze() +
                0.3 * torch.nn.functional.normalize(reconstruction_errors, dim=0) +
                0.3 * structural_scores
            )
            
            # Calcule le seuil adaptatif
            threshold = self._compute_threshold(combined_scores)
            
            # Identifie les anomalies
            for i, score in enumerate(combined_scores):
                if score > threshold:
                    anomalies.append({
                        'node_idx': i,
                        'score': score.item(),
                        'severity': (score / threshold).item(),
                        'reconstruction_error': reconstruction_errors[i].item(),
                        'structural_score': structural_scores[i].item(),
                        'anomaly_score': anomaly_scores[i].item()
                    })
            
            # Trie par sévérité
            anomalies.sort(key=lambda x: x['severity'], reverse=True)
            
        return anomalies
        
    def _compute_structural_scores(self, h: torch.Tensor, 
                                 edge_index: torch.Tensor) -> torch.Tensor:
        """Calcule les scores structurels pour chaque nœud"""
        src, dst = edge_index
        
        # Calcule les différences avec les voisins
        diffs = []
        for i in range(h.size(0)):
            mask_src = src == i
            mask_dst = dst == i
            
            if mask_src.any() or mask_dst.any():
                neighbor_indices = torch.cat([dst[mask_src], src[mask_dst]])
                neighbor_embeds = h[neighbor_indices]
                diff = torch.norm(h[i] - neighbor_embeds.mean(dim=0))
                diffs.append(diff)
            else:
                diffs.append(torch.tensor(0.0))
                
        return torch.tensor(diffs)
        
    def _compute_threshold(self, scores: torch.Tensor) -> float:
        """Calcule le seuil adaptatif pour la détection"""
        # Utilise la méthode MAD (Median Absolute Deviation)
        median = scores.median()
        mad = torch.median(torch.abs(scores - median))
        
        # Seuil = médiane + 3 * MAD
        return median + 3 * mad