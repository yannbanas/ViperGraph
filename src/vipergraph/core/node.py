from collections import defaultdict
from datetime import datetime
from threading import RLock
from typing import Any, Dict, Set, Optional


class Node:
    """Représente un nœud du graphe avec support des versions et du verrouillage.

    Attributes:
        node_id (str): L'identifiant unique du nœud.
        incoming_edges (Dict[str, Set['Edge']]): Dictionnaire des relations entrantes.
        outgoing_edges (Dict[str, Set['Edge']]): Dictionnaire des relations sortantes.
    """

    def __init__(self, node_id: str, labels: Optional[Set[str]] = None,
                 properties: Optional[Dict[str, Any]] = None):
        """Initialise un nœud avec un ID, des étiquettes et des propriétés.

        Args:
            node_id (str): Identifiant unique du nœud.
            labels (Optional[Set[str]]): Ensemble des étiquettes associées au nœud.
            properties (Optional[Dict[str, Any]]): Dictionnaire des propriétés du nœud.
        """
        self.node_id = node_id
        self._labels = labels or set()
        self._properties = properties or {}
        self.incoming_edges: Dict[str, Set['Edge']] = defaultdict(set)
        self.outgoing_edges: Dict[str, Set['Edge']] = defaultdict(set)
        self._deleted = False
        self._lock = RLock()
        self._property_history = []
        self._label_history = []
        self._version = 0
        self._created_at = datetime.now().isoformat()
        self._updated_at = self._created_at

    @property
    def labels(self) -> Set[str]:
        """Renvoie une copie des étiquettes associées au nœud.

        Returns:
            Set[str]: Les étiquettes du nœud.
        """
        return self._labels.copy()

    def add_label(self, label: str, transaction_id: Optional[str] = None) -> None:
        """Ajoute une étiquette au nœud avec enregistrement historique.

        Args:
            label (str): L'étiquette à ajouter.
            transaction_id (Optional[str]): L'identifiant de la transaction (facultatif).
        """
        with self._lock:
            if label not in self._labels:
                self._label_history.append({
                    "operation": "add",
                    "label": label,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "transaction_id": transaction_id
                })
                self._labels.add(label)
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def remove_label(self, label: str, transaction_id: Optional[str] = None) -> None:
        """Supprime une étiquette du nœud avec enregistrement historique.

        Args:
            label (str): L'étiquette à supprimer.
            transaction_id (Optional[str]): L'identifiant de la transaction (facultatif).
        """
        with self._lock:
            if label in self._labels:
                self._label_history.append({
                    "operation": "remove",
                    "label": label,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "transaction_id": transaction_id
                })
                self._labels.discard(label)
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def has_label(self, label: str) -> bool:
        """Vérifie si le nœud contient une étiquette spécifique.

        Args:
            label (str): L'étiquette à vérifier.

        Returns:
            bool: True si l'étiquette est présente, sinon False.
        """
        return label in self._labels

    def set_property(self, key: str, value: Any, transaction_id: Optional[str] = None) -> None:
        """Ajoute ou met à jour une propriété avec enregistrement historique.

        Args:
            key (str): Le nom de la propriété.
            value (Any): La valeur de la propriété.
            transaction_id (Optional[str]): L'identifiant de la transaction (facultatif).
        """
        with self._lock:
            old_value = self._properties.get(key)
            self._property_history.append({
                "key": key,
                "old_value": old_value,
                "new_value": value,
                "timestamp": datetime.now().isoformat(),
                "version": self._version,
                "transaction_id": transaction_id
            })
            self._properties[key] = value
            self._version += 1
            self._updated_at = datetime.now().isoformat()

    def get_property(self, key: str, default: Any = None) -> Any:
        """Renvoie la valeur d'une propriété.

        Args:
            key (str): Le nom de la propriété.
            default (Any): La valeur par défaut si la propriété n'existe pas.

        Returns:
            Any: La valeur de la propriété ou la valeur par défaut.
        """
        return self._properties.get(key, default)

    def remove_property(self, key: str, transaction_id: Optional[str] = None) -> None:
        """Supprime une propriété avec enregistrement historique.

        Args:
            key (str): Le nom de la propriété à supprimer.
            transaction_id (Optional[str]): L'identifiant de la transaction (facultatif).
        """
        with self._lock:
            if key in self._properties:
                old_value = self._properties[key]
                self._property_history.append({
                    "key": key,
                    "old_value": old_value,
                    "new_value": None,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "operation": "remove",
                    "transaction_id": transaction_id
                })
                del self._properties[key]
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def get_details(self) -> Dict[str, Any]:
        """Renvoie un dictionnaire contenant les détails complets du nœud.

        Returns:
            Dict[str, Any]: Un dictionnaire avec les métadonnées et les propriétés du nœud.
        """
        return {
            "id": self.node_id,
            "labels": list(self._labels),
            "properties": self._properties,
            "relations": {
                "incoming": {edge_type: len(edges) for edge_type, edges in self.incoming_edges.items()},
                "outgoing": {edge_type: len(edges) for edge_type, edges in self.outgoing_edges.items()},
            },
            "metadata": {
                "created": self._created_at,
                "updated": self._updated_at,
                "version": self._version,
                "deleted": self._deleted
            }
        }

    def __str__(self) -> str:
        """Renvoie une représentation lisible du nœud.

        Returns:
            str: Une chaîne décrivant le nœud.
        """
        return f"Node(id={self.node_id}, labels={self._labels}, properties={self._properties})"
