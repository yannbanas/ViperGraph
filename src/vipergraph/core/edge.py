from datetime import datetime
from threading import RLock
from typing import Any, Dict, Optional
from uuid import uuid4
import weakref


class Edge:
    """Représente une relation dans le graphe avec support des versions et références faibles.

    Cette classe relie deux nœuds et offre des fonctionnalités avancées pour gérer
    les propriétés, l'historique des modifications et la validation.

    Attributes:
        id (str): Identifiant unique de l'arête.
        source (weakref.ref): Référence faible vers le nœud source.
        target (weakref.ref): Référence faible vers le nœud cible.
        edge_type (str): Type de l'arête.
        _properties (Dict[str, Any]): Dictionnaire des propriétés de l'arête.
        _deleted (bool): Indique si l'arête est supprimée.
        _version (int): Version actuelle de l'arête.
        _created_at (str): Date et heure de création au format ISO 8601.
        _updated_at (str): Date et heure de la dernière mise à jour au format ISO 8601.
    """

    def __init__(self, source: 'Node', target: 'Node', edge_type: str,
                 properties: Optional[Dict[str, Any]] = None):
        """Initialise une relation entre deux nœuds.

        Args:
            source (Node): Nœud source de la relation.
            target (Node): Nœud cible de la relation.
            edge_type (str): Type de la relation.
            properties (Optional[Dict[str, Any]]): Propriétés initiales de la relation.
        """
        self.id = str(uuid4())
        self.source = weakref.ref(source)
        self.target = weakref.ref(target)
        self.edge_type = edge_type
        self._properties = properties or {}
        self._deleted = False
        self._lock = RLock()
        self._property_history = []
        self._type_history = []
        self._version = 0
        self._created_at = datetime.now().isoformat()
        self._updated_at = self._created_at

    def update_type(self, new_type: str, transaction_id: Optional[str] = None) -> str:
        """Met à jour le type de la relation avec enregistrement historique.

        Args:
            new_type (str): Nouveau type de la relation.
            transaction_id (Optional[str]): Identifiant de la transaction (facultatif).

        Returns:
            str: L'ancien type de la relation avant la mise à jour.
        """
        with self._lock:
            old_type = self.edge_type
            self._type_history.append({
                "old_type": old_type,
                "new_type": new_type,
                "timestamp": datetime.now().isoformat(),
                "version": self._version,
                "transaction_id": transaction_id
            })
            self.edge_type = new_type
            self._version += 1
            self._updated_at = datetime.now().isoformat()
            return old_type

    def is_valid(self) -> bool:
        """Vérifie si la relation est toujours valide.

        Une relation est valide si :
        - Le nœud source et le nœud cible existent encore.
        - Aucun des deux nœuds n'est marqué comme supprimé.
        - La relation elle-même n'est pas supprimée.

        Returns:
            bool: True si la relation est valide, sinon False.
        """
        source = self.source()
        target = self.target()
        return (
            source is not None and
            target is not None and
            not source._deleted and
            not target._deleted and
            not self._deleted
        )

    def set_property(self, key: str, value: Any, transaction_id: Optional[str] = None) -> None:
        """Ajoute ou met à jour une propriété avec enregistrement historique.

        Args:
            key (str): Nom de la propriété.
            value (Any): Valeur de la propriété.
            transaction_id (Optional[str]): Identifiant de la transaction (facultatif).
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
        """Récupère la valeur d'une propriété.

        Args:
            key (str): Nom de la propriété.
            default (Any): Valeur par défaut si la propriété n'existe pas.

        Returns:
            Any: Valeur de la propriété ou la valeur par défaut.
        """
        return self._properties.get(key, default)

    def get_details(self) -> Dict[str, Any]:
        """Renvoie un dictionnaire contenant les détails complets de la relation.

        Returns:
            Dict[str, Any]: Un dictionnaire contenant les métadonnées et les propriétés.
        """
        return {
            "id": self.id,
            "type": self.edge_type,
            "source": self.source().node_id if self.source() else None,
            "target": self.target().node_id if self.target() else None,
            "properties": self._properties,
            "metadata": {
                "created": self._created_at,
                "updated": self._updated_at,
                "version": self._version,
                "deleted": self._deleted
            }
        }

    def __str__(self) -> str:
        """Renvoie une représentation lisible de la relation.

        Returns:
            str: Une chaîne décrivant la relation.
        """
        src = self.source()
        tgt = self.target()
        return f"Edge({src.node_id if src else 'None'}-[{self.edge_type}]->{tgt.node_id if tgt else 'None'})"
