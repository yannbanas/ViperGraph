import threading
from datetime import datetime
from typing import Any, Dict, List, Set, Union
from collections import defaultdict


class Schema:
    """Gestionnaire de schéma pour valider les nœuds, relations, contraintes et index.

    Cette classe permet de définir et de valider des propriétés, des contraintes et
    des index associés aux nœuds et relations d'un graphe.

    Attributes:
        node_labels (Dict[str, Dict[str, PropertyDefinition]]): Définitions des propriétés des nœuds.
        edge_types (Dict[str, Dict[str, PropertyDefinition]]): Définitions des propriétés des relations.
        constraints (Dict[str, List[SchemaConstraint]]): Contraintes associées aux nœuds.
        indexes (Dict[str, Set[str]]): Index associés aux nœuds et relations.
        version (int): Version actuelle du schéma.
        history (List[Dict[str, Any]]): Historique des modifications du schéma.
        _lock (threading.RLock): Verrou pour les modifications sécurisées.
    """

    def __init__(self) -> None:
        """Initialise un schéma avec des structures vides."""
        self.node_labels: Dict[str, Dict[str, PropertyDefinition]] = defaultdict(dict)
        self.edge_types: Dict[str, Dict[str, PropertyDefinition]] = defaultdict(dict)
        self.constraints: Dict[str, List['SchemaConstraint']] = defaultdict(list)
        self.indexes: Dict[str, Set[str]] = defaultdict(set)
        self.version: int = 0
        self.history: List[Dict[str, Any]] = []
        self._lock: threading.RLock = threading.RLock()

    def validate_node(self, label: str, properties: Dict[str, Any], graph: 'GraphDB') -> bool:
        """Valide un nœud par rapport au schéma.

        Args:
            label (str): Le label du nœud à valider.
            properties (Dict[str, Any]): Les propriétés du nœud.
            graph (GraphDB): Instance de la base de données graphe.

        Returns:
            bool: True si le nœud respecte les définitions et contraintes, sinon False.
        """
        if label not in self.node_labels:
            return True  # Aucun schéma défini pour ce label

        # Validation des propriétés
        for prop_name, prop_def in self.node_labels[label].items():
            if prop_def.required and prop_name not in properties:
                return False
            if prop_name in properties and not self._validate_property(properties[prop_name], prop_def):
                return False

        # Validation des contraintes
        for constraint in self.constraints[label]:
            value = properties.get(constraint.property)
            if not constraint.validate(value, graph):
                return False

        return True

    def _validate_property(self, value: Any, prop_def: 'PropertyDefinition') -> bool:
        """Valide une propriété par rapport à sa définition.

        Args:
            value (Any): Valeur de la propriété à valider.
            prop_def (PropertyDefinition): Définition de la propriété.

        Returns:
            bool: True si la propriété respecte sa définition, sinon False.
        """
        if value is None:
            return not prop_def.required

        if prop_def.type == 'ARRAY':
            if not isinstance(value, (list, tuple)):
                return False
            if prop_def.array_type:
                return all(isinstance(item, prop_def.array_type.value) for item in value)
            return True

        try:
            return isinstance(value, prop_def.type.value)
        except Exception as e:
            print(f"Error validating property: {e}")
            return False

    def add_node_definition(self, label: str, properties: Dict[str, 'PropertyDefinition']) -> None:
        """Ajoute une définition de nœud au schéma.

        Args:
            label (str): Le label du nœud.
            properties (Dict[str, PropertyDefinition]): Les propriétés associées au nœud.
        """
        with self._lock:
            self.node_labels[label].update(properties)
            self._add_to_history("add_node_definition", {
                "label": label,
                "properties": {k: v.__dict__ for k, v in properties.items()}
            })

    def add_edge_definition(self, edge_type: str, properties: Dict[str, 'PropertyDefinition']) -> None:
        """Ajoute une définition de relation au schéma.

        Args:
            edge_type (str): Le type de la relation.
            properties (Dict[str, PropertyDefinition]): Les propriétés associées à la relation.
        """
        with self._lock:
            self.edge_types[edge_type].update(properties)
            self._add_to_history("add_edge_definition", {
                "edge_type": edge_type,
                "properties": {k: v.__dict__ for k, v in properties.items()}
            })

    def add_constraint(self, label: str, constraint: 'SchemaConstraint') -> None:
        """Ajoute une contrainte à un type de nœud.

        Args:
            label (str): Le label du nœud.
            constraint (SchemaConstraint): La contrainte à ajouter.
        """
        with self._lock:
            self.constraints[label].append(constraint)
            self._add_to_history("add_constraint", {
                "label": label,
                "constraint": {
                    "type": constraint.type,
                    "property": constraint.property,
                    "params": constraint.params
                }
            })
            logger.info(f"Added constraint {constraint.type} on {label}.{constraint.property}")

    def add_index(self, label: str, property_name: str) -> None:
        """Ajoute un index à un type de nœud ou de relation.

        Args:
            label (str): Le label du nœud ou de la relation.
            property_name (str): Le nom de la propriété à indexer.
        """
        with self._lock:
            self.indexes[label].add(property_name)

    def _add_to_history(self, operation: str, details: Dict[str, Any]) -> None:
        """Ajoute une modification à l'historique du schéma.

        Args:
            operation (str): Type de modification effectuée.
            details (Dict[str, Any]): Détails de la modification.
        """
        with self._lock:
            self.history.append({
                "operation": operation,
                "details": details,
                "version": self.version,
                "timestamp": datetime.now().isoformat()
            })
            self.version += 1

    def get_node_definition(self, label: str) -> Dict[str, 'PropertyDefinition']:
        """Récupère la définition d'un type de nœud.

        Args:
            label (str): Le label du nœud.

        Returns:
            Dict[str, PropertyDefinition]: La définition des propriétés du nœud.
        """
        return self.node_labels.get(label, {})

    def get_edge_definition(self, edge_type: str) -> Dict[str, 'PropertyDefinition']:
        """Récupère la définition d'un type de relation.

        Args:
            edge_type (str): Le type de la relation.

        Returns:
            Dict[str, PropertyDefinition]: La définition des propriétés de la relation.
        """
        return self.edge_types.get(edge_type, {})

    def get_constraints(self, label: str) -> List['SchemaConstraint']:
        """Récupère les contraintes associées à un label.

        Args:
            label (str): Le label du nœud.

        Returns:
            List[SchemaConstraint]: Liste des contraintes du nœud.
        """
        return self.constraints.get(label, [])

    def get_indexes(self, label: str) -> Set[str]:
        """Récupère les index associés à un label.

        Args:
            label (str): Le label du nœud ou de la relation.

        Returns:
            Set[str]: Ensemble des propriétés indexées.
        """
        return self.indexes.get(label, set())
