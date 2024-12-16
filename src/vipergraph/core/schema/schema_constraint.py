import re
from typing import Any, Dict


class SchemaConstraint:
    """Représente une contrainte de schéma avec des mécanismes de validation.

    Cette classe prend en charge plusieurs types de contraintes :
    - Unique : Vérifie que la valeur est unique dans le graphe.
    - Required : Vérifie que la valeur n'est pas nulle.
    - Regex : Vérifie que la valeur correspond à une expression régulière.
    - Range : Vérifie que la valeur est dans une plage définie.

    Attributes:
        type (str): Le type de contrainte (unique, required, regex, range).
        property (str): Le nom de la propriété sur laquelle la contrainte s'applique.
        params (Dict[str, Any]): Paramètres supplémentaires pour la contrainte.
    """

    def __init__(self, constraint_type: str, property_name: str, params: Dict[str, Any] = None) -> None:
        """Initialise une contrainte de schéma.

        Args:
            constraint_type (str): Le type de contrainte.
            property_name (str): Le nom de la propriété concernée.
            params (Dict[str, Any], optional): Paramètres supplémentaires pour la contrainte.
        """
        self.type: str = constraint_type
        self.property: str = property_name
        self.params: Dict[str, Any] = params or {}

    def validate(self, value: Any, graph: 'GraphDB') -> bool:
        """Valide une valeur par rapport à la contrainte.

        Args:
            value (Any): La valeur à valider.
            graph (GraphDB): Instance de la base de données graphe.

        Returns:
            bool: True si la valeur respecte la contrainte, sinon False.
        """
        if value is None:
            # Les valeurs nulles sont autorisées sauf si la contrainte est "required".
            return self.type != "required"

        if self.type == "unique":
            return self._validate_unique(value, graph)

        if self.type == "required":
            return self._validate_required(value)

        if self.type == "regex":
            return self._validate_regex(value)

        if self.type == "range":
            return self._validate_range(value)

        # Par défaut, la contrainte est considérée comme valide.
        return True

    def _validate_unique(self, value: Any, graph: 'GraphDB') -> bool:
        """Valide qu'une valeur est unique dans le graphe.

        Args:
            value (Any): La valeur à vérifier.
            graph (GraphDB): Instance de la base de données graphe.

        Returns:
            bool: True si la valeur est unique, sinon False.
        """
        search_props = {self.property: value}
        existing_nodes = graph.find_nodes(properties=search_props)
        return len(existing_nodes) == 0

    def _validate_required(self, value: Any) -> bool:
        """Valide qu'une valeur n'est pas nulle.

        Args:
            value (Any): La valeur à vérifier.

        Returns:
            bool: True si la valeur est non nulle, sinon False.
        """
        return value is not None

    def _validate_regex(self, value: Any) -> bool:
        """Valide qu'une valeur correspond à un motif regex.

        Args:
            value (Any): La valeur à vérifier.

        Returns:
            bool: True si la valeur correspond au motif, sinon False.
        """
        pattern = self.params.get("pattern")
        if not pattern:
            return True
        return bool(re.match(pattern, str(value)))

    def _validate_range(self, value: Any) -> bool:
        """Valide qu'une valeur est dans une plage définie.

        Args:
            value (Any): La valeur à vérifier.

        Returns:
            bool: True si la valeur est dans la plage, sinon False.
        """
        min_val = self.params.get("min")
        max_val = self.params.get("max")
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True
