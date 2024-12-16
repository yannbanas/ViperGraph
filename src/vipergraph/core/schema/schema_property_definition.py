from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from schema_property_type import PropertyType


@dataclass
class PropertyDefinition:
    """Représente la définition d'une propriété dans le schéma.

    Cette classe fournit une structure pour définir les types, les contraintes et
    les propriétés par défaut des attributs associés aux nœuds ou relations du graphe.

    Attributes:
        type (PropertyType): Le type attendu pour la propriété.
        required (bool): Indique si la propriété est obligatoire.
        default (Any): La valeur par défaut de la propriété.
        array_type (Optional[PropertyType]): Le type des éléments si la propriété est un tableau.
        constraints (List[Callable[[Any], bool]]): Liste de fonctions de validation supplémentaires.
    """

    type: PropertyType
    required: bool = False
    default: Any = None
    array_type: Optional[PropertyType] = None
    constraints: List[Callable[[Any], bool]] = field(default_factory=list)

    def __post_init__(self):
        """Valide et initialise la liste des contraintes."""
        if self.constraints is None:
            self.constraints = []
