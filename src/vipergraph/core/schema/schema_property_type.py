from enum import Enum
from datetime import datetime
from typing import Any


class PropertyType(Enum):
    """Enumération des types de propriétés supportés.

    Chaque type de propriété est associé à son type Python natif, utilisé pour valider
    et manipuler les données dans le schéma.

    Attributes:
        STRING (type): Correspond au type `str`.
        INTEGER (type): Correspond au type `int`.
        FLOAT (type): Correspond au type `float`.
        BOOLEAN (type): Correspond au type `bool`.
        DATETIME (type): Correspond au type `datetime`.
        LIST (type): Correspond au type `list`.
        TEXT (type): Correspond au type `str`, utilisé pour des chaînes longues.
        NULL (type): Correspond au type `NoneType`.
        MAP (type): Correspond au type `dict`.
        ARRAY (type): Correspond au type `list`, utilisé pour des collections homogènes.
    """

    STRING = str
    INTEGER = int
    FLOAT = float
    BOOLEAN = bool
    DATETIME = datetime
    LIST = list
    TEXT = str
    NULL = type(None)
    MAP = dict
    ARRAY = list

    def python_type(self) -> Any:
        """Renvoie le type Python correspondant à l'énumération.

        Returns:
            type: Le type Python associé à l'énumération.
        """
        return self.value
