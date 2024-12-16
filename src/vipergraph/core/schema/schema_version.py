from datetime import datetime
from typing import Any, Dict, List, Optional


class SchemaVersion:
    """Représente une version d'un schéma avec son historique.

    Attributes:
        version (int): Numéro de la version.
        changes (Dict[str, Any]): Changements apportés dans cette version.
        timestamp (datetime): Timestamp de création de la version.
        next_version (Optional[SchemaVersion]): Référence vers la version suivante.
        previous_version (Optional[SchemaVersion]): Référence vers la version précédente.
    """

    def __init__(self, version: int, changes: Dict[str, Any], timestamp: datetime) -> None:
        """Initialise une instance de version du schéma.

        Args:
            version (int): Numéro de la version.
            changes (Dict[str, Any]): Changements apportés.
            timestamp (datetime): Horodatage de la version.
        """
        self.version: int = version
        self.changes: Dict[str, Any] = changes
        self.timestamp: datetime = timestamp
        self.next_version: Optional['SchemaVersion'] = None
        self.previous_version: Optional['SchemaVersion'] = None