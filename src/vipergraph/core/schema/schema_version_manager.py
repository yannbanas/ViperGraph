import datetime
from typing import Any, Dict, List, Optional
from schema_version import SchemaVersion


class SchemaVersionManager:
    """Gestionnaire des versions du schéma.

    Permet d'ajouter, récupérer et naviguer entre les différentes versions du schéma.

    Attributes:
        versions (Dict[int, SchemaVersion]): Dictionnaire des versions indexées par numéro.
        current_version (Optional[SchemaVersion]): La version actuelle du schéma.
        first_version (Optional[SchemaVersion]): La première version enregistrée.
    """

    def __init__(self) -> None:
        """Initialise le gestionnaire des versions du schéma."""
        self.versions: Dict[int, SchemaVersion] = {}
        self.current_version: Optional[SchemaVersion] = None
        self.first_version: Optional[SchemaVersion] = None

    def add_version(self, changes: Dict[str, Any]) -> int:
        """Ajoute une nouvelle version du schéma.

        Args:
            changes (Dict[str, Any]): Changements apportés dans cette version.

        Returns:
            int: Numéro de la version ajoutée.
        """
        version = SchemaVersion(
            version=len(self.versions) + 1,
            changes=changes,
            timestamp=datetime.now()
        )
        self.versions[version.version] = version

        if self.current_version:
            version.previous_version = self.current_version
            self.current_version.next_version = version
        else:
            self.first_version = version

        self.current_version = version
        return version.version

    def get_version(self, version: int) -> Optional[SchemaVersion]:
        """Récupère une version spécifique du schéma.

        Args:
            version (int): Numéro de la version à récupérer.

        Returns:
            Optional[SchemaVersion]: La version demandée ou None si elle n'existe pas.
        """
        return self.versions.get(version)

    def get_changes_between_versions(self, from_version: int, to_version: int) -> List[Dict[str, Any]]:
        """Récupère les changements entre deux versions du schéma.

        Args:
            from_version (int): Version de départ.
            to_version (int): Version cible.

        Returns:
            List[Dict[str, Any]]: Liste des changements entre les deux versions.
        """
        changes = []
        current = self.versions.get(from_version)

        while current and current.version <= to_version:
            changes.append(current.changes)
            current = current.next_version

        return changes

    def can_upgrade_to_version(self, target_version: int) -> bool:
        """Vérifie si une mise à niveau vers une version spécifique est possible.

        Args:
            target_version (int): Numéro de la version cible.

        Returns:
            bool: True si la mise à niveau est possible, sinon False.
        """
        if target_version not in self.versions:
            return False

        current = self.first_version
        while current:
            if current.version == target_version:
                return True
            current = current.next_version

        return False
