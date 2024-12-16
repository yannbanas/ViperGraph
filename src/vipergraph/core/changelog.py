import threading
from datetime import datetime
from typing import Any, Dict, List
from uuid import uuid4


class ChangeLog:
    """Journal des modifications pour enregistrer l'historique détaillé des opérations.

    Cette classe fournit un mécanisme sécurisé pour journaliser les opérations d'une
    transaction, avec un verrou pour la sécurité en multithreading.

    Attributes:
        changes (List[Dict[str, Any]]): Liste des modifications enregistrées.
        timestamp (datetime): Horodatage de la création du journal.
        transaction_id (str): Identifiant unique de la transaction associée.
    """

    def __init__(self) -> None:
        """Initialise le journal des modifications."""
        self.changes: List[Dict[str, Any]] = []
        self.timestamp: datetime = datetime.now()
        self.transaction_id: str = str(uuid4())
        self._lock: threading.Lock = threading.Lock()

    def add_change(self, operation: str, details: Dict[str, Any]) -> None:
        """Ajoute une modification au journal avec verrouillage pour thread-safety.

        Args:
            operation (str): Type d'opération enregistrée (ex : "create", "update").
            details (Dict[str, Any]): Détails spécifiques de l'opération.
        """
        with self._lock:
            change_entry = {
                "operation": operation,
                "details": details,
                "timestamp": datetime.now().isoformat(),
                "transaction_id": self.transaction_id
            }
            self.changes.append(change_entry)

    def get_changes_since(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """Récupère les modifications ajoutées après un certain horodatage.

        Args:
            timestamp (datetime): L'horodatage à partir duquel filtrer les modifications.

        Returns:
            List[Dict[str, Any]]: Liste des modifications enregistrées après l'horodatage.
        """
        return [
            change for change in self.changes
            if datetime.fromisoformat(change["timestamp"]) > timestamp
        ]
