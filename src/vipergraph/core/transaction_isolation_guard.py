import datetime
from typing import Any, Optional, Set, Dict

from isolation_level import IsolationLevel


class TransactionIsolationGuard:
    """Gestionnaire d'isolation pour les transactions.

    Cette classe assure la gestion des niveaux d'isolation pour une transaction
    en surveillant les lectures et écritures pour éviter les anomalies comme :
    - Phantom Reads
    - Non-Repeatable Reads
    - Write Conflicts

    Attributes:
        transaction (Transaction): La transaction en cours.
        read_set (Set[str]): Ensemble des clés lues dans la transaction.
        write_set (Set[str]): Ensemble des clés écrites dans la transaction.
        snapshot (Optional[Dict[str, Any]]): Snapshot capturé pour les niveaux d'isolation élevés.
        version_numbers (Dict[str, int]): Versions des clés surveillées pour les conflits d'écriture.
        commit_timestamp (Optional[datetime]): Timestamp au moment du commit.
    """

    def __init__(self, transaction: 'Transaction') -> None:
        """Initialise le gestionnaire d'isolation pour une transaction.

        Args:
            transaction (Transaction): Instance de la transaction associée.
        """
        self.transaction: 'Transaction' = transaction
        self.read_set: Set[str] = set()
        self.write_set: Set[str] = set()
        self.snapshot: Optional[Dict[str, Any]] = None
        self.version_numbers: Dict[str, int] = {}
        self.commit_timestamp: Optional[datetime] = None

    def before_read(self, key: str) -> None:
        """Vérifie la validité d'une opération de lecture.

        Cette méthode détecte les anomalies selon le niveau d'isolation :
        - Phantom Read pour SERIALIZABLE.
        - Non-Repeatable Read pour REPEATABLE_READ.

        Args:
            key (str): La clé à lire dans la transaction.

        Raises:
            ValueError: Si une anomalie est détectée.
        """
        isolation_level = self.transaction.isolation_level

        if isolation_level == IsolationLevel.SERIALIZABLE:
            if key not in self.snapshot:
                raise ValueError(f"Phantom read detected for key '{key}'.")

        elif isolation_level == IsolationLevel.REPEATABLE_READ:
            if key in self.read_set and key not in self.snapshot:
                raise ValueError(f"Non-repeatable read detected for key '{key}'.")

        self.read_set.add(key)

    def before_write(self, key: str) -> None:
        """Vérifie la validité d'une opération d'écriture.

        Cette méthode détecte les conflits d'écriture en comparant les versions des clés.

        Args:
            key (str): La clé à écrire dans la transaction.

        Raises:
            ValueError: Si un conflit d'écriture est détecté.
        """
        current_version = self.transaction.graph.get_version(key)

        if key in self.write_set:
            if current_version != self.version_numbers.get(key):
                raise ValueError(f"Write conflict detected for key '{key}'.")

        self.write_set.add(key)
        self.version_numbers[key] = current_version
