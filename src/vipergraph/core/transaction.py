import threading
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple
from uuid import uuid4
from changelog import ChangeLog
from isolation_level import IsolationLevel
from transaction_isolation_guard import TransactionIsolationGuard

class Transaction:
    """Gère une transaction avec un support complet des propriétés ACID.

    Cette classe implémente la gestion des transactions, y compris le support de l'isolation
    des niveaux, le rollback, les snapshots et le commit avec vérification des conflits
    de sérialisation.

    Attributes:
        graph (GraphDB): Référence à la base de données du graphe.
        id (str): Identifiant unique de la transaction.
        isolation_level (IsolationLevel): Niveau d'isolation de la transaction.
        operations (List[Tuple[str, Dict[str, Any]]]): Liste des opérations enregistrées.
        snapshots (Dict[str, Any]): Instantanés pour l'isolation REPEATABLE READ et SERIALIZABLE.
        locks (Set[str]): Ensemble des verrous acquis pendant la transaction.
        committed (bool): Indique si la transaction a été commitée.
        changelog (ChangeLog): Journal des modifications effectuées dans la transaction.
        start_time (datetime): Timestamp de début de la transaction.
    """

    def __init__(self, graph: 'GraphDB', isolation_level: 'IsolationLevel' = 'READ_COMMITTED'):
        """Initialise une transaction.

        Args:
            graph (GraphDB): Instance de la base de données du graphe.
            isolation_level (IsolationLevel): Niveau d'isolation de la transaction.
        """
        self.graph = graph
        self.id = str(uuid4())
        self.isolation_level = isolation_level
        self.operations: List[Tuple[str, Dict[str, Any]]] = []
        self.snapshots: Dict[str, Any] = {}
        self.locks: Set[str] = set()
        self.committed = False
        self.nested_transactions: List['Transaction'] = []
        self.changelog = ChangeLog()
        self._active = False
        self.isolation_guard = TransactionIsolationGuard(self)
        self.start_time = datetime.now()

    def __enter__(self) -> 'Transaction':
        """Démarre la transaction pour une utilisation avec le gestionnaire de contexte."""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Gère la sortie du gestionnaire de contexte avec commit ou rollback."""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def begin(self) -> None:
        """Démarre la transaction et prend un snapshot si nécessaire."""
        self._active = True
        if self.isolation_level in ('REPEATABLE_READ', 'SERIALIZABLE'):
            self._take_snapshot()
        self.graph.transaction_manager.register_transaction(self)

    def _take_snapshot(self) -> None:
        """Prend un snapshot complet pour les niveaux d'isolation élevés."""
        self.snapshots = {
            'nodes': {
                node_id: self._node_snapshot(node)
                for node_id, node in self.graph.nodes.items()
            },
            'timestamp': datetime.now()
        }
        self.isolation_guard.snapshot = self.snapshots

    def _node_snapshot(self, node: 'Node') -> Dict[str, Any]:
        """Crée un snapshot des propriétés d'un nœud.

        Args:
            node (Node): Le nœud à capturer.

        Returns:
            Dict[str, Any]: Un dictionnaire contenant les propriétés du nœud.
        """
        return {
            'properties': node._properties.copy(),
            'labels': node._labels.copy(),
            'version': node._version
        }

    def add_operation(self, operation_type: str, **params) -> None:
        """Ajoute une opération avec vérification d'isolation.

        Args:
            operation_type (str): Type de l'opération (ex: read, write).
            **params: Paramètres de l'opération.

        Raises:
            ValueError: Si la transaction n'est pas active.
        """
        if not self._active:
            raise ValueError("Transaction not active")

        if operation_type.startswith("read_"):
            self.isolation_guard.before_read(params.get("key"))
        elif operation_type.startswith("write_"):
            self.isolation_guard.before_write(params.get("key"))

        self.operations.append((operation_type, params))
        self.changelog.add_change(operation_type, params)

    def commit(self) -> None:
        """Commit la transaction après validation complète."""
        if not self._active:
            raise ValueError("Transaction not active")

        try:
            with self.graph.lock:
                if self.isolation_level == 'SERIALIZABLE':
                    self._check_serialization_conflicts()
                self._validate_schema_constraints()
                self._check_deadlocks()
                self._execute_operations()
                for nested_tx in self.nested_transactions:
                    if not nested_tx.committed:
                        nested_tx.commit()

                self.committed = True
                self.isolation_guard.commit_timestamp = datetime.now()
                self._release_locks()
                self._active = False
                print(f"Transaction {self.id} committed successfully")

        except Exception as e:
            self.rollback()
            raise e

    def rollback(self) -> None:
        """Annule la transaction et restaure l'état précédent."""
        if not self._active:
            return

        try:
            for nested_tx in self.nested_transactions:
                nested_tx.rollback()

            if self.isolation_level in ('REPEATABLE_READ', 'SERIALIZABLE'):
                self._restore_snapshot()

            self.operations = []
            self._release_locks()
            self._active = False
            print(f"Transaction {self.id} rolled back")

        except Exception as e:
            print(f"Error during rollback of transaction {self.id}: {e}")
            raise

    def _check_serialization_conflicts(self) -> None:
        """Vérifie les conflits de sérialisation."""
        snapshot_time = self.snapshots['timestamp']
        current_state = {
            'nodes': {
                node_id: self._node_snapshot(node)
                for node_id, node in self.graph.nodes.items()
            }
        }
        for node_id, current_node in current_state['nodes'].items():
            snapshot_node = self.snapshots['nodes'].get(node_id)
            if snapshot_node and current_node['version'] != snapshot_node['version']:
                raise ValueError(f"Serialization conflict detected on node {node_id}")

    def _execute_operations(self) -> None:
        """Exécute toutes les opérations enregistrées."""
        for op_type, params in self.operations:
            if op_type == "create_node":
                self.graph._create_node_internal(**params)
            elif op_type == "create_edge":
                self.graph._create_edge_internal(**params)
            elif op_type == "update_node":
                self.graph._update_node_internal(**params)
            elif op_type == "delete_node":
                self.graph._delete_node_internal(**params)

    def _release_locks(self) -> None:
        """Libère tous les verrous acquis pendant la transaction."""
        for lock_id in self.locks:
            self.graph.transaction_manager.release_lock(lock_id, self.id)
        self.locks.clear()
