import itertools
from pathlib import Path
import random
from typing import Dict, Set, List, Any, Optional, Callable, Iterator, Tuple, Union, Type
from enum import Enum, auto
from dataclasses import dataclass
from collections import defaultdict, deque
import struct
import mmap
import zlib
import json
import time
import weakref
import logging
import threading
from datetime import datetime, timedelta
from uuid import uuid4
import re
import os
import copy
import statistics
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.logging import RichHandler
from gnnmng import *

# Configuration du logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

console = Console()

# Enums de base
class TransactionType(Enum):
    READ = auto()
    WRITE = auto()

class IsolationLevel(Enum):
    READ_UNCOMMITTED = auto()
    READ_COMMITTED = auto()
    REPEATABLE_READ = auto()
    SERIALIZABLE = auto()

class PropertyType(Enum):
    """Types de propriétés supportés avec leur type Python correspondant"""
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

class IndexType(Enum):
    BTREE = "btree"
    HASH = "hash"
    FULLTEXT = "fulltext"
    SPATIAL = "spatial"
    COMPOSITE = "composite"

class CacheLevel:
    MEMORY = "memory"
    DISK = "disk"
    OFF_HEAP = "off_heap"

@dataclass
class PropertyDefinition:
    """Définition d'une propriété avec validation et contraintes"""
    def __init__(self, 
                 type: PropertyType, 
                 required: bool = False, 
                 default: Any = None, 
                 array_type: Optional[PropertyType] = None,
                 constraints: List[Callable[[Any], bool]] = None):
        self.type = type
        self.required = required
        self.default = default
        self.array_type = array_type
        self.constraints = constraints or []

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []

class DeadlockDetector:
    """Détecteur de deadlocks amélioré"""
    def __init__(self):
        self.wait_for_graph: Dict[str, Set[str]] = defaultdict(set)
        self.lock = threading.Lock()

    def would_create_cycle(self, waiting_tx: str, holding_tx: str) -> bool:
        """Vérifie si une nouvelle attente créerait un cycle"""
        with self.lock:
            # Ajoute temporairement l'arête
            self.wait_for_graph[waiting_tx].add(holding_tx)
            has_cycle = self._detect_cycle()
            # Enlève l'arête temporaire
            self.wait_for_graph[waiting_tx].remove(holding_tx)
            return has_cycle

    def _detect_cycle(self) -> bool:
        """Détecte un cycle dans le graphe d'attente"""
        visited = set()
        path = set()

        def visit(tx: str) -> bool:
            if tx in path:
                return True
            if tx in visited:
                return False
            
            visited.add(tx)
            path.add(tx)
            
            for next_tx in self.wait_for_graph.get(tx, set()):
                if visit(next_tx):
                    return True
            
            path.remove(tx)
            return False

        return any(visit(tx) for tx in self.wait_for_graph)

class TransactionIsolationGuard:
    """Gestionnaire avancé d'isolation des transactions"""
    def __init__(self, transaction: 'Transaction'):
        self.transaction = transaction
        self.read_set = set()
        self.write_set = set()
        self.snapshot = None
        self.version_numbers = {}
        self.commit_timestamp = None

    def before_read(self, key: str):
        """Vérifie la validité de la lecture"""
        if self.transaction.isolation_level == IsolationLevel.SERIALIZABLE:
            if key not in self.snapshot:
                raise ValueError("Phantom read detected")
        elif self.transaction.isolation_level == IsolationLevel.REPEATABLE_READ:
            if key in self.read_set and key not in self.snapshot:
                raise ValueError("Non-repeatable read detected")
        self.read_set.add(key)

    def before_write(self, key: str):
        """Vérifie la validité de l'écriture"""
        if key in self.write_set:
            current_version = self.transaction.graph.get_version(key)
            if current_version != self.version_numbers.get(key):
                raise ValueError("Write conflict detected")
        self.write_set.add(key)
        self.version_numbers[key] = self.transaction.graph.get_version(key)

class ChangeLog:
    """Journal des modifications avec historique détaillé"""
    def __init__(self):
        self.changes: List[Dict[str, Any]] = []
        self.timestamp = datetime.now()
        self.transaction_id = str(uuid4())
        self._lock = threading.Lock()

    def add_change(self, operation: str, details: Dict[str, Any]):
        with self._lock:
            self.changes.append({
                "operation": operation,
                "details": details,
                "timestamp": datetime.now().isoformat(),
                "transaction_id": self.transaction_id
            })

    def get_changes_since(self, timestamp: datetime) -> List[Dict[str, Any]]:
        return [change for change in self.changes 
                if datetime.fromisoformat(change["timestamp"]) > timestamp]

class SchemaVersion:
    def __init__(self, version: int, changes: Dict[str, Any], timestamp: datetime):
        self.version = version
        self.changes = changes
        self.timestamp = timestamp
        self.next_version = None
        self.previous_version = None

class SchemaVersionManager:
    def __init__(self):
        self.versions: Dict[int, SchemaVersion] = {}
        self.current_version = None
        self.first_version = None

    def add_version(self, changes: Dict[str, Any]) -> int:
        """Ajoute une nouvelle version du schéma"""
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
        """Récupère une version spécifique du schéma"""
        return self.versions.get(version)

    def get_changes_between_versions(self, from_version: int, to_version: int) -> List[Dict[str, Any]]:
        """Récupère les changements entre deux versions"""
        changes = []
        current = self.versions.get(from_version)
        
        while current and current.version <= to_version:
            changes.append(current.changes)
            current = current.next_version
            
        return changes

    def can_upgrade_to_version(self, target_version: int) -> bool:
        """Vérifie si une mise à niveau vers une version est possible"""
        if target_version not in self.versions:
            return False
            
        current = self.first_version
        while current:
            if current.version == target_version:
                return True
            current = current.next_version
            
        return False
    
class SchemaRollbackManager:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.backup_schemas: Dict[int, Dict[str, Any]] = {}
        self.rollback_logs: List[Dict[str, Any]] = []

    def create_checkpoint(self) -> int:
        """Crée un point de sauvegarde du schéma actuel"""
        checkpoint_id = len(self.backup_schemas) + 1
        
        # Sauvegarde l'état actuel du schéma
        schema_backup = {
            "node_labels": copy.deepcopy(self.graph.schema.node_labels),
            "constraints": copy.deepcopy(self.graph.schema.constraints),
            "version": self.graph.schema.version
        }
        
        self.backup_schemas[checkpoint_id] = schema_backup
        return checkpoint_id

    def rollback_to_checkpoint(self, checkpoint_id: int) -> bool:
        """Restaure le schéma à un point de sauvegarde"""
        if checkpoint_id not in self.backup_schemas:
            return False
            
        backup = self.backup_schemas[checkpoint_id]
        
        try:
            # Sauvegarde l'état actuel pour le logging
            previous_state = {
                "version": self.graph.schema.version,
                "timestamp": datetime.now().isoformat()
            }
            
            # Restaure le schéma
            self.graph.schema.node_labels = copy.deepcopy(backup["node_labels"])
            self.graph.schema.constraints = copy.deepcopy(backup["constraints"])
            self.graph.schema.version = backup["version"]
            
            # Log le rollback
            self.rollback_logs.append({
                "checkpoint_id": checkpoint_id,
                "previous_state": previous_state,
                "timestamp": datetime.now().isoformat()
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False

    def get_rollback_history(self) -> List[Dict[str, Any]]:
        """Récupère l'historique des rollbacks"""
        return self.rollback_logs

class SchemaConstraint:
    """Contrainte de schéma avec validation"""
    def __init__(self, constraint_type: str, property_name: str, 
                 params: Dict[str, Any] = None):
        self.type = constraint_type
        self.property = property_name
        self.params = params or {}

    def validate(self, value: Any, graph: 'GraphDB') -> bool:
        if value is None:
            return True  # Les valeurs nulles sont autorisées sauf si required=True

        if self.type == "unique":
            search_props = {self.property: value}
            existing = graph.find_nodes(properties=search_props)
            return len(existing) == 0
        elif self.type == "required":
            return value is not None
        elif self.type == "regex":
            pattern = self.params.get("pattern")
            return bool(re.match(pattern, str(value))) if pattern else True
        elif self.type == "range":
            min_val = self.params.get("min")
            max_val = self.params.get("max")
            if min_val is not None and value < min_val:
                return False
            if max_val is not None and value > max_val:
                return False
            return True
        return True

class Node:
    """Nœud du graphe avec support des versions et verrouillage"""
    def __init__(self, node_id: str, labels: Set[str] = None, 
                 properties: Dict[str, Any] = None):
        self.node_id = node_id
        self._labels = labels or set()
        self._properties = properties or {}
        self.incoming_edges: Dict[str, Set['Edge']] = defaultdict(set)
        self.outgoing_edges: Dict[str, Set['Edge']] = defaultdict(set)
        self._deleted = False
        self._lock = threading.RLock()
        self._property_history = []
        self._label_history = []
        self._version = 0
        self._created_at = datetime.now().isoformat()
        self._updated_at = self._created_at

    @property
    def labels(self) -> Set[str]:
        return self._labels.copy()

    def add_label(self, label: str, transaction_id: str = None):
        """Ajoute un label avec historique"""
        with self._lock:
            if label not in self._labels:
                self._label_history.append({
                    "operation": "add",
                    "label": label,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "transaction_id": transaction_id
                })
                self._labels.add(label)
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def remove_label(self, label: str, transaction_id: str = None):
        """Supprime un label avec historique"""
        with self._lock:
            if label in self._labels:
                self._label_history.append({
                    "operation": "remove",
                    "label": label,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "transaction_id": transaction_id
                })
                self._labels.discard(label)
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def has_label(self, label: str) -> bool:
        """Vérifie si le nœud a un label"""
        return label in self._labels

    def set_property(self, key: str, value: Any, transaction_id: str = None):
        """Set une propriété avec historique"""
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
        """Get une propriété"""
        return self._properties.get(key, default)

    def remove_property(self, key: str, transaction_id: str = None):
        """Supprime une propriété avec historique"""
        with self._lock:
            if key in self._properties:
                old_value = self._properties[key]
                self._property_history.append({
                    "key": key,
                    "old_value": old_value,
                    "new_value": None,
                    "timestamp": datetime.now().isoformat(),
                    "version": self._version,
                    "operation": "remove",
                    "transaction_id": transaction_id
                })
                del self._properties[key]
                self._version += 1
                self._updated_at = datetime.now().isoformat()

    def get_details(self) -> Dict[str, Any]:
        """Retourne les détails complets du nœud"""
        return {
            "ID": self.node_id,
            "Labels": list(self._labels),
            "Properties": self._properties,
            "Relations": {
                "Incoming": {
                    edge_type: len(edges) 
                    for edge_type, edges in self.incoming_edges.items()
                },
                "Outgoing": {
                    edge_type: len(edges) 
                    for edge_type, edges in self.outgoing_edges.items()
                }
            },
            "Metadata": {
                "Created": self._created_at,
                "Updated": self._updated_at,
                "Version": self._version,
                "Deleted": self._deleted
            }
        }

    def __str__(self) -> str:
        return f"Node(id={self.node_id}, labels={self._labels}, properties={self._properties})"

class Edge:
    """Relation du graphe avec support des versions et référence faible"""
    def __init__(self, source: Node, target: Node, edge_type: str, 
                 properties: Dict[str, Any] = None):
        self.id = str(uuid4())
        self.source = weakref.ref(source)
        self.target = weakref.ref(target)
        self.edge_type = edge_type
        self._properties = properties or {}
        self._deleted = False
        self._lock = threading.RLock()
        self._property_history = []
        self._type_history = []
        self._version = 0
        self._created_at = datetime.now().isoformat()
        self._updated_at = self._created_at

    def update_type(self, new_type: str, transaction_id: str = None):
        """Met à jour le type de relation avec historique"""
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
        """Vérifie si la relation est toujours valide"""
        source = self.source()
        target = self.target()
        return (source is not None and target is not None and 
                not source._deleted and not target._deleted and 
                not self._deleted)

    def set_property(self, key: str, value: Any, transaction_id: str = None):
        """Set une propriété avec historique"""
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
        """Get une propriété"""
        return self._properties.get(key, default)

    def get_details(self) -> Dict[str, Any]:
        """Retourne les détails complets de la relation"""
        return {
            "ID": self.id,
            "Type": self.edge_type,
            "Source": self.source().node_id if self.source() else None,
            "Target": self.target().node_id if self.target() else None,
            "Properties": self._properties,
            "Metadata": {
                "Created": self._created_at,
                "Updated": self._updated_at,
                "Version": self._version,
                "Deleted": self._deleted
            }
        }

    def __str__(self) -> str:
        src = self.source()
        tgt = self.target()
        return f"Edge({src.node_id if src else 'None'}-[{self.edge_type}]->{tgt.node_id if tgt else 'None'})"
    
class Transaction:
    """Transaction avec support ACID complet"""
    def __init__(self, graph: 'GraphDB', 
                 isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        self.graph = graph
        self.id = str(uuid4())
        self.isolation_level = isolation_level
        self.operations: List[Tuple[str, Dict[str, Any]]] = []
        self.snapshots: Dict[str, Any] = {}
        self.locks: Set[str] = set()
        self.committed = False
        self.parent_transaction = None
        self.nested_transactions: List['Transaction'] = []
        self.changelog = ChangeLog()
        self._active = False
        self.isolation_guard = TransactionIsolationGuard(self)
        self.start_time = datetime.now()

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
            return False

    def begin(self):
        """Démarre la transaction avec isolation appropriée"""
        self._active = True
        if self.isolation_level in (IsolationLevel.REPEATABLE_READ, 
                                  IsolationLevel.SERIALIZABLE):
            self._take_snapshot()
        self.graph.transaction_manager.register_transaction(self)

    def _take_snapshot(self):
        """Prend un snapshot complet pour l'isolation"""
        self.snapshots = {
            'nodes': {
                node_id: self._node_snapshot(node)
                for node_id, node in self.graph.nodes.items()
            },
            'edge_types': self.graph.edge_types.copy(),
            'timestamp': datetime.now()
        }
        self.isolation_guard.snapshot = self.snapshots

    def _node_snapshot(self, node: Node) -> Dict[str, Any]:
        """Crée un snapshot d'un nœud"""
        return {
            'properties': node._properties.copy(),
            'labels': node._labels.copy(),
            'version': node._version
        }

    def add_operation(self, operation_type: str, **params):
        """Ajoute une opération avec vérification d'isolation"""
        if not self._active:
            raise ValueError("Transaction not active")
        
        # Vérifie l'isolation avant d'ajouter l'opération
        if operation_type.startswith("read_"):
            self.isolation_guard.before_read(params.get("key"))
        elif operation_type.startswith("write_"):
            self.isolation_guard.before_write(params.get("key"))
            
        self.operations.append((operation_type, params))
        self.changelog.add_change(operation_type, params)

    def commit(self):
        """Commit la transaction avec vérification complète"""
        if not self._active:
            raise ValueError("Transaction not active")

        try:
            with self.graph.lock:
                # Vérifie les contraintes d'isolation
                if self.isolation_level == IsolationLevel.SERIALIZABLE:
                    self._check_serialization_conflicts()

                # Vérifie les contraintes de schéma
                self._validate_schema_constraints()

                # Vérifie les deadlocks
                self._check_deadlocks()

                # Exécute les opérations
                for op_type, params in self.operations:
                    self._execute_operation(op_type, params)

                # Commit les transactions imbriquées
                for nested_tx in self.nested_transactions:
                    if not nested_tx.committed:
                        nested_tx.commit()

                self.committed = True
                self.isolation_guard.commit_timestamp = datetime.now()
                self._release_locks()
                self._active = False

                # Log le commit
                logger.info(f"Transaction {self.id} committed successfully")
                self.graph.transaction_manager.log_commit(self)

        except Exception as e:
            self.rollback()
            raise e

    def rollback(self):
        """Rollback la transaction et restaure l'état"""
        if not self._active:
            return

        try:
            # Rollback les transactions imbriquées
            for nested_tx in self.nested_transactions:
                nested_tx.rollback()

            # Restaure le snapshot si nécessaire
            if self.isolation_level in (IsolationLevel.REPEATABLE_READ, 
                                      IsolationLevel.SERIALIZABLE):
                self._restore_snapshot()

            self.operations = []
            self._release_locks()
            self._active = False

            # Log le rollback
            logger.info(f"Transaction {self.id} rolled back")
            self.graph.transaction_manager.log_rollback(self)

        except Exception as e:
            logger.error(f"Error during rollback of transaction {self.id}: {e}")
            raise

    def _check_serialization_conflicts(self):
        """Vérifie les conflits de sérialisation"""
        if not self.snapshots:
            return

        snapshot_time = self.snapshots['timestamp']
        current_state = {
            'nodes': {
                node_id: self._node_snapshot(node)
                for node_id, node in self.graph.nodes.items()
            }
        }

        # Vérifie les modifications depuis le snapshot
        for node_id, current_node in current_state['nodes'].items():
            if node_id in self.snapshots['nodes']:
                snapshot_node = self.snapshots['nodes'][node_id]
                if (current_node['version'] != snapshot_node['version'] and
                    node_id not in self.write_set):
                    raise ValueError(f"Serialization conflict detected on node {node_id}")

    def _validate_schema_constraints(self):
        """Valide les contraintes de schéma pour toutes les opérations"""
        for op_type, params in self.operations:
            if op_type == "create_node" or op_type == "update_node":
                labels = params.get("labels", set())
                properties = params.get("properties", {})
                for label in labels:
                    if not self.graph.schema.validate_node(label, properties, self.graph):
                        raise ValueError(f"Schema validation failed for label {label}")

    def _check_deadlocks(self):
        """Vérifie les deadlocks potentiels"""
        for lock_id in self.locks:
            if self.graph.transaction_manager.would_deadlock(self.id, lock_id):
                raise ValueError("Potential deadlock detected")

    def _execute_operation(self, op_type: str, params: Dict[str, Any]):
        """Exécute une opération spécifique"""
        if op_type == "create_node":
            self.graph._create_node_internal(**params)
        elif op_type == "create_edge":
            self.graph._create_edge_internal(**params)
        elif op_type == "update_node":
            self.graph._update_node_internal(**params)
        elif op_type == "delete_node":
            self.graph._delete_node_internal(**params)
        elif op_type == "merge_node":
            self.graph._merge_node_internal(**params)

    def _release_locks(self):
        """Libère tous les locks"""
        for lock_id in self.locks:
            self.graph.transaction_manager.release_lock(lock_id, self.id)
        self.locks.clear()

class Schema:
    def __init__(self):
        self.node_labels: Dict[str, Dict[str, PropertyDefinition]] = defaultdict(dict)
        self.edge_types: Dict[str, Dict[str, PropertyDefinition]] = defaultdict(dict)
        self.constraints: Dict[str, List[SchemaConstraint]] = defaultdict(list)
        self.indexes: Dict[str, Set[str]] = defaultdict(set)
        self.version = 0
        self.history: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def validate_node(self, label: str, properties: Dict[str, Any], graph: 'GraphDB') -> bool:
        """Valide un nœud par rapport au schéma"""
        if label not in self.node_labels:
            return True  # Si pas de définition, on accepte tout

        # Vérifie les définitions de propriétés
        for prop_name, prop_def in self.node_labels[label].items():
            if prop_def.required and prop_name not in properties:
                return False
            if prop_name in properties:
                value = properties[prop_name]
                if not self._validate_property(value, prop_def):
                    return False

        # Vérifie les contraintes
        for constraint in self.constraints[label]:
            value = properties.get(constraint.property)
            if not constraint.validate(value, graph):
                return False

        return True

    def _validate_property(self, value: Any, prop_def: PropertyDefinition) -> bool:
        """Valide une propriété par rapport à sa définition"""
        if value is None:
            return not prop_def.required

        # Validation spéciale pour les tableaux
        if prop_def.type == PropertyType.ARRAY:
            if not isinstance(value, (list, tuple)):
                return False
            if prop_def.array_type:
                return all(isinstance(item, prop_def.array_type.value) for item in value)
            return True

        # Validation du type de base
        try:
            return isinstance(value, prop_def.type.value)
        except Exception as e:
            logger.error(f"Error validating property: {e}")
            return False

    def add_node_definition(self, label: str, properties: Dict[str, PropertyDefinition]):
        """Ajoute une définition de nœud au schéma"""
        with self._lock:
            self.node_labels[label].update(properties)
            self._add_to_history("add_node_definition", {
                "label": label,
                "properties": {k: v.__dict__ for k, v in properties.items()}
            })

    def add_edge_definition(self, edge_type: str, properties: Dict[str, PropertyDefinition]):
        """Ajoute une définition de relation au schéma"""
        with self._lock:
            self.edge_types[edge_type].update(properties)
            self._add_to_history("add_edge_definition", {
                "edge_type": edge_type,
                "properties": {k: v.__dict__ for k, v in properties.items()}
            })

    def add_constraint(self, label: str, constraint: SchemaConstraint):
        """Ajoute une contrainte au schéma"""
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

    def add_index(self, label: str, property_name: str):
        """Ajoute un index au schéma"""
        with self._lock:
            self.indexes[label].add(property_name)

    def _add_to_history(self, operation: str, details: Dict[str, Any]):
        """Ajoute une modification à l'historique du schéma"""
        with self._lock:
            self.history.append({
                "operation": operation,
                "details": details,
                "version": self.version,
                "timestamp": datetime.now().isoformat()
            })
            self.version += 1

    def get_node_definition(self, label: str) -> Dict[str, PropertyDefinition]:
        """Récupère la définition d'un type de nœud"""
        return self.node_labels.get(label, {})

    def get_edge_definition(self, edge_type: str) -> Dict[str, PropertyDefinition]:
        """Récupère la définition d'un type de relation"""
        return self.edge_types.get(edge_type, {})

    def get_constraints(self, label: str) -> List[SchemaConstraint]:
        """Récupère les contraintes pour un label"""
        return self.constraints.get(label, [])

    def get_indexes(self, label: str) -> Set[str]:
        """Récupère les index pour un label"""
        return self.indexes.get(label, set())

class MultiLevelCache:
    """Cache multi-niveaux avec différentes stratégies de stockage"""
    def __init__(self):
        self.memory_cache = LRUCache(1000)  # Premier niveau (plus rapide)
        self.off_heap_cache = OffHeapCache(10000)  # Deuxième niveau
        self.disk_cache = DiskCache("cache_dir", 100000)  # Troisième niveau
        self.statistics = defaultdict(int)

    def get(self, key: str, level: CacheLevel = CacheLevel.MEMORY) -> Optional[Any]:
        """Récupère une valeur du cache avec stratégie en cascade"""
        self.statistics["gets"] += 1
        
        # Essaie d'abord le cache mémoire
        value = self.memory_cache.get(key)
        if value is not None:
            self.statistics["memory_hits"] += 1
            return value

        # Puis le cache off-heap
        if level != CacheLevel.MEMORY:
            value = self.off_heap_cache.get(key)
            if value is not None:
                self.statistics["off_heap_hits"] += 1
                self.memory_cache.put(key, value)
                return value

        # Enfin le cache disque
        if level == CacheLevel.DISK:
            value = self.disk_cache.get(key)
            if value is not None:
                self.statistics["disk_hits"] += 1
                self.off_heap_cache.put(key, value)
                return value

        self.statistics["misses"] += 1
        return None

    def put(self, key: str, value: Any, level: CacheLevel = CacheLevel.MEMORY):
        """Stocke une valeur dans le cache avec stratégie de niveau"""
        self.statistics["puts"] += 1
        
        if level == CacheLevel.MEMORY:
            self.memory_cache.put(key, value)
        elif level == CacheLevel.OFF_HEAP:
            self.off_heap_cache.put(key, value)
            self.memory_cache.put(key, value)
        else:
            self.disk_cache.put(key, value)
            self.off_heap_cache.put(key, value)
            self.memory_cache.put(key, value)

    def clear(self):
        """Vide tous les niveaux de cache"""
        self.memory_cache = LRUCache(1000)
        self.off_heap_cache = OffHeapCache(10000)
        self.disk_cache.clear()
        self.statistics = defaultdict(int)

class LRUCache:
    """Cache LRU simple en mémoire"""
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache: Dict[str, Any] = {}
        self.usage: Dict[str, datetime] = {}
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Récupère une valeur du cache"""
        with self.lock:
            if key in self.cache:
                self.usage[key] = datetime.now()
                return self.cache[key]
            return None

    def put(self, key: str, value: Any):
        """Ajoute une valeur dans le cache"""
        with self.lock:
            if len(self.cache) >= self.capacity:
                oldest_key = min(self.usage.items(), key=lambda x: x[1])[0]
                del self.cache[oldest_key]
                del self.usage[oldest_key]
            
            self.cache[key] = value
            self.usage[key] = datetime.now()

class OffHeapCache:
    """Cache stocké hors du tas Java/Python"""
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._data = {}  # Dans une implémentation réelle, utiliserait la mémoire système
        self._access_times = {}
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            value = self._data.get(key)
            if value is not None:
                self._access_times[key] = datetime.now()
            return value

    def put(self, key: str, value: Any):
        with self.lock:
            if len(self._data) >= self.capacity:
                oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
                del self._data[oldest_key]
                del self._access_times[oldest_key]
            
            self._data[key] = value
            self._access_times[key] = datetime.now()

class DiskCache:
    """Cache persistant sur disque utilisant JSON"""
    def __init__(self, cache_dir: str, capacity: int):
        self.cache_dir = cache_dir
        self.capacity = capacity
        self.lock = threading.Lock()
        os.makedirs(cache_dir, exist_ok=True)
        self._index_file = os.path.join(cache_dir, "index.json")
        self._load_index()

    def _load_index(self):
        """Charge l'index depuis le fichier"""
        with self.lock:
            try:
                if os.path.exists(self._index_file):
                    with open(self._index_file, 'r', encoding='utf-8') as f:
                        self._index = json.load(f)
                else:
                    self._index = {}
            except json.JSONDecodeError:
                self._index = {}
                logger.error("Erreur de chargement de l'index, création d'un nouvel index")

    def _save_index(self):
        """Sauvegarde l'index sur le disque"""
        with self.lock:
            try:
                with open(self._index_file, 'w', encoding='utf-8') as f:
                    json.dump(self._index, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde de l'index: {e}")

    def get(self, key: str) -> Optional[Any]:
        """Récupère une valeur du cache disque"""
        with self.lock:
            file_path = self._index.get(key)
            if file_path and os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except json.JSONDecodeError:
                    self._remove(key)
                    return None
            return None

    def put(self, key: str, value: Any):
        """Stocke une valeur dans le cache disque"""
        with self.lock:
            if len(self._index) >= self.capacity:
                oldest_key = next(iter(self._index))
                self._remove(oldest_key)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            file_name = f"{key}_{timestamp}.json"
            file_path = os.path.join(self.cache_dir, file_name)

            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(value, f, ensure_ascii=False, indent=2)
                
                self._index[key] = file_path
                self._save_index()
            except Exception as e:
                logger.error(f"Erreur lors du stockage de la valeur pour {key}: {e}")
                if os.path.exists(file_path):
                    os.remove(file_path)

    def clear(self):
        """Vide le cache"""
        with self.lock:
            for key in list(self._index.keys()):
                self._remove(key)
            if os.path.exists(self._index_file):
                try:
                    os.remove(self._index_file)
                except OSError as e:
                    logger.error(f"Erreur lors de la suppression de l'index: {e}")

    def _remove(self, key: str):
        """Supprime une entrée du cache"""
        with self.lock:
            file_path = self._index.pop(key, None)
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    self._save_index()
                except OSError as e:
                    logger.error(f"Erreur lors de la suppression du fichier {file_path}: {e}")

class CacheSerializer:
    """Sérialiseur personnalisé pour objets complexes"""
    @staticmethod
    def serialize(obj: Any) -> Dict[str, Any]:
        """Sérialise un objet en dictionnaire JSON"""
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        elif hasattr(obj, "__dict__"):
            return {
                "__type__": obj.__class__.__name__,
                "value": {k: CacheSerializer.serialize(v) for k, v in obj.__dict__.items()}
            }
        elif isinstance(obj, (list, tuple)):
            return [CacheSerializer.serialize(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: CacheSerializer.serialize(v) for k, v in obj.items()}
        elif isinstance(obj, set):
            return {"__type__": "set", "value": list(obj)}
        return obj

    @staticmethod
    def deserialize(data: Any) -> Any:
        """Désérialise un dictionnaire JSON en objet"""
        if isinstance(data, dict) and "__type__" in data:
            type_name = data["__type__"]
            if type_name == "datetime":
                return datetime.fromisoformat(data["value"])
            elif type_name == "set":
                return set(data["value"])
            else:
                # Pour les objets personnalisés
                obj = type(type_name, (), {})()
                for k, v in data["value"].items():
                    setattr(obj, k, CacheSerializer.deserialize(v))
                return obj
        elif isinstance(data, list):
            return [CacheSerializer.deserialize(x) for x in data]
        elif isinstance(data, dict):
            return {k: CacheSerializer.deserialize(v) for k, v in data.items()}
        return data

class SmartCache(MultiLevelCache):
    """Version améliorée du cache multi-niveaux avec sérialisation intelligente"""
    def __init__(self):
        super().__init__()
        self.serializer = CacheSerializer()
        self.metadata = defaultdict(dict)

    def put(self, key: str, value: Any, level: CacheLevel = CacheLevel.MEMORY, 
            metadata: Dict[str, Any] = None):
        """Stocke une valeur avec métadonnées"""
        serialized_value = self.serializer.serialize(value)
        super().put(key, serialized_value, level)
        
        if metadata:
            self.metadata[key].update(metadata)
            self.metadata[key]["last_updated"] = datetime.now().isoformat()

    def get(self, key: str, level: CacheLevel = CacheLevel.MEMORY) -> Optional[Any]:
        """Récupère une valeur avec désérialisation"""
        value = super().get(key, level)
        if value is not None:
            return self.serializer.deserialize(value)
        return None

    def get_with_metadata(self, key: str, 
                         level: CacheLevel = CacheLevel.MEMORY) -> Tuple[Optional[Any], Dict[str, Any]]:
        """Récupère une valeur et ses métadonnées"""
        value = self.get(key, level)
        return value, self.metadata.get(key, {})

    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques détaillées du cache"""
        return {
            "memory": {
                "size": len(self.memory_cache.cache),
                "capacity": self.memory_cache.capacity
            },
            "off_heap": {
                "size": len(self.off_heap_cache._data),
                "capacity": self.off_heap_cache.capacity
            },
            "disk": self.disk_cache.get_stats() if hasattr(self.disk_cache, 'get_stats') else {},
            "metadata": {
                "entries": len(self.metadata),
                "keys": list(self.metadata.keys())
            },
            "hits": {
                "memory": self.statistics["memory_hits"],
                "off_heap": self.statistics["off_heap_hits"],
                "disk": self.statistics["disk_hits"],
                "misses": self.statistics["misses"]
            }
        }

# Pattern Matcher Intelligent
class PatternMatcher:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.patterns = {}
        self.pattern_frequencies = defaultdict(int)
        self.min_pattern_size = 2
        self.max_pattern_size = 10
        self._cache = {}

    def learn_patterns(self) -> Dict[str, Any]:
        """Apprend automatiquement les motifs récurrents dans le graphe"""
        patterns_found = {}
        
        # Explore différentes tailles de motifs
        for size in range(self.min_pattern_size, self.max_pattern_size + 1):
            subgraphs = self._find_subgraphs(size)
            
            # Compte la fréquence des motifs
            for subgraph in subgraphs:
                pattern_key = self._get_pattern_signature(subgraph)
                self.pattern_frequencies[pattern_key] += 1
                
                if self.pattern_frequencies[pattern_key] > 1:
                    patterns_found[pattern_key] = {
                        'frequency': self.pattern_frequencies[pattern_key],
                        'size': size,
                        'nodes': len(subgraph['nodes']),
                        'edges': len(subgraph['edges']),
                        'example': subgraph
                    }
        
        self.patterns = patterns_found
        return patterns_found

    def find_similar_patterns(self, seed_pattern: Dict[str, Any], 
                            similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Trouve des motifs similaires à un motif donné"""
        similar_patterns = []
        seed_signature = self._get_pattern_signature(seed_pattern)
        
        for pattern_key, pattern_info in self.patterns.items():
            similarity = self._calculate_pattern_similarity(
                seed_signature,
                pattern_key
            )
            
            if similarity >= similarity_threshold:
                similar_patterns.append({
                    'pattern': pattern_info['example'],
                    'similarity': similarity,
                    'frequency': pattern_info['frequency']
                })
        
        return sorted(similar_patterns, key=lambda x: x['similarity'], reverse=True)

    def _find_subgraphs(self, size: int) -> List[Dict[str, Any]]:
        """Trouve tous les sous-graphes de taille donnée"""
        subgraphs = []
        visited = set()
        
        for node_id in self.graph.nodes:
            if node_id not in visited:
                subgraph = self._extract_subgraph(node_id, size)
                if subgraph and len(subgraph['nodes']) == size:
                    subgraphs.append(subgraph)
                    visited.update(subgraph['nodes'])
                    
        return subgraphs

    def _extract_subgraph(self, start_node: str, size: int) -> Dict[str, Any]:
        """Extrait un sous-graphe à partir d'un nœud de départ"""
        subgraph = {'nodes': set(), 'edges': []}
        queue = deque([(start_node, 0)])
        
        while queue and len(subgraph['nodes']) < size:
            node_id, depth = queue.popleft()
            if node_id in subgraph['nodes']:
                continue
                
            subgraph['nodes'].add(node_id)
            node = self.graph.nodes[node_id]
            
            # Ajoute les relations sortantes
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target_id = edge.target().node_id
                        if len(subgraph['nodes']) < size:
                            queue.append((target_id, depth + 1))
                        subgraph['edges'].append({
                            'source': node_id,
                            'target': target_id,
                            'type': edge.edge_type
                        })
                        
        return subgraph if subgraph['nodes'] else None

    def _get_pattern_signature(self, pattern: Dict[str, Any]) -> str:
        """Génère une signature unique pour un motif"""
        # Crée une représentation canonique du motif
        edge_types = sorted(set(edge['type'] for edge in pattern['edges']))
        node_degrees = sorted(
            len([e for e in pattern['edges'] 
                 if e['source'] == node or e['target'] == node])
            for node in pattern['nodes']
        )
        
        return f"E{len(edge_types)}:{'|'.join(edge_types)}_N{len(node_degrees)}:{'|'.join(map(str, node_degrees))}"

    def _calculate_pattern_similarity(self, pattern1: str, pattern2: str) -> float:
        """Calcule la similarité entre deux motifs"""
        # Utilise la distance de Levenshtein normalisée
        max_len = max(len(pattern1), len(pattern2))
        if max_len == 0:
            return 1.0
            
        distance = self._levenshtein_distance(pattern1, pattern2)
        return 1 - (distance / max_len)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calcule la distance de Levenshtein entre deux chaînes"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
            
        return previous_row[-1]

# Gestion Temporelle Avancée
class TemporalManager:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.temporal_snapshots = {}
        self.evolution_models = {}
        self.prediction_window = timedelta(days=30)
        self._cache = {}

    def _get_node_history(self, node: 'Node') -> Dict[str, Any]:
        """Récupère l'historique complet d'un nœud"""
        history = {
            'properties': defaultdict(list),
            'labels': [],
            'connections': [],
            'timestamps': set()
        }

        # Historique des propriétés
        for change in node._property_history:
            history['properties'][change['key']].append({
                'old_value': change['old_value'],
                'new_value': change['new_value'],
                'timestamp': change['timestamp']
            })
            history['timestamps'].add(change['timestamp'])

        # Historique des labels
        for change in node._label_history:
            history['labels'].append({
                'operation': change['operation'],
                'label': change['label'],
                'timestamp': change['timestamp']
            })
            history['timestamps'].add(change['timestamp'])

        # Historique des connexions
        for edges in node.outgoing_edges.values():
            for edge in edges:
                if edge.is_valid():
                    history['connections'].append({
                        'type': edge.edge_type,
                        'target': edge.target().node_id,
                        'timestamp': edge._created_at
                    })
                    history['timestamps'].add(edge._created_at)

        # Trie les timestamps
        history['timestamps'] = sorted(list(history['timestamps']))

        return history

    def _extrapolate_value(self, values: List[Dict[str, Any]], window: timedelta) -> Any:
        """Extrapole une valeur future basée sur l'historique"""
        if not values:
            return None

        if len(values) == 1:
            return values[0]['new_value']

        # Analyse la tendance pour les valeurs numériques
        if all(isinstance(v['new_value'], (int, float)) for v in values):
            changes = []
            for i in range(1, len(values)):
                prev_val = float(values[i-1]['new_value'])
                curr_val = float(values[i]['new_value'])
                prev_time = datetime.fromisoformat(values[i-1]['timestamp'])
                curr_time = datetime.fromisoformat(values[i]['timestamp'])
                time_diff = (curr_time - prev_time).total_seconds()
                if time_diff > 0:
                    change_rate = (curr_val - prev_val) / time_diff
                    changes.append(change_rate)

            if changes:
                avg_change = sum(changes) / len(changes)
                last_value = float(values[-1]['new_value'])
                future_change = avg_change * window.total_seconds()
                return last_value + future_change

        # Pour les valeurs non numériques, retourne la dernière valeur
        return values[-1]['new_value']

    def _predict_new_connections(self, node: 'Node', 
                               predicted_count: int) -> List[Dict[str, Any]]:
        """Prédit de nouvelles connexions potentielles"""
        predictions = []
        
        # Analyse des motifs de connexion existants
        connection_types = defaultdict(int)
        for edges in node.outgoing_edges.values():
            for edge in edges:
                if edge.is_valid():
                    connection_types[edge.edge_type] += 1

        # Trouve les types de connexions les plus probables
        if connection_types:
            total_connections = sum(connection_types.values())
            for edge_type, count in connection_types.items():
                probability = count / total_connections
                predicted_edges = int(predicted_count * probability)
                
                if predicted_edges > 0:
                    predictions.append({
                        'type': edge_type,
                        'count': predicted_edges,
                        'probability': probability
                    })

        return predictions
    
    def _analyze_edge_patterns(self) -> List[Dict[str, Any]]:
        """Analyse les motifs dans la création des relations"""
        patterns = []
        edge_counts = defaultdict(int)
        edge_timing = defaultdict(list)
        type_combinations = defaultdict(int)

        # Analyse des relations existantes
        for node in self.graph.nodes.values():
            node_edge_types = set()
            
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        edge_type = edge.edge_type
                        edge_counts[edge_type] += 1
                        edge_timing[edge_type].append(
                            datetime.fromisoformat(edge._created_at)
                        )
                        node_edge_types.add(edge_type)

            # Analyse des combinaisons de types de relations
            if len(node_edge_types) >= 2:
                for type1, type2 in itertools.combinations(node_edge_types, 2):
                    type_combinations[(type1, type2)] += 1

        # Détection des motifs temporels
        for edge_type, timestamps in edge_timing.items():
            if len(timestamps) >= 2:
                # Calcul des intervalles entre créations
                intervals = []
                sorted_timestamps = sorted(timestamps)
                for i in range(1, len(sorted_timestamps)):
                    interval = (sorted_timestamps[i] - sorted_timestamps[i-1]).total_seconds()
                    intervals.append(interval)

                # Calcul des statistiques
                avg_interval = sum(intervals) / len(intervals)
                frequency = edge_counts[edge_type] / len(self.graph.nodes)

                pattern = {
                    'type': edge_type,
                    'frequency': frequency,
                    'avg_interval': avg_interval,
                    'total_occurrences': edge_counts[edge_type],
                    'temporal_stats': {
                        'min_interval': min(intervals) if intervals else 0,
                        'max_interval': max(intervals) if intervals else 0,
                        'std_dev': statistics.stdev(intervals) if len(intervals) > 1 else 0
                    }
                }

                # Analyse des motifs répétitifs
                pattern['recurring_patterns'] = self._detect_recurring_intervals(intervals)
                patterns.append(pattern)

        # Ajout des motifs de co-occurrence
        for (type1, type2), count in type_combinations.items():
            if count > 1:
                patterns.append({
                    'type': 'co_occurrence',
                    'edge_types': [type1, type2],
                    'frequency': count / len(self.graph.nodes),
                    'count': count
                })

        return sorted(patterns, key=lambda x: x['frequency'], reverse=True)

    def _detect_recurring_intervals(self, intervals: List[float]) -> List[Dict[str, Any]]:
        """Détecte les intervalles qui se répètent régulièrement"""
        recurring = []
        if len(intervals) < 3:
            return recurring

        # Conversion en heures pour une meilleure lisibilité
        intervals_hours = [i/3600 for i in intervals]
        
        # Détection des intervalles similaires
        clusters = defaultdict(list)
        for interval in intervals_hours:
            # Regroupe les intervalles similaires (±10%)
            matched = False
            for center in clusters.keys():
                if abs(interval - center) / center <= 0.1:  # 10% de tolérance
                    clusters[center].append(interval)
                    matched = True
                    break
            if not matched:
                clusters[interval].append(interval)

        # Analyse des clusters significatifs
        for center, values in clusters.items():
            if len(values) >= 2:  # Au moins 2 occurrences
                recurring.append({
                    'interval_hours': round(center, 2),
                    'occurrences': len(values),
                    'variance': statistics.variance(values) if len(values) > 1 else 0
                })

        return sorted(recurring, key=lambda x: x['occurrences'], reverse=True)

    def _generate_predicted_edges(self, pattern: Dict[str, Any], 
                                window: timedelta) -> List[Dict[str, Any]]:
        """Génère des prédictions de relations basées sur un motif"""
        predictions = []
        
        if 'type' not in pattern:
            return predictions

        # Pour les motifs de type simple
        if pattern['type'] != 'co_occurrence':
            # Calcule le nombre attendu de nouvelles relations
            rate = pattern['frequency']
            expected_new = rate * window.total_seconds() / (24 * 3600)  # Convertit en jours
            
            if expected_new >= 0.5:  # Seuil minimum pour prédire
                predictions.append({
                    'type': pattern['type'],
                    'expected_count': round(expected_new),
                    'confidence': min(1.0, pattern['frequency']),
                    'time_frame': str(window)
                })

        # Pour les motifs de co-occurrence
        else:
            type1, type2 = pattern['edge_types']
            if pattern['frequency'] > 0.3:  # Seuil significatif
                predictions.append({
                    'types': [type1, type2],
                    'expected_count': round(pattern['frequency'] * 2),  # Estimation simple
                    'confidence': pattern['frequency'],
                    'type': 'co_occurrence'
                })

        return predictions
    
    def _calculate_growth_acceleration(self) -> Dict[str, float]:
        """Calcule l'accélération de la croissance du graphe"""
        acceleration = {
            'nodes': 0.0,
            'edges': 0.0,
            'overall': 0.0,
            'periods': []
        }

        # Récupère les snapshots triés par date
        snapshots = sorted(
            self.temporal_snapshots.values(),
            key=lambda x: datetime.fromisoformat(x['timestamp'])
        )

        if len(snapshots) < 3:  # Besoin d'au moins 3 points pour calculer l'accélération
            return acceleration

        # Calcul des taux de croissance pour chaque période
        growth_rates = []
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            prev_time = datetime.fromisoformat(prev['timestamp'])
            curr_time = datetime.fromisoformat(curr['timestamp'])
            time_diff = (curr_time - prev_time).total_seconds()
            
            if time_diff == 0:
                continue

            # Calcul du taux de croissance pour cette période
            node_growth = (len(curr['nodes']) - len(prev['nodes'])) / time_diff
            edge_growth = (len(curr['edges']) - len(prev['edges'])) / time_diff
            
            growth_rates.append({
                'period_start': prev['timestamp'],
                'period_end': curr['timestamp'],
                'node_growth_rate': node_growth,
                'edge_growth_rate': edge_growth,
                'overall_growth_rate': (node_growth + edge_growth) / 2
            })

        # Calcul de l'accélération (changement dans les taux de croissance)
        if len(growth_rates) >= 2:
            for i in range(1, len(growth_rates)):
                prev_rate = growth_rates[i-1]
                curr_rate = growth_rates[i]
                
                time_diff = (datetime.fromisoformat(curr_rate['period_end']) - 
                           datetime.fromisoformat(prev_rate['period_end'])).total_seconds()
                
                if time_diff > 0:
                    # Accélération des nœuds
                    node_acceleration = ((curr_rate['node_growth_rate'] - 
                                       prev_rate['node_growth_rate']) / time_diff)
                    
                    # Accélération des relations
                    edge_acceleration = ((curr_rate['edge_growth_rate'] - 
                                       prev_rate['edge_growth_rate']) / time_diff)
                    
                    period_acceleration = {
                        'period_start': prev_rate['period_end'],
                        'period_end': curr_rate['period_end'],
                        'node_acceleration': node_acceleration,
                        'edge_acceleration': edge_acceleration,
                        'overall_acceleration': (node_acceleration + edge_acceleration) / 2
                    }
                    
                    acceleration['periods'].append(period_acceleration)
                    
                    # Met à jour les accélérations moyennes
                    acceleration['nodes'] += node_acceleration
                    acceleration['edges'] += edge_acceleration
                    acceleration['overall'] += period_acceleration['overall_acceleration']

            # Calcule les moyennes
            period_count = len(acceleration['periods'])
            if period_count > 0:
                acceleration['nodes'] /= period_count
                acceleration['edges'] /= period_count
                acceleration['overall'] /= period_count

        # Ajoute des métadonnées supplémentaires
        acceleration['metadata'] = {
            'analysis_timestamp': datetime.now().isoformat(),
            'number_of_periods': len(acceleration['periods']),
            'total_timespan': (
                datetime.fromisoformat(snapshots[-1]['timestamp']) -
                datetime.fromisoformat(snapshots[0]['timestamp'])
            ).total_seconds() if snapshots else 0
        }

        return acceleration

    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyse les motifs temporels dans l'évolution du graphe"""
        analysis = {
            'growth_patterns': {},
            'seasonal_patterns': {},
            'stability_metrics': {},
            'change_points': []
        }
        
        # Analyse la croissance et l'accélération
        growth_rates = self._calculate_growth_rates()
        acceleration = self._calculate_growth_acceleration()
        
        analysis['growth_patterns'] = {
            'overall_rate': growth_rates['overall'],
            'node_growth': growth_rates['nodes'],
            'edge_growth': growth_rates['edges'],
            'acceleration': acceleration
        }
        
        # Détecte les motifs saisonniers
        seasonal_patterns = self._detect_seasonal_patterns()
        analysis['seasonal_patterns'] = seasonal_patterns
        
        # Calcule la stabilité
        stability = self._calculate_stability_metrics()
        analysis['stability_metrics'] = stability
        
        # Ajoute des métriques d'évolution avancées
        analysis.update({
            'evolution_metrics': {
                'growth_acceleration': acceleration['overall'],
                'growth_consistency': self._calculate_growth_consistency(),
                'change_frequency': self._calculate_change_frequency(),
            },
            'metadata': {
                'analysis_timestamp': datetime.now().isoformat(),
                'number_of_snapshots': len(self.temporal_snapshots),
                'covered_timespan': self._calculate_covered_timespan()
            }
        })
        
        return analysis

    def _calculate_growth_consistency(self) -> float:
        """Calcule la cohérence de la croissance"""
        growth_rates = self._calculate_growth_rates()
        if not growth_rates.get('nodes', {}) or not growth_rates.get('edges', {}):
            return 0.0
            
        # Calcule la variance des taux de croissance
        node_rates = [rate for rate in growth_rates['nodes'].values() if isinstance(rate, (int, float))]
        edge_rates = [rate for rate in growth_rates['edges'].values() if isinstance(rate, (int, float))]
        
        if not node_rates or not edge_rates:
            return 0.0
            
        try:
            node_variance = statistics.variance(node_rates) if len(node_rates) > 1 else 0
            edge_variance = statistics.variance(edge_rates) if len(edge_rates) > 1 else 0
            
            # Normalise la cohérence (1 = très cohérent, 0 = incohérent)
            max_variance = max(node_variance, edge_variance)
            if max_variance == 0:
                return 1.0
                
            return 1.0 / (1.0 + max_variance)
            
        except statistics.StatisticsError:
            return 0.0

    def _calculate_change_frequency(self) -> Dict[str, float]:
        """Calcule la fréquence des changements"""
        frequencies = {
            'nodes': 0.0,
            'edges': 0.0,
            'properties': 0.0
        }
        
        total_time = self._calculate_covered_timespan()
        if total_time.total_seconds() == 0:
            return frequencies
            
        # Compte les changements
        for node in self.graph.nodes.values():
            frequencies['properties'] += len(node._property_history)
            
        # Normalise par le temps total
        total_seconds = total_time.total_seconds()
        for key in frequencies:
            frequencies[key] = frequencies[key] / total_seconds
            
        return frequencies

    def _calculate_covered_timespan(self) -> timedelta:
        """Calcule la période totale couverte par les snapshots"""
        if not self.temporal_snapshots:
            return timedelta()
            
        timestamps = [
            datetime.fromisoformat(snapshot['timestamp'])
            for snapshot in self.temporal_snapshots.values()
        ]
        
        return max(timestamps) - min(timestamps)
    
    def _detect_change_points(self) -> List[Dict[str, Any]]:
        """Détecte les points de changement significatifs dans l'évolution du graphe"""
        change_points = []
        
        # Récupère les snapshots triés par date
        snapshots = sorted(
            self.temporal_snapshots.values(),
            key=lambda x: datetime.fromisoformat(x['timestamp'])
        )

        if len(snapshots) < 2:
            return change_points

        # Analyse les changements entre snapshots consécutifs
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            # Calcule les métriques de changement
            changes = {
                'timestamp': curr['timestamp'],
                'node_changes': {
                    'added': set(curr['nodes'].keys()) - set(prev['nodes'].keys()),
                    'removed': set(prev['nodes'].keys()) - set(curr['nodes'].keys()),
                    'modified': set()
                },
                'edge_changes': {
                    'added': [],
                    'removed': []
                },
                'property_changes': defaultdict(list),
                'significance': 0.0
            }

            # Détecte les modifications de nœuds
            for node_id in set(prev['nodes'].keys()) & set(curr['nodes'].keys()):
                prev_node = prev['nodes'][node_id]
                curr_node = curr['nodes'][node_id]
                
                # Vérifie les changements de propriétés
                prop_changes = []
                for prop, value in curr_node['properties'].items():
                    if prop not in prev_node['properties'] or prev_node['properties'][prop] != value:
                        prop_changes.append({
                            'property': prop,
                            'old_value': prev_node['properties'].get(prop),
                            'new_value': value
                        })
                
                if prop_changes:
                    changes['node_changes']['modified'].add(node_id)
                    changes['property_changes'][node_id].extend(prop_changes)

            # Détecte les changements de relations
            prev_edges = {(e['source'], e['target'], e['type']) for e in prev['edges']}
            curr_edges = {(e['source'], e['target'], e['type']) for e in curr['edges']}
            
            changes['edge_changes']['added'] = list(curr_edges - prev_edges)
            changes['edge_changes']['removed'] = list(prev_edges - curr_edges)

            # Calcule la significativité du changement
            total_changes = (
                len(changes['node_changes']['added']) +
                len(changes['node_changes']['removed']) +
                len(changes['node_changes']['modified']) +
                len(changes['edge_changes']['added']) +
                len(changes['edge_changes']['removed'])
            )
            
            total_elements = (
                len(prev['nodes']) +
                len(prev['edges'])
            )
            
            if total_elements > 0:
                changes['significance'] = total_changes / total_elements

            # Ajoute des métadonnées supplémentaires
            changes['metadata'] = {
                'time_since_last_change': (
                    datetime.fromisoformat(curr['timestamp']) -
                    datetime.fromisoformat(prev['timestamp'])
                ).total_seconds(),
                'total_changes': total_changes,
                'affected_elements': total_elements
            }

            # Ne conserve que les changements significatifs
            if changes['significance'] > 0.1:  # Seuil arbitraire de 10%
                # Convertit les sets en listes pour la sérialisation JSON
                changes['node_changes']['added'] = list(changes['node_changes']['added'])
                changes['node_changes']['removed'] = list(changes['node_changes']['removed'])
                changes['node_changes']['modified'] = list(changes['node_changes']['modified'])
                
                change_points.append(changes)

        # Trie les points de changement par significativité
        change_points.sort(key=lambda x: x['significance'], reverse=True)

        return change_points

    def create_snapshot(self, timestamp: datetime = None) -> str:
        """Crée un snapshot de l'état du graphe à un moment donné"""
        timestamp = timestamp or datetime.now()
        
        # Vérifie que le timestamp n'est pas dans le futur
        if timestamp > datetime.now():
            raise ValueError("Cannot create snapshot with future timestamp")
            
        snapshot_id = str(uuid4())
        
        snapshot = {
            'nodes': {},
            'edges': [],
            'timestamp': timestamp.isoformat(),
            'metadata': {
                'node_count': len(self.graph.nodes),
                'edge_count': self.graph.statistics['edges_count']
            }
        }
        
        # Capture l'état des nœuds
        for node_id, node in self.graph.nodes.items():
            if not node._deleted:
                snapshot['nodes'][node_id] = {
                    'labels': list(node.labels),
                    'properties': node._properties.copy(),
                    'version': node._version,
                    'created_at': node._created_at,
                    'updated_at': node._updated_at
                }
                
                # Capture les relations existantes à ce moment
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        if edge.is_valid() and datetime.fromisoformat(edge._created_at) <= timestamp:
                            snapshot['edges'].append({
                                'source': node_id,
                                'target': edge.target().node_id,
                                'type': edge.edge_type,
                                'properties': edge._properties.copy(),
                                'created_at': edge._created_at
                            })
        
        self.temporal_snapshots[snapshot_id] = snapshot
        return snapshot_id

    def predict_evolution(self, window: timedelta = None) -> Dict[str, Any]:
        """Prédit l'évolution future du graphe"""
        window = window or self.prediction_window
        predictions = {
            'nodes': {},
            'edges': [],
            'metrics': {},
            'confidence': {}
        }
        
        # Analyse les tendances historiques
        growth_rates = self._calculate_growth_rates()
        
        # Prédit les changements de nœuds
        for node_id, node in self.graph.nodes.items():
            if not node._deleted:
                node_prediction = self._predict_node_evolution(
                    node, 
                    growth_rates,
                    window
                )
                predictions['nodes'][node_id] = node_prediction
        
        # Prédit les changements de relations
        edge_predictions = self._predict_edge_evolution(growth_rates, window)
        predictions['edges'] = edge_predictions
        
        # Calcule les métriques globales prédites
        predictions['metrics'] = {
            'predicted_node_count': len(predictions['nodes']),
            'predicted_edge_count': len(predictions['edges']),
            'growth_rate': growth_rates['overall'],
            'prediction_window': window.days
        }
        
        return predictions

    def reconstruct_state(self, timestamp: datetime) -> Dict[str, Any]:
        """Reconstruit l'état du graphe à un moment donné"""
        # Trouve le snapshot le plus proche avant le timestamp
        closest_snapshot = None
        closest_time_diff = timedelta.max
        
        for snapshot in self.temporal_snapshots.values():
            snapshot_time = datetime.fromisoformat(snapshot['timestamp'])
            time_diff = timestamp - snapshot_time
            
            if timedelta() <= time_diff < closest_time_diff:
                closest_snapshot = snapshot
                closest_time_diff = time_diff
        
        if not closest_snapshot:
            raise ValueError("No suitable snapshot found for reconstruction")
        
        # Applique les changements intervenus entre le snapshot et le timestamp
        reconstructed_state = self._apply_changes_until(
            closest_snapshot,
            timestamp
        )
        
        return reconstructed_state

    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyse les motifs temporels dans l'évolution du graphe"""
        analysis = {
            'growth_patterns': {},
            'seasonal_patterns': {},
            'stability_metrics': {},
            'change_points': []
        }
        
        # Analyse la croissance
        growth_rates = self._calculate_growth_rates()
        analysis['growth_patterns'] = {
            'overall_rate': growth_rates['overall'],
            'node_growth': growth_rates['nodes'],
            'edge_growth': growth_rates['edges'],
            'acceleration': self._calculate_growth_acceleration()
        }
        
        # Détecte les motifs saisonniers
        seasonal_patterns = self._detect_seasonal_patterns()
        analysis['seasonal_patterns'] = seasonal_patterns
        
        # Calcule la stabilité
        stability = self._calculate_stability_metrics()
        analysis['stability_metrics'] = stability
        
        # Détecte les points de changement
        change_points = self._detect_change_points()
        analysis['change_points'] = change_points
        
        return analysis

    def _calculate_growth_rates(self) -> Dict[str, float]:
        """Calcule les taux de croissance basés sur l'historique"""
        rates = {
            'overall': 0.0,
            'nodes': {},
            'edges': {},
            'properties': defaultdict(float)
        }
        
        snapshots = sorted(
            self.temporal_snapshots.values(),
            key=lambda x: datetime.fromisoformat(x['timestamp'])
        )
        
        if len(snapshots) < 2:
            return rates
            
        # Calcule les taux de croissance entre snapshots consécutifs
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            time_diff = (datetime.fromisoformat(curr['timestamp']) - 
                        datetime.fromisoformat(prev['timestamp'])).days
            
            if time_diff == 0:
                continue
            
            # Croissance des nœuds
            node_growth = (len(curr['nodes']) - len(prev['nodes'])) / time_diff
            rates['nodes']['daily'] = node_growth
            
            # Croissance des relations
            edge_growth = (len(curr['edges']) - len(prev['edges'])) / time_diff
            rates['edges']['daily'] = edge_growth
            
            # Croissance globale
            rates['overall'] = (node_growth + edge_growth) / 2
        
        return rates

    def _predict_node_evolution(self, node: 'Node', growth_rates: Dict[str, float], 
                              window: timedelta) -> Dict[str, Any]:
        """Prédit l'évolution d'un nœud spécifique"""
        prediction = {
            'properties': {},
            'probability_of_change': 0.0,
            'new_connections': []
        }
        
        # Analyse l'historique des modifications
        history = self._get_node_history(node)
        
        # Prédit les changements de propriétés
        for prop, values in history['properties'].items():
            if len(values) >= 2:
                prediction['properties'][prop] = self._extrapolate_value(
                    values,
                    window
                )
        
        # Prédit la probabilité de nouveaux liens
        connection_history = history.get('connections', [])
        if connection_history:
            avg_new_connections = len(connection_history) / max(1, (
                datetime.now() - datetime.fromisoformat(connection_history[0]['timestamp'])
            ).days)
            
            predicted_connections = int(avg_new_connections * window.days)
            prediction['new_connections'] = self._predict_new_connections(
                node,
                predicted_connections
            )
        
        return prediction

    def _predict_edge_evolution(self, growth_rates: Dict[str, float], 
                              window: timedelta) -> List[Dict[str, Any]]:
        """Prédit l'évolution des relations"""
        predicted_edges = []
        
        # Prédit de nouvelles relations basées sur les motifs existants
        edge_patterns = self._analyze_edge_patterns()
        
        for pattern in edge_patterns:
            probability = pattern['frequency'] / max(1, len(self.graph.edge_types))
            if probability > 0.3:  # Seuil arbitraire
                predicted_edges.extend(
                    self._generate_predicted_edges(pattern, window)
                )
        
        return predicted_edges

    def _apply_changes_until(self, snapshot: Dict[str, Any], 
                           target_time: datetime) -> Dict[str, Any]:
        """Applique les changements jusqu'à un moment donné"""
        state = copy.deepcopy(snapshot)
        
        # Récupère tous les changements entre le snapshot et le timestamp cible
        changes = self._get_changes_between(
            datetime.fromisoformat(snapshot['timestamp']),
            target_time
        )
        
        # Applique les changements dans l'ordre chronologique
        for change in sorted(changes, key=lambda x: x['timestamp']):
            if change['type'] == 'node_update':
                node_id = change['node_id']
                if node_id in state['nodes']:
                    # Met à jour les propriétés du nœud
                    state['nodes'][node_id]['properties'].update(change['properties'])
                    state['nodes'][node_id]['version'] = change['version']
            
            elif change['type'] == 'node_delete':
                state['nodes'].pop(change['node_id'], None)
                # Supprime aussi les relations associées
                state['edges'] = [
                    edge for edge in state['edges']
                    if edge['source'] != change['node_id'] 
                    and edge['target'] != change['node_id']
                ]
            
            elif change['type'] == 'edge_create':
                state['edges'].append(change['edge'])
            
            elif change['type'] == 'edge_delete':
                state['edges'] = [
                    edge for edge in state['edges']
                    if not (edge['source'] == change['source'] and 
                           edge['target'] == change['target'] and
                           edge['type'] == change['edge_type'])
                ]
        
        return state

    def _get_changes_between(self, start_time: datetime, 
                           end_time: datetime) -> List[Dict[str, Any]]:
        """Récupère tous les changements entre deux timestamps"""
        changes = []
        
        # Parcourt tous les nœuds pour collecter leurs historiques
        for node_id, node in self.graph.nodes.items():
            # Historique des propriétés
            for change in node._property_history:
                change_time = datetime.fromisoformat(change['timestamp'])
                if start_time <= change_time <= end_time:
                    changes.append({
                        'type': 'node_update',
                        'node_id': node_id,
                        'properties': {change['key']: change['new_value']},
                        'version': change['version'],
                        'timestamp': change_time
                    })
            
            # Historique des labels
            for change in node._label_history:
                change_time = datetime.fromisoformat(change['timestamp'])
                if start_time <= change_time <= end_time:
                    changes.append({
                        'type': 'node_update',
                        'node_id': node_id,
                        'labels': {'operation': change['operation'], 
                                 'label': change['label']},
                        'version': change['version'],
                        'timestamp': change_time
                    })
        
        # Trie les changements par timestamp
        changes.sort(key=lambda x: x['timestamp'])
        return changes

    def _detect_seasonal_patterns(self) -> Dict[str, Any]:
        """Détecte les motifs saisonniers dans l'évolution du graphe"""
        patterns = {
            'daily': {},
            'weekly': {},
            'monthly': {},
            'confidence': {}
        }
        
        # Analyse les modifications par période
        changes_by_hour = defaultdict(int)
        changes_by_day = defaultdict(int)
        changes_by_month = defaultdict(int)
        
        for node in self.graph.nodes.values():
            for change in node._property_history:
                timestamp = datetime.fromisoformat(change['timestamp'])
                changes_by_hour[timestamp.hour] += 1
                changes_by_day[timestamp.weekday()] += 1
                changes_by_month[timestamp.month] += 1
        
        # Normalise les résultats
        total_changes = sum(changes_by_hour.values())
        if total_changes > 0:
            patterns['daily'] = {
                hour: count/total_changes 
                for hour, count in changes_by_hour.items()
            }
            patterns['weekly'] = {
                day: count/total_changes 
                for day, count in changes_by_day.items()
            }
            patterns['monthly'] = {
                month: count/total_changes 
                for month, count in changes_by_month.items()
            }
            
            # Calcule la confiance basée sur la quantité de données
            patterns['confidence'] = min(1.0, total_changes / 1000)
        
        return patterns

    def _calculate_stability_metrics(self) -> Dict[str, float]:
        """Calcule les métriques de stabilité du graphe"""
        metrics = {
            'node_stability': 0.0,
            'edge_stability': 0.0,
            'property_stability': 0.0,
            'overall_stability': 0.0
        }
        
        total_nodes = len(self.graph.nodes)
        if total_nodes == 0:
            return metrics
        
        # Calcule la stabilité des nœuds
        node_changes = sum(
            len(node._property_history) + len(node._label_history)
            for node in self.graph.nodes.values()
        )
        metrics['node_stability'] = 1.0 - (node_changes / (total_nodes * 100))
        
        # Calcule la stabilité des relations
        edge_count = self.graph.statistics['edges_count']
        if edge_count > 0:
            edge_changes = sum(
                1 for node in self.graph.nodes.values()
                for edges in node.outgoing_edges.values()
                for edge in edges
                if len(edge._property_history) > 0
            )
            metrics['edge_stability'] = 1.0 - (edge_changes / (edge_count * 100))
        
        # Calcule la stabilité globale
        metrics['overall_stability'] = (
            metrics['node_stability'] + metrics['edge_stability']
        ) / 2
        
        return metrics

# Support des Hypergraphes
class HypergraphManager:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.hyperedges = {}
        self.node_to_hyperedge = defaultdict(set)
        self.hyperedge_types = set()
        self._cache = {}

    def create_hyperedge(self, nodes: List[str], edge_type: str, 
                        properties: Dict[str, Any] = None) -> str:
        """Crée une hyperarête connectant plusieurs nœuds"""
        # Vérifie que tous les nœuds existent
        for node_id in nodes:
            if node_id not in self.graph.nodes:
                raise ValueError(f"Node {node_id} not found")
        
        hyperedge_id = str(uuid4())
        
        hyperedge = {
            'nodes': set(nodes),
            'type': edge_type,
            'properties': properties or {},
            'created_at': datetime.now().isoformat(),
            'version': 0
        }
        
        self.hyperedges[hyperedge_id] = hyperedge
        self.hyperedge_types.add(edge_type)
        
        # Met à jour les index
        for node_id in nodes:
            self.node_to_hyperedge[node_id].add(hyperedge_id)
        
        return hyperedge_id

    def get_hyperedge(self, hyperedge_id: str) -> Dict[str, Any]:
        """Récupère une hyperarête par son ID"""
        if hyperedge_id not in self.hyperedges:
            raise ValueError(f"Hyperedge {hyperedge_id} not found")
        return self.hyperedges[hyperedge_id]

    def get_node_hyperedges(self, node_id: str) -> List[Dict[str, Any]]:
        """Récupère toutes les hyperarêtes connectées à un nœud"""
        return [
            self.hyperedges[edge_id]
            for edge_id in self.node_to_hyperedge[node_id]
            if edge_id in self.hyperedges
        ]

    def add_node_to_hyperedge(self, hyperedge_id: str, node_id: str):
        """Ajoute un nœud à une hyperarête existante"""
        if hyperedge_id not in self.hyperedges:
            raise ValueError(f"Hyperedge {hyperedge_id} not found")
        if node_id not in self.graph.nodes:
            raise ValueError(f"Node {node_id} not found")
            
        hyperedge = self.hyperedges[hyperedge_id]
        hyperedge['nodes'].add(node_id)
        hyperedge['version'] += 1
        self.node_to_hyperedge[node_id].add(hyperedge_id)

    def remove_node_from_hyperedge(self, hyperedge_id: str, node_id: str):
        """Retire un nœud d'une hyperarête"""
        if hyperedge_id not in self.hyperedges:
            raise ValueError(f"Hyperedge {hyperedge_id} not found")
            
        hyperedge = self.hyperedges[hyperedge_id]
        if node_id in hyperedge['nodes']:
            hyperedge['nodes'].remove(node_id)
            hyperedge['version'] += 1
            self.node_to_hyperedge[node_id].discard(hyperedge_id)
            
            # Supprime l'hyperarête si elle ne contient plus qu'un seul nœud
            if len(hyperedge['nodes']) <= 1:
                self.delete_hyperedge(hyperedge_id)

    def delete_hyperedge(self, hyperedge_id: str):
        """Supprime une hyperarête"""
        if hyperedge_id not in self.hyperedges:
            raise ValueError(f"Hyperedge {hyperedge_id} not found")
            
        hyperedge = self.hyperedges[hyperedge_id]
        
        # Met à jour les index
        for node_id in hyperedge['nodes']:
            self.node_to_hyperedge[node_id].discard(hyperedge_id)
            
        del self.hyperedges[hyperedge_id]

    def find_hyperedges_by_type(self, edge_type: str) -> List[Dict[str, Any]]:
        """Trouve toutes les hyperarêtes d'un type donné"""
        return [
            edge for edge in self.hyperedges.values()
            if edge['type'] == edge_type
        ]

    def analyze_hypergraph(self) -> Dict[str, Any]:
        """Analyse la structure du hypergraphe"""
        analysis = {
            'statistics': {
                'hyperedge_count': len(self.hyperedges),
                'type_distribution': defaultdict(int),
                'avg_nodes_per_edge': 0,
                'max_nodes_per_edge': 0
            },
            'centrality': self._calculate_hypergraph_centrality(),
            'clusters': self._detect_hypergraph_clusters(),
            'connectivity': self._analyze_hypergraph_connectivity()
        }
        
        # Calcule les statistiques
        total_nodes = 0
        for edge in self.hyperedges.values():
            analysis['statistics']['type_distribution'][edge['type']] += 1
            nodes_count = len(edge['nodes'])
            total_nodes += nodes_count
            analysis['statistics']['max_nodes_per_edge'] = max(
                analysis['statistics']['max_nodes_per_edge'],
                nodes_count
            )
        
        if self.hyperedges:
            analysis['statistics']['avg_nodes_per_edge'] = (
                total_nodes / len(self.hyperedges)
            )
        
        return analysis

    def _calculate_hypergraph_centrality(self) -> Dict[str, float]:
        """Calcule la centralité des nœuds dans le hypergraphe"""
        centrality = defaultdict(float)
        
        for node_id in self.graph.nodes:
            # Compte le nombre d'hyperarêtes connectées
            edge_count = len(self.node_to_hyperedge[node_id])
            
            # Calcule l'importance basée sur la taille des hyperarêtes
            importance = sum(
                len(self.hyperedges[edge_id]['nodes'])
                for edge_id in self.node_to_hyperedge[node_id]
            )
            
            if edge_count > 0:
                centrality[node_id] = importance / edge_count
        
        return dict(centrality)

    def _detect_hypergraph_clusters(self) -> List[Set[str]]:
        """Détecte les clusters dans le hypergraphe"""
        clusters = []
        visited = set()
        
        def explore_cluster(start_node: str) -> Set[str]:
            cluster = {start_node}
            queue = deque([start_node])
            
            while queue:
                node_id = queue.popleft()
                # Explore les nœuds connectés via les hyperarêtes
                for edge_id in self.node_to_hyperedge[node_id]:
                    for connected_node in self.hyperedges[edge_id]['nodes']:
                        if connected_node not in visited:
                            visited.add(connected_node)
                            cluster.add(connected_node)
                            queue.append(connected_node)
            
            return cluster
        
        # Trouve tous les clusters
        for node_id in self.graph.nodes:
            if node_id not in visited:
                visited.add(node_id)
                cluster = explore_cluster(node_id)
                if len(cluster) > 1:  # Ignore les nœuds isolés
                    clusters.append(cluster)
        
        return clusters

    def _analyze_hypergraph_connectivity(self) -> Dict[str, Any]:
        """Analyse la connectivité du hypergraphe"""
        analysis = {
            'components': [],
            'avg_connectivity': 0.0,
            'isolated_nodes': set(),
            'bridge_edges': set()
        }
        
        # Trouve les composantes connexes
        components = self._detect_hypergraph_clusters()
        analysis['components'] = [list(comp) for comp in components]
        
        # Identifie les nœuds isolés
        for node_id in self.graph.nodes:
            if not self.node_to_hyperedge[node_id]:
                analysis['isolated_nodes'].add(node_id)
        
        # Identifie les hyperarêtes "pont"
        for edge_id, edge in self.hyperedges.items():
            # Une hyperarête est un pont si sa suppression augmente le nombre de composantes
            edge_nodes = edge['nodes'].copy()
            edge['nodes'].clear()
            
            new_components = self._detect_hypergraph_clusters()
            if len(new_components) > len(components):
                analysis['bridge_edges'].add(edge_id)
            
            edge['nodes'] = edge_nodes
        
        # Calcule la connectivité moyenne
        if self.graph.nodes:
            total_connectivity = sum(
                len(self.node_to_hyperedge[node_id])
                for node_id in self.graph.nodes
            )
            analysis['avg_connectivity'] = total_connectivity / len(self.graph.nodes)
        
        return analysis

class BinaryStorageFormat:
    HEADER_FORMAT = "!QIIII"  
    NODE_HEADER_FORMAT = "!40sII"  # Augmenté à 40 pour le padding
    EDGE_HEADER_FORMAT = "!40s40sII"  # Augmenté à 40
    INDEX_ENTRY_FORMAT = "!40sQ"  # Augmenté à 40
    
    MAGIC_NUMBER = 0x47444230
    VERSION = 1
    
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    NODE_HEADER_SIZE = struct.calcsize(NODE_HEADER_FORMAT)
    EDGE_HEADER_SIZE = struct.calcsize(EDGE_HEADER_FORMAT)
    INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FORMAT)

class BinaryStorageManager:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.data_file = self.base_path / "graph.bin"
        self.index_file = self.base_path / "index.bin"
        self.lock = threading.RLock()
        self.page_size = mmap.PAGESIZE
        self.cache = LRUCache(1000)
        self.dirty_pages = set()

    def save_graph(self, graph: 'GraphDB'):
        with self.lock:
            with open(self.data_file, "wb") as f:
                # Write placeholder header
                header = struct.pack(
                    BinaryStorageFormat.HEADER_FORMAT,
                    BinaryStorageFormat.MAGIC_NUMBER,
                    BinaryStorageFormat.VERSION,
                    len(graph.nodes),
                    graph.statistics["edges_count"],
                    0
                )
                f.write(header)
                
                # Calculate checksum starting with header (excluding checksum field)
                checksum = zlib.crc32(header[:-8])
                
                # Write and checksum nodes
                for node in graph.nodes.values():
                    node_data = self._write_node(f, node)
                    checksum = zlib.crc32(node_data, checksum)
                
                # Write and checksum edges
                for node in graph.nodes.values():
                    for edges in node.outgoing_edges.values():
                        for edge in edges:
                            if edge.is_valid():
                                edge_data = self._write_edge(f, edge)
                                checksum = zlib.crc32(edge_data, checksum)
                
                # Update header with final checksum
                f.seek(0)
                final_header = struct.pack(
                    BinaryStorageFormat.HEADER_FORMAT,
                    BinaryStorageFormat.MAGIC_NUMBER,
                    BinaryStorageFormat.VERSION,
                    len(graph.nodes),
                    graph.statistics["edges_count"],
                    checksum
                )
                f.write(final_header)

    def load_graph(self, graph: 'GraphDB'):
        with self.lock:
            with open(self.data_file, "rb") as f:
                header = f.read(BinaryStorageFormat.HEADER_SIZE)
                magic, version, node_count, edge_count, stored_checksum = struct.unpack(
                    BinaryStorageFormat.HEADER_FORMAT, header
                )
                
                if magic != BinaryStorageFormat.MAGIC_NUMBER:
                    raise ValueError("Invalid file format")
                if version != BinaryStorageFormat.VERSION:
                    raise ValueError(f"Incompatible version: {version}")
                
                # Calculate checksum starting with header (excluding checksum field)
                checksum = zlib.crc32(header[:-8])
                
                # Read and load nodes
                for _ in range(node_count):
                    node_data, node = self._read_node_data(f)
                    checksum = zlib.crc32(node_data, checksum)
                    graph._create_node_internal(node["id"], node["labels"], node["properties"])
                
                # Read and load edges
                for _ in range(edge_count):
                    edge_data, edge = self._read_edge_data(f)
                    checksum = zlib.crc32(edge_data, checksum)
                    graph._create_edge_internal(
                        edge["source_id"],
                        edge["target_id"],
                        edge["type"],
                        edge["properties"]
                    )
                
                if checksum != stored_checksum:
                    raise ValueError("Data corruption detected: checksum mismatch")

    def _write_node(self, f, node: 'Node') -> bytes:
        node_id_padded = node.node_id.ljust(40, '\x00').encode()
        labels_data = zlib.compress(json.dumps(list(node.labels)).encode())
        props_data = zlib.compress(json.dumps(node._properties).encode())
        
        node_header = struct.pack(
            BinaryStorageFormat.NODE_HEADER_FORMAT,
            node_id_padded,
            len(labels_data),
            len(props_data)
        )
        
        data = node_header + labels_data + props_data
        f.write(data)
        return data

    def _write_edge(self, f, edge: 'Edge') -> bytes:
        source_id = edge.source().node_id.ljust(40, '\x00').encode()
        target_id = edge.target().node_id.ljust(40, '\x00').encode()
        type_data = edge.edge_type.encode()
        props_data = zlib.compress(json.dumps(edge._properties).encode())
        
        edge_header = struct.pack(
            BinaryStorageFormat.EDGE_HEADER_FORMAT,
            source_id,
            target_id,
            len(type_data),
            len(props_data)
        )
        
        data = edge_header + type_data + props_data
        f.write(data)
        return data

    def _read_node_data(self, f) -> Tuple[bytes, Dict[str, Any]]:
        start_pos = f.tell()
        
        # Read header
        node_header = f.read(BinaryStorageFormat.NODE_HEADER_SIZE)
        node_id, labels_len, props_len = struct.unpack(
            BinaryStorageFormat.NODE_HEADER_FORMAT,
            node_header
        )
        
        # Read data
        labels_data = f.read(labels_len)
        props_data = f.read(props_len)
        
        # Calculate raw data for checksum
        end_pos = f.tell()
        f.seek(start_pos)
        raw_data = f.read(end_pos - start_pos)
        f.seek(end_pos)
        
        # Parse data
        node = {
            "id": node_id.decode().strip('\x00'),
            "labels": set(json.loads(zlib.decompress(labels_data).decode())),
            "properties": json.loads(zlib.decompress(props_data).decode())
        }
        
        return raw_data, node

    def _read_edge_data(self, f) -> Tuple[bytes, Dict[str, Any]]:
        start_pos = f.tell()
        
        # Read header
        edge_header = f.read(BinaryStorageFormat.EDGE_HEADER_SIZE)
        source_id, target_id, type_len, props_len = struct.unpack(
            BinaryStorageFormat.EDGE_HEADER_FORMAT,
            edge_header
        )
        
        # Read data
        type_data = f.read(type_len)
        props_data = f.read(props_len)
        
        # Calculate raw data for checksum
        end_pos = f.tell()
        f.seek(start_pos)
        raw_data = f.read(end_pos - start_pos)
        f.seek(end_pos)
        
        # Parse data
        edge = {
            "source_id": source_id.decode().strip('\x00'),
            "target_id": target_id.decode().strip('\x00'),
            "type": type_data.decode(),
            "properties": json.loads(zlib.decompress(props_data).decode())
        }
        
        return raw_data, edge

    def _write_index(self, f, node_offsets: Dict[str, int], edge_offsets: List[Tuple[str, int]]):
        for node_id, offset in node_offsets.items():
            entry = struct.pack(
                BinaryStorageFormat.INDEX_ENTRY_FORMAT,
                node_id.ljust(40, '\x00').encode(),
                offset
            )
            f.write(entry)
        
        for edge_id, offset in edge_offsets:
            entry = struct.pack(
                BinaryStorageFormat.INDEX_ENTRY_FORMAT,
                edge_id.ljust(40, '\x00').encode(),
                offset
            )
            f.write(entry)

    def _read_index(self, f) -> Tuple[Dict[str, int], Dict[str, int]]:
        node_offsets = {}
        edge_offsets = {}
        
        while True:
            entry_data = f.read(BinaryStorageFormat.INDEX_ENTRY_SIZE)
            if not entry_data:
                break
                
            id_bytes, offset = struct.unpack(
                BinaryStorageFormat.INDEX_ENTRY_FORMAT,
                entry_data
            )
            uuid_str = id_bytes.decode().strip('\x00')
            
            if '-' in uuid_str:
                node_offsets[uuid_str] = offset
            else:
                edge_offsets[uuid_str] = offset
        
        return node_offsets, edge_offsets

class PageManager:
    def __init__(self, file_path: str, page_size: int = mmap.PAGESIZE):
        self.file_path = file_path
        self.page_size = page_size
        self.pages = {}
        self.dirty_pages = set()
        self.lock = threading.Lock()

    def get_page(self, page_number: int) -> bytearray:
        with self.lock:
            if page_number not in self.pages:
                self._load_page(page_number)
            return self.pages[page_number]

    def _load_page(self, page_number: int):
        with open(self.file_path, "rb") as f:
            f.seek(page_number * self.page_size)
            data = f.read(self.page_size)
            self.pages[page_number] = bytearray(data)

    def write_page(self, page_number: int, data: bytearray):
        with self.lock:
            self.pages[page_number] = data
            self.dirty_pages.add(page_number)

    def flush(self):
        with self.lock:
            with open(self.file_path, "rb+") as f:
                for page_number in sorted(self.dirty_pages):
                    f.seek(page_number * self.page_size)
                    f.write(self.pages[page_number])
            self.dirty_pages.clear()

class BackupManager:
    """Gestionnaire de sauvegardes"""
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.backup_dir = "backups"
        os.makedirs(self.backup_dir, exist_ok=True)

    def _serialize_property_definition(self, prop_def: PropertyDefinition) -> Dict[str, Any]:
        """Sérialise une définition de propriété"""
        return {
            "type": prop_def.type.name,  # Utilise le nom de l'enum au lieu de la valeur
            "required": prop_def.required,
            "default": prop_def.default,
            "array_type": prop_def.array_type.name if prop_def.array_type else None,
            "constraints": None  # Les fonctions ne peuvent pas être sérialisées
        }

    def _serialize_schema(self, schema: Schema) -> Dict[str, Any]:
        """Sérialise le schéma"""
        return {
            "node_labels": {
                label: {
                    prop_name: self._serialize_property_definition(prop_def)
                    for prop_name, prop_def in properties.items()
                }
                for label, properties in schema.node_labels.items()
            },
            "edge_types": {
                edge_type: {
                    prop_name: self._serialize_property_definition(prop_def)
                    for prop_name, prop_def in properties.items()
                }
                for edge_type, properties in schema.edge_types.items()
            },
            "constraints": {
                label: [
                    {
                        "type": constraint.type,
                        "property": constraint.property,
                        "params": constraint.params
                    }
                    for constraint in constraints
                ]
                for label, constraints in schema.constraints.items()
            },
            "version": schema.version
        }

    def create_backup(self) -> str:
        """Crée une sauvegarde complète"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{self.backup_dir}/backup_{timestamp}.json"
        
        # Prépare les données pour la sérialisation
        data = {
            "nodes": {
                node_id: {
                    "labels": list(node.labels),
                    "properties": node._properties,
                    "version": node._version,
                    "created_at": node._created_at,
                    "updated_at": node._updated_at,
                    "deleted": node._deleted
                }
                for node_id, node in self.graph.nodes.items()
            },
            "edges": [],  # Liste pour stocker toutes les relations
            "edge_types": list(self.graph.edge_types),
            "schema": self._serialize_schema(self.graph.schema),
            "metadata": {
                "timestamp": timestamp,
                "version": "1.0",
                "statistics": {
                    "nodes_count": self.graph.statistics["nodes_count"],
                    "edges_count": self.graph.statistics["edges_count"],
                    "labels_count": dict(self.graph.statistics["labels_count"]),
                    "edge_types_count": dict(self.graph.statistics["edge_types_count"])
                }
            }
        }

        # Sauvegarde des relations
        for node_id, node in self.graph.nodes.items():
            for edge_type, edges in node.outgoing_edges.items():
                for edge in edges:
                    if edge.is_valid():  # Ne sauvegarde que les relations valides
                        data["edges"].append({
                            "id": edge.id,
                            "source_id": node_id,
                            "target_id": edge.target().node_id if edge.target() else None,
                            "type": edge.edge_type,
                            "properties": edge._properties,
                            "created_at": edge._created_at,
                            "updated_at": edge._updated_at,
                            "version": edge._version
                        })
        
        # Sauvegarde dans le fichier
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Created backup: {backup_file}")
        return backup_file

    def _restore_schema(self, schema_data: Dict[str, Any]):
        """Restaure le schéma depuis les données sérialisées"""
        # Crée un nouveau schéma
        self.graph.schema = Schema()
        
        # Restaure les définitions de nœuds
        for label, properties in schema_data["node_labels"].items():
            restored_properties = {}
            for prop_name, prop_data in properties.items():
                prop_type = PropertyType[prop_data["type"]]
                array_type = PropertyType[prop_data["array_type"]] if prop_data["array_type"] else None
                restored_properties[prop_name] = PropertyDefinition(
                    type=prop_type,
                    required=prop_data["required"],
                    default=prop_data["default"],
                    array_type=array_type
                )
            self.graph.schema.add_node_definition(label, restored_properties)
        
        # Restaure les contraintes
        for label, constraints in schema_data["constraints"].items():
            for constraint_data in constraints:
                constraint = SchemaConstraint(
                    constraint_data["type"],
                    constraint_data["property"],
                    constraint_data["params"]
                )
                self.graph.schema.add_constraint(label, constraint)

    def restore_from_backup(self, backup_file: str):
        """Restaure depuis une sauvegarde"""
        with open(backup_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Réinitialise la base de données
        with self.graph.lock:
            self.graph.nodes.clear()
            self.graph.edge_types.clear()
            
            # Restaure le schéma
            self._restore_schema(data["schema"])
            
            # Restaure les nœuds
            for node_id, node_data in data["nodes"].items():
                # Crée le nœud avec son ID spécifique
                self.graph._create_node_internal(
                    node_id,
                    set(node_data["labels"]),
                    node_data["properties"]
                )
            
            # Restaure les relations
            if "edges" in data:
                for edge_data in data["edges"]:
                    self.graph._create_edge_internal(
                        edge_data["source_id"],
                        edge_data["target_id"],
                        edge_data["type"],
                        edge_data["properties"]
                    )
            
            # Recrée les index
            self.graph.setup_initial_indexes()
        
        logger.info(f"Restored from backup: {backup_file}")

    def list_backups(self) -> List[str]:
        """Liste tous les fichiers de backup disponibles"""
        return [f for f in os.listdir(self.backup_dir) 
                if f.startswith("backup_") and f.endswith(".json")]

    def get_backup_info(self, backup_file: str) -> Dict[str, Any]:
        """Obtient les informations sur une sauvegarde"""
        full_path = os.path.join(self.backup_dir, backup_file)
        if not os.path.exists(full_path):
            raise ValueError(f"Backup file not found: {backup_file}")
            
        with open(full_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return {
            "timestamp": data["metadata"]["timestamp"],
            "version": data["metadata"]["version"],
            "nodes_count": len(data["nodes"]),
            "schema_labels": list(data["schema"]["node_labels"].keys()),
            "file_size": os.path.getsize(full_path)
        }

    def cleanup_old_backups(self, max_age_days: int = 30):
        """Nettoie les anciennes sauvegardes"""
        current_time = datetime.now()
        for backup_file in self.list_backups():
            file_path = os.path.join(self.backup_dir, backup_file)
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            
            if (current_time - file_time).days > max_age_days:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed old backup: {backup_file}")
                except OSError as e:
                    logger.error(f"Error removing backup {backup_file}: {e}")

class StoredProcedure:
    """Procédure stockée avancée"""
    def __init__(self, name: str, params: List[str], code: str, 
                 description: str = "", tags: List[str] = None):
        self.name = name
        self.params = params
        self.code = code
        self.description = description
        self.tags = tags or []
        self.compiled = compile(code, f"procedure_{name}", "exec")
        self.created_at = datetime.now().isoformat()
        self.last_executed = None
        self.execution_count = 0
        self.average_execution_time = 0
        self._lock = threading.RLock()

    def execute(self, graph: 'GraphDB', **kwargs) -> Any:
        """Exécute la procédure stockée avec mesure des performances"""
        start_time = time.time()
        try:
            # Prépare le contexte d'exécution
            context = {
                'graph': graph,
                'logger': logger,
                'datetime': datetime,
                'result': None,
                **kwargs
            }
            
            # Exécute le code
            with self._lock:
                exec(self.compiled, context)
                execution_time = time.time() - start_time
                self._update_statistics(execution_time)
            
            return context.get('result')
        except Exception as e:
            logger.error(f"Error executing stored procedure {self.name}: {e}")
            raise

    def _update_statistics(self, execution_time: float):
        """Met à jour les statistiques d'exécution"""
        self.execution_count += 1
        self.last_executed = datetime.now().isoformat()
        self.average_execution_time = (
            (self.average_execution_time * (self.execution_count - 1) + 
             execution_time) / self.execution_count
        )

    def get_info(self) -> Dict[str, Any]:
        """Retourne les informations sur la procédure"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.params,
            "tags": self.tags,
            "statistics": {
                "execution_count": self.execution_count,
                "average_execution_time": self.average_execution_time,
                "last_executed": self.last_executed
            },
            "metadata": {
                "created_at": self.created_at
            }
        }

class PerformanceMonitor:
    """Moniteur de performances avancé"""
    def __init__(self):
        self.metrics: Dict[str, List[float]] = defaultdict(list)
        self.lock = threading.Lock()
        self.start_time = datetime.now()
        self.last_reset = self.start_time
        
        # Métriques détaillées
        self.detailed_metrics = {
            "query_execution": defaultdict(list),
            "node_operations": defaultdict(int),
            "edge_operations": defaultdict(int),
            "cache_operations": defaultdict(int),
            "transaction_stats": defaultdict(list)
        }

    def record_metric(self, name: str, value: float):
        """Enregistre une métrique"""
        with self.lock:
            self.metrics[name].append(value)
            # Garde uniquement les 1000 dernières mesures
            if len(self.metrics[name]) > 1000:
                self.metrics[name] = self.metrics[name][-1000:]

    def record_query_execution(self, query: str, execution_time: float):
        """Enregistre l'exécution d'une requête"""
        with self.lock:
            # Simplifie la requête pour le regroupement
            simplified_query = self._simplify_query(query)
            self.detailed_metrics["query_execution"][simplified_query].append(execution_time)

    def record_operation(self, category: str, operation: str, count: int = 1):
        """Enregistre une opération"""
        with self.lock:
            if category == "node":
                self.detailed_metrics["node_operations"][operation] += count
            elif category == "edge":
                self.detailed_metrics["edge_operations"][operation] += count
            elif category == "cache":
                self.detailed_metrics["cache_operations"][operation] += count

    def record_transaction(self, duration: float, operation_count: int, success: bool):
        """Enregistre les statistiques d'une transaction"""
        with self.lock:
            self.detailed_metrics["transaction_stats"]["duration"].append(duration)
            self.detailed_metrics["transaction_stats"]["operation_count"].append(operation_count)
            self.detailed_metrics["transaction_stats"]["success_rate"].append(1 if success else 0)

    def get_statistics(self, name: str = None) -> Dict[str, Any]:
        """Retourne les statistiques pour une métrique ou toutes les métriques"""
        with self.lock:
            if name:
                values = self.metrics.get(name, [])
                if not values:
                    return {}
                
                return {
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values),
                    "last": values[-1]
                }
            else:
                return {
                    name: self.get_statistics(name)
                    for name in self.metrics
                }

    def get_detailed_statistics(self) -> Dict[str, Any]:
        """Retourne les statistiques détaillées de toutes les métriques"""
        with self.lock:
            stats = {
                "uptime": str(datetime.now() - self.start_time),
                "last_reset": str(datetime.now() - self.last_reset),
                "query_stats": self._calculate_query_stats(),
                "operation_stats": {
                    "nodes": dict(self.detailed_metrics["node_operations"]),
                    "edges": dict(self.detailed_metrics["edge_operations"]),
                    "cache": dict(self.detailed_metrics["cache_operations"])
                },
                "transaction_stats": self._calculate_transaction_stats()
            }
            return stats

    def _calculate_query_stats(self) -> Dict[str, Any]:
        """Calcule les statistiques des requêtes"""
        stats = {}
        for query, times in self.detailed_metrics["query_execution"].items():
            if times:
                stats[query] = {
                    "count": len(times),
                    "avg_time": sum(times) / len(times),
                    "min_time": min(times),
                    "max_time": max(times)
                }
        return stats

    def _calculate_transaction_stats(self) -> Dict[str, Any]:
        """Calcule les statistiques des transactions"""
        tx_stats = self.detailed_metrics["transaction_stats"]
        if not tx_stats.get("duration"):
            return {}

        durations = tx_stats["duration"]
        op_counts = tx_stats["operation_count"]
        success_rate = tx_stats["success_rate"]

        return {
            "total_transactions": len(durations),
            "avg_duration": sum(durations) / len(durations),
            "avg_operations": sum(op_counts) / len(op_counts),
            "success_rate": sum(success_rate) / len(success_rate) * 100
        }

    def _simplify_query(self, query: str) -> str:
        """Simplifie une requête pour le regroupement"""
        # Remplace les valeurs littérales par des placeholders
        query = re.sub(r"'[^']*'", "'?'", query)
        query = re.sub(r'"[^"]*"', '"?"', query)
        query = re.sub(r'\b\d+\b', '?', query)
        return query

    def reset_metrics(self):
        """Réinitialise toutes les métriques"""
        with self.lock:
            self.metrics.clear()
            self.detailed_metrics = {
                "query_execution": defaultdict(list),
                "node_operations": defaultdict(int),
                "edge_operations": defaultdict(int),
                "cache_operations": defaultdict(int),
                "transaction_stats": defaultdict(list)
            }
            self.last_reset = datetime.now()

class IndexManager:
    """Gestionnaire des index"""
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.indexes: Dict[str, Dict[str, Set[Node]]] = defaultdict(lambda: defaultdict(set))
        self.unique_indexes: Set[str] = set()
        self.fulltext_indexes: Dict[str, Dict[str, Set[Node]]] = defaultdict(lambda: defaultdict(set))
        self.composite_indexes: Dict[Tuple[str, ...], Dict[Tuple, Set[Node]]] = {}
        self._lock = threading.RLock()

    def create_index(self, property_name: str, unique: bool = False):
        """Crée un index sur une propriété"""
        with self._lock:
            if unique:
                self.unique_indexes.add(property_name)
            
            # Indexe les nœuds existants
            for node in self.graph.nodes.values():
                value = node.get_property(property_name)
                if value is not None:
                    self.indexes[property_name][str(value)].add(node)
                    
            logger.info(f"Created {'unique ' if unique else ''}index on {property_name}")

    def rebuild_indexes(self):
        """Reconstruit tous les index"""
        with self._lock:
            # Sauvegarde des définitions d'index
            index_definitions = {
                prop: prop in self.unique_indexes
                for prop in self.indexes.keys()
            }
            fulltext_props = set(self.fulltext_indexes.keys())
            composite_props = {
                props: index
                for props, index in self.composite_indexes.items()
            }
            
            # Réinitialise tous les index
            self.indexes.clear()
            self.fulltext_indexes.clear()
            self.composite_indexes.clear()
            
            # Reconstruit les index simples
            for prop_name, is_unique in index_definitions.items():
                self.create_index(prop_name, is_unique)
                
            # Reconstruit les index fulltext
            for prop_name in fulltext_props:
                self.create_fulltext_index(prop_name)
                
            # Reconstruit les index composites
            for props in composite_props:
                self.create_composite_index(list(props))
                
            logger.info("All indexes rebuilt successfully")
            return {
                "rebuilt_indexes": len(index_definitions),
                "rebuilt_fulltext": len(fulltext_props),
                "rebuilt_composite": len(composite_props)
            }
    
    def create_fulltext_index(self, property_name: str):
        """Crée un index full-text"""
        with self._lock:
            # Indexe les nœuds existants
            for node in self.graph.nodes.values():
                text = str(node.get_property(property_name) or "")
                words = self._tokenize_text(text)
                for word in words:
                    self.fulltext_indexes[property_name][word.lower()].add(node)
                    
            logger.info(f"Created full-text index on {property_name}")

    def create_composite_index(self, property_names: List[str]):
        """Crée un index composite sur plusieurs propriétés"""
        with self._lock:
            key = tuple(sorted(property_names))
            self.composite_indexes[key] = defaultdict(set)
            
            # Indexe les nœuds existants
            for node in self.graph.nodes.values():
                values = tuple(node.get_property(prop) for prop in key)
                if all(v is not None for v in values):
                    self.composite_indexes[key][values].add(node)
                    
            logger.info(f"Created composite index on {property_names}")

    def update_indexes(self, node: Node, old_props: Dict[str, Any], new_props: Dict[str, Any]):
        """Met à jour les index pour un nœud modifié"""
        with self._lock:
            # Met à jour les index simples
            for prop_name in set(old_props) | set(new_props):
                if prop_name in self.indexes:
                    old_value = old_props.get(prop_name)
                    new_value = new_props.get(prop_name)
                    
                    if old_value is not None:
                        self.indexes[prop_name][str(old_value)].discard(node)
                    if new_value is not None:
                        self.indexes[prop_name][str(new_value)].add(node)

            # Met à jour les index full-text
            for prop_name in self.fulltext_indexes:
                if prop_name in old_props or prop_name in new_props:
                    old_text = str(old_props.get(prop_name) or "")
                    new_text = str(new_props.get(prop_name) or "")
                    
                    old_words = self._tokenize_text(old_text)
                    new_words = self._tokenize_text(new_text)
                    
                    # Retire les anciens mots
                    for word in old_words:
                        self.fulltext_indexes[prop_name][word.lower()].discard(node)
                    
                    # Ajoute les nouveaux mots
                    for word in new_words:
                        self.fulltext_indexes[prop_name][word.lower()].add(node)

            # Met à jour les index composites
            for properties, index in self.composite_indexes.items():
                old_values = tuple(old_props.get(prop) for prop in properties)
                new_values = tuple(new_props.get(prop) for prop in properties)
                
                if all(v is not None for v in old_values):
                    index[old_values].discard(node)
                if all(v is not None for v in new_values):
                    index[new_values].add(node)

    def _tokenize_text(self, text: str) -> Set[str]:
        """Tokenize un texte pour l'index full-text"""
        return set(re.findall(r'\w+', text.lower()))

    def query_index(self, property_name: str, value: Any) -> Set[Node]:
        """Interroge un index"""
        with self._lock:
            return self.indexes[property_name][str(value)]

    def query_fulltext(self, property_name: str, text: str) -> Set[Node]:
        """Interroge un index full-text"""
        with self._lock:
            words = self._tokenize_text(text)
            if not words:
                return set()
            
            # Commence avec tous les nœuds du premier mot
            result = self.fulltext_indexes[property_name][next(iter(words))]
            
            # Intersection avec les nœuds des autres mots
            for word in words:
                result &= self.fulltext_indexes[property_name][word]
                
            return result

    def query_composite(self, properties: List[str], values: List[Any]) -> Set[Node]:
        """Interroge un index composite"""
        with self._lock:
            key = tuple(sorted(properties))
            if key not in self.composite_indexes:
                raise KeyError(f"No composite index found for {properties}")
                
            return self.composite_indexes[key][tuple(values)]

    def get_index_stats(self) -> Dict[str, Dict[str, Any]]:
        """Retourne les statistiques des index"""
        stats = {}
        
        with self._lock:
            # Stats des index simples
            for prop, index in self.indexes.items():
                stats[prop] = {
                    "type": "simple",
                    "unique": prop in self.unique_indexes,
                    "entries": sum(len(nodes) for nodes in index.values()),
                    "distinct_values": len(index)
                }
            
            # Stats des index full-text
            for prop, index in self.fulltext_indexes.items():
                stats[f"fulltext_{prop}"] = {
                    "type": "fulltext",
                    "entries": sum(len(nodes) for nodes in index.values()),
                    "distinct_words": len(index)
                }
            
            # Stats des index composites
            for props, index in self.composite_indexes.items():
                stats[f"composite_{'_'.join(props)}"] = {
                    "type": "composite",
                    "properties": props,
                    "entries": sum(len(nodes) for nodes in index.values()),
                    "distinct_combinations": len(index)
                }
                
        return stats
    
class TransactionLog:
    """Journal des transactions amélioré"""
    def __init__(self):
        self.log: List[Dict[str, Any]] = []
        self.lock = threading.Lock()

    def log_commit(self, transaction: Transaction):
        """Enregistre un commit avec détails"""
        with self.lock:
            self.log.append({
                "type": "commit",
                "transaction_id": transaction.id,
                "timestamp": datetime.now().isoformat(),
                "operations": len(transaction.operations),
                "isolation_level": transaction.isolation_level.name,
                "duration": (datetime.now() - transaction.start_time).total_seconds(),
                "changes": transaction.changelog.changes
            })

    def log_rollback(self, transaction: Transaction):
        """Enregistre un rollback avec détails"""
        with self.lock:
            self.log.append({
                "type": "rollback",
                "transaction_id": transaction.id,
                "timestamp": datetime.now().isoformat(),
                "operations": len(transaction.operations),
                "isolation_level": transaction.isolation_level.name,
                "duration": (datetime.now() - transaction.start_time).total_seconds(),
                "reason": "user_initiated"  # Peut être modifié pour inclure la raison
            })

    def get_transaction_history(self, 
                              start_time: datetime = None, 
                              end_time: datetime = None) -> List[Dict[str, Any]]:
        """Récupère l'historique des transactions avec filtres"""
        with self.lock:
            if not (start_time or end_time):
                return self.log.copy()
            
            filtered_log = []
            for entry in self.log:
                entry_time = datetime.fromisoformat(entry["timestamp"])
                if (not start_time or entry_time >= start_time) and \
                   (not end_time or entry_time <= end_time):
                    filtered_log.append(entry)
            
            return filtered_log
        
class TransactionManager:
    """Gestionnaire de transactions avancé"""
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        self.active_transactions: Dict[str, Transaction] = {}
        self.locks: Dict[str, threading.Lock] = {}
        self.lock_owners: Dict[str, str] = {}
        self.deadlock_detector = DeadlockDetector()
        self.transaction_log = TransactionLog()
        self.manager_lock = threading.RLock()

    def register_transaction(self, transaction: Transaction):
        """Enregistre une nouvelle transaction"""
        with self.manager_lock:
            self.active_transactions[transaction.id] = transaction

    def begin_transaction(self, 
                         isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> Transaction:
        """Démarre une nouvelle transaction"""
        tx = Transaction(self.graph, isolation_level)
        self.register_transaction(tx)
        logger.info(f"Started transaction {tx.id} with isolation level {isolation_level.name}")
        return tx

    def acquire_lock(self, resource_id: str, transaction_id: str) -> bool:
        """Acquiert un lock pour une transaction"""
        with self.manager_lock:
            if resource_id not in self.locks:
                self.locks[resource_id] = threading.Lock()

            if self.locks[resource_id].acquire(blocking=False):
                self.lock_owners[resource_id] = transaction_id
                return True

            # Vérifie les deadlocks avant d'attendre
            if self.would_deadlock(transaction_id, resource_id):
                return False

            # Attend le lock
            self.locks[resource_id].acquire()
            self.lock_owners[resource_id] = transaction_id
            return True

    def release_lock(self, resource_id: str, transaction_id: str):
        """Libère un lock"""
        with self.manager_lock:
            if (resource_id in self.lock_owners and 
                self.lock_owners[resource_id] == transaction_id):
                self.locks[resource_id].release()
                del self.lock_owners[resource_id]

    def would_deadlock(self, transaction_id: str, resource_id: str) -> bool:
        """Vérifie si une attente de lock créerait un deadlock"""
        if resource_id in self.lock_owners:
            holding_tx = self.lock_owners[resource_id]
            return self.deadlock_detector.would_create_cycle(transaction_id, holding_tx)
        return False

    def log_commit(self, transaction: Transaction):
        """Enregistre un commit dans le journal"""
        self.transaction_log.log_commit(transaction)

    def log_rollback(self, transaction: Transaction):
        """Enregistre un rollback dans le journal"""
        self.transaction_log.log_rollback(transaction)

    def cleanup_transaction(self, transaction: Transaction):
        """Nettoie les ressources d'une transaction"""
        with self.manager_lock:
            # Libère tous les verrous
            for resource_id in list(self.lock_owners.keys()):
                if self.lock_owners[resource_id] == transaction.id:
                    self.release_lock(resource_id, transaction.id)
            
            # Supprime la transaction de la liste des transactions actives
            if transaction.id in self.active_transactions:
                del self.active_transactions[transaction.id]
                
            # Force la libération des ressources
            transaction._active = False
            transaction.committed = False
            
            logger.info(f"Cleaned up transaction {transaction.id}")

class GraphAnalytics:
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph

    def detect_communities(self, algorithm: str = "louvain") -> Dict[str, Set[str]]:
        """Détecte les communautés dans le graphe"""
        if algorithm == "louvain":
            return self._louvain_community_detection()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

    def _louvain_community_detection(self) -> Dict[str, Set[str]]:
        """Version corrigée de la détection de communautés"""
        communities = {}
        nodes = list(self.graph.nodes.values())
        
        # Initialisation
        for node in nodes:
            node._properties['community'] = node.node_id
            communities[node.node_id] = {node.node_id}
        
        improvement = True
        while improvement:
            improvement = False
            for node in nodes:
                old_community = node._properties['community']
                best_community = self._find_best_community(node)
                
                if best_community != old_community:
                    # Met à jour la communauté
                    communities[old_community].remove(node.node_id)
                    if best_community not in communities:
                        communities[best_community] = set()
                    communities[best_community].add(node.node_id)
                    node._properties['community'] = best_community
                    improvement = True
                    
                    # Nettoie les communautés vides
                    if not communities[old_community]:
                        del communities[old_community]
        
        return {k: v for k, v in communities.items() if len(v) > 0}

    def calculate_centrality(self, metric: str = "betweenness") -> Dict[str, float]:
        """Calcule la centralité des nœuds"""
        if metric == "betweenness":
            return self._betweenness_centrality()
        elif metric == "eigenvector":
            return self._eigenvector_centrality()
        elif metric == "pagerank":
            return self._pagerank()
        else:
            raise ValueError(f"Unsupported centrality metric: {metric}")

    def _betweenness_centrality(self) -> Dict[str, float]:
        """Calcule la centralité d'intermédiarité"""
        centrality = defaultdict(float)
        nodes = list(self.graph.nodes.keys())
        
        for s in nodes:
            # Calcul des plus courts chemins depuis s
            stack = []
            pred = {w: [] for w in nodes}
            sigma = defaultdict(int)
            sigma[s] = 1
            d = {w: -1 for w in nodes}
            d[s] = 0
            queue = deque([s])
            
            while queue:
                v = queue.popleft()
                stack.append(v)
                node = self.graph.nodes[v]
                
                # Parcours des voisins
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        w = edge.target().node_id
                        if d[w] < 0:
                            queue.append(w)
                            d[w] = d[v] + 1
                        if d[w] == d[v] + 1:
                            sigma[w] += sigma[v]
                            pred[w].append(v)
            
            # Accumulation des dépendances
            delta = defaultdict(float)
            while stack:
                w = stack.pop()
                for v in pred[w]:
                    c = sigma[v] / sigma[w] * (1 + delta[w])
                    delta[v] += c
                if w != s:
                    centrality[w] += delta[w]
        
        return centrality

    def identify_influencers(self, method: str = "combined", 
                           threshold: float = 0.8) -> List[str]:
        """Identifie les nœuds influents du graphe"""
        scores = {}
        
        # Calcul des différentes métriques
        betweenness = self._betweenness_centrality()
        pagerank = self._pagerank()
        degree = self._calculate_degree_centrality()
        
        if method == "combined":
            # Combine les différentes métriques avec des poids
            for node_id in self.graph.nodes:
                scores[node_id] = (
                    0.4 * betweenness.get(node_id, 0) +
                    0.4 * pagerank.get(node_id, 0) +
                    0.2 * degree.get(node_id, 0)
                )
        else:
            scores = betweenness  # Utilise uniquement la centralité d'intermédiarité
            
        # Sélectionne les nœuds au-dessus du seuil
        max_score = max(scores.values())
        threshold_value = max_score * threshold
        
        influencers = [
            node_id for node_id, score in scores.items()
            if score >= threshold_value
        ]
        
        return sorted(influencers, key=lambda x: scores[x], reverse=True)

    def _pagerank(self, damping: float = 0.85, max_iter: int = 100) -> Dict[str, float]:
        """Version corrigée de PageRank"""
        nodes = list(self.graph.nodes.keys())
        n = len(nodes)
        if n == 0:
            return {}
            
        # Initialisation avec normalisation
        rank = {node: 1.0/n for node in nodes}
        
        for _ in range(max_iter):
            new_rank = {node: (1 - damping) / n for node in nodes}
            
            for node_id in nodes:
                node = self.graph.nodes[node_id]
                out_degree = sum(len(edges) for edges in node.outgoing_edges.values())
                if out_degree > 0:
                    for edges in node.outgoing_edges.values():
                        for edge in edges:
                            if edge.is_valid():
                                target_id = edge.target().node_id
                                new_rank[target_id] += damping * rank[node_id] / out_degree
            
            # Normalisation pour éviter l'explosion des valeurs
            total = sum(new_rank.values())
            if total > 0:
                new_rank = {k: v/total for k, v in new_rank.items()}
                
            # Vérification de la convergence
            diff = sum(abs(new_rank[node] - rank[node]) for node in nodes)
            rank = new_rank
            
            if diff < 1e-6:
                break
                
        return rank

    def _calculate_degree_centrality(self) -> Dict[str, float]:
        """Calcule la centralité de degré"""
        max_degree = 0
        centrality = {}
        
        for node_id, node in self.graph.nodes.items():
            degree = sum(len(edges) for edges in node.outgoing_edges.values())
            centrality[node_id] = degree
            max_degree = max(max_degree, degree)
            
        # Normalisation
        if max_degree > 0:
            for node_id in centrality:
                centrality[node_id] /= max_degree
                
        return centrality

    def _calculate_modularity(self, nodes: List[Node]) -> float:
        """Calcule la modularité du graphe"""
        modularity = 0.0
        m = sum(len(edges) for node in nodes for edges in node.outgoing_edges.values())
        if m == 0:
            return 0.0
            
        # Calcul de la modularité
        for node in nodes:
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target = edge.target()
                        if target:
                            ki = len(node.outgoing_edges)
                            kj = len(target.outgoing_edges)
                            modularity += 1 - (ki * kj) / (2 * m)
                            
        return modularity / (2 * m)
    
    def _find_best_community(self, node: Node) -> str:
        """Trouve la meilleure communauté pour un nœud"""
        best_gain = 0
        best_community = str(node.node_id)  # Par défaut, sa propre communauté
        
        # Calcule le gain de modularité pour chaque communauté voisine
        neighbors = set()
        for edges in node.outgoing_edges.values():
            for edge in edges:
                if edge.is_valid():
                    target = edge.target()
                    if target:
                        neighbors.add(target.node_id)
        
        for neighbor_id in neighbors:
            gain = self._calculate_modularity_gain(node, neighbor_id)
            if gain > best_gain:
                best_gain = gain
                best_community = neighbor_id
                
        return best_community

    def _calculate_modularity_gain(self, node: Node, target_community: str) -> float:
        """Calcule le gain de modularité pour un déplacement de nœud"""
        ki = sum(len(edges) for edges in node.outgoing_edges.values())
        ki_in = sum(
            1 for edges in node.outgoing_edges.values()
            for edge in edges
            if edge.is_valid() and edge.target().node_id == target_community
        )
        m = sum(
            len(edges) for n in self.graph.nodes.values()
            for edges in n.outgoing_edges.values()
        )
        if m == 0:
            return 0
        
        return (ki_in / m) - (ki * ki) / (2 * m * m)

    def _move_node_to_community(self, node: Node, community: str):
        """Déplace un nœud vers une communauté"""
        node._properties['community'] = community

    # Algorithmes de détection de communautés de Leiden
    def leiden_community_detection(self, resolution: float = 1.0) -> Dict[str, Set[str]]:
        """Algorithme de Leiden amélioré avec gestion des erreurs"""
        communities = {}
        nodes = list(self.graph.nodes.values())
        
        # Initialisation
        for node in nodes:
            comm_id = str(uuid4())[:8]
            communities[comm_id] = {node.node_id}
            node._properties['community'] = comm_id
        
        for _ in range(5):  # Limite à 5 itérations
            changed = False
            for node in nodes:
                old_comm = node._properties['community']
                if old_comm not in communities:
                    continue
                    
                new_comm = self._find_leiden_community(node, communities, resolution)
                if new_comm != old_comm and new_comm in communities:
                    self._move_node(node, old_comm, new_comm, communities)
                    changed = True
                    
            if not changed:
                break
                
        return {k: v for k, v in communities.items() if v}  # Nettoie les communautés vides

    def _communities_equal(self, comm1: Dict[str, Set[str]], 
                        comm2: Dict[str, Set[str]]) -> bool:
        """Compare deux ensembles de communautés"""
        if len(comm1) != len(comm2):
            return False
            
        members1 = {n for s in comm1.values() for n in s}
        members2 = {n for s in comm2.values() for n in s}
        return members1 == members2
    
    def _local_moving_phase(self, communities: Dict[str, Set[str]], resolution: float) -> bool:
        """Phase d'optimisation locale de Leiden"""
        improvement = False
        nodes = list(self.graph.nodes.values())
        random.shuffle(nodes)  # Ordre aléatoire
        
        for node in nodes:
            old_community = node._properties['community']
            best_community = self._find_leiden_community(node, communities, resolution)
            
            if best_community != old_community:
                self._move_node(node, old_community, best_community, communities)
                improvement = True
                
        return improvement

    def _refinement_phase(self, communities: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        """Phase de raffinement de Leiden"""
        refined_communities = {}
        
        for comm_id, members in communities.items():
            subgroups = self._find_subgroups(members)
            for sg_id, sg_members in subgroups.items():
                refined_communities[f"{comm_id}_{sg_id}"] = sg_members
                
        return refined_communities

    def _find_subgroups(self, members: Set[str]) -> Dict[str, Set[str]]:
        """Trouve les sous-groupes dans une communauté"""
        subgraph_nodes = {node_id: self.graph.nodes[node_id] for node_id in members}
        subgroups = {}
        visited = set()
        
        for node_id in members:
            if node_id not in visited:
                subgroup = self._explore_subgroup(node_id, subgraph_nodes, visited)
                if subgroup:
                    sg_id = str(uuid4())[:8]
                    subgroups[sg_id] = subgroup
                    
        return subgroups

    def _explore_subgroup(self, start_node: str, nodes: Dict[str, Node], 
                         visited: Set[str]) -> Set[str]:
        """Explore un sous-groupe par DFS"""
        subgroup = set()
        stack = [start_node]
        
        while stack:
            node_id = stack.pop()
            if node_id not in visited:
                visited.add(node_id)
                subgroup.add(node_id)
                node = nodes[node_id]
                
                # Ajoute les voisins non visités
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        if edge.is_valid():
                            target = edge.target()
                            if target.node_id in nodes and target.node_id not in visited:
                                stack.append(target.node_id)
                                
        return subgroup

    def _find_leiden_community(self, node: Node, communities: Dict[str, Set[str]], 
                            resolution: float) -> str:
        """Version corrigée de la recherche de communauté Leiden"""
        current_community = node._properties['community']
        best_quality = self._calculate_quality(node, current_community, communities, resolution)
        best_community = current_community
        
        # Collecte les communautés voisines
        neighbor_communities = set()
        for edges in node.outgoing_edges.values():
            for edge in edges:
                if edge.is_valid():
                    target = edge.target()
                    neighbor_comm = target._properties.get('community')
                    if neighbor_comm in communities:  # Vérifie si la communauté existe
                        neighbor_communities.add(neighbor_comm)
                        
        # Évalue chaque communauté voisine
        for comm in neighbor_communities:
            quality = self._calculate_quality(node, comm, communities, resolution)
            if quality > best_quality:
                best_quality = quality
                best_community = comm
                
        return best_community

    def _calculate_quality(self, node: Node, community: str, 
                        communities: Dict[str, Set[str]], resolution: float) -> float:
        """Calcule la qualité d'une communauté pour un nœud"""
        if community not in communities:
            return float('-inf')  # Communauté invalide
            
        internal_edges = 0
        external_edges = 0
        
        for edges in node.outgoing_edges.values():
            for edge in edges:
                if edge.is_valid():
                    target = edge.target()
                    target_comm = target._properties.get('community')
                    if target_comm == community:
                        internal_edges += 1
                    else:
                        external_edges += 1
                        
        community_size = len(communities[community])
        total_edges = internal_edges + external_edges
        
        if total_edges == 0:
            return 0
            
        return (internal_edges / total_edges) - (resolution * community_size / 
            (2 * max(1, self.graph.statistics["edges_count"])))

    def _move_node(self, node: Node, old_community: str, new_community: str, 
                  communities: Dict[str, Set[str]]):
        """Déplace un nœud vers une nouvelle communauté"""
        communities[old_community].remove(node.node_id)
        if new_community not in communities:
            communities[new_community] = set()
        communities[new_community].add(node.node_id)
        node._properties['community'] = new_community
        
        if not communities[old_community]:
            del communities[old_community]

    def _aggregation_phase(self, communities: Dict[str, Set[str]]) -> bool:
        """Phase d'agrégation de Leiden"""
        if len(communities) <= 1:
            return False
            
        merged = False
        pairs = list(itertools.combinations(communities.keys(), 2))
        random.shuffle(pairs)
        
        for comm1, comm2 in pairs:
            if comm1 in communities and comm2 in communities:
                if self._should_merge(comm1, comm2, communities):
                    self._merge_communities(comm1, comm2, communities)
                    merged = True
                    
        return merged

    def _should_merge(self, comm1: str, comm2: str, 
                     communities: Dict[str, Set[str]]) -> bool:
        """Décide si deux communautés doivent être fusionnées"""
        edges_between = self._count_edges_between(comm1, comm2, communities)
        possible_edges = len(communities[comm1]) * len(communities[comm2])
        
        if possible_edges == 0:
            return False
            
        density = edges_between / possible_edges
        return density > 0.5  # Seuil arbitraire, à ajuster

    def _count_edges_between(self, comm1: str, comm2: str, 
                           communities: Dict[str, Set[str]]) -> int:
        """Compte les arêtes entre deux communautés"""
        count = 0
        for node_id in communities[comm1]:
            node = self.graph.nodes[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target = edge.target()
                        if target.node_id in communities[comm2]:
                            count += 1
        return count

    def _merge_communities(self, comm1: str, comm2: str, 
                         communities: Dict[str, Set[str]]):
        """Fusionne deux communautés"""
        members = communities[comm1] | communities[comm2]
        new_comm = str(uuid4())[:8]
        communities[new_comm] = members
        
        for node_id in members:
            self.graph.nodes[node_id]._properties['community'] = new_comm
            
        del communities[comm1]
        del communities[comm2]

    def calculate_modularity(self) -> float:
        """Calcule la modularité globale du graphe"""
        total_edges = self.graph.statistics["edges_count"]
        if total_edges == 0:
            return 0
            
        Q = 0
        for node in self.graph.nodes.values():
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        target = edge.target()
                        if node._properties.get('community') == target._properties.get('community'):
                            Q += 1 - (len(node.outgoing_edges) * len(target.outgoing_edges)) / (2 * total_edges)
                            
        return Q / (2 * total_edges)

    def get_community_stats(self) -> Dict[str, Any]:
        """Retourne des statistiques sur les communautés"""
        stats = {
            "number_of_communities": 0,
            "avg_size": 0,
            "max_size": 0,
            "min_size": float('inf'),
            "modularity": self.calculate_modularity(),
            "communities": {}
        }
        
        communities = {}
        for node in self.graph.nodes.values():
            comm = node._properties.get('community')
            if comm:
                if comm not in communities:
                    communities[comm] = set()
                communities[comm].add(node.node_id)
        
        if communities:
            sizes = [len(members) for members in communities.values()]
            stats.update({
                "number_of_communities": len(communities),
                "avg_size": sum(sizes) / len(sizes),
                "max_size": max(sizes),
                "min_size": min(sizes),
                "communities": {
                    comm: {
                        "size": len(members),
                        "members": list(members)
                    } for comm, members in communities.items()
                }
            })
            
        return stats
    
    def dfs(self, start_node_id: str) -> List[str]:
        visited = set()
        path = []
        
        def dfs_recursive(node_id):
            visited.add(node_id)
            path.append(node_id)
            node = self.graph.nodes[node_id]
            
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        next_id = edge.target().node_id
                        if next_id not in visited:
                            dfs_recursive(next_id)
                            
        dfs_recursive(start_node_id)
        return path

    def bfs(self, start_node_id: str) -> List[str]:
        visited = {start_node_id}
        queue = deque([start_node_id])
        path = []
        
        while queue:
            node_id = queue.popleft()
            path.append(node_id)
            node = self.graph.nodes[node_id]
            
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        next_id = edge.target().node_id
                        if next_id not in visited:
                            visited.add(next_id)
                            queue.append(next_id)
        return path

    def shortest_path(self, start_id: str, end_id: str) -> List[str]:
        queue = deque([(start_id, [start_id])])
        visited = {start_id}
        
        while queue:
            node_id, path = queue.popleft()
            if node_id == end_id:
                return path
                
            node = self.graph.nodes[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        next_id = edge.target().node_id
                        if next_id not in visited:
                            visited.add(next_id)
                            queue.append((next_id, path + [next_id]))
        return []

    def strongly_connected_components(self) -> List[Set[str]]:
        """Algorithme de Kosaraju"""
        def dfs_forward(node_id, visited, order):
            visited.add(node_id)
            node = self.graph.nodes[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        next_id = edge.target().node_id
                        if next_id not in visited:
                            dfs_forward(next_id, visited, order)
            order.append(node_id)

        def dfs_reverse(node_id, visited, component):
            visited.add(node_id)
            component.add(node_id)
            node = self.graph.nodes[node_id]
            for edges in node.incoming_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        prev_id = edge.source().node_id
                        if prev_id not in visited:
                            dfs_reverse(prev_id, visited, component)

        # Première passe
        visited = set()
        order = []
        for node_id in self.graph.nodes:
            if node_id not in visited:
                dfs_forward(node_id, visited, order)

        # Deuxième passe
        visited.clear()
        components = []
        for node_id in reversed(order):
            if node_id not in visited:
                component = set()
                dfs_reverse(node_id, visited, component)
                components.append(component)
        
        return components

    def minimum_spanning_tree(self, weight_property: str = "weight") -> List[Edge]:
        """Algorithme de Kruskal"""
        edges = []
        for node in self.graph.nodes.values():
            for edge_list in node.outgoing_edges.values():
                edges.extend(edge_list)
        
        edges.sort(key=lambda e: e.get_property(weight_property, 0))
        mst = []
        disjoint_set = {node_id: node_id for node_id in self.graph.nodes}

        def find(x):
            if disjoint_set[x] != x:
                disjoint_set[x] = find(disjoint_set[x])
            return disjoint_set[x]

        def union(x, y):
            disjoint_set[find(x)] = find(y)

        for edge in edges:
            if edge.is_valid():
                source = edge.source().node_id
                target = edge.target().node_id
                if find(source) != find(target):
                    union(source, target)
                    mst.append(edge)

        return mst

    def detect_cycles(self) -> List[List[str]]:
        """Détecte tous les cycles dans le graphe"""
        cycles = []
        visited = set()
        
        def find_cycles(node_id: str, path: List[str], start_id: str):
            if node_id in path[1:]:
                cycle = path[path.index(node_id):]
                cycles.append(cycle)
                return
                
            if node_id in visited:
                return
                
            visited.add(node_id)
            path.append(node_id)
            
            node = self.graph.nodes[node_id]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        next_id = edge.target().node_id
                        if next_id == start_id and len(path) > 2:
                            cycles.append(path + [start_id])
                        elif next_id not in path:
                            find_cycles(next_id, path.copy(), start_id)
                            
            path.pop()
            visited.remove(node_id)
        
        for node_id in self.graph.nodes:
            find_cycles(node_id, [], node_id)
            
        return cycles

    def graph_coloring(self) -> Dict[str, int]:
        """Coloration de graphe avec l'algorithme glouton"""
        colors = {}
        degree = {
            node_id: sum(len(edges) for edges in node.outgoing_edges.values())
            for node_id, node in self.graph.nodes.items()
        }
        
        nodes = sorted(self.graph.nodes.keys(), 
                      key=lambda x: degree[x], reverse=True)
        
        for node_id in nodes:
            used_colors = set()
            node = self.graph.nodes[node_id]
            
            # Collecte des couleurs voisines
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        neighbor_id = edge.target().node_id
                        if neighbor_id in colors:
                            used_colors.add(colors[neighbor_id])
            
            # Trouve la première couleur disponible
            color = 0
            while color in used_colors:
                color += 1
            colors[node_id] = color
            
        return colors

    def k_core_decomposition(self) -> Dict[str, int]:
        """Décomposition k-core du graphe"""
        degrees = {}
        node_by_degree = defaultdict(set)
        max_degree = 0
        
        # Initialisation
        for node_id, node in self.graph.nodes.items():
            degree = sum(len(edges) for edges in node.outgoing_edges.values())
            degrees[node_id] = degree
            node_by_degree[degree].add(node_id)
            max_degree = max(max_degree, degree)
        
        k_cores = {}
        k = 0
        
        while any(node_by_degree.values()):
            while not node_by_degree[k] and k <= max_degree:
                k += 1
            if k > max_degree:
                break
                
            while node_by_degree[k]:
                node_id = node_by_degree[k].pop()
                k_cores[node_id] = k
                
                # Met à jour les voisins
                node = self.graph.nodes[node_id]
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        if edge.is_valid():
                            neighbor_id = edge.target().node_id
                            if neighbor_id not in k_cores:
                                old_degree = degrees[neighbor_id]
                                new_degree = old_degree - 1
                                degrees[neighbor_id] = new_degree
                                node_by_degree[old_degree].remove(neighbor_id)
                                node_by_degree[new_degree].add(neighbor_id)
                                
        return k_cores

    def random_walk(self, start_node_id: str, steps: int = 10) -> List[str]:
        """Effectue une marche aléatoire dans le graphe"""
        path = [start_node_id]
        current_id = start_node_id
        
        for _ in range(steps):
            node = self.graph.nodes[current_id]
            neighbors = []
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        neighbors.append(edge.target().node_id)
                        
            if not neighbors:
                break
                
            current_id = random.choice(neighbors)
            path.append(current_id)
            
        return path

    def clustering_coefficient(self, node_id: str = None) -> Union[float, Dict[str, float]]:
        """Calcule le coefficient de clustering"""
        def calculate_node_coefficient(nid: str) -> float:
            node = self.graph.nodes[nid]
            neighbors = set()
            
            # Collecte les voisins
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        neighbors.add(edge.target().node_id)
                        
            if len(neighbors) < 2:
                return 0
                
            max_edges = len(neighbors) * (len(neighbors) - 1) / 2
            actual_edges = 0
            
            # Compte les connexions entre voisins
            for n1 in neighbors:
                node1 = self.graph.nodes[n1]
                for n2 in neighbors:
                    if n1 < n2:  # Évite le double comptage
                        for edges in node1.outgoing_edges.values():
                            for edge in edges:
                                if edge.is_valid() and edge.target().node_id == n2:
                                    actual_edges += 1
                                    
            return actual_edges / max_edges if max_edges > 0 else 0
        
        if node_id:
            return calculate_node_coefficient(node_id)
        
        # Calcul global
        coefficients = {
            nid: calculate_node_coefficient(nid)
            for nid in self.graph.nodes
        }
        return coefficients
    
    def astar_path(self, start_id: str, end_id: str) -> List[str]:
        """A* pathfinding avec distances euclidiennes comme heuristique"""
        def heuristic(node1_id: str, node2_id: str) -> float:
            node1 = self.graph.nodes[node1_id]
            node2 = self.graph.nodes[node2_id]
            x1, y1 = node1.get_property('x'), node1.get_property('y')
            x2, y2 = node2.get_property('x'), node2.get_property('y')
            return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
        
        open_set = {start_id}
        closed_set = set()
        
        came_from = {}
        g_score = {start_id: 0}
        f_score = {start_id: heuristic(start_id, end_id)}
        
        while open_set:
            current = min(open_set, key=lambda x: f_score[x])
            
            if current == end_id:
                path = []
                while current in came_from:
                    path.append(current)
                    current = came_from[current]
                path.append(start_id)
                return path[::-1]
            
            open_set.remove(current)
            closed_set.add(current)
            
            node = self.graph.nodes[current]
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        neighbor = edge.target().node_id
                        if neighbor in closed_set:
                            continue
                            
                        tentative_g_score = g_score[current] + edge.get_property('distance')
                        
                        if neighbor not in open_set:
                            open_set.add(neighbor)
                        elif tentative_g_score >= g_score.get(neighbor, float('inf')):
                            continue
                        
                        came_from[neighbor] = current
                        g_score[neighbor] = tentative_g_score
                        f_score[neighbor] = g_score[neighbor] + heuristic(neighbor, end_id)
        
        return []

    def find_biconnected_components(self) -> List[Set[str]]:
        """Détection optimisée des composantes biconnexes"""
        def dfs(u: str, parent: str = None) -> None:
            nonlocal time
            time += 1
            disc[u] = low[u] = time
            
            for edges in self.graph.nodes[u].outgoing_edges.values():
                for edge in edges:
                    if not edge.is_valid():
                        continue
                        
                    v = edge.target().node_id
                    if v not in disc:
                        stack.append((min(u, v), max(u, v)))  # Normalise l'ordre
                        dfs(v, u)
                        low[u] = min(low[u], low[v])
                        
                        if low[v] >= disc[u]:
                            component = set()
                            while stack and stack[-1] != (min(u, v), max(u, v)):
                                component.update(stack.pop())
                            if stack:
                                component.update(stack.pop())
                            if len(component) > 2:  # Ignore les composantes triviales
                                components.add(frozenset(component))
                    
                    elif v != parent and disc[v] < disc[u]:
                        stack.append((min(u, v), max(u, v)))
                        low[u] = min(low[u], disc[v])

        disc = {}
        low = {}
        stack = []
        components = set()
        time = 0
        
        for node_id in self.graph.nodes:
            if node_id not in disc:
                dfs(node_id)
        
        return [set(c) for c in components]

    def ford_fulkerson(self, source_id: str, sink_id: str) -> int:
        """Algorithme de Ford-Fulkerson pour le flux maximum"""
        def bfs(residual: Dict[str, Dict[str, int]]) -> List[str]:
            visited = {source_id}
            paths = {source_id: [source_id]}
            queue = deque([source_id])
            
            while queue:
                u = queue.popleft()
                for v in residual[u]:
                    if residual[u][v] > 0 and v not in visited:
                        visited.add(v)
                        paths[v] = paths[u] + [v]
                        if v == sink_id:
                            return paths[v]
                        queue.append(v)
            return []
        
        # Initialise le graphe résiduel
        residual = defaultdict(lambda: defaultdict(int))
        for node_id, node in self.graph.nodes.items():
            for edges in node.outgoing_edges.values():
                for edge in edges:
                    if edge.is_valid():
                        capacity = edge.get_property('capacity', 1)  # Capacité par défaut = 1
                        residual[node_id][edge.target().node_id] = capacity
        
        max_flow = 0
        path = bfs(residual)
        
        while path:
            flow = float('inf')
            for i in range(len(path)-1):
                flow = min(flow, residual[path[i]][path[i+1]])
            
            for i in range(len(path)-1):
                u, v = path[i], path[i+1]
                residual[u][v] -= flow
                residual[v][u] += flow
            
            max_flow += flow
            path = bfs(residual)
        
        return max_flow

    def is_subgraph_isomorphic(self, pattern: Dict[str, Set[str]]) -> List[Dict[str, str]]:
        """Détection optimisée d'isomorphisme avec déduplication"""
        def get_canonical_form(match: Dict[str, str]) -> frozenset:
            return frozenset(sorted(match.values()))

        def find_matches(current_match: Dict[str, str], used: Set[str]) -> None:
            if len(current_match) == len(pattern):
                canonical = get_canonical_form(current_match)
                if canonical not in unique_matches:
                    unique_matches.add(canonical)
                    matches.append(current_match.copy())
                return

            # Sélection du prochain nœud pattern avec le plus de contraintes
            next_pattern_node = min(
                (n for n in pattern if n not in current_match),
                key=lambda n: -len([x for x in pattern[n] if x in current_match])
            )

            # Filtrage des candidats valides
            pattern_neighbors = pattern[next_pattern_node]
            mapped_neighbors = {current_match[n] for n in pattern_neighbors if n in current_match}

            for node_id, node in self.graph.nodes.items():
                if node_id in used:
                    continue

                node_neighbors = {edge.target().node_id 
                                for edges in node.outgoing_edges.values()
                                for edge in edges if edge.is_valid()}

                if mapped_neighbors.issubset(node_neighbors):
                    current_match[next_pattern_node] = node_id
                    used.add(node_id)
                    find_matches(current_match, used)
                    used.remove(node_id)
                    current_match.pop(next_pattern_node)

        matches = []
        unique_matches = set()
        find_matches({}, set())
        return matches

class CypherConverter:
    """Gère la conversion entre le graphe et les requêtes Cypher"""
    
    def __init__(self, graph: 'GraphDB'):
        self.graph = graph
        
    def export_to_cypher(self) -> str:
        """Exporte tout le graphe en requêtes Cypher"""
        cypher_queries = []
        
        # Export du schéma
        cypher_queries.extend(self._export_schema())
        
        # Export des nœuds
        for node_id, node in self.graph.nodes.items():
            if not node._deleted:
                # Crée la requête CREATE pour le nœud
                labels = ':'.join(node.labels) if node.labels else ''
                properties = self._format_properties(node._properties)
                query = f"CREATE (n:{labels} {properties})"
                cypher_queries.append(query)
        
        # Export des relations
        for node_id, node in self.graph.nodes.items():
            if not node._deleted:
                for edge_type, edges in node.outgoing_edges.items():
                    for edge in edges:
                        if edge.is_valid():
                            target = edge.target()
                            properties = self._format_properties(edge._properties)
                            # Crée la requête MATCH...CREATE pour la relation
                            query = (
                                f"MATCH (a), (b) "
                                f"WHERE id(a) = '{node_id}' AND id(b) = '{target.node_id}' "
                                f"CREATE (a)-[r:{edge_type} {properties}]->(b)"
                            )
                            cypher_queries.append(query)
        
        return ';\n'.join(cypher_queries) + ';'
    
    def import_from_cypher(self, cypher_queries: str) -> Dict[str, Any]:
        """Importe des données depuis des requêtes Cypher"""
        results = {
            "nodes_created": 0,
            "relationships_created": 0,
            "constraints_created": 0,
            "errors": []
        }
        
        # Sépare les requêtes
        queries = [q.strip() for q in cypher_queries.split(';') if q.strip()]
        
        try:
            with self.graph.begin_transaction() as tx:
                for query in queries:
                    try:
                        if query.upper().startswith('CREATE CONSTRAINT'):
                            self._handle_constraint(query, results)
                        elif query.upper().startswith('CREATE INDEX'):
                            self._handle_index(query, results)
                        elif 'CREATE' in query.upper() and '-[' in query:
                            self._handle_relationship(query, tx, results)
                        elif query.upper().startswith('CREATE'):
                            self._handle_node(query, tx, results)
                    except Exception as e:
                        results["errors"].append(f"Error processing query: {query}\nError: {str(e)}")
                        
                if not results["errors"]:
                    tx.commit()
                else:
                    tx.rollback()
                    
        except Exception as e:
            results["errors"].append(f"Transaction error: {str(e)}")
            
        return results
    
    def import_from_memgraph(self, host: str = 'localhost', port: int = 7687,
                        username: str = None, password: str = None) -> Dict[str, Any]:
        """Importe des données depuis une instance Memgraph avec meilleure gestion des relations"""
        try:
            import mgclient
        except ImportError:
            raise ImportError("Please install mgclient package to use Memgraph import")
            
        results = {
            "nodes_imported": 0,
            "relationships_imported": 0,
            "errors": [],
            "debug_info": {}
        }
        
        try:
            # Connexion à Memgraph
            conn = mgclient.connect(host=host, port=port, username=username, password=password)
            cursor = conn.cursor()
            
            # Récupère et compte tous les nœuds
            cursor.execute("MATCH (n) RETURN count(n)")
            total_nodes = cursor.fetchone()[0]
            results["debug_info"]["total_nodes_in_memgraph"] = total_nodes
            
            # Récupère et compte toutes les relations
            cursor.execute("MATCH ()-[r]->() RETURN count(r)")
            total_relationships = cursor.fetchone()[0]
            results["debug_info"]["total_relationships_in_memgraph"] = total_relationships
            
            print(f"Found {total_nodes} nodes and {total_relationships} relationships in Memgraph")
            
            # Importe les données
            with self.graph.begin_transaction() as tx:
                # Récupère tous les nœuds avec leurs labels et propriétés
                cursor.execute("MATCH (n) RETURN id(n), labels(n), properties(n)")
                node_mapping = {}  # Pour mapper les IDs Memgraph aux nouveaux IDs
                
                for node_data in cursor.fetchall():
                    memgraph_id, labels, properties = node_data
                    try:
                        labels = set(labels)
                        created_node = self.graph.create_node(labels, properties)
                        node_mapping[memgraph_id] = created_node.node_id
                        results["nodes_imported"] += 1
                        
                        if results["nodes_imported"] % 1000 == 0:
                            print(f"Imported {results['nodes_imported']} nodes...")
                    except Exception as e:
                        results["errors"].append(f"Error importing node {memgraph_id}: {str(e)}")
                
                print("Starting relationship import...")
                
                # Récupère toutes les relations avec leurs propriétés
                cursor.execute("""
                    MATCH (start)-[r]->(end)
                    RETURN id(start), type(r), id(end), properties(r)
                """)
                
                for rel_data in cursor.fetchall():
                    try:
                        start_id, rel_type, end_id, properties = rel_data
                        # Utilise le mapping pour obtenir les nouveaux IDs
                        new_start_id = node_mapping.get(start_id)
                        new_end_id = node_mapping.get(end_id)
                        
                        if new_start_id and new_end_id:
                            self.graph.create_edge(new_start_id, new_end_id, rel_type, properties)
                            results["relationships_imported"] += 1
                            
                            if results["relationships_imported"] % 1000 == 0:
                                print(f"Imported {results['relationships_imported']} relationships...")
                        else:
                            results["errors"].append(
                                f"Missing node mapping for relationship: {start_id}->{end_id}"
                            )
                    except Exception as e:
                        results["errors"].append(
                            f"Error importing relationship {start_id}->{end_id}: {str(e)}"
                        )
                
                print("Import completed. Committing transaction...")
                tx.commit()
                
        except Exception as e:
            results["errors"].append(f"Memgraph import error: {str(e)}")
            print(f"Critical error during import: {str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()
                
        # Affiche les statistiques finales détaillées
        print("\nImport Statistics:")
        print(f"Nodes in Memgraph: {results['debug_info'].get('total_nodes_in_memgraph', 'unknown')}")
        print(f"Relationships in Memgraph: {results['debug_info'].get('total_relationships_in_memgraph', 'unknown')}")
        print(f"Nodes imported: {results['nodes_imported']}")
        print(f"Relationships imported: {results['relationships_imported']}")
        print(f"Errors encountered: {len(results['errors'])}")
        
        if results["errors"]:
            print("\nFirst 5 errors:")
            for error in results["errors"][:5]:
                print(f"- {error}")
                
        return results
    
    def _format_properties(self, props: Dict[str, Any]) -> str:
        """Formate les propriétés pour Cypher"""
        if not props:
            return '{}'
            
        formatted = []
        for key, value in props.items():
            if isinstance(value, str):
                formatted.append(f"{key}: '{value}'")
            elif isinstance(value, (list, set)):
                formatted.append(f"{key}: {list(value)}")
            elif isinstance(value, dict):
                formatted.append(f"{key}: {json.dumps(value)}")
            elif isinstance(value, datetime):
                formatted.append(f"{key}: datetime('{value.isoformat()}')")
            elif value is None:
                formatted.append(f"{key}: null")
            else:
                formatted.append(f"{key}: {value}")
                
        return '{' + ', '.join(formatted) + '}'
    
    def _export_schema(self) -> List[str]:
        """Exporte le schéma en requêtes Cypher"""
        queries = []
        
        # Export des contraintes
        for label, constraints in self.graph.schema.constraints.items():
            for constraint in constraints:
                if constraint.type == "unique":
                    queries.append(
                        f"CREATE CONSTRAINT ON (n:{label}) "
                        f"ASSERT n.{constraint.property} IS UNIQUE"
                    )
                elif constraint.type == "required":
                    queries.append(
                        f"CREATE CONSTRAINT ON (n:{label}) "
                        f"ASSERT exists(n.{constraint.property})"
                    )
        
        # Export des index
        for label, indexes in self.graph.schema.indexes.items():
            for property_name in indexes:
                queries.append(f"CREATE INDEX ON :{label}({property_name})")
                
        return queries
    
    def _handle_constraint(self, query: str, results: Dict[str, Any]):
        """Traite une requête de contrainte"""
        # Extrait le label et la propriété de la requête
        match = re.search(r"ON \((\w+):(\w+)\) ASSERT (\w+)\.(\w+)", query)
        if match:
            var, label, node_var, prop = match.groups()
            if "UNIQUE" in query:
                constraint = SchemaConstraint("unique", prop)
            else:
                constraint = SchemaConstraint("required", prop)
            self.graph.schema.add_constraint(label, constraint)
            results["constraints_created"] += 1
    
    def _handle_index(self, query: str, results: Dict[str, Any]):
        """Traite une requête d'index"""
        match = re.search(r"ON :(\w+)\((\w+)\)", query)
        if match:
            label, prop = match.groups()
            self.graph.index_manager.create_index(prop)
    
    def _handle_node(self, query: str, tx: 'Transaction', results: Dict[str, Any]):
        """Traite une requête de création de nœud"""
        # Extrait les labels et propriétés
        labels_match = re.findall(r':(\w+)', query)
        labels = set(labels_match)
        
        # Extrait les propriétés
        props_match = re.search(r'\{(.+?)\}', query)
        properties = {}
        if props_match:
            props_str = props_match.group(1)
            # Parse les propriétés
            pairs = re.findall(r'(\w+):\s*([^,}]+)', props_str)
            for key, value in pairs:
                try:
                    # Convertit la valeur en type Python approprié
                    properties[key] = eval(value)
                except:
                    properties[key] = value.strip("'\"")
        
        # Crée le nœud
        self.graph._create_node_with_transaction(labels, properties, tx)
        results["nodes_created"] += 1
    
    def _handle_relationship(self, query: str, tx: 'Transaction', results: Dict[str, Any]):
        """Traite une requête de création de relation"""
        # Extrait les IDs des nœuds source et cible
        ids_match = re.search(r"id\(a\)\s*=\s*'(.+?)'\s*AND\s*id\(b\)\s*=\s*'(.+?)'", query)
        if not ids_match:
            return
            
        source_id, target_id = ids_match.groups()
        
        # Extrait le type de relation
        type_match = re.search(r'\[r:(\w+)', query)
        if not type_match:
            return
            
        rel_type = type_match.group(1)
        
        # Extrait les propriétés
        props_match = re.search(r'\{(.+?)\}', query)
        properties = {}
        if props_match:
            props_str = props_match.group(1)
            pairs = re.findall(r'(\w+):\s*([^,}]+)', props_str)
            for key, value in pairs:
                try:
                    properties[key] = eval(value)
                except:
                    properties[key] = value.strip("'\"")
        
        # Crée la relation
        self.graph._create_edge_with_transaction(source_id, target_id, rel_type, properties, tx)
        results["relationships_created"] += 1

class GraphDB:
    """Base de données graphe avec fonctionnalités avancées"""
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.edge_types: Set[str] = set()
        self.schema = Schema()
        self.lock = threading.RLock()
        self.index_manager = IndexManager(self)
        self.stored_procedures: Dict[str, StoredProcedure] = {}
        self.transaction_manager = TransactionManager(self)
        self.backup_manager = BackupManager(self)
        self.performance_monitor = PerformanceMonitor()
        self.schema_version_manager = SchemaVersionManager()
        self.schema_rollback_manager = SchemaRollbackManager(self)
        self.analytics = GraphAnalytics(self)
        self.pattern_matcher = PatternMatcher(self)
        self.temporal_manager = TemporalManager(self)
        self.hypergraph_manager = HypergraphManager(self)
        self.cypher_converter = CypherConverter(self)
        self.gnn_manager = GNNManager(self)
        # Cache multi-niveaux
        self.cache = SmartCache()
        self.binary_storage = BinaryStorageManager("data/binary_storage")
        
        # Statistiques et monitoring
        self.statistics = {
            "nodes_count": 0,
            "edges_count": 0,
            "labels_count": defaultdict(int),
            "edge_types_count": defaultdict(int),
            "last_updated": datetime.now().isoformat(),
            "query_stats": {
                "total_queries": 0,
                "cached_hits": 0,
                "avg_execution_time": 0
            },
            "cache_stats": {
                "hits": 0,
                "misses": 0,
                "memory_usage": 0
            }
        }
        
        # Initialisation
        self.setup_initial_indexes()
        logger.info("GraphDB initialized with advanced features")

    def export_to_cypher(self, file_path: str = None) -> str:
        """
        Exporte le graphe en requêtes Cypher.
        
        Args:
            file_path: Chemin du fichier où sauvegarder les requêtes (optionnel)
            
        Returns:
            str: Les requêtes Cypher générées
        """
        cypher_queries = self.cypher_converter.export_to_cypher()
        
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(cypher_queries)
                
        return cypher_queries
    
    def import_from_cypher(self, source: Union[str, Path]) -> Dict[str, Any]:
        """
        Importe des données depuis des requêtes Cypher.
        
        Args:
            source: Chemin du fichier Cypher ou string contenant les requêtes
            
        Returns:
            Dict contenant les statistiques d'import
        """
        if isinstance(source, (str, Path)) and os.path.exists(str(source)):
            with open(str(source), 'r', encoding='utf-8') as f:
                cypher_queries = f.read()
        else:
            cypher_queries = source
            
        return self.cypher_converter.import_from_cypher(cypher_queries)
    
    def import_from_memgraph(self, host: str = 'localhost', port: int = 7687,
                           username: str = None, password: str = None) -> Dict[str, Any]:
        """
        Importe des données depuis une instance Memgraph.
        
        Args:
            host: Hôte Memgraph
            port: Port Memgraph
            username: Nom d'utilisateur (optionnel)
            password: Mot de passe (optionnel)
            
        Returns:
            Dict contenant les statistiques d'import
        """
        return self.cypher_converter.import_from_memgraph(host, port, username, password)
    
    # Méthodes pour Pattern Matcher
    def learn_graph_patterns(self) -> Dict[str, Any]:
        """Apprend et retourne les motifs récurrents dans le graphe"""
        return self.pattern_matcher.learn_patterns()

    def find_similar_patterns(self, seed_pattern: Dict[str, Any], 
                            similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Trouve des motifs similaires à un motif donné"""
        return self.pattern_matcher.find_similar_patterns(seed_pattern, similarity_threshold)

    # Méthodes pour Gestion Temporelle
    def create_temporal_snapshot(self, timestamp: datetime = None) -> str:
        """Crée un snapshot temporel du graphe"""
        return self.temporal_manager.create_snapshot(timestamp)

    def predict_graph_evolution(self, window: timedelta = None) -> Dict[str, Any]:
        """Prédit l'évolution future du graphe"""
        return self.temporal_manager.predict_evolution(window)

    def reconstruct_historical_state(self, timestamp: datetime) -> Dict[str, Any]:
        """Reconstruit l'état historique du graphe"""
        return self.temporal_manager.reconstruct_state(timestamp)

    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyse les motifs temporels dans l'évolution du graphe"""
        return self.temporal_manager.analyze_temporal_patterns()

    # Méthodes pour Hypergraphes
    def create_hyperedge(self, nodes: List[str], edge_type: str, 
                        properties: Dict[str, Any] = None) -> str:
        """Crée une hyperarête connectant plusieurs nœuds"""
        return self.hypergraph_manager.create_hyperedge(nodes, edge_type, properties)

    def get_hyperedge(self, hyperedge_id: str) -> Dict[str, Any]:
        """Récupère une hyperarête par son ID"""
        return self.hypergraph_manager.get_hyperedge(hyperedge_id)

    def get_node_hyperedges(self, node_id: str) -> List[Dict[str, Any]]:
        """Récupère toutes les hyperarêtes connectées à un nœud"""
        return self.hypergraph_manager.get_node_hyperedges(node_id)

    def analyze_hypergraph(self) -> Dict[str, Any]:
        """Analyse la structure du hypergraphe"""
        return self.hypergraph_manager.analyze_hypergraph()

    def add_to_hyperedge(self, hyperedge_id: str, node_id: str):
        """Ajoute un nœud à une hyperarête existante"""
        self.hypergraph_manager.add_node_to_hyperedge(hyperedge_id, node_id)

    def remove_from_hyperedge(self, hyperedge_id: str, node_id: str):
        """Retire un nœud d'une hyperarête"""
        self.hypergraph_manager.remove_node_from_hyperedge(hyperedge_id, node_id)

    # Méthode utilitaire pour l'analyse globale
    def analyze_graph_structure(self) -> Dict[str, Any]:
        """Analyse complète de la structure du graphe incluant les nouvelles fonctionnalités"""
        analysis = {
            'basic_metrics': {
                'nodes': len(self.nodes),
                'edges': self.statistics['edges_count'],
                'hyperedges': len(self.hypergraph_manager.hyperedges)
            },
            'patterns': self.pattern_matcher.learn_patterns(),
            'temporal': self.temporal_manager.analyze_temporal_patterns(),
            'hypergraph': self.hypergraph_manager.analyze_hypergraph(),
            'timestamp': datetime.now().isoformat()
        }

        # Ajoute des métriques avancées
        analysis.update({
            'advanced_metrics': {
                'pattern_complexity': self._calculate_pattern_complexity(),
                'temporal_stability': self._calculate_temporal_stability(),
                'structural_cohesion': self._calculate_structural_cohesion()
            }
        })

        return analysis

    def _calculate_pattern_complexity(self) -> float:
        """Calcule la complexité des motifs dans le graphe"""
        patterns = self.pattern_matcher.patterns
        if not patterns:
            return 0.0

        complexity_scores = []
        for pattern_info in patterns.values():
            size_score = pattern_info['size'] / 10  # Normalise la taille
            frequency_score = pattern_info['frequency'] / max(1, len(self.nodes))
            complexity_scores.append(size_score * frequency_score)

        return sum(complexity_scores) / len(complexity_scores)

    def _calculate_temporal_stability(self) -> float:
        """Calcule la stabilité temporelle du graphe"""
        stability_metrics = self.temporal_manager._calculate_stability_metrics()
        return stability_metrics['overall_stability']

    def _calculate_structural_cohesion(self) -> float:
        """Calcule la cohésion structurelle du graphe"""
        # Combine les métriques de cohésion du graphe standard et du hypergraphe
        hypergraph_analysis = self.hypergraph_manager.analyze_hypergraph()
        
        # Calcule la densité du graphe standard
        total_possible_edges = len(self.nodes) * (len(self.nodes) - 1) / 2
        graph_density = self.statistics['edges_count'] / max(1, total_possible_edges)
        
        # Calcule la densité du hypergraphe
        hypergraph_density = len(self.hypergraph_manager.hyperedges) / max(1, len(self.nodes))
        
        # Combine les métriques (moyenne pondérée)
        return (0.7 * graph_density + 0.3 * hypergraph_density)

    def export_graph_analysis(self, file_path: str):
        """Exporte une analyse complète du graphe vers un fichier JSON"""
        analysis = self.analyze_graph_structure()
        
        # Convertit l'analyse en format JSON-compatible
        json_analysis = self._convert_to_json_serializable(analysis)
        
        # Ajoute des métadonnées
        json_analysis['metadata'] = {
            'export_timestamp': datetime.now().isoformat(),
            'graph_version': self.schema.version,
            'analysis_version': '1.0'
        }
        
        # Sauvegarde l'analyse
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_analysis, f, indent=2, ensure_ascii=False)

    def _convert_to_json_serializable(self, obj):
        """Convertit récursivement un objet en format JSON-compatible"""
        if isinstance(obj, dict):
            return {key: self._convert_to_json_serializable(value) for key, value in obj.items()}
            
        elif isinstance(obj, (list, tuple)):
            return [self._convert_to_json_serializable(item) for item in list(obj)]
            
        elif isinstance(obj, set):
            return [self._convert_to_json_serializable(item) for item in list(obj)]
            
        elif isinstance(obj, defaultdict):
            return {key: self._convert_to_json_serializable(value) for key, value in dict(obj).items()}
            
        elif isinstance(obj, datetime):
            return obj.isoformat()
            
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
            
        elif hasattr(obj, '__dict__'):
            return self._convert_to_json_serializable(obj.__dict__)
            
        # Pour les types qui ne peuvent pas être convertis directement en JSON
        return str(obj)

    def import_temporal_data(self, data: Dict[str, Any]) -> bool:
        """Importe des données temporelles dans le graphe"""
        try:
            if 'snapshots' in data:
                for snapshot in data['snapshots']:
                    self.temporal_manager.temporal_snapshots[snapshot['id']] = snapshot
                    
            if 'evolution_models' in data:
                self.temporal_manager.evolution_models.update(data['evolution_models'])
                
            return True
        except Exception as e:
            logger.error(f"Error importing temporal data: {e}")
            return False
        
    def detect_communities(self, algorithm: str = "louvain") -> Dict[str, Set[str]]:
        return self.analytics.detect_communities(algorithm)

    def calculate_centrality(self, metric: str = "betweenness") -> Dict[str, float]:
        return self.analytics.calculate_centrality(metric)

    def identify_influencers(self, method: str = "combined", threshold: float = 0.8) -> List[str]:
        return self.analytics.identify_influencers(method, threshold)
    
    def setup_initial_indexes(self):
        """Configure les index initiaux"""
        self.index_manager.create_index("created_at")
        self.index_manager.create_index("updated_at")
        logger.info("Initial indexes created")

    def create_node(self, labels: Set[str], properties: Dict[str, Any] = None) -> Node:
        """Crée un nouveau nœud avec ID automatique"""
        with self.lock:
            node_id = str(uuid4())
            return self._create_node_internal(node_id, labels, properties or {})

    def _create_node_internal(self, node_id: str, labels: Set[str], properties: Dict[str, Any]) -> Node:
        """Création interne d'un nœud avec validation"""
        with self.lock:
            # Validation du schéma
            for label in labels:
                if not self.schema.validate_node(label, properties, self):
                    raise ValueError(f"Node validation failed for label {label}")

            # Vérifie si le nœud existe
            if node_id in self.nodes:
                raise ValueError(f"Node {node_id} already exists")

            # Crée le nœud
            start_time = time.time()
            node = Node(node_id, labels, properties)
            self.nodes[node_id] = node

            # Met à jour les statistiques
            self.statistics["nodes_count"] += 1
            for label in labels:
                self.statistics["labels_count"][label] += 1

            # Met à jour les index
            self.index_manager.update_indexes(node, {}, properties)

            # Mesure les performances
            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_creation", duration)

            # Met en cache
            self.cache.put(f"node:{node_id}", node)

            logger.info(f"Created node: {node}")
            return node

    def create_edge(self, source_id: str, target_id: str, 
                   edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Crée une nouvelle relation"""
        return self._create_edge_internal(source_id, target_id, edge_type, properties)

    def _create_edge_internal(self, source_id: str, target_id: str, 
                            edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Création interne d'une relation avec validation"""
        with self.lock:
            source = self.nodes.get(source_id)
            target = self.nodes.get(target_id)
            
            if not (source and target):
                raise ValueError("Source or target node not found")

            start_time = time.time()
            
            # Crée la relation
            edge = Edge(source, target, edge_type, properties or {})
            
            # Met à jour les connexions
            source.outgoing_edges[edge_type].add(edge)
            target.incoming_edges[edge_type].add(edge)
            self.edge_types.add(edge_type)

            # Met à jour les statistiques
            self.statistics["edges_count"] += 1
            self.statistics["edge_types_count"][edge_type] += 1

            # Mesure les performances
            duration = time.time() - start_time
            self.performance_monitor.record_metric("edge_creation", duration)

            logger.info(f"Created edge: {edge}")
            return edge

    def create_relationship(self, source_id: str, target_id: str, 
                       edge_type: str, properties: Dict[str, Any] = None) -> Edge:
        """Alias pour create_edge pour une meilleure lisibilité"""
        return self.create_edge(source_id, target_id, edge_type, properties)
    
    def _update_node_internal(self, node_id: str, properties: Dict[str, Any]) -> Node:
        """Mise à jour interne d'un nœud"""
        with self.lock:
            node = self.nodes.get(node_id)
            if not node:
                raise ValueError(f"Node {node_id} not found")

            start_time = time.time()
            
            # Sauvegarde les anciennes propriétés
            old_properties = node._properties.copy()
            
            # Met à jour les propriétés
            for key, value in properties.items():
                node.set_property(key, value)
            
            # Met à jour les index
            self.index_manager.update_indexes(node, old_properties, node._properties)
            
            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_update", duration)
            
            return node

    def _delete_node_internal(self, node_id: str, force: bool = False):
        """Suppression interne d'un nœud"""
        with self.lock:
            node = self.nodes.get(node_id)
            if not node:
                raise ValueError(f"Node {node_id} not found")

            # Vérifie les relations
            has_edges = bool(node.incoming_edges or node.outgoing_edges)
            if has_edges and not force:
                raise ValueError("Node has relationships. Use force=True to delete anyway")

            start_time = time.time()

            # Supprime les relations si force=True
            if force:
                for edges in list(node.incoming_edges.values()):
                    for edge in list(edges):
                        self._delete_edge(edge)
                for edges in list(node.outgoing_edges.values()):
                    for edge in list(edges):
                        self._delete_edge(edge)

            # Met à jour les statistiques
            self.statistics["nodes_count"] -= 1
            for label in node.labels:
                self.statistics["labels_count"][label] -= 1

            # Met à jour les index
            self.index_manager.update_indexes(node, node._properties, {})

            # Supprime le nœud
            del self.nodes[node_id]

            duration = time.time() - start_time
            self.performance_monitor.record_metric("node_deletion", duration)

            # Invalide le cache
            self.cache.put(f"node:{node_id}", None)
            
            logger.info(f"Deleted node: {node_id}")

    def _merge_node_internal(self, labels: Set[str], match_properties: Dict[str, Any],
                           create_properties: Dict[str, Any] = None) -> Tuple[Node, bool]:
        """Fusion interne d'un nœud (création si n'existe pas)"""
        with self.lock:
            # Recherche les nœuds existants
            existing_nodes = self.find_nodes(labels, match_properties)
            
            if existing_nodes:
                # Met à jour le nœud existant si nécessaire
                node = existing_nodes[0]
                if create_properties:
                    self._update_node_internal(node.node_id, create_properties)
                return node, False
            else:
                # Crée un nouveau nœud
                properties = {**match_properties, **(create_properties or {})}
                node = self.create_node(labels, properties)
                return node, True

    def execute_stored_procedure(self, name: str, **kwargs) -> Any:
        """
        Exécute une procédure stockée avec les paramètres donnés.
        
        Args:
            name: Nom de la procédure stockée à exécuter
            **kwargs: Arguments à passer à la procédure
        
        Returns:
            Le résultat de l'exécution de la procédure
        
        Raises:
            ValueError: Si la procédure n'existe pas
        """
        if name not in self.stored_procedures:
            raise ValueError(f"Stored procedure '{name}' not found")
            
        procedure = self.stored_procedures[name]
        
        start_time = time.time()
        try:
            result = procedure.execute(self, **kwargs)
            duration = time.time() - start_time
            
            # Enregistrement des métriques de performance
            self.performance_monitor.record_metric(
                f"stored_procedure_{name}_execution_time", 
                duration
            )
            
            logger.info(f"Executed stored procedure {name} in {duration:.3f} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Error executing stored procedure {name}: {e}")
            raise

    def save_to_binary(self):
        """Sauvegarde le graphe au format binaire"""
        self.binary_storage.save_graph(self)

    def load_from_binary(self):
        """Charge le graphe depuis le format binaire"""
        self.binary_storage.load_graph(self)
        
    def backup(self) -> str:
        """
        Crée une sauvegarde complète de la base de données.
        
        Returns:
            str: Chemin du fichier de sauvegarde créé
        """
        try:
            start_time = time.time()
            
            # Obtient un verrou global pour assurer la cohérence
            with self.lock:
                backup_file = self.backup_manager.create_backup()
                
            duration = time.time() - start_time
            self.performance_monitor.record_metric("backup_duration", duration)
            
            logger.info(f"Created backup: {backup_file} in {duration:.2f} seconds")
            return backup_file
            
        except Exception as e:
            logger.error(f"Error during backup: {e}")
            raise

    def restore(self, backup_file: str):
        """
        Restaure la base de données depuis une sauvegarde.
        
        Args:
            backup_file: Chemin vers le fichier de sauvegarde
        """
        try:
            start_time = time.time()
            
            # Vérifie que le fichier existe
            if not os.path.exists(backup_file):
                raise ValueError(f"Backup file not found: {backup_file}")
                
            # Restaure depuis la sauvegarde
            self.backup_manager.restore_from_backup(backup_file)
            
            duration = time.time() - start_time
            self.performance_monitor.record_metric("restore_duration", duration)
            
            logger.info(f"Restored from backup: {backup_file} in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error during restore: {e}")
            raise

    def list_backups(self) -> List[str]:
        """
        Liste tous les fichiers de backup disponibles.
        
        Returns:
            List[str]: Liste des chemins des fichiers de backup
        """
        return self.backup_manager.list_backups()

    def get_backup_info(self, backup_file: str) -> Dict[str, Any]:
        """
        Obtient les informations sur une sauvegarde.
        
        Args:
            backup_file: Nom du fichier de sauvegarde
        
        Returns:
            Dict contenant les métadonnées de la sauvegarde
        """
        return self.backup_manager.get_backup_info(backup_file)

    def cleanup_old_backups(self, max_age_days: int = 30):
        """
        Nettoie les anciennes sauvegardes.
        
        Args:
            max_age_days: Âge maximum en jours des sauvegardes à conserver
        """
        self.backup_manager.cleanup_old_backups(max_age_days)
        
    def get_version(self, key: str) -> int:
        """Obtient la version d'une ressource"""
        if key.startswith("node:"):
            node_id = key.split(":", 1)[1]
            node = self.nodes.get(node_id)
            return node._version if node else -1
        return 0

    def optimize(self) -> Dict[str, Any]:
        """
        Optimise la base de données en effectuant diverses opérations de maintenance.
        
        Returns:
            Dict contenant les résultats des optimisations effectuées
        """
        start_time = time.time()
        results = {
            "optimizations": [],
            "improvements": {},
            "errors": []
        }

        try:
            with self.lock:
                # 1. Nettoyage des nœuds et relations marqués comme supprimés
                deleted_nodes = self._cleanup_deleted_nodes()
                results["optimizations"].append("deleted_nodes_cleanup")
                results["improvements"]["deleted_nodes_removed"] = deleted_nodes

                # 2. Optimisation des index
                index_stats = self._optimize_indexes()
                results["optimizations"].append("index_optimization")
                results["improvements"]["index_stats"] = index_stats

                # 3. Nettoyage et optimisation du cache
                cache_stats = self._optimize_cache()
                results["optimizations"].append("cache_optimization")
                results["improvements"]["cache_stats"] = cache_stats

                # 4. Compactage des structures de données
                compact_stats = self._compact_data_structures()
                results["optimizations"].append("data_compaction")
                results["improvements"]["compaction_stats"] = compact_stats

                # 5. Réorganisation des relations pour une meilleure performance
                edge_stats = self._optimize_edge_storage()
                results["optimizations"].append("edge_optimization")
                results["improvements"]["edge_stats"] = edge_stats

            duration = time.time() - start_time
            results["duration"] = duration
            results["timestamp"] = datetime.now().isoformat()

            # Enregistre les métriques de performance
            self.performance_monitor.record_metric("optimization_duration", duration)
            logger.info(f"Database optimization completed in {duration:.2f} seconds")

            return results

        except Exception as e:
            logger.error(f"Error during optimization: {e}")
            results["errors"].append(str(e))
            return results

    def _cleanup_deleted_nodes(self) -> int:
        """Nettoie les nœuds et relations marqués comme supprimés"""
        deleted_count = 0
        nodes_to_remove = []

        # Identifie les nœuds à supprimer
        for node_id, node in self.nodes.items():
            if node._deleted:
                nodes_to_remove.append(node_id)
                deleted_count += 1

        # Supprime les nœuds
        for node_id in nodes_to_remove:
            del self.nodes[node_id]

        return deleted_count

    def _optimize_indexes(self) -> Dict[str, Any]:
        """Optimise les index de la base de données"""
        stats = {
            "rebuilt_indexes": 0,
            "cleaned_entries": 0,
            "index_sizes": {}
        }

        # Reconstruit les index
        index_stats = self.index_manager.get_index_stats()
        for prop_name, index_info in index_stats.items():
            old_size = index_info.get("entries", 0)
            self.index_manager.create_index(prop_name)  # Recrée l'index
            new_stats = self.index_manager.get_index_stats()
            new_size = new_stats[prop_name]["entries"] if prop_name in new_stats else 0
            
            stats["rebuilt_indexes"] += 1
            stats["cleaned_entries"] += max(0, old_size - new_size)
            stats["index_sizes"][prop_name] = new_size

        return stats

    def _optimize_cache(self) -> Dict[str, Any]:
        """Optimise le cache"""
        stats = {
            "before": self.cache.get_stats(),
        }
        
        # Force un nettoyage complet du cache
        self.cache.clear()
        
        stats["after"] = self.cache.get_stats()
        stats["cleared_entries"] = stats["before"].get("entries", 0)

        return stats

    def _compact_data_structures(self) -> Dict[str, Any]:
        """Compacte les structures de données internes"""
        stats = {
            "nodes_before": len(self.nodes),
            "edge_types_before": len(self.edge_types)
        }

        # Compacte les structures
        self.nodes = {k: v for k, v in self.nodes.items() if not v._deleted}
        self.edge_types = {et for et in self.edge_types}

        stats["nodes_after"] = len(self.nodes)
        stats["edge_types_after"] = len(self.edge_types)
        stats["space_saved"] = stats["nodes_before"] - stats["nodes_after"]

        return stats

    def _optimize_edge_storage(self) -> Dict[str, Any]:
        """Optimise le stockage des relations"""
        stats = {
            "reorganized_edges": 0,
            "cleaned_references": 0
        }

        # Nettoie les références invalides dans les relations
        for node in self.nodes.values():
            # Optimise les relations sortantes
            for edge_type in list(node.outgoing_edges.keys()):
                valid_edges = {edge for edge in node.outgoing_edges[edge_type] if edge.is_valid()}
                stats["cleaned_references"] += len(node.outgoing_edges[edge_type]) - len(valid_edges)
                node.outgoing_edges[edge_type] = valid_edges
                stats["reorganized_edges"] += len(valid_edges)

            # Optimise les relations entrantes
            for edge_type in list(node.incoming_edges.keys()):
                valid_edges = {edge for edge in node.incoming_edges[edge_type] if edge.is_valid()}
                stats["cleaned_references"] += len(node.incoming_edges[edge_type]) - len(valid_edges)
                node.incoming_edges[edge_type] = valid_edges
                stats["reorganized_edges"] += len(valid_edges)

        return stats

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Génère un rapport détaillé des performances de la base de données.

        Returns:
            Dict contenant les métriques de performance diverses
        """
        report = {
            "performance_metrics": {
                "general_stats": self.get_statistics(),
                "cache": {
                    "stats": self.cache.get_stats(),
                    "configuration": {
                        "memory_cache_size": self.cache.memory_cache.capacity,
                        "off_heap_cache_size": self.cache.off_heap_cache.capacity,
                    }
                },
                "transactions": {
                    "stats": self.transaction_manager.transaction_log.get_transaction_history(),
                    "active_count": len(self.transaction_manager.active_transactions)
                },
                "operations": {
                    "node_operations": self.performance_monitor.detailed_metrics["node_operations"],
                    "edge_operations": self.performance_monitor.detailed_metrics["edge_operations"]
                },
                "query_performance": {
                    "execution_times": self.performance_monitor.detailed_metrics["query_execution"],
                    "cached_query_ratio": self.statistics["query_stats"]["cached_hits"] / 
                                        max(1, self.statistics["query_stats"]["total_queries"])
                },
                "memory_usage": {
                    "nodes_count": len(self.nodes),
                    "edge_types_count": len(self.edge_types),
                    "cache_memory_usage": self.statistics["cache_stats"]["memory_usage"]
                },
                "index_performance": self.index_manager.get_index_stats()
            },
            "timestamp": datetime.now().isoformat(),
            "uptime": str(datetime.now() - self.performance_monitor.start_time),
            "version": "1.0"
        }

        # Ajout des métriques détaillées du moniteur de performance
        report["performance_metrics"].update(
            self.performance_monitor.get_detailed_statistics()
        )

        logger.info("Generated performance report")
        return report

    def health_check(self) -> Dict[str, Any]:
        """
        Effectue une vérification complète de l'état de santé de la base de données.
        
        Returns:
            Dict contenant les indicateurs de santé et les alertes éventuelles
        """
        start_time = time.time()
        health_status = {
            "status": "healthy",  # Par défaut, sera mis à jour si des problèmes sont trouvés
            "timestamp": datetime.now().isoformat(),
            "checks": {},
            "alerts": [],
            "metrics": {}
        }

        try:
            # Vérification de la cohérence des nœuds et relations
            node_check = self._check_nodes_consistency()
            health_status["checks"]["nodes"] = node_check

            # Vérification des index
            index_check = self._check_indexes()
            health_status["checks"]["indexes"] = index_check

            # Vérification du cache
            cache_check = self._check_cache_health()
            health_status["checks"]["cache"] = cache_check

            # Vérification des transactions
            tx_check = self._check_transactions()
            health_status["checks"]["transactions"] = tx_check

            # Métriques de performance
            health_status["metrics"] = {
                "response_times": {
                    "avg": self.performance_monitor.get_statistics("response_time").get("avg", 0),
                    "max": self.performance_monitor.get_statistics("response_time").get("max", 0)
                },
                "memory_usage": {
                    "nodes": len(self.nodes),
                    "cache": self.cache.get_stats()
                },
                "operations": {
                    "success_rate": self._calculate_success_rate()
                }
            }

            # Détermination du statut global
            if any(check.get("status") == "critical" for check in health_status["checks"].values()):
                health_status["status"] = "critical"
            elif any(check.get("status") == "warning" for check in health_status["checks"].values()):
                health_status["status"] = "warning"

            # Durée de la vérification
            health_status["duration"] = time.time() - start_time

        except Exception as e:
            health_status["status"] = "error"
            health_status["error"] = str(e)
            logger.error(f"Error during health check: {e}")

        return health_status

    def _check_nodes_consistency(self) -> Dict[str, Any]:
        """Vérifie la cohérence des nœuds et des relations"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }
        
        try:
            # Vérifie les références circulaires
            circular_refs = []
            visited = set()
            
            for node in self.nodes.values():
                if not node._deleted and node.node_id not in visited:
                    path = []
                    if self._check_circular_references(node, visited, path):
                        circular_refs.append(path)

            # Vérifie les relations orphelines
            orphaned_edges = []
            for node in self.nodes.values():
                for edges in node.outgoing_edges.values():
                    for edge in edges:
                        if not edge.is_valid():
                            orphaned_edges.append(edge.id)

            result["details"] = {
                "total_nodes": len(self.nodes),
                "total_edges": sum(len(edges) for node in self.nodes.values() 
                                for edges in node.outgoing_edges.values()),
                "circular_references": len(circular_refs),
                "orphaned_edges": len(orphaned_edges)
            }

            if circular_refs:
                result["status"] = "warning"
                result["issues"].append({
                    "type": "circular_reference",
                    "count": len(circular_refs),
                    "details": circular_refs[:5]  # Limite aux 5 premiers pour éviter un rapport trop long
                })

            if orphaned_edges:
                result["status"] = "warning"
                result["issues"].append({
                    "type": "orphaned_edges",
                    "count": len(orphaned_edges),
                    "edge_ids": orphaned_edges[:5]
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_indexes(self) -> Dict[str, Any]:
        """Vérifie l'état des index"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            index_stats = self.index_manager.get_index_stats()
            result["details"] = index_stats

            # Vérifie la cohérence des index
            for prop, stats in index_stats.items():
                if stats["entries"] == 0:
                    result["issues"].append({
                        "type": "empty_index",
                        "property": prop
                    })

            if result["issues"]:
                result["status"] = "warning"

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_cache_health(self) -> Dict[str, Any]:
        """Vérifie l'état du cache"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            cache_stats = self.cache.get_stats()
            result["details"] = cache_stats

            # Vérifie le taux de hit du cache
            hit_rate = cache_stats.get("hits", 0) / max(1, cache_stats.get("hits", 0) + cache_stats.get("misses", 0))
            if hit_rate < 0.5:  # Seuil d'avertissement
                result["status"] = "warning"
                result["issues"].append({
                    "type": "low_cache_hit_rate",
                    "hit_rate": hit_rate
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _check_transactions(self) -> Dict[str, Any]:
        """Vérifie l'état des transactions"""
        result = {
            "status": "healthy",
            "details": {},
            "issues": []
        }

        try:
            active_tx = len(self.transaction_manager.active_transactions)
            result["details"]["active_transactions"] = active_tx

            if active_tx > 100:  # Seuil d'avertissement
                result["status"] = "warning"
                result["issues"].append({
                    "type": "high_active_transactions",
                    "count": active_tx
                })

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _calculate_success_rate(self) -> float:
        """Calcule le taux de succès des opérations"""
        total_ops = sum(self.performance_monitor.detailed_metrics["node_operations"].values())
        if total_ops == 0:
            return 1.0

        failed_ops = (self.performance_monitor.detailed_metrics["node_operations"].get("failed", 0) +
                    self.performance_monitor.detailed_metrics["edge_operations"].get("failed", 0))
        
        return (total_ops - failed_ops) / total_ops

    def _check_circular_references(self, node: Node, visited: Set[str], path: List[str]) -> bool:
        """Vérifie les références circulaires dans le graphe"""
        if node.node_id in path:
            path.append(node.node_id)
            return True
            
        if node.node_id in visited:
            return False

        visited.add(node.node_id)
        path.append(node.node_id)

        for edges in node.outgoing_edges.values():
            for edge in edges:
                target = edge.target()
                if target and self._check_circular_references(target, visited, path):
                    return True

        path.pop()
        return False

    def find_nodes(self, labels: Set[str] = None, properties: Dict[str, Any] = None) -> List[Node]:
        """Recherche des nœuds par labels et/ou propriétés"""
        with self.lock:
            results = []
            for node in self.nodes.values():
                if not node._deleted:
                    # Vérifie les labels
                    if labels and not (node.labels & labels):
                        continue

                    # Vérifie les propriétés
                    if properties:
                        match = True
                        for key, value in properties.items():
                            if node.get_property(key) != value:
                                match = False
                                break
                        if not match:
                            continue

                    results.append(node)
            return results
        
    def register_stored_procedure(self, name: str, code: str, params: List[str] = None, 
                                description: str = "", tags: List[str] = None) -> StoredProcedure:
        """Enregistre une nouvelle procédure stockée"""
        procedure = StoredProcedure(name, params or [], code, description, tags)
        self.stored_procedures[name] = procedure
        logger.info(f"Registered stored procedure: {name}")
        return procedure
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> Transaction:
        """Démarre une nouvelle transaction"""
        return self.transaction_manager.begin_transaction(isolation_level)

    def _create_node_with_transaction(self, labels: Set[str], properties: Dict[str, Any] = None, tx: Transaction = None) -> Node:
        """Crée un nœud dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._create_node_with_transaction(labels, properties, new_tx)
        
        node = self.create_node(labels, properties)
        tx.add_operation("create_node", node=node)
        return node

    def _create_edge_with_transaction(self, source_id: str, target_id: str, edge_type: str, 
                                    properties: Dict[str, Any] = None, tx: Transaction = None) -> Edge:
        """Crée une relation dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._create_edge_with_transaction(source_id, target_id, edge_type, properties, new_tx)
        
        edge = self.create_edge(source_id, target_id, edge_type, properties)
        tx.add_operation("create_edge", edge=edge)
        return edge

    def _update_node_with_transaction(self, node_id: str, properties: Dict[str, Any], tx: Transaction = None) -> Node:
        """Met à jour un nœud dans le contexte d'une transaction"""
        if tx is None:
            with self.begin_transaction() as new_tx:
                return self._update_node_with_transaction(node_id, properties, new_tx)
        
        node = self._update_node_internal(node_id, properties)
        tx.add_operation("update_node", node=node, properties=properties)
        return node
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retourne les statistiques complètes"""
        with self.lock:
            stats = {
                **self.statistics,
                "index_stats": self.index_manager.get_index_stats(),
                "cache_stats": self.cache.get_stats(),
                "performance": self.performance_monitor.get_detailed_statistics()
            }
            return stats

    def _delete_edge(self, edge: Edge):
        """Supprime une relation"""
        if not edge.is_valid():
            return

        source = edge.source()
        target = edge.target()
        
        if source:
            source.outgoing_edges[edge.edge_type].discard(edge)
        if target:
            target.incoming_edges[edge.edge_type].discard(edge)
            
        # Met à jour les statistiques
        self.statistics["edges_count"] -= 1
        self.statistics["edge_types_count"][edge.edge_type] -= 1
    
    def compress_history(self, older_than_days: int = 30, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Compresse les données historiques plus anciennes qu'un certain nombre de jours.
        
        Args:
            older_than_days: Âge en jours au-delà duquel compresser les données
            batch_size: Nombre de nœuds à traiter par lot pour éviter la surcharge mémoire
        
        Returns:
            Dict contenant les statistiques de compression
        """
        stats = {
            "compressed_entries": 0,
            "space_saved_bytes": 0,
            "nodes_processed": 0,
            "timestamp": datetime.now().isoformat()
        }
        
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        
        try:
            with self.lock:
                # Traitement par lots pour gérer les grandes bases de données
                node_batches = [list(self.nodes.values())[i:i + batch_size] 
                              for i in range(0, len(self.nodes), batch_size)]
                
                for batch in node_batches:
                    for node in batch:
                        # Compression de l'historique des propriétés
                        original_size = len(node._property_history)
                        node._property_history = [
                            entry for entry in node._property_history
                            if datetime.fromisoformat(entry["timestamp"]) > cutoff_date
                        ]
                        compressed_size = len(node._property_history)
                        stats["compressed_entries"] += original_size - compressed_size
                        
                        # Compression de l'historique des labels
                        original_label_size = len(node._label_history)
                        node._label_history = [
                            entry for entry in node._label_history
                            if datetime.fromisoformat(entry["timestamp"]) > cutoff_date
                        ]
                        compressed_label_size = len(node._label_history)
                        stats["compressed_entries"] += original_label_size - compressed_label_size
                        
                        stats["nodes_processed"] += 1
                
                # Enregistre les métriques
                self.performance_monitor.record_metric(
                    "history_compression",
                    stats["compressed_entries"]
                )
                
                logger.info(f"Compressed history: {stats['compressed_entries']} entries removed")
                
        except Exception as e:
            logger.error(f"Error during history compression: {e}")
            stats["error"] = str(e)
            
        return stats

    def validate_import_data(self, data: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
        """
        Valide les données avant leur import dans la base de données.
        
        Args:
            data: Dictionnaire contenant les données à importer
            
        Returns:
            Tuple contenant:
            - bool: True si valide, False sinon
            - List[str]: Liste des erreurs trouvées
            - Dict[str, Any]: Statistiques de validation
        """
        errors = []
        warnings = []
        stats = {
            "nodes_checked": 0,
            "edges_checked": 0,
            "schema_violations": 0
        }
        
        try:
            # 1. Vérification de la structure de base
            required_keys = {"nodes", "edges", "schema", "metadata"}
            if not all(key in data for key in required_keys):
                missing = required_keys - set(data.keys())
                errors.append(f"Missing required sections: {missing}")
                return False, errors, stats
            
            # 2. Validation du schéma
            if "schema" in data:
                for label, properties in data["schema"].get("node_labels", {}).items():
                    for prop_name, prop_def in properties.items():
                        if "type" not in prop_def:
                            errors.append(f"Missing type definition for {label}.{prop_name}")
                        else:
                            try:
                                PropertyType[prop_def["type"]]
                            except KeyError:
                                errors.append(f"Invalid property type for {label}.{prop_name}: {prop_def['type']}")
            
            # 3. Validation des nœuds
            for node_id, node_data in data.get("nodes", {}).items():
                stats["nodes_checked"] += 1
                
                if "labels" not in node_data:
                    errors.append(f"Missing labels for node {node_id}")
                
                if "properties" not in node_data:
                    errors.append(f"Missing properties for node {node_id}")
                else:
                    # Vérifie si les propriétés correspondent au schéma
                    for label in node_data.get("labels", []):
                        schema_props = data["schema"]["node_labels"].get(label, {})
                        for prop_name, prop_def in schema_props.items():
                            if prop_def.get("required", False) and prop_name not in node_data["properties"]:
                                errors.append(f"Missing required property {prop_name} for node {node_id}")
                                stats["schema_violations"] += 1
            
            # 4. Validation des relations
            for edge_data in data.get("edges", []):
                stats["edges_checked"] += 1
                
                if "source_id" not in edge_data or "target_id" not in edge_data:
                    errors.append(f"Missing source or target for edge {edge_data.get('id', 'unknown')}")
                
                if "type" not in edge_data:
                    errors.append(f"Missing type for edge {edge_data.get('id', 'unknown')}")
                
                # Vérifie si les nœuds source et cible existent
                if edge_data.get("source_id") not in data["nodes"]:
                    errors.append(f"Source node {edge_data.get('source_id')} not found for edge {edge_data.get('id', 'unknown')}")
                
                if edge_data.get("target_id") not in data["nodes"]:
                    errors.append(f"Target node {edge_data.get('target_id')} not found for edge {edge_data.get('id', 'unknown')}")
            
            if len(warnings) > 0:
                logger.warning(f"Import validation warnings: {warnings}")
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            logger.error(f"Error during import validation: {e}")
        
        return len(errors) == 0, errors, stats

    def merge_graph(self, other_graph: 'GraphDB', 
                   conflict_strategy: str = "skip",
                   batch_size: int = 1000) -> Dict[str, Any]:
        """
        Fusionne un autre graphe dans celui-ci.
        
        Args:
            other_graph: Le graphe à fusionner
            conflict_strategy: Stratégie en cas de conflit ('skip', 'overwrite', 'rename')
            batch_size: Nombre d'éléments à traiter par lot
            
        Returns:
            Dict contenant les statistiques de fusion
        """
        stats = {
            "nodes_merged": 0,
            "edges_merged": 0,
            "conflicts_resolved": 0,
            "errors": [],
            "warnings": []
        }
        
        try:
            with self.lock:
                # 1. Fusion du schéma
                self._merge_schema(other_graph.schema, stats)
                
                # 2. Fusion des nœuds par lots
                node_batches = [list(other_graph.nodes.items())[i:i + batch_size] 
                              for i in range(0, len(other_graph.nodes), batch_size)]
                
                for batch in node_batches:
                    for node_id, node in batch:
                        try:
                            self._merge_node(node_id, node, conflict_strategy, stats)
                        except Exception as e:
                            stats["errors"].append(f"Error merging node {node_id}: {str(e)}")
                
                # 3. Fusion des relations par lots
                for node in other_graph.nodes.values():
                    for edge_type, edges in node.outgoing_edges.items():
                        try:
                            self._merge_edges(edges, conflict_strategy, stats)
                        except Exception as e:
                            stats["errors"].append(f"Error merging edges: {str(e)}")
                
                # 4. Mise à jour des index
                self.index_manager.rebuild_indexes()
                
                logger.info(f"Graph merge completed: {stats['nodes_merged']} nodes, "
                          f"{stats['edges_merged']} edges merged")
                
        except Exception as e:
            stats["errors"].append(f"Merge error: {str(e)}")
            logger.error(f"Error during graph merge: {e}")
        
        return stats

    def verify_data_integrity(self) -> Dict[str, Any]:
        """Vérifie l'intégrité complète des données avec validation exhaustive des contraintes."""
        console.print("[yellow]Starting data integrity verification...[/yellow]")
        
        with self.lock:
            results = {
                "status": "ok",
                "errors_found": [],
                "warnings": [],
                "statistics": {
                    "nodes_checked": 0,
                    "edges_checked": 0,
                    "indexes_checked": 0,
                    "issues_found": 0,
                    "repairs_made": 0
                }
            }

            try:
                # Affichage de l'aperçu initial
                console.print(f"\n[bold cyan]Database Overview:[/bold cyan]")
                console.print(f"Total nodes: {len(self.nodes)}")
                console.print(f"Total schemas: {len(self.schema.node_labels)}")
                
                # Affichage des schémas et contraintes
                console.print("\n[bold cyan]Schema Definitions and Constraints:[/bold cyan]")
                for label in self.schema.node_labels:
                    constraints = self.schema.get_constraints(label)
                    required_fields = {c.property for c in constraints if c.type == "required"}
                    unique_fields = {c.property for c in constraints if c.type == "unique"}
                    
                    console.print(f"\nSchema for {label}:")
                    console.print(f"Required fields: {list(required_fields)}")
                    console.print(f"Unique fields: {list(unique_fields)}")

                # 1. Vérification des nœuds
                console.print("\n[bold cyan]Node Validation:[/bold cyan]")
                
                for node_id, node in self.nodes.items():
                    results["statistics"]["nodes_checked"] += 1
                    
                    if node._deleted:
                        continue

                    node_name = node.get_property('name', 'unnamed')
                    console.print(f"\n[bold]Checking node: {node_name} ({node_id})[/bold]")
                    console.print(f"Labels: {node.labels}")
                    console.print(f"Properties: {node._properties}")

                    for label in node.labels:
                        schema = self.schema.get_node_definition(label)
                        if not schema:
                            warning = f"No schema defined for label {label}"
                            console.print(f"[yellow]Warning: {warning}[/yellow]")
                            results["warnings"].append(warning)
                            continue

                        console.print(f"\nValidating against schema for {label}")
                        
                        # 1.1 Vérification des contraintes required
                        constraints = self.schema.get_constraints(label)
                        required_fields = {c.property for c in constraints if c.type == "required"}
                        
                        for field_name in required_fields:
                            is_missing = (
                                field_name not in node._properties or
                                node._properties[field_name] is None or
                                node._properties[field_name] == ""
                            )
                            if is_missing:
                                error = f"Missing required field: {field_name}"
                                console.print(f"[red]Error: {error}[/red]")
                                results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                results["statistics"]["issues_found"] += 1

                        # 1.2 Vérification des contraintes unique
                        unique_fields = {c.property for c in constraints if c.type == "unique"}
                        for field_name in unique_fields:
                            value = node.get_property(field_name)
                            if value is not None:
                                similar_nodes = [
                                    n for n in self.nodes.values()
                                    if (label in n.labels and
                                        n.node_id != node_id and
                                        n.get_property(field_name) == value and
                                        not n._deleted)
                                ]
                                if similar_nodes:
                                    duplicate_names = [n.get_property('name', 'unnamed') for n in similar_nodes]
                                    error = f"Unique constraint violation on {field_name}: value '{value}' also used by {duplicate_names}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                    results["statistics"]["issues_found"] += 1

                        # 1.3 Vérification des types de données
                        for field_name, prop_def in schema.items():
                            if field_name in node._properties and node._properties[field_name] is not None:
                                value = node._properties[field_name]
                                expected_type = prop_def.type.value
                                if not isinstance(value, expected_type):
                                    error = f"Invalid type for {field_name}: got {type(value).__name__}, expected {prop_def.type.name}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Node {node_name} ({node_id}): {error}")
                                    results["statistics"]["issues_found"] += 1

                # 2. Vérification des relations
                console.print("\n[bold cyan]Relationship Validation:[/bold cyan]")
                for node in self.nodes.values():
                    for edge_type, edges in node.outgoing_edges.items():
                        for edge in edges:
                            results["statistics"]["edges_checked"] += 1
                            
                            if edge.is_valid():
                                source_name = edge.source().get_property('name', 'unnamed')
                                target_name = edge.target().get_property('name', 'unnamed')
                                
                                console.print(f"\nChecking relationship: {source_name} -[{edge_type}]-> {target_name}")

                                # 2.1 Vérifie si le type existe
                                if edge_type not in self.schema.edge_types:
                                    error = f"Undefined relationship type: {edge_type}"
                                    console.print(f"[red]Error: {error}[/red]")
                                    results["errors_found"].append(f"Edge {edge.id}: {error}")
                                    results["statistics"]["issues_found"] += 1
                                    continue

                                # 2.2 Vérifie les propriétés
                                edge_schema = self.schema.get_edge_definition(edge_type)
                                if edge_schema:
                                    for field_name, prop_def in edge_schema.items():
                                        if prop_def.required and field_name not in edge._properties:
                                            error = f"Missing required property: {field_name}"
                                            console.print(f"[red]Error: {error}[/red]")
                                            results["errors_found"].append(f"Edge {edge.id}: {error}")
                                            results["statistics"]["issues_found"] += 1
                                            continue

                                        if field_name in edge._properties:
                                            value = edge._properties[field_name]
                                            if value is not None and not isinstance(value, prop_def.type.value):
                                                error = f"Invalid type for {field_name}"
                                                console.print(f"[red]Error: {error}[/red]")
                                                results["errors_found"].append(f"Edge {edge.id}: {error}")
                                                results["statistics"]["issues_found"] += 1

                # Mise à jour du statut final et affichage des résultats
                if results["errors_found"]:
                    results["status"] = "error"
                    console.print(f"\n[red]Found {len(results['errors_found'])} errors[/red]")
                    for error in results["errors_found"]:
                        console.print(f"[red] • {error}[/red]")
                elif results["warnings"]:
                    console.print(f"\n[yellow]Found {len(results['warnings'])} warnings[/yellow]")
                    for warning in results["warnings"]:
                        console.print(f"[yellow] • {warning}[/yellow]")
                else:
                    console.print("\n[green]All verifications passed successfully[/green]")

                # Affichage des statistiques
                console.print("\n[bold cyan]Verification Statistics:[/bold cyan]")
                stats_table = Table(show_header=True, header_style="bold magenta")
                stats_table.add_column("Metric", style="cyan")
                stats_table.add_column("Value", style="green")
                for key, value in results["statistics"].items():
                    stats_table.add_row(key, str(value))
                console.print(stats_table)

            except Exception as e:
                results["status"] = "error"
                error = f"Verification error: {str(e)}"
                console.print(f"[red]Error: {error}[/red]")
                results["errors_found"].append(error)
                results["statistics"]["issues_found"] += 1

            return results
    
    def _apply_schema_changes(self, node: Node, new_schema: Dict[str, Any], results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Applique les changements de schéma à un nœud spécifique et retourne les changements appliqués.
        
        Args:
            node: Le nœud à modifier
            new_schema: Le nouveau schéma à appliquer
            results: Dictionnaire des résultats pour les statistiques
            
        Returns:
            Liste des changements appliqués
        """
        changes_applied = []
        
        try:
            for label in node.labels:
                if label in new_schema:
                    new_properties = new_schema[label]
                    
                    # Applique les modifications de propriétés existantes
                    for prop_name, prop_def in new_properties.items():
                        current_value = node.get_property(prop_name)
                        
                        # Gestion des valeurs manquantes pour les propriétés requises
                        if current_value is None and prop_def.required:
                            node.set_property(prop_name, prop_def.default)
                            changes_applied.append({
                                "type": "add_property",
                                "node_id": node.node_id,
                                "property": prop_name,
                                "value": prop_def.default
                            })
                            continue
                            
                        # Si la propriété existe, vérifie le type
                        if current_value is not None:
                            try:
                                # Conversion du type si nécessaire
                                if isinstance(current_value, str) and prop_def.type == PropertyType.INTEGER:
                                    new_value = int(float(current_value))
                                    node.set_property(prop_name, new_value)
                                    changes_applied.append({
                                        "type": "convert_type",
                                        "node_id": node.node_id,
                                        "property": prop_name,
                                        "old_value": current_value,
                                        "new_value": new_value
                                    })
                                elif isinstance(current_value, str) and prop_def.type == PropertyType.FLOAT:
                                    new_value = float(current_value)
                                    node.set_property(prop_name, new_value)
                                    changes_applied.append({
                                        "type": "convert_type",
                                        "node_id": node.node_id,
                                        "property": prop_name,
                                        "old_value": current_value,
                                        "new_value": new_value
                                    })
                                # Ajouter d'autres conversions si nécessaire
                            
                            except (ValueError, TypeError) as e:
                                results["errors"].append(
                                    f"Failed to convert property {prop_name} for node {node.node_id}: {str(e)}"
                                )
            
            return changes_applied
            
        except Exception as e:
            results["errors"].append(f"Error applying changes to node {node.node_id}: {str(e)}")
            return changes_applied
        
    def _migrate_property(self, node: Node, prop_name: str, 
                         prop_def: PropertyDefinition) -> bool:
        """Migre une propriété selon la nouvelle définition"""
        current_value = node.get_property(prop_name)
        
        # Si la propriété n'existe pas et est requise
        if current_value is None and prop_def.required:
            node.set_property(prop_name, prop_def.default)
            return True
            
        # Si le type doit être converti
        if current_value is not None:
            try:
                new_value = self._convert_property_value(current_value, prop_def.type)
                if new_value != current_value:
                    node.set_property(prop_name, new_value)
                    return True
            except ValueError as e:
                logger.warning(f"Could not convert property {prop_name} for node {node.node_id}: {e}")
                
        return False

    def _convert_property_value(self, value: Any, target_type: PropertyType) -> Any:
        """Convertit une valeur vers le nouveau type"""
        if target_type == PropertyType.STRING:
            return str(value)
        elif target_type == PropertyType.INTEGER:
            return int(float(value))
        elif target_type == PropertyType.FLOAT:
            return float(value)
        elif target_type == PropertyType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'y')
            return bool(value)
        elif target_type == PropertyType.DATETIME:
            if isinstance(value, str):
                return datetime.fromisoformat(value)
            raise ValueError(f"Cannot convert {type(value)} to datetime")
        elif target_type == PropertyType.LIST:
            if isinstance(value, (list, tuple, set)):
                return list(value)
            return [value]
        
        return value

    def _update_schema_definition(self, new_schema: Dict[str, Any], 
                                results: Dict[str, Any]):
        """Met à jour la définition du schéma"""
        # Sauvegarde l'ancien schéma
        old_schema = copy.deepcopy(self.schema.node_labels)
        
        try:
            # Met à jour les définitions de nœuds
            for label, properties in new_schema.items():
                self.schema.add_node_definition(label, properties)
                
            # Met à jour les contraintes
            self._update_schema_constraints(new_schema, old_schema, results)
            
        except Exception as e:
            # Restaure l'ancien schéma en cas d'erreur
            self.schema.node_labels = old_schema
            raise ValueError(f"Failed to update schema: {e}")

    def _update_schema_constraints(self, new_schema: Dict[str, Any], 
                                 old_schema: Dict[str, Any],
                                 results: Dict[str, Any]):
        """Met à jour les contraintes du schéma"""
        for label, props in new_schema.items():
            # Ajoute les nouvelles contraintes
            for prop_name, prop_def in props.items():
                if prop_def.required:
                    constraint = SchemaConstraint("required", prop_name)
                    self.schema.add_constraint(label, constraint)
                    results["statistics"]["constraints_added"] += 1
                
            # Vérifie les anciennes contraintes à supprimer
            old_props = old_schema.get(label, {})
            for prop_name, prop_def in old_props.items():
                if prop_name not in props and prop_def.required:
                    # Supprime la contrainte
                    self.schema.constraints[label] = [
                        c for c in self.schema.get_constraints(label)
                        if not (c.type == "required" and c.property == prop_name)
                    ]
                    results["statistics"]["constraints_removed"] += 1

    def _merge_schema(self, other_schema: Schema, stats: Dict[str, Any]):
        """Fusionne le schéma d'un autre graphe"""
        try:
            # Fusion des définitions de nœuds
            for label, properties in other_schema.node_labels.items():
                if label not in self.schema.node_labels:
                    self.schema.add_node_definition(label, properties)
                else:
                    # Fusionne les propriétés
                    for prop_name, prop_def in properties.items():
                        if prop_name not in self.schema.node_labels[label]:
                            self.schema.node_labels[label][prop_name] = prop_def
            
            # Fusion des contraintes
            for label, constraints in other_schema.constraints.items():
                for constraint in constraints:
                    exists = any(
                        c.type == constraint.type and c.property == constraint.property
                        for c in self.schema.get_constraints(label)
                    )
                    if not exists:
                        self.schema.add_constraint(label, constraint)
                        
            return True
        except Exception as e:
            stats["errors"].append(f"Schema merge error: {str(e)}")
            return False

    def _merge_node(self, node_id: str, node: 'Node', conflict_strategy: str, 
                    stats: Dict[str, Any]) -> bool:
        """Fusionne un nœud dans le graphe"""
        try:
            if node_id in self.nodes:
                if conflict_strategy == "skip":
                    stats["warnings"].append(f"Skipped existing node: {node_id}")
                    return False
                elif conflict_strategy == "overwrite":
                    existing_node = self.nodes[node_id]
                    # Met à jour les propriétés
                    for key, value in node._properties.items():
                        existing_node.set_property(key, value)
                    # Met à jour les labels
                    for label in node.labels - existing_node.labels:
                        existing_node.add_label(label)
                    stats["conflicts_resolved"] += 1
                    return True
                elif conflict_strategy == "rename":
                    new_id = f"{node_id}_{str(uuid4())[:8]}"
                    self.nodes[new_id] = node
                    stats["conflicts_resolved"] += 1
                    return True
            else:
                self.nodes[node_id] = node
                stats["nodes_merged"] += 1
                return True
                
        except Exception as e:
            stats["errors"].append(f"Error merging node {node_id}: {str(e)}")
            return False

    def _merge_edges(self, edges: Set['Edge'], conflict_strategy: str, 
                    stats: Dict[str, Any]) -> int:
        """Fusionne un ensemble de relations dans le graphe"""
        merged_count = 0
        for edge in edges:
            try:
                if not edge.is_valid():
                    continue
                    
                source = self.nodes.get(edge.source().node_id)
                target = self.nodes.get(edge.target().node_id)
                
                if source and target:
                    # Vérifie si une relation similaire existe déjà
                    existing = False
                    for existing_edge in source.outgoing_edges.get(edge.edge_type, set()):
                        if (existing_edge.target().node_id == target.node_id and 
                            existing_edge._properties == edge._properties):
                            existing = True
                            break
                    
                    if not existing:
                        new_edge = self.create_edge(
                            source.node_id,
                            target.node_id,
                            edge.edge_type,
                            edge._properties
                        )
                        merged_count += 1
                        stats["edges_merged"] += 1
                    
            except Exception as e:
                stats["errors"].append(f"Error merging edge: {str(e)}")
                
        return merged_count
