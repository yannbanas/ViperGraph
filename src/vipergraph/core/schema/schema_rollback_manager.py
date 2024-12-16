import copy
from datetime import datetime
from typing import Any, Dict, List


class SchemaRollbackManager:
    """Gère les points de sauvegarde et les rollbacks du schéma.

    Cette classe permet de créer des sauvegardes du schéma, d'effectuer des rollbacks
    vers des états précédents et de maintenir un journal des rollbacks effectués.

    Attributes:
        graph (GraphDB): Instance de la base de données du graphe.
        backup_schemas (Dict[int, Dict[str, Any]]): Dictionnaire des points de sauvegarde.
        rollback_logs (List[Dict[str, Any]]): Historique des rollbacks.
    """

    def __init__(self, graph: 'GraphDB') -> None:
        """Initialise le gestionnaire de rollbacks pour un graphe.

        Args:
            graph (GraphDB): Instance de la base de données associée.
        """
        self.graph: 'GraphDB' = graph
        self.backup_schemas: Dict[int, Dict[str, Any]] = {}
        self.rollback_logs: List[Dict[str, Any]] = []

    def create_checkpoint(self) -> int:
        """Crée un point de sauvegarde du schéma actuel.

        Returns:
            int: Identifiant du point de sauvegarde créé.
        """
        checkpoint_id = len(self.backup_schemas) + 1

        # Sauvegarde l'état actuel du schéma
        schema_backup = {
            "node_labels": copy.deepcopy(self.graph.schema.node_labels),
            "constraints": copy.deepcopy(self.graph.schema.constraints),
            "version": self.graph.schema.version
        }

        self.backup_schemas[checkpoint_id] = schema_backup
        print(f"Checkpoint {checkpoint_id} created successfully.")
        return checkpoint_id

    def rollback_to_checkpoint(self, checkpoint_id: int) -> bool:
        """Restaure le schéma à un point de sauvegarde spécifique.

        Args:
            checkpoint_id (int): Identifiant du point de sauvegarde.

        Returns:
            bool: True si le rollback a réussi, sinon False.
        """
        if checkpoint_id not in self.backup_schemas:
            print(f"Checkpoint {checkpoint_id} does not exist.")
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

            print(f"Rollback to checkpoint {checkpoint_id} succeeded.")
            return True

        except Exception as e:
            print(f"Rollback to checkpoint {checkpoint_id} failed: {e}")
            return False

    def get_rollback_history(self) -> List[Dict[str, Any]]:
        """Récupère l'historique des rollbacks effectués.

        Returns:
            List[Dict[str, Any]]: Liste des logs de rollbacks.
        """
        return self.rollback_logs
