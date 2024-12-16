from enum import Enum, auto


class IsolationLevel(Enum):
    """Représente les niveaux d'isolation des transactions dans un système ACID.

    Les niveaux d'isolation définissent le degré de séparation entre les transactions
    concurrentes pour éviter des anomalies telles que :
    - Dirty Reads
    - Non-Repeatable Reads
    - Phantom Reads

    Attributes:
        READ_UNCOMMITTED: Permet de lire des données non validées (Dirty Reads).
        READ_COMMITTED: Garantit que seules les données validées sont visibles.
        REPEATABLE_READ: Empêche les lectures non répétables.
        SERIALIZABLE: Offre l'isolation la plus stricte en interdisant les Phantom Reads.
    """

    READ_UNCOMMITTED = auto()
    READ_COMMITTED = auto()
    REPEATABLE_READ = auto()
    SERIALIZABLE = auto()
