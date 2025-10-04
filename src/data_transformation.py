"""
Script de transformation des données via dbt.

Fonctionnalités principales :
- Lance la commande `dbt run` en utilisant le projet défini dans `src/elt`.
- Capture et journalise les sorties standard et erreurs.
- Fournit une fonction principale `data_transformation()` pour exécuter le pipeline ELT.
"""

import logging
import subprocess

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


def data_transformation() -> None:
    """
    Exécute la commande `dbt run` pour lancer les transformations ELT.

    Cette fonction lance le processus `dbt run` via `subprocess.run`
    dans le répertoire du projet `src/elt`.
    Les sorties standard et d’erreur sont capturées et loguées via `logger`.

    Raises:
        subprocess.CalledProcessError: si la commande `dbt run` échoue.
    """
    logger.info("🚀 Démarrage de la commande dbt run")

    try:
        result: subprocess.CompletedProcess[str] = subprocess.run(
            ["dbt", "run", "--project-dir", "src/elt", "--profiles-dir", "src/elt"],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("✅ dbt run terminé avec succès")
        logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        logger.error("❌ Erreur pendant le dbt run")
        logger.error(e.stderr)


if __name__ == "__main__":
    data_transformation()
