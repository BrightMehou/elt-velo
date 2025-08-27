import logging
import subprocess

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def data_transformation():
    logger.info("Démarrage de la commande dbt run")
    try:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "src/elt", "--profiles-dir", "src/elt"],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("dbt run terminé avec succès")
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("Erreur pendant le dbt run")
        logger.error(e.stderr)


if __name__ == "__main__":
    data_transformation()
