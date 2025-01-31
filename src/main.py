import logging
from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements,
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data,
)
from data_ingestion import get_realtime_bicycle_data, get_commune_data


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Process start.")
    # data ingestion

    logger.info("Data ingestion started.")
    get_realtime_bicycle_data()
    get_commune_data()
    logger.info("Data ingestion ended.")

    # data consolidation
    logger.info("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()

    logger.info("Consolidation data ended.")

    # data agregation
    logger.info("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    logger.info("Agregate data ended.")


if __name__ == "__main__":
    main()
