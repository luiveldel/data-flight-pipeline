import logging
from .openflights import OpenFlights
from spark_jobs.openflights.dictionary import OPENFLIGHTS_FILES

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OpenFlightsClient(OpenFlights):
    def __repr__(self) -> str:
        return "OpenFlightsClient()"


def extract_openflights(raw_dir: str = "/opt/airflow/data/raw/openflights") -> None:
    client = OpenFlightsClient()
    for key, filename in OPENFLIGHTS_FILES.items():
        client.get_openflights_dat_response(filename, raw_dir)
