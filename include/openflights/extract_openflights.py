import logging
import os
from typing import List

from .openflights import OpenFlights
from spark_jobs.openflights.dictionary import OPENFLIGHTS_FILES

logger = logging.getLogger(__name__)


class OpenFlightsClient(OpenFlights):
    def __repr__(self) -> str:
        return "OpenFlightsClient()"


def extract_openflights(raw_dir: str = "/opt/airflow/data/raw/openflights") -> List[str]:
    """Extract OpenFlights data files.
    
    Args:
        raw_dir: Directory to save the downloaded files
        
    Returns:
        List of paths to downloaded files
    """
    os.makedirs(raw_dir, exist_ok=True)
    
    client = OpenFlightsClient()
    paths: List[str] = []
    
    for key, filename in OPENFLIGHTS_FILES.items():
        logger.info(f"Downloading {key} ({filename})")
        try:
            client.get_openflights_dat_response(filename, raw_dir)
            file_path = os.path.join(raw_dir, filename)
            paths.append(file_path)
            logger.info(f"Downloaded {filename} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            raise
    
    logger.info(f"Extracted {len(paths)} OpenFlights files to {raw_dir}")
    return paths
