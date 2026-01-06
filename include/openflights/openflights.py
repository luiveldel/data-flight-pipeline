from abc import ABC, abstractmethod

import os
import requests
import logging

logger = logging.getLogger(__name__)


class OpenFlights(ABC):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self._url = os.environ.get("OPENFLIGHTS_BASE_URL")

    @abstractmethod
    def __repr__(self) -> str:
        "Subclasses must implement __repr__ for logging."
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()

    def _check_config(self) -> None:
        if not self._url:
            raise ValueError("OPENFLIGHTS_BASE_URL is not defined.")

    def get_openflights_dat_response(
        self,
        filename: str,
        output_dir: str,
    ) -> str:
        """Get the OpenFlights data file response."""
        url = f"{self._url}/{filename}"
        self._check_config()

        try:
            logger.info(f"Downloading {filename} to {output_dir}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            os.makedirs(output_dir, exist_ok=True)
            dest_path = os.path.join(output_dir, filename)
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded {filename}")
            return dest_path
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            raise
