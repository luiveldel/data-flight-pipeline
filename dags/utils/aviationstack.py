from abc import ABC, abstractmethod
import os
import requests
from typing import Dict, Any, Optional
import logging
import time

logger = logging.getLogger(__name__)


class AviationStack(ABC):
    """
    Class attributes shared by all AviationStack classes.
    """

    _url: str = os.environ.get("AVIATIONSTACK_BASE_URL", "http://api.aviationstack.com/v1")
    _api_key: str = os.environ.get("AVIATIONSTACK_API_KEY", "")
    _limit: int = int(os.environ.get("AVIATIONSTACK_LIMIT", 100))

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def __repr__(self) -> str:
        "Subclasses must implement __repr__ for logging."
        ...

    def __str__(self) -> str:
        return self.__repr__()

    def _check_config(self) -> None:
        if not self._api_key:
            raise ValueError("AVIATIONSTACK_API_KEY is not defined.")
        if not self._url:
            raise ValueError("AVIATIONSTACK_BASE_URL is not defined.")

    def _build_base_params(self) -> Dict[str, Any]:
        return {
            "access_key": self._api_key,
            "limit": self._limit,
        }

    def get_aviationstack_json_response(
        self,
        endpoint: str = "/flights",
        extra_params: Optional[Dict[str, Any]] = None,
        offset: int = 0,
        max_retries: int = 3,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        Makes a GET request to AviationStack and returns the JSON response.

        - endpoint: endpoint path, e.g. "/flights".
        - extra_params: filters (flight_date, dep_iata, etc.).
        - offset: for pagination.
        """
        self._check_config()

        params = self._build_base_params()
        params["offset"] = offset
        if extra_params:
            params.update(extra_params)

        url = f"{self._url.rstrip('/')}{endpoint}"

        retries = 0
        response = None

        while retries < max_retries:
            try:
                logger.info(f"GET {url} params={params}")
                response = requests.get(url=url, params=params, timeout=timeout)
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                logger.warning(
                    f"Request exception: {e} (retry {retries}/{max_retries})"
                )
                if retries >= max_retries:
                    logger.error("Max retries exceeded when calling AviationStack.")
                    raise

                time.sleep(2 * retries)

        if response is None:
            raise RuntimeError("No response received from AviationStack.")

        # Simple 503 (overload) handling.
        if response.status_code == 503:
            logger.warning(
                "AviationStack overloaded (503). Waiting 15 seconds before raising error..."
            )
            time.sleep(15)

        response.raise_for_status()
        json_resp = response.json()
        logger.info(
            f"AviationStack response pagination={json_resp.get('pagination', {})}"
        )
        return json_resp
