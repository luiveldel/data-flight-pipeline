from abc import ABC, abstractmethod
import os
import random
import requests
from typing import Dict, Any, Optional
import logging
import time

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base exception for API errors."""
    pass


class RateLimitError(APIError):
    """Exception for rate limit errors (429)."""
    pass


class APIValidationError(APIError):
    """Exception for invalid API responses."""
    pass


class AviationStack(ABC):
    """
    Base class for AviationStack API clients.
    
    Features:
    - Connection pooling with requests.Session
    - Rate limiting between requests
    - Exponential backoff with jitter
    - Response validation
    """

    # Rate limiting: minimum seconds between requests
    MIN_REQUEST_INTERVAL = 1.0
    
    # Last request timestamp (class-level to share across instances)
    _last_request_time: float = 0.0

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        # Read environment variables at instantiation time, not import time
        self._url = os.environ.get("AVIATIONSTACK_BASE_URL")
        self._api_key = os.environ.get("AVIATIONSTACK_API_KEY")
        self._limit = int(os.environ.get("AVIATIONSTACK_LIMIT", 100))
        
        # Create a session for connection pooling
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "DataFlightPipeline/1.0",
        })

    def __del__(self):
        """Close the session when the object is destroyed."""
        if hasattr(self, "_session"):
            self._session.close()

    @abstractmethod
    def __repr__(self) -> str:
        """Subclasses must implement __repr__ for logging."""
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()

    def _check_config(self) -> None:
        """Validate required configuration."""
        if not self._api_key:
            raise ValueError("AVIATIONSTACK_API_KEY is not defined.")
        if not self._url:
            raise ValueError("AVIATIONSTACK_BASE_URL is not defined.")
        if not self._limit:
            raise ValueError("AVIATIONSTACK_LIMIT is not defined.")

    def _build_base_params(self) -> Dict[str, Any]:
        """Build base query parameters."""
        return {
            "access_key": self._api_key,
            "limit": self._limit,
        }

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - AviationStack._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            sleep_time = self.MIN_REQUEST_INTERVAL - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        AviationStack._last_request_time = time.time()

    def _calculate_backoff(self, retry: int, base: float = 2.0, max_backoff: float = 60.0) -> float:
        """Calculate exponential backoff with jitter.
        
        Args:
            retry: Current retry count (0-based)
            base: Base for exponential calculation
            max_backoff: Maximum backoff time in seconds
            
        Returns:
            Backoff time in seconds
        """
        # Exponential backoff: base^retry
        backoff = min(base ** retry, max_backoff)
        # Add jitter (±25%)
        jitter = backoff * 0.25 * (2 * random.random() - 1)
        return backoff + jitter

    def _validate_response(self, json_resp: Dict[str, Any]) -> None:
        """Validate the API response structure.
        
        Args:
            json_resp: The JSON response from the API
            
        Raises:
            APIValidationError: If the response is invalid
        """
        # Check for API error response
        if "error" in json_resp:
            error = json_resp["error"]
            error_code = error.get("code", "unknown")
            error_message = error.get("message", "Unknown error")
            raise APIValidationError(f"API error {error_code}: {error_message}")

        # Validate pagination exists for flight endpoints
        if "pagination" not in json_resp:
            logger.warning("Response missing 'pagination' field")

    def get_aviationstack_json_response(
        self,
        endpoint: str = "/flights",
        extra_params: Optional[Dict[str, Any]] = None,
        offset: int = 0,
        max_retries: int = 5,
        timeout: tuple[float, float] = (10.0, 60.0),
    ) -> Dict[str, Any]:
        """
        Makes a GET request to AviationStack and returns the JSON response.

        Args:
            endpoint: endpoint path, e.g. "/flights".
            extra_params: filters (flight_date, dep_iata, etc.).
            offset: for pagination.
            max_retries: maximum number of retry attempts.
            timeout: tuple of (connect_timeout, read_timeout) in seconds.
            
        Returns:
            JSON response as a dictionary.
            
        Raises:
            APIError: If the request fails after all retries.
            RateLimitError: If rate limited and retries exhausted.
            APIValidationError: If the response is invalid.
        """
        self._check_config()

        params = self._build_base_params()
        params["offset"] = offset
        if extra_params:
            params.update(extra_params)

        url = f"{self._url.rstrip('/')}{endpoint}"
        
        # Apply rate limiting
        self._rate_limit()

        for retry in range(max_retries):
            try:
                logger.info(f"GET {url} params={params} (attempt {retry + 1}/{max_retries})")
                response = self._session.get(url=url, params=params, timeout=timeout)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    backoff = max(retry_after, self._calculate_backoff(retry))
                    logger.warning(
                        f"Rate limited (429). Retry-After: {retry_after}s, "
                        f"sleeping {backoff:.1f}s before retry"
                    )
                    if retry < max_retries - 1:
                        time.sleep(backoff)
                        continue
                    raise RateLimitError(
                        f"Rate limit exceeded after {max_retries} retries"
                    )
                
                # Handle server errors (5xx) with retry
                if response.status_code >= 500:
                    backoff = self._calculate_backoff(retry)
                    logger.warning(
                        f"Server error ({response.status_code}). "
                        f"Sleeping {backoff:.1f}s before retry"
                    )
                    if retry < max_retries - 1:
                        time.sleep(backoff)
                        continue
                
                response.raise_for_status()
                json_resp = response.json()
                
                # Validate response
                self._validate_response(json_resp)
                
                logger.info(
                    f"AviationStack response pagination={json_resp.get('pagination', {})}"
                )
                return json_resp

            except requests.exceptions.Timeout as e:
                backoff = self._calculate_backoff(retry)
                logger.warning(
                    f"Request timeout: {e} (retry {retry + 1}/{max_retries}), "
                    f"sleeping {backoff:.1f}s"
                )
                if retry < max_retries - 1:
                    time.sleep(backoff)
                else:
                    raise APIError(f"Request timeout after {max_retries} retries: {e}") from e

            except requests.exceptions.ConnectionError as e:
                backoff = self._calculate_backoff(retry)
                logger.warning(
                    f"Connection error: {e} (retry {retry + 1}/{max_retries}), "
                    f"sleeping {backoff:.1f}s"
                )
                if retry < max_retries - 1:
                    time.sleep(backoff)
                else:
                    raise APIError(f"Connection error after {max_retries} retries: {e}") from e

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                raise APIError(f"Request failed: {e}") from e

        raise APIError(f"Request failed after {max_retries} retries")
