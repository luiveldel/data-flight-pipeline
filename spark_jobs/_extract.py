from typing import List
from .utils.aviationstack import AviationStack
import os
import math
import json
import logging

logger = logging.getLogger(__name__)

class FlightsClient(AviationStack):
    def __repr__(self) -> str:
        return f"FlightsClient(limit={self._limit})"

def extract_flights(
    raw_dir: str,
    max_pages: int = 1,
) -> List[str]:
    """
    EXTRACT:
    - Llama a /v1/flights (AviationStack) con paginación.
    - Guarda cada respuesta en un JSON en raw_dir.
    - Devuelve lista de paths JSON.
    """
    client = FlightsClient()

    os.makedirs(raw_dir, exist_ok=True)

    first = client.get_aviationstack_json_response(
        endpoint="/flights",
        offset=0,
    )
    pagination = first.get("pagination", {})
    total = pagination.get("total", 0)
    count = pagination.get("count", 0)
    logger.info(f"First page: count={count}, total={total}")

    paths: List[str] = []

    first_path = os.path.join(raw_dir, "flights_page_0.json")
    with open(first_path, "w", encoding="utf-8") as f:
        json.dump(first, f)
    paths.append(first_path)

    total_pages = min(max_pages, max(1, math.ceil(total / client._limit)))
    logger.info(f"Will fetch up to {total_pages} pages")

    for page in range(1, total_pages):
        data = client.get_aviationstack_json_response(
            endpoint="/flights",
            offset=page * client._limit,
        )
        path = os.path.join(raw_dir, f"flights_page_{page}.json")

        logger.info(f"Saving page {page} to {path}")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        paths.append(path)

    logger.info(f"Extracted {len(paths)} pages into {raw_dir}")

    return paths
