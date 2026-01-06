from pydantic import BaseModel


class AviationStackParams(BaseModel):
    raw_dir: str
    bronze_dir: str
    max_pages: int = 1


class OpenFlightsParams(BaseModel):
    raw_dir: str
    bronze_dir: str
