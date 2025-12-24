import typer
from include.api.extract_api import extract_flights


def main(
    raw_dir: str = typer.Argument(..., help="Local directory to save JSON files"),
    max_pages: int = typer.Argument(1, help="Maximum number of pages to fetch"),
    execution_date: str = typer.Argument(
        "1970-01-01", help="Execution date for partitioning"
    ),
) -> None:
    extract_flights(
        raw_dir=raw_dir,
        max_pages=max_pages,
        execution_date=execution_date,
    )


if __name__ == "__main__":
    typer.run(main)
