import typer
from include.api.extract_api import extract_flights as extract_api_flights
from include.openflights.extract_openflights import extract_openflights as extract_of

app = typer.Typer()


@app.command()
def flights(
    raw_dir: str = typer.Argument(..., help="Local directory to save JSON files"),
    max_pages: int = typer.Option(1, help="Maximum number of pages to fetch"),
    execution_date: str = typer.Option(
        "1970-01-01", help="Execution date for partitioning"
    ),
) -> None:
    extract_api_flights(
        raw_dir=raw_dir,
        max_pages=max_pages,
        execution_date=execution_date,
    )


@app.command()
def openflights(
    raw_dir: str = typer.Argument(..., help="Local directory to save .dat files"),
) -> None:
    extract_of(raw_dir=raw_dir)


if __name__ == "__main__":
    app()
