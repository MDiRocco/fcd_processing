
"""Program to extract FCD data in specific areas delimited by polygon."""
import logging
from pathlib import Path

import extract_fcd
import typer

app = typer.Typer()


@app.command()
def extract(
    input_folder: str = typer.Option(help='Path for the data folder.', show_default=False),
    polygon_path: str = typer.Option(help='Path for the data folder.', show_default=False),
    multiple: bool = typer.Option(default=False, help='Boolean for multiple-day run.'),
) -> bool:
    """Extract data from folder.

    Args:
        input_folder (str): Path for the configuration folder.
        polygon_path (str): Path for the poligon.
        multiple (bool): Set to True for multiple day procesing

    Returns:
        Run the extraction process
    """
    if (Path(input_folder).exists() and Path(polygon_path).exists()):
        return extract_fcd.extraction_run(Path(input_folder), Path(polygon_path), multiple)
    else:
        logging.error('{0}, {1}'.format(input_folder, Path(input_folder).exists()))
        logging.error('{0}, {1}'.format(polygon_path, Path(polygon_path).exists()))
        logging.error('Input not valid or not found')


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)  # noqa:WPS323
    app()
