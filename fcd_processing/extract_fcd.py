"""Filter the FCD files to extract the data in specific areas delimited by polygon."""
import logging
import multiprocessing as mp
import os
import shutil
import subprocess
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

GLOBAL_CRS = '4326'
# HEADER_DATI = (  # noqa:WPS317
#     'ID', 'Lat', 'Long', 'Direction', 'Velocity', 'Timestamp', 'EngineStat',
#     'GpsQuality', 'ID car', 'IDSystem', 'Class', 'Odometro',
# )
# HEADER_DATI = ''
COLUMNS_NAME, COLUMNS_DTYPE = ['', '']
SUPPRESS_DTYPEWARNING = {'ID car': str}  # noqa: WPS407 # DtypeWarning: Columns (8) have mixed types
INPUT_FOLDER = Path.cwd() / 'input_data'
OUTPUT_DATA = Path.cwd() / 'output_data'
OUTPUT_DATA.mkdir(exist_ok=True)
CONFIG_FILE = Path.cwd() / 'config' / 'data_frame_config.yaml'

SPLITTED_INPUT = Path.cwd() / 'splitted_input'
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)  # noqa:WPS323


def filter_by_polygon(chunk_polygon):
    """Return a dataframe contiainig the data relative to specific polygon.

    Args:
        chunk_polygon (tupla): path to the file, polygon


    Returns:
        GeoDataFrame: Dataframe filtered
    """
    chunk = pd.read_csv(chunk_polygon[0], names=COLUMNS_NAME, dtype=COLUMNS_DTYPE)

    polygon = chunk_polygon[1]

    return gpd.sjoin(
        gpd.GeoDataFrame(
            chunk,
            geometry=gpd.points_from_xy(chunk['Long'], chunk['Lat']),
            crs=GLOBAL_CRS,
        ),
        polygon,
        predicate='within',
    )


def split_file(data_file, chunksize):
    """Split the huge input in chunks.

    Args:
        data_file (Path): Path to the file
        chunksize (int): Number of line to read each file

    Returns:
        List: List of path for each chunk
    """
    if (Path(SPLITTED_INPUT).exists()):
        shutil.rmtree(SPLITTED_INPUT)
    os.makedirs(SPLITTED_INPUT)
    subprocess.call(
        [
            '/usr/bin/split',
            '--lines={0}'.format(chunksize),
            '--numeric-suffixes',
            data_file,
            'split',
        ],
        cwd=SPLITTED_INPUT,
    )

    return [SPLITTED_INPUT / data_file for data_file in SPLITTED_INPUT.iterdir()]


def extract_data_from_archive(archive_file):
    """Extract file from archives.

    Args:
        archive_file (Path): Path of the archive

    Returns:
        String: Name of the extracted file
    """
    path_folder = archive_file.parent
    with zipfile.ZipFile(archive_file, mode='r') as archive:
        if len(archive.namelist()) > 1:
            logging.error('ARCHIVE HAS MORE THAN ONE ELEMENT, ABORT')
            # print('ARCHIVE HAS MORE THAN ONE ELEMENT, ABORT')
            return ''
        for txf_file in archive.namelist():
            try:
                file_extracted = archive.extract(txf_file, path_folder)
            except zipfile.BadZipfile:
                logging.error('An error occurred while extracting the file')
                return None
            os.rename(path_folder / file_extracted, '{0}.csv'.format(path_folder / archive_file.stem))
            return '{0}.csv'.format(path_folder / archive_file.stem)


def load_csv_chunk_mp(path_folder, polygon):
    """Load the data using chunks.

    Args:
        path_folder (Path): Folder containing the data
        polygon (GeoDataFrame): GeoDataFrame

    Returns:
        GeoDataFrame: Data loaded in Dataframe
    """
    n_worker = int(mp.cpu_count() / 4)  # mp.cpu_count()=32
    logging.info('multiprocessing 1 worker: {0}'.format(n_worker))

    gdf = gpd.GeoDataFrame()
    chunksize = 10 ** 6

    pool = mp.Pool(processes=n_worker)
    job_list = []
    file_name = ''
    file_to_process = []

    archive_file = path_folder
    if (archive_file.name.endswith('zip')):
        extracted_data = extract_data_from_archive(archive_file)
        if extracted_data:
            file_to_process.append(extracted_data)
    for data_file_str in file_to_process:
        data_file = Path(data_file_str)
        if (data_file.name.endswith('csv') or data_file.name.endswith('txt')):
            file_name = data_file
            logging.info('PROCESSING FILE: {0}'.format(path_folder / data_file))
            file_paths = split_file(data_file, chunksize)
            arg_items = [(chunk, polygon) for chunk in file_paths]
            temp_gdf = pool.map_async(filter_by_polygon, arg_items)
            job_list.append(temp_gdf.get())
            shutil.rmtree(SPLITTED_INPUT)
            os.remove(path_folder / data_file)
    if not job_list:
        logging.error('job_list empty, check data files')
        return (None, None)
    pool.close()

    gdf = pd.concat([gdf for map_res in job_list for gdf in map_res])

    out_name = '_'.join(file_name.name.split('_')[:4])
    return (out_name, gdf)


def set_config_var():
    """Summary.

    Returns:
        _type_: _description_
    """
    with open(CONFIG_FILE, 'r') as in_file:
        config_data = yaml.safe_load(in_file)['HEADER_DATI']
    return [config_data.keys(), config_data]


def extraction_run(data_folder, polygon_path, multiple):
    """Run the Main program for a single day of data.

    Args:
        data_folder (str): Sting of the data folder.
        polygon_path (str): Path for the poligon.
        multiple (bool): Set to True for multiple day procesing
    """
    global COLUMNS_NAME, COLUMNS_DTYPE
    COLUMNS_NAME, COLUMNS_DTYPE = set_config_var()

    polygon = gpd.read_file(polygon_path)
    polygon.to_crs('EPSG:{0}'.format(GLOBAL_CRS), inplace=True)

    if multiple:
        for object_in_folder in data_folder.iterdir():
            start = time.time()
            if object_in_folder.is_dir():
                for archive_file in object_in_folder.iterdir():
                    file_name, gdf = load_csv_chunk_mp(archive_file, polygon)
            elif object_in_folder.is_file():
                file_name, gdf = load_csv_chunk_mp(object_in_folder, polygon)
            logging.info('Extraction time {0}'.format(time.time() - start))
            if file_name:
                gdf.to_csv(OUTPUT_DATA / '{0}_{1}.csv'.format(file_name, polygon_path.stem.split('.')[0]))
                # gdf.to_file(filename=OUTPUT_DATA / 'data_split_{0}_{1}.shp.zip'.format(file_name, polygon_path), driver='ESRI Shapefile')

    else:
        start = time.time()
        file_name, gdf = load_csv_chunk_mp(data_folder, polygon)
        logging.info('Extraction time {0}'.format(time.time() - start))
        gdf.to_csv(OUTPUT_DATA / '{0}_{1}.csv'.format(file_name, polygon_path.stem.split('.')[0]))
