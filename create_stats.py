"""Compute per-country road-surface statistics from merged HOTOSM files."""

import argparse
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd

from config_utils import DEFAULT_CONFIG_PATH, get_path, get_section, load_config


LOGGER = logging.getLogger(__name__)


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Create per-country statistics from merged files.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to shared config.json")
    return parser.parse_args()


def parse_identifier(filename, pattern):
    """Extract country and optional part code from a merged file name."""
    match = re.search(pattern, filename)
    if not match:
        return None

    country = match.group(1)
    part = match.group(2) if match.lastindex and match.lastindex >= 2 else None
    identifier = f"{country}_{part}" if part else country
    return identifier.lower(), country.lower(), part


def create_stats(identifier, filepath):
    """Create one stats row from a single geospatial file."""
    _, country, part = identifier
    result = {
        "country": country,
        "part": part,
        "total_road_length": None,
        "total_unpaved_length": None,
        "total_paved_length": None,
        "total_missing_length": None,
        "provided_length": None,
    }

    try:
        gdf = gpd.read_file(filepath)
        result["total_road_length"] = gdf["osm_length"].sum() / 1000
        result["total_unpaved_length"] = (
            gdf.loc[gdf["combined_surface_DL_priority"] == "unpaved", "osm_length"].sum() / 1000
        )
        result["total_paved_length"] = (
            gdf.loc[gdf["combined_surface_DL_priority"] == "paved", "osm_length"].sum() / 1000
        )
        result["total_missing_length"] = (
            gdf.loc[gdf["combined_surface_DL_priority"].isna(), "osm_length"].sum() / 1000
        )
        result["provided_length"] = gdf.loc[gdf["surface"].isna(), "predicted_length"].sum() / 1000
    except Exception as error:  # pylint: disable=broad-exception-caught
        LOGGER.exception("Failed to compute stats for %s: %s", filepath, error)
    return result


def discover_files(path_to_files, pattern):
    """Discover and deduplicate files by country or part identifier."""
    unique_identifiers = set()
    files = {}

    for filename in os.listdir(path_to_files):
        parsed = parse_identifier(filename, pattern)
        if parsed is None:
            continue

        identifier_key, country, part = parsed
        if identifier_key in unique_identifiers:
            continue

        unique_identifiers.add(identifier_key)
        files[(identifier_key, country, part)] = os.path.abspath(os.path.join(path_to_files, filename))

    LOGGER.info("Discovered %d files for statistics generation.", len(files))
    return files


def main(config_path):
    """Run statistics generation workflow."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    config = load_config(config_path)
    script_config = get_section(config, "create_stats")

    input_path = get_path(config, script_config["input_path_key"])
    output_path = os.path.abspath(os.path.join(input_path, script_config["output_filename"]))
    workers = int(script_config.get("workers", os.cpu_count() or 1))
    pattern = script_config["pattern"]

    files = discover_files(input_path, pattern)
    country_data = []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(create_stats, identifier, filepath) for identifier, filepath in files.items()]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    country_data.append(result)
            except Exception as error:  # pylint: disable=broad-exception-caught
                LOGGER.exception("Worker failure while creating stats: %s", error)

    if not country_data:
        LOGGER.warning("No statistics were produced.")
        return

    frame = pd.DataFrame(country_data)
    numeric_columns = [
        "total_road_length",
        "total_unpaved_length",
        "total_paved_length",
        "total_missing_length",
        "provided_length",
    ]
    grouped = frame.groupby("country", as_index=False)[numeric_columns].sum(min_count=1)
    grouped.to_csv(output_path, index=False)
    LOGGER.info("Wrote country statistics to %s", output_path)


if __name__ == "__main__":
    ARGS = parse_args()
    main(ARGS.config)




