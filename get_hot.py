"""Download and extract latest HOTOSM roads resources by country."""

import argparse
import json
import logging
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging

from config_utils import DEFAULT_CONFIG_PATH, get_path, get_section, load_config


LOGGER = logging.getLogger(__name__)


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Download latest HOTOSM roads resources.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to shared config.json")
    return parser.parse_args()


def serialize(obj):
    """Serialize Python objects (including datetime) into JSON-safe values."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {key: serialize(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [serialize(item) for item in obj]
    return obj


def ensure_directories(*directories):
    """Create all required directories if they do not exist."""
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        LOGGER.info("Ensured directory exists: %s", directory)


def rename_file(current_file_name, new_file_name):
    """Rename a file with warning-level error handling."""
    try:
        os.rename(current_file_name, new_file_name)
    except FileNotFoundError:
        LOGGER.warning("The file '%s' does not exist.", current_file_name)
    except FileExistsError:
        LOGGER.warning("A file named '%s' already exists.", new_file_name)
    except OSError as error:
        LOGGER.warning("Failed to rename '%s' to '%s': %s", current_file_name, new_file_name, error)


def unzip_file(zip_file_name, extract_to_directory):
    """Unzip an archive into the target directory."""
    os.makedirs(extract_to_directory, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_file_name, "r") as zip_ref:
            zip_ref.extractall(extract_to_directory)
    except FileNotFoundError:
        LOGGER.warning("The file '%s' does not exist.", zip_file_name)
    except zipfile.BadZipFile:
        LOGGER.warning("The file '%s' is not a valid zip file.", zip_file_name)
    except OSError as error:
        LOGGER.warning("Failed to unzip '%s': %s", zip_file_name, error)


def extract_first_wildcard(test_string, pattern):
    """Extract the first wildcard capture group from a regex pattern."""
    if not test_string:
        return None
    match = re.search(pattern, test_string)
    if match:
        return match.group(1)
    return None


def parse_hdx_timestamp(timestamp_str):
    """Parse HDX timestamp values in the formats used by resource metadata."""
    for date_format in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(timestamp_str, date_format)
        except ValueError:
            continue
    LOGGER.warning("Could not parse timestamp '%s'.", timestamp_str)
    return None


def write_to_file(data, filename):
    """Write JSON content to a file with pretty formatting."""
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, default=serialize, indent=4)


def build_country_data(datasets, pattern):
    """Build mapping of ISO3 country code to latest resource metadata."""
    country_data = {}

    for dataset_index, dataset in enumerate(datasets):
        for resource_index, resource in enumerate(dataset.get_resources()):
            timestamp_str = resource.get("created")
            url = resource.get("download_url")
            name = resource.get("name")
            country_iso3 = extract_first_wildcard(name, pattern)

            if not all([timestamp_str, url, name, country_iso3]):
                continue

            timestamp_dt = parse_hdx_timestamp(timestamp_str)
            if timestamp_dt is None:
                continue

            existing = country_data.get(country_iso3)
            if existing and timestamp_dt <= existing["timestamp"]:
                continue

            country_data[country_iso3] = {
                "url": url,
                "timestamp": timestamp_dt,
                "dataset": dataset_index,
                "resources": resource_index,
                "name": name,
            }

    LOGGER.info("Prepared metadata for %d countries.", len(country_data))
    return country_data


def download_resource(datasets, data, directory_to_download):
    """Download and extract one selected resource."""
    dataset_index = data["dataset"]
    resource_index = data["resources"]
    resource = datasets[dataset_index].get_resources()[resource_index]

    url, path = resource.download(directory_to_download)
    LOGGER.info("Resource URL %s downloaded to %s", url, path)

    if os.path.exists(path):
        root, extension = os.path.splitext(path)
        zip_path = path if extension.lower() == ".zip" else f"{root}.zip"
        if zip_path != path:
            rename_file(path, zip_path)
        unzip_file(zip_path, directory_to_download)


def cleanup_files(directory_to_download, extensions):
    """Remove temporary files by extension from the download directory."""
    for filename in os.listdir(directory_to_download):
        if any(filename.endswith(extension) for extension in extensions):
            file_path = os.path.join(directory_to_download, filename)
            os.remove(file_path)
            LOGGER.info("Removed temporary file: %s", file_path)


def main(config_path):
    """Run metadata discovery, download, and extraction flow."""
    setup_logging()
    config = load_config(config_path)
    script_config = get_section(config, "get_hot")

    directory_to_download = get_path(config, "paths.hotosm.download_dir")
    metadata_dir = get_path(config, "paths.hotosm.metadata_dir")
    ensure_directories(directory_to_download, metadata_dir)

    Configuration.create(
        hdx_site=script_config["hdx_site"],
        user_agent=script_config["user_agent"],
        hdx_read_only=bool(script_config["hdx_read_only"]),
    )

    datasets = Dataset.search_in_hdx(script_config["search_query"])
    LOGGER.info("Found %d datasets for query '%s'.", len(datasets), script_config["search_query"])

    country_data = build_country_data(datasets, script_config["pattern"])
    metadata_path = os.path.abspath(os.path.join(metadata_dir, script_config["country_data_filename"]))
    write_to_file(country_data, metadata_path)
    LOGGER.info("Wrote metadata file to %s", metadata_path)

    if not country_data:
        LOGGER.warning("No countries were processed.")
        return

    start_time = time.time()
    workers = int(script_config.get("workers", max(1, (os.cpu_count() or 1) * 4)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_resource, datasets, data, directory_to_download): country
            for country, data in list(country_data.items())[0:2]
        }
    
        for future in as_completed(futures):
            country = futures[future]
            try:
                future.result()
            except Exception as error:  # pylint: disable=broad-exception-caught
                LOGGER.exception("Error downloading data for %s: %s", country, error)

    cleanup_files(directory_to_download, script_config.get("cleanup_extensions", [".txt", ".zip"]))

    end_time = time.time()
    n_countries = len(country_data)
    LOGGER.info("It took %.2f seconds to download %d countries.", end_time - start_time, n_countries)
    LOGGER.info("Average time per country: %.2f seconds", (end_time - start_time) / n_countries)


if __name__ == "__main__":
    ARGS = parse_args()
    main(ARGS.config)







