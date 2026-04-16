"""Compare available HOTOSM country stats with available prediction-country coverage."""

import argparse
import logging
import os

import pandas as pd
import pycountry

from config_utils import DEFAULT_CONFIG_PATH, get_path, get_section, load_config


LOGGER = logging.getLogger(__name__)


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Compare HOTOSM and prediction country coverage.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to shared config.json")
    return parser.parse_args()


def alpha2_to_alpha3(alpha2_code):
    """Convert ISO alpha-2 country code to alpha-3."""
    country = pycountry.countries.get(alpha_2=alpha2_code.upper())
    return country.alpha_3 if country else None


def discover_prediction_countries(partition_root):
    """Discover country codes from partition directory structure."""
    continents = [
        os.path.abspath(os.path.join(partition_root, entry))
        for entry in os.listdir(partition_root)
        if os.path.isdir(os.path.join(partition_root, entry))
    ]

    countries_alpha_2 = [
        entry.split("=")[-1]
        for continent in continents
        for entry in os.listdir(continent)
        if os.path.isdir(os.path.join(continent, entry))
    ]

    countries_alpha_3 = []
    countries_without_alpha3 = []
    for alpha2 in countries_alpha_2:
        alpha3 = alpha2_to_alpha3(alpha2)
        if alpha3:
            countries_alpha_3.append(alpha3.lower())
        else:
            countries_without_alpha3.append(alpha2.lower())

    return set(countries_alpha_3), countries_without_alpha3


def write_country_list(filepath, countries):
    """Write country list into a one-column CSV."""
    pd.DataFrame({"country": sorted(countries)}).to_csv(filepath, index=False)
    LOGGER.info("Wrote %d rows to %s", len(countries), filepath)


def main(config_path):
    """Run comparison workflow and export CSV summaries."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    config = load_config(config_path)
    script_config = get_section(config, "compare_hot_our_data")

    country_stats_filepath = get_path(config, "paths.hotosm.country_stats_file")
    output_filepath = get_path(config, "paths.hotosm.comparison_dir")
    partition_filepath = get_path(config, "paths.predictions.final_partitioned_filtered_dir")

    os.makedirs(output_filepath, exist_ok=True)

    country_stats = pd.read_csv(country_stats_filepath)
    countries_predictions, countries_wo_alpha_3 = discover_prediction_countries(partition_filepath)

    country_column = script_config["country_column"]
    total_road_length_column = script_config["total_road_length_column"]

    countries_wo_info_frame = country_stats[country_stats[total_road_length_column] == 0.0]
    countries_not_in_hotosm = country_stats[
        ~country_stats[country_column].astype(str).str.lower().isin(countries_predictions)
    ][country_column].unique()
    countries_in_hotosm_wo_info = countries_wo_info_frame[
        countries_wo_info_frame[country_column].astype(str).str.lower().isin(countries_predictions)
    ][country_column].unique()
    countries_wo_info = country_stats[country_stats[total_road_length_column] == 0.0][country_column].unique()

    output_filenames = script_config["output_filenames"]
    write_country_list(
        os.path.abspath(os.path.join(output_filepath, output_filenames["countries_not_in_hotosm"])),
        countries_not_in_hotosm,
    )
    write_country_list(
        os.path.abspath(os.path.join(output_filepath, output_filenames["countries_in_hotosm_wo_info"])),
        countries_in_hotosm_wo_info,
    )
    write_country_list(
        os.path.abspath(os.path.join(output_filepath, output_filenames["countries_wo_info"])),
        countries_wo_info,
    )
    write_country_list(
        os.path.abspath(os.path.join(output_filepath, output_filenames["countries_without_alpha3"])),
        countries_wo_alpha_3,
    )


if __name__ == "__main__":
    ARGS = parse_args()
    main(ARGS.config)

