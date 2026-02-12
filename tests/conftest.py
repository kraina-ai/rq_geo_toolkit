"""Common components for tests."""

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest
from overturemaps.core import get_latest_release
from pytest import Item


def pytest_runtest_setup(item: Item) -> None:
    """Setup python encoding before `pytest_runtest_call(item)`."""
    os.environ["PYTHONIOENCODING"] = "utf-8"


@pytest.fixture(autouse=True, scope="session")  # type: ignore
def copy_geocode_cache() -> None:
    """Load cached geocoding results."""
    existing_cache_directory = Path(__file__).parent / "test_files" / "geocoding_cache"
    geocoding_cache_directory = Path("cache")
    geocoding_cache_directory.mkdir(exist_ok=True)
    for file_path in existing_cache_directory.glob("*.json"):
        destination_path = geocoding_cache_directory / file_path.name
        shutil.copy(file_path, destination_path)

def load_biggest_overture_place_file_from_stac() -> str:
    """Load the biggest place type file from Overture Maps STAC catalog."""
    latest_om_release = get_latest_release()
    stac_catalog = pd.read_parquet(
        f"https://stac.overturemaps.org/{latest_om_release}/collections.parquet"
    )
    s3_url = str(
        stac_catalog[stac_catalog["collection"] == "place"]
        .sort_values("num_rows", ascending=False)["assets"]
        .iloc[0]["aws"]["alternate"]["s3"]["href"]
    )
    return s3_url
