"""Tests for dedicated geocoding function."""

from typing import Union

import pytest
from osmnx.geocoder import geocode_to_gdf
from unittest.mock import patch

from geopy.geocoders.nominatim import Nominatim

from rq_geo_toolkit.geocode import geocode_to_geometry
from rq_geo_toolkit._exceptions import QueryNotGeocodedError
from rq_geo_toolkit._geopandas_api_version import GEOPANDAS_NEW_API


@pytest.mark.parametrize(  # type: ignore
    "query",
    [
        "Vatican",
        "Monaco",
        "Dolnośląskie",
        ["United Kingdom", "Greater London"],
        ["Madrid", "Barcelona", "Seville"],
    ],
)
def test_geocoding(query: Union[str, list[str]]) -> None:
    """Test if geocoding works the same as osmnx."""
    if GEOPANDAS_NEW_API:
        assert geocode_to_gdf(query).union_all().equals(geocode_to_geometry(query))
    else:
        assert geocode_to_gdf(query).unary_union.equals(geocode_to_geometry(query))


@pytest.mark.parametrize(  # type: ignore
    "query",
    [
        "Broadway",
        "nonexistent_query",
        ["Dolnośląskie", "Broadway"],
    ],
)
def test_geocoding_errors(query: Union[str, list[str]]) -> None:
    """Test if geocoding throws error for two wrong queries."""
    with (
        patch.object(Nominatim, "geocode", return_value=None) as mock_method,
        pytest.raises(QueryNotGeocodedError),
    ):
        geocode_to_geometry(query)
        mock_method.assert_called_once()
