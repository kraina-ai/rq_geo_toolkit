"""Geocoding module for getting a geometry from query using Nominatim."""

import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Union, cast, overload

from geopy.geocoders.nominatim import Nominatim
from geopy.location import Location
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from rq_geo_toolkit._exceptions import QueryNotGeocodedError

USER_AGENT = "RQ Geo Toolkit Python package (https://github.com/kraina-ai/rq_geo_toolkit)"


@overload
def geocode_to_geometry(query: str) -> BaseGeometry: ...


@overload
def geocode_to_geometry(query: list[str]) -> BaseGeometry: ...


def geocode_to_geometry(query: Union[str, list[str]]) -> BaseGeometry:
    """Geocode a query to a (Multi)Polygon geometry using Nominatim."""
    if not isinstance(query, str):
        return unary_union([geocode_to_geometry(sub_query) for sub_query in query])

    h = hashlib.new("sha256")
    h.update(query.encode())
    query_hash = h.hexdigest()
    query_file_path = Path("cache").resolve() / f"{query_hash}.json"

    if not query_file_path.exists():
        query_results = Nominatim(user_agent=USER_AGENT).geocode(
            query, geometry="geojson", exactly_one=False
        )

        if not query_results:
            raise QueryNotGeocodedError(f"Zero results from Nominatim for query '{query}'.")

        polygon_result = _get_first_polygon(query_results)

        if not polygon_result:
            raise QueryNotGeocodedError(f"No polygon found for query '{query}'.")

        query_file_path.parent.mkdir(parents=True, exist_ok=True)
        query_file_path.write_text(json.dumps(polygon_result))
    else:
        polygon_result = json.loads(query_file_path.read_text())

    return unary_union(shape(polygon_result))


def _get_first_polygon(results: list[Location]) -> Optional[dict[str, Any]]:
    """
    Choose first result of geometry type (Multi)Polygon from list of results.

    Inspired by OSMnx implementation.
    """
    polygon_types = {"Polygon", "MultiPolygon"}

    # sorting fix from https://github.com/gboeing/osmnx/pull/1290/files
    for result in sorted(results, key=lambda location: location.raw["importance"], reverse=True):
        geojson_dict = cast(dict[str, Any], result.raw["geojson"])
        if geojson_dict["type"] in polygon_types:
            return geojson_dict

    return None
