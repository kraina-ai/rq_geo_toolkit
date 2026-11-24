"""Tests for Folium layers."""

from typing import Any, Union

import folium
import pytest

from rq_geo_toolkit.folium import MapLibreLayer


@pytest.mark.parametrize(
    "style,expected_js",
    [
        ("https://example.com/style.json", '"https://example.com/style.json"'),
        ({"version": 8, "layers": []}, '{"layers": [], "version": 8}'),
    ],
)  # type: ignore
def test_maplibre_layer(style: Union[str, dict[str, Any]], expected_js: str) -> None:
    """Test that the style is drawn by the MapLibre GL JS instead of raster tiles."""
    layer = MapLibreLayer(style)
    folium_map = folium.Map(tiles=layer)
    html = folium_map.get_root().render()

    assert f"L.maplibreGL({{style: {expected_js}}})" in html
    assert f"{layer.get_name()}.addTo({folium_map.get_name()})" in html
    assert "leaflet-maplibre-gl.js" in html
    assert "L.tileLayer(" not in html


def test_layer_control() -> None:
    """Test that the layer is a named base layer."""
    folium_map = folium.Map(tiles=MapLibreLayer("https://example.com/style.json", name="Example"))
    folium.LayerControl().add_to(folium_map)

    assert '"Example"' in folium_map.get_root().render()


def test_max_zoom() -> None:
    """Test that the default zoom limit doesn't replace the limit of the map."""
    folium_map = folium.Map(tiles=MapLibreLayer("https://example.com/style.json"), maxZoom=22)
    name = folium_map.get_name()

    html = folium_map.get_root().render()
    assert f"if ({name}.options.maxZoom === undefined) {name}.setMaxZoom(20);" in html
