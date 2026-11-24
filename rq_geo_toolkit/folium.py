"""Folium layers."""

from typing import Any, Union

from folium import TileLayer
from folium.elements import JSCSSMixin
from jinja2 import Template


class MapLibreLayer(JSCSSMixin, TileLayer):  # type: ignore[misc]
    """
    Basemap of a MapLibre style, drawn by MapLibre GL JS under the data of a Leaflet map.

    It can be used as the `tiles` of a `folium.Map` or `GeoDataFrame.explore()`. Every map needs a
    separate object.
    """

    _template = Template("""
        {% macro script(this, kwargs) %}
            var {{ this.get_name() }} = L.maplibreGL({style: {{ this.style|tojson }}});
            {{ this._parent.get_name() }}.setMaxZoom(20);
        {% endmacro %}
        """)

    # The last major version of MapLibre GL JS that is not an ES module, as the plugin needs.
    default_js = [
        ("maplibre_gl", "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.1/dist/maplibre-gl.js"),
        (
            "maplibre_gl_leaflet",
            "https://cdn.jsdelivr.net/npm/@maplibre/maplibre-gl-leaflet@0.1.4/leaflet-maplibre-gl.js",
        ),
    ]
    default_css = [
        ("maplibre_gl_css", "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.1/dist/maplibre-gl.css"),
    ]

    def __init__(self, style: Union[str, dict[str, Any]], name: str = "Basemap") -> None:
        """
        Create a basemap.

        Args:
            style (Union[str, dict[str, Any]]): URL of a MapLibre style, or the style itself.
            name (str, optional): Name of the layer in the layer control. Defaults to "Basemap".
        """
        # The attribution is read from the style by the plugin.
        super().__init__(tiles="MapLibre", attr=" ", name=name)
        self.style = style
