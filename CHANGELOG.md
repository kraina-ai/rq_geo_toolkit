# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2025.5.1] - 2025-05-15

### Changed

- Simplified and refactored sorting by geometry

## [2025.5.0] - 2025-05-03

## [2025.4.4] - 2025-04-13

### Fixed

- Sorting geocoding results by importance after download from Nominatim

## [2025.4.3] - 2025-04-10

### Added

- Option to compress DuckDB SQL query instead of existing parquet file

## [2025.4.2] - 2025-04-10

### Added

- Option to keep input after sorting with dedicated `remove_input_file` parameter

### Changed

- Modified sorted parquet writing by manipulating loaded tables in memory instead of writing by chunk

## [2025.4.1] - 2025-04-06

### Added

- Option to pass parquet compression with level and number of rows per row group

## [2025.4.0] - 2025-04-04

### Added

- GeoParquet file sorting
- GeoParquet file compression
- Geocoder logic
- Rich utility constants
- Georelated constants
- DuckDB initialization function

[unreleased]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.5.1...HEAD
[2025.5.1]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.5.0...2025.5.1
[2025.5.0]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.4.4...2025.5.0
[2025.4.4]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.4.3...2025.4.4
[2025.4.3]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.4.2...2025.4.3
[2025.4.2]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.4.1...2025.4.2
[2025.4.1]: https://github.com/kraina-ai/rq_geo_toolkit/compare/2025.4.0...2025.4.1
[2025.4.0]: https://github.com/kraina-ai/rq_geo_toolkit/compare/7d20aec8f2d1f49031920ef62084c59c9a3b8098...2025.4.0
