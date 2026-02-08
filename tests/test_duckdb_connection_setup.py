"""Test if provisioning duckdb connection works."""

import tempfile
from pathlib import Path

import duckdb
import pytest

from rq_geo_toolkit.duckdb import run_query_with_memory_monitoring, set_up_duckdb_connection


def test_local_file_name() -> None:
    """Test if db file name is created deterministically."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(tmp_dir_path=tmp_dir_name),
    ):
        files_in_tmp_dir = list(Path(tmp_dir_name).glob("*.duckdb"))
        assert len(files_in_tmp_dir) == 1
        assert files_in_tmp_dir[0].stem == "db"


def test_randomized_local_file_name() -> None:
    """Test if db file name is randomised."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=tmp_dir_name, duckdb_conn_kwargs={"randomize_db_file_name": True}
        ),
    ):
        files_in_tmp_dir = list(Path(tmp_dir_name).glob("*.duckdb"))
        assert len(files_in_tmp_dir) == 1
        assert files_in_tmp_dir[0].stem != "db"


def test_query_provisioning() -> None:
    """Test if query provisioning is executed."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=tmp_dir_name,
            duckdb_conn_kwargs={"provisioning_queries": ["SET threads = 1;"]},
        ) as conn,
    ):
        threads_limit_provisioned = conn.sql(
            "SELECT current_setting('threads') AS threads"
        ).fetchone()[0]
        assert threads_limit_provisioned == 1


def test_conn_config() -> None:
    """Test if connection config is used."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=tmp_dir_name, duckdb_conn_kwargs={"config_kwargs": {"threads": 1}}
        ) as conn,
    ):
        threads_limit_provisioned = conn.sql(
            "SELECT current_setting('threads') AS threads"
        ).fetchone()[0]
        assert threads_limit_provisioned == 1


def test_loading_official_extensions() -> None:
    """Test if loading official extensions work."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=tmp_dir_name,
            duckdb_conn_kwargs={"official_extensions_to_load": ["vss"]},
        ) as conn,
    ):
        vss_loaded = conn.sql(
            """
            SELECT EXISTS (
                FROM duckdb_extensions() WHERE loaded AND extension_name = 'vss'
            ) AS vss_loaded;
            """
        ).fetchone()[0]
        assert vss_loaded


def test_missing_vss_official_extension() -> None:
    """Test if missing community extension throws error."""
    with (
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
        set_up_duckdb_connection(tmp_dir_path=tmp_dir_name) as conn,
    ):
        vss_loaded = conn.sql(
            """
            SELECT EXISTS (
                FROM duckdb_extensions() WHERE loaded AND extension_name = 'vss'
            ) AS vss_loaded;
            """
        ).fetchone()[0]
        assert not vss_loaded


def test_loading_community_extensions() -> None:
    """Test if loading community extensions work."""
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name:
        run_query_with_memory_monitoring(
            "SELECT h3_latlng_to_cell(37.7887987, -122.3931578, 9)",
            tmp_dir_path=Path(tmp_dir_name),
            duckdb_conn_kwargs={"community_extensions_to_load": ["h3"]},
        )


def test_missing_h3_community_extension() -> None:
    """Test if missing community extension throws error."""
    with (
        pytest.raises(duckdb.CatalogException),
        tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name,
    ):
        run_query_with_memory_monitoring(
            "SELECT h3_latlng_to_cell(37.7887987, -122.3931578, 9)", tmp_dir_path=Path(tmp_dir_name)
        )
