# ruff: noqa D
import multiprocessing
import os

import duckdb
import psutil
import quackosm as qosm
from psutil._common import bytes2human


def display_resources() -> None:
    print(
        "CPU count (duckdb):",
        duckdb.sql("SELECT current_setting('threads') AS threads").limit(1).fetchall()[0][0],
    )
    print("CPU count (multiprocessing):", multiprocessing.cpu_count())
    print("CPU count (psutil):", psutil.cpu_count())
    print("CPU count (psutil physical):", psutil.cpu_count(logical=False))

    print(
        "Memory (duckdb):",
        duckdb.sql("SELECT current_setting('memory_limit') AS memlimit").limit(1).fetchall()[0][0],
    )
    print("Memory (psutil):", bytes2human(psutil.virtual_memory().total))
    print("Memory (psutil, all):", psutil.virtual_memory())

    try:
        print("CPU affinity (os):", os.sched_getaffinity(0))  # type: ignore[attr-defined]
        print("CPU affinity (psutil):", psutil.Process().cpu_affinity())
        print("Memory limit (cgroup):", bytes2human(int(open("/sys/fs/cgroup/memory.max").read())))
    except Exception:
        pass


if __name__ == "__main__":
    display_resources()
    qosm.convert_osm_extract_to_parquet(
        osm_extract_query="Latvia",
        # osm_extract_query="Monaco",
        # osm_extract_query="Portugal",
        # osm_extract_query="czech-republic",
        # osm_extract_query="Poland",
        osm_extract_source="Geofabrik",
        ignore_cache=True,
        verbosity_mode="verbose",
        # sort_result=False,
        sort_result=True,
    )

    # compress_parquet_with_duckdb(
    #     input_file_path=Path(
    #         "files/geofabrik_europe_poland_nofilter_noclip_compact.parquet"
    #     ),
    #     # output_file_path=Path('files/geofabrik_europe_latvia_nofilter_noclip_compact.parquet'),
    #     output_file_path=Path(
    #         "files/geofabrik_europe_poland_nofilter_noclip_compact_2.parquet"
    #     ),
    #     # compression=self.compression,
    #     # compression_level=self.compression_level,
    #     # row_group_size=self.row_group_size,
    #     # working_directory=self.tmp_dir_path,
    # )
