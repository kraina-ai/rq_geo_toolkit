FROM python:3.12-slim-bookworm


RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git \
 && pip install --no-cache-dir \
    pdm coverage codecov-cli \
 && apt-get purge -y --auto-remove \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml pdm.lock pdm.toml README.md tests/low_resources/run_tests.sh .
RUN pdm install --skip post_install --no-self

COPY rq_geo_toolkit rq_geo_toolkit

RUN ls -la

RUN pdm install --skip post_install

COPY tests/test_sorting.py .

RUN ls -la
CMD ["pdm", "run", "pytest", "-s", "-v", "test_sorting.py"]
