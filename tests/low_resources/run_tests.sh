echo $GITHUB_SHA
coverage run --data-file=.coverage.base.tests --source=rq_geo_toolkit -m \
    pdm run pytest -v -s --durations=20 test_sorting.py
coverage combine
coverage xml -o coverage.xml
coverage report -m
codecov --verbose upload-process --disable-search --fail-on-error \
    -F low-resources-test -f coverage.xml -C $GITHUB_SHA -t $CODECOV_TOKEN
