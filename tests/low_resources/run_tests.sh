echo $LAST_COMMIT_SHA
pdm run coverage run --data-file=.coverage.base.tests --source=rq_geo_toolkit -m \
    pytest -v -s --durations=20 tests/test_sorting.py
pdm run coverage combine
pdm run coverage xml -o coverage.xml
pdm run coverage report -m
pdm run codecov --verbose upload-process --disable-search --fail-on-error \
    -F low-resources-test -f coverage.xml -C $LAST_COMMIT_SHA -t $CODECOV_TOKEN \
    --git-service github -B $GITHUB_REF -r $GITHUB_REPOSITORY
