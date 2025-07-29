docker build --progress plain -f tests/low_resources/Dockerfile -t rq-geo-toolkit_low-resources-test .
docker run --rm --memory=512m -v "$(pwd)/files:/app/files" \
    -e GITHUB_ACTION -e GITHUB_RUN_ID -e GITHUB_REF -e GITHUB_REPOSITORY \
    -e GITHUB_SHA -e GITHUB_HEAD_REF -e CODECOV_TOKEN  -e LAST_COMMIT_SHA \
    rq-geo-toolkit_low-resources-test /bin/bash /app/run_tests.sh
