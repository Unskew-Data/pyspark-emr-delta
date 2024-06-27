#!/bin/bash

python -m pip install poetry && \
    poetry export -f requirements.txt --without-hashes -o requirements.txt && \
    rm -rf dist/ && mkdir -p dist/ && \
    poetry run pip install . -r requirements.txt -t dist/tmp && \
    cd dist/tmp && \
    find . -name "*.pyc" -delete && \
    zip -r ../packages.zip . -x 'numpy*/*' -x 'pandas*/*' -x 'pyarrow*/*' && \
    cd ../..

spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-core_2.12:2.0.1 --py-files dist/packages.zip pyspark_emr_delta/main.py
