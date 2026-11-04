FROM python:3.12-slim

RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# The SDK build to benchmark.
# build_images.sh sets both from sdk.env; the defaults build the upstream main branch.
ARG SDK_REPO=https://github.com/logicalclocks/hopsworks-api.git
ARG SDK_REF=main
ENV SDK_REF=${SDK_REF}

RUN pip install --no-cache-dir \
    "locust>=2.32,<3" \
    "hopsworks[python] @ git+${SDK_REPO}@${SDK_REF}#subdirectory=python"

WORKDIR /work
ENTRYPOINT ["locust"]
