# Resolve the official Node 24 bookworm tag to a digest before building.
# Git, compilers, Shell, CA certificates and Python come from buildpack-deps.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root
RUN printf 'Acquire::Retries "2";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\n' \
       > /etc/apt/apt.conf.d/80-probe-timeouts \
    && node -e "const fs=require('fs');const p='/etc/apt/sources.list.d/debian.sources';fs.writeFileSync(p,fs.readFileSync(p,'utf8').replaceAll('http://','https://'));" \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN install -d -o node -g node /workspace /home/node/.cache
ENV HOME=/home/node \
    npm_config_cache=/workspace/.cache/npm \
    PIP_CACHE_DIR=/workspace/.cache/pip
WORKDIR /workspace
USER node
CMD ["sleep", "infinity"]
