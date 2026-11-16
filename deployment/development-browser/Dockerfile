# linux/amd64. All downloads are prepared and hash-checked before offline build.
FROM mcr.microsoft.com/playwright/python:v1.61.0-noble@sha256:80fd7c1aad9600ea348572dd46ca00b9ea31d890831f5838fc61319ab79900d2
USER root
COPY wheelhouse/ /opt/krim-browser/wheelhouse/
COPY debs/ /opt/krim-browser/debs/
COPY worker-requirements.txt /opt/krim-browser/requirements.txt
RUN python3 -c 'import sys; assert sys.version_info[:2] == (3, 12)' \
    && python3 -m venv --without-pip /opt/krim-browser/venv \
    && python3 -m pip --python /opt/krim-browser/venv/bin/python install --no-index --no-deps --require-hashes \
       --find-links=/opt/krim-browser/wheelhouse -r /opt/krim-browser/requirements.txt \
    && /opt/krim-browser/venv/bin/python -c 'import importlib.metadata as m; assert m.version("playwright") == "1.61.0"' \
    && dpkg --refuse-downgrade -i /opt/krim-browser/debs/*.deb \
    && rm -rf /opt/krim-browser/wheelhouse /opt/krim-browser/debs \
    && mkdir -p /opt/krim-browser/app /run/krim-preview /run/krim-ca
COPY app/ /opt/krim-browser/app/
RUN /opt/krim-browser/venv/bin/python -I -c 'import sys; sys.path.insert(0,"/opt/krim-browser/app"); import worker,entrypoint; from playwright.async_api import async_playwright' \
    && test -x /usr/bin/certutil \
    && test "$(dpkg-query -W -f='${Version}' libnss3-tools)" = '2:3.98-1ubuntu0.3' \
    && test "$(dpkg-query -W -f='${Version}' libnss3)" = '2:3.98-1ubuntu0.3' \
    && test -d /ms-playwright/chromium-1228 \
    && test -d /ms-playwright/chromium_headless_shell-1228
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp/krim-home \
    XDG_CONFIG_HOME=/tmp/krim-home/.config XDG_CACHE_HOME=/tmp/krim-home/.cache \
    NODE_EXTRA_CA_CERTS=/run/krim-ca/preview-ca.pem
USER 1000:1000
WORKDIR /opt/krim-browser/app
ENTRYPOINT ["/opt/krim-browser/venv/bin/python", "-I", "-c", "import sys,runpy;sys.path.insert(0,'/opt/krim-browser/app');runpy.run_path('/opt/krim-browser/app/entrypoint.py',run_name='__main__')"]
