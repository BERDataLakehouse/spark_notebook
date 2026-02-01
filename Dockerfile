ARG BASE_TAG=main
ARG BASE_REGISTRY=ghcr.io/berdatalakehouse/
FROM ${BASE_REGISTRY}spark_notebook_base:${BASE_TAG}

COPY configs/extensions /configs/extensions/
COPY configs/skel/* /etc/skel
COPY scripts/entrypoint.sh /entrypoint.sh
COPY configs/jupyter_docker_stacks_hooks /usr/local/bin/before-notebook.d
COPY configs/ipython_startup  /configs/ipython_startup

ENV SPARK_CONNECT_DEFAULTS_TEMPLATE=/configs/spark-defaults.conf.template
COPY configs/spark-defaults.conf.template ${SPARK_CONNECT_DEFAULTS_TEMPLATE}

COPY configs/jupyter_server_config.py /etc/jupyter/jupyter_server_config.py

COPY scripts/patch_jupyter_ai.py /tmp/patch_jupyter_ai.py
RUN python3 /tmp/patch_jupyter_ai.py && rm /tmp/patch_jupyter_ai.py

COPY scripts/patch_s3contents.py /tmp/patch_s3contents.py
RUN python3 /tmp/patch_s3contents.py && rm /tmp/patch_s3contents.py

COPY scripts/patch_s3contents_init.py /tmp/patch_s3contents_init.py
RUN python3 /tmp/patch_s3contents_init.py && rm /tmp/patch_s3contents_init.py

COPY notebook_utils /tmp/notebook_utils
RUN eval "$(conda shell.bash hook)" && uv pip install --no-deps --system /tmp/notebook_utils && rm -rf /tmp/notebook_utils

RUN eval "$(conda shell.bash hook)" && uv pip install --no-deps --system "git+https://github.com/kbase/data-lakehouse-ingest.git@v0.0.4"

WORKDIR /home
ENTRYPOINT ["/entrypoint.sh"]