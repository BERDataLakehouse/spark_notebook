ARG BASE_TAG=pr-156
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
COPY configs/grouped_s3_contents.py /etc/jupyter/grouped_s3_contents.py

COPY scripts/patch_jupyter_ai.py /tmp/patch_jupyter_ai.py
RUN python3 /tmp/patch_jupyter_ai.py && rm /tmp/patch_jupyter_ai.py

COPY scripts/patch_s3contents.py /tmp/patch_s3contents.py
RUN python3 /tmp/patch_s3contents.py && rm /tmp/patch_s3contents.py

# TODO: Remove this Polaris E2E override once Polaris is ready.
# Generated clients should be published and consumed through spark_notebook_base.
# For now, install locally regenerated clients after the base image so tests use
# the current sibling repos without rebuilding or publishing spark_notebook_base.
COPY --from=minio_manager_service_client . /tmp/minio_manager_service_client
COPY --from=datalake_mcp_server_client . /tmp/datalake-mcp-server-client
RUN eval "$(conda shell.bash hook)" \
    && uv pip install --reinstall --no-deps --system \
        /tmp/minio_manager_service_client \
        /tmp/datalake-mcp-server-client \
    && rm -rf /tmp/minio_manager_service_client /tmp/datalake-mcp-server-client

COPY notebook_utils /tmp/notebook_utils
RUN eval "$(conda shell.bash hook)" && uv pip install --no-deps --system /tmp/notebook_utils && rm -rf /tmp/notebook_utils

RUN eval "$(conda shell.bash hook)" && uv pip install --no-deps --system "git+https://github.com/kbase/data-lakehouse-ingest.git@v0.0.8"

WORKDIR /home
ENTRYPOINT ["/entrypoint.sh"]
