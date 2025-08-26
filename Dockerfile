ARG BASE_TAG=main
FROM ghcr.io/berdatalakehouse/spark_notebook_base:${BASE_TAG}

COPY configs/extensions /configs/extensions/
COPY configs/skel/* /etc/skel
COPY scripts/entrypoint.sh /entrypoint.sh
COPY configs/jupyter_docker_stacks_hooks /usr/local/bin/before-notebook.d
COPY configs/ipython_startup  /configs/ipython_startup

COPY notebook_utils /tmp/notebook_utils
RUN uv pip install --system /tmp/notebook_utils && rm -rf /tmp/notebook_utils

WORKDIR /home
ENTRYPOINT ["/entrypoint.sh"]