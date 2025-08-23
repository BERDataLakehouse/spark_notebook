ARG BASE_TAG=latest
FROM ghcr.io/berdatalakehouse/spark_notebook_base:${BASE_TAG}

COPY configs/extensions /configs/extensions/
COPY configs/skel/* /etc/skel
COPY scripts/entrypoint.sh /entrypoint.sh
COPY configs/jupyter_docker_stacks_hooks /usr/local/bin/before-notebook.d
COPY configs/ipython_startup  /configs/ipython_startup

WORKDIR /home
ENTRYPOINT ["/entrypoint.sh"]