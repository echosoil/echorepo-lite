# Reuse the proven ECHOREPO scientific stack, with a Hub-compatible version.
FROM quay.io/jupyter/scipy-notebook:2026-07-28

USER root
RUN python -m pip install --no-cache-dir "jupyterhub==5.5.5" \
    && mkdir -p /home/jovyan/work \
    && chown -R jovyan:users /home/jovyan/work

# Library can be mounted read-only; API client is bundled in the image.
COPY notebooks/lib/ /opt/echorepo/lib/
ENV PYTHONPATH=/opt/echorepo
ENV ECHOREPO_URL=http://research-api:8080
USER jovyan
WORKDIR /home/jovyan/work
CMD ["jupyterhub-singleuser"]
