# spark-uc: Spark 4.2.0 (python3) + Delta Lake + Unity Catalog + S3A
#
# Image with the Delta Lake and Unity Catalog connector jars pre-fetched at
# build time. The official apache/spark image downloads those jars
# (~105 MB, 57 artifacts) on every fresh container when using --packages;
# here the download happens once, at build time, and the runtime is a cache
# hit in Ivy (no download).
#
# Final user: uid 1001 (non-root), home /home/app. The Ivy cache lives in
# /home/app/.ivy2.5.2, owned by the final user — Ivy must write the
# resolution report even when everything is a cache hit.
#
# S3 (Floci/real S3): the official Spark image does NOT include hadoop-aws
# (S3A) — without it ``s3a://`` fails with "No FileSystem for scheme
# 's3a'". Here hadoop-aws is pre-baked alongside Delta/UC; at runtime the
# ``--packages`` includes org.apache.hadoop:hadoop-aws and Ivy is a cache
# hit.
#
# Pinned versions — do not float without retesting:
#   - io.delta:delta-spark_4.2_2.13:4.4.1
#   - io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0
#   - org.apache.hadoop:hadoop-aws:3.5.0 (Spark 4.2's Hadoop 3.5.0)
#
# IMPORTANT (empirically validated in 2026-10):
#   - The Delta artifact for Spark 4.2 is the SUFFIXED delta-spark_4.2_2.13
#     (available since Delta 4.4.0). The delta-spark_2.13:4.3.1 resolves
#     against Spark 4.1.x and breaks at runtime on Spark 4.2.0
#     (NoSuchMethodError in CatalogStorageFormat).
#   - Delta 4.4.x requires the session to have a DeltaCatalog registered
#     (otherwise CREATE/INSERT on delta tables fails with
#     DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG). That is
#     why the runtime conf registers spark_catalog=DeltaCatalog and the UC
#     catalog under the UC catalog's name (e.g. demo).
#
# Build:
#   docker build -f Dockerfile.uc -t spark-uc:0.3.0 .
#
# Usage (same pattern as conftest.py, with Unity Catalog):
#   docker run --rm spark-uc:0.2.0 \
#     /opt/spark/sbin/start-connect-server.sh --wait \
#       --packages io.delta:delta-spark_4.2_2.13:4.4.1,io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0,org.apache.hadoop:hadoop-aws:3.5.0 \
#       --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#       --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#       --conf spark.sql.catalog.demo=io.unitycatalog.spark.UCSingleCatalog \
#       --conf spark.sql.catalog.demo.uri=http://<uc>:8080 \
#       --conf spark.sql.catalog.demo.type=static \
#       --conf spark.sql.defaultCatalog=demo

FROM apache/spark:4.2.0-python3

ARG DELTA_VERSION=4.4.1
ARG UC_VERSION=0.6.0
# S3A: version of the Hadoop bundled with Spark 4.2.0 (hadoop-client-3.5.0).
ARG HADOOP_AWS_VERSION=3.5.0
ARG APP_UID=1001
ARG APP_HOME=/home/app

# JVM's HOME (= Ivy cache dir: $HOME/.ivy2.5.2). Applies at container
# runtime (the final uid uses /home/app and finds the pre-fetched cache).
ENV HOME ${APP_HOME}

# Named user with a fixed uid: Hadoop (UgiLinuxImpl) calls getpwuid for the
# uid at runtime — a uid with no entry in /etc/passwd breaks Spark
# ("Invalid UID, could not determine effective user").
USER root
RUN useradd -u "${APP_UID}" -d "${APP_HOME}" -s /bin/false appuser

# Pre-fetch: resolves Delta + UC connector (and transitive dependencies).
# Runs as root. BuildKit detail: at build time the sandbox pins the JVM's
# user.home to /root (ignoring HOME), so the cache lands in
# /root/.ivy2.5.2 — we copy it to the final user's home.
USER root
RUN /opt/spark/bin/spark-sql \
      --master 'local[1]' \
      --packages "io.delta:delta-spark_4.2_2.13:${DELTA_VERSION},io.unitycatalog:unitycatalog-spark_4.2_2.13:${UC_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      -e 'SELECT 1' \
    && mkdir -p "${APP_HOME}" \
    && cp -a /root/.ivy2.5.2 "${APP_HOME}/.ivy2.5.2" \
    && chown -R "${APP_UID}:${APP_UID}" "${APP_HOME}" \
      /opt/spark/work-dir /opt/spark/data

# Final user (non-root), home /home/app with the Ivy cache ready.
USER ${APP_UID}
