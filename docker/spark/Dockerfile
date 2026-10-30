# Một image dùng chung cho Spark master, worker và driver (pipeline)
# -> cùng phiên bản Python/Spark ở mọi node, tránh lỗi lệch phiên bản.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

WORKDIR /opt/project
ENTRYPOINT ["/usr/bin/tini", "--"]
