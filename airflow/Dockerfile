FROM apache/airflow:latest-python3.11


ENV AIRFLOW_HOME=/opt/airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER root

RUN apt-get update -qq \
    && apt-get install -qq -y --no-install-recommends \
        vim \
        curl \
        gcc \
        python3-dev \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
USER root
ENV PATH=$PATH:/root/.local/bin

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=410.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fsSL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
        --bash-completion=false \
        --path-update=false \
        --usage-reporting=false \
        --additional-components alpha beta kubectl \
        --quiet \
    && rm -rf "${TMP_DIR}" \
    && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
    && gcloud --version


WORKDIR ${AIRFLOW_HOME}

COPY /scripts /scripts
RUN chmod -R +x /scripts

USER ${AIRFLOW_UID}