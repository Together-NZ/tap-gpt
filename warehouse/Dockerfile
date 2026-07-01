FROM meltano/meltano:latest

RUN apt-get update && apt-get install -y openssh-client git curl

WORKDIR /project

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN bash -c '\
  for i in {1..3}; do meltano install loader target-bigquery     && break || sleep 5; done && \
  for i in {1..3}; do meltano install orchestrator airflow       && break || sleep 5; done && \
  for i in {1..3}; do meltano install transformer dbt-bigquery   && break || sleep 5; done && \
  for i in {1..3}; do meltano install file files-airflow          && break || sleep 5; done'

RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery google-cloud-storage pandas pyarrow

RUN for v in /project/.meltano/*/*/venv/bin/python; do \
  "$v" -m ensurepip --upgrade || true; \
  "$v" -m pip install -U "setuptools<81" || true; \
done


# Install Meltano plugins (will use meltano.yml)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano install

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone git@github.com:Together-NZ/tap-hivestack.git /tmp/tap-hivestack || true && \
    git clone git@github.com:Together-NZ/tap-ttd.git /tmp/tap-ttd || true && \
    git clone https://github.com/Together-NZ/tap_snapchat_ads.git /tmp/tap_snapchat_ads || true

RUN curl -sSL -o dv360_standard.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json
RUN curl -sSL -o dv360_youtube.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps
    
ENTRYPOINT ["meltano"]
