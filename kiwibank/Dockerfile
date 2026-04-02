FROM meltano/meltano:latest

RUN apt-get update && apt-get install -y openssh-client git

WORKDIR /project

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN meltano install

RUN pip install \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps

ENTRYPOINT ["meltano"]
