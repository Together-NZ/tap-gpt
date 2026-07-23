# Base Meltano image
FROM meltano/meltano:latest

RUN apt-get update && apt-get install -y openssh-client git curl

WORKDIR /project

COPY . .

# Download the DV360 query schema files used by tap-dv360.
RUN curl -fsSL -o dv360_standard.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json \
    && curl -fsSL -o dv360_youtube.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    bash -c '\
        function retry() { for i in {1..3}; do "$@" && break || sleep 5; done; } && \
        retry meltano install extractor tap-facebook && \
        retry meltano install extractor tap-ga4 && \
        retry meltano install extractor tap-dv360 && \
        retry meltano install extractor tap-tiktok && \
        retry meltano install loader target-bigquery && \
        retry meltano install transformer dbt-bigquery\
    '

RUN pip install \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps

ENTRYPOINT ["meltano"]
