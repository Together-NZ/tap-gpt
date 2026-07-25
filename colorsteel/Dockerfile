FROM meltano/meltano:v3-python3.10

RUN apt-get update && apt-get install -y openssh-client git curl

WORKDIR /project

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -U "setuptools<81"

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone git@github.com:Together-NZ/tap-pinterest-ads.git /tmp/tap-pinterest-ads

COPY . .

RUN curl -fsSL -o dv360_standard.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json \
    && curl -fsSL -o dv360_youtube.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano install

RUN pip install \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

# Ensure plugin venvs have pip + pkg_resources (setuptools)
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
