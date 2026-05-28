FROM meltano/meltano:v3-python3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -U "setuptools<81"

COPY . .

RUN --mount=type=ssh \
    mkdir -p /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && ssh -T git@github.com || true \
    && meltano install
# Optional: Additional Python packages
RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery pandas db-dtypes pyhumps

RUN pip install --no-cache-dir "pyhumps>=3.8.0"
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps
RUN curl -sSL -o dv360_standard.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json
RUN curl -sSL -o dv360_youtube.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json
    
EXPOSE 5000 
ENTRYPOINT ["meltano"]
