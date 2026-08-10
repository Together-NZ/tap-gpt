# Base Meltano image
FROM meltano/meltano:v3-python3.10

# Enable SSH agent forwarding
# Required for cloning private GitHub repos via SSH
# Must be enabled via `--ssh default` during build
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /project

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -U "setuptools<81"

# Copy project files
COPY . .

# Install Meltano plugins (will use meltano.yml)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano install

# Install any additional Python libraries
RUN pip install --no-cache-dir \
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
      "$v" -m pip install --no-cache-dir -U "setuptools<81" || true; \
    done

# dbt: download packages (git deps → transform/dbt_packages)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps

# Entrypoint for container
ENTRYPOINT ["meltano"]
