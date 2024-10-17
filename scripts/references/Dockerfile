FROM rstudio/r-base:4.0-bullseye

# Set working directory
WORKDIR /usr/local/src/myscripts

# Set environment variable for renv version
ENV RENV_VERSION 0.14.0

# Update apt-get and install system dependencies
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    dpkg-dev zlib1g-dev libssl-dev libffi-dev \
    libcurl4-openssl-dev curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install R packages including remotes and renv
RUN R -e "install.packages('remotes', repos = 'https://cloud.r-project.org')" && \
    R -e "remotes::install_github('rstudio/renv', ref = '${RENV_VERSION}')" || true

# Install synapser with specific version
RUN R -e "remotes::install_version('synapser', version = '0.11.7', repos = c('http://ran.synapse.org', 'http://cran.fhcrc.org'))"

# Set Python environment variable for R
ENV PYTHON /usr/local/lib/R/site-library/PythonEmbedInR/bin/python3.6

# Copy only renv.lock first to leverage Docker cache for dependencies
COPY renv.lock renv.lock

# Restore R environment with renv
RUN R -e "renv::restore()"

# Copy the local project files into the container
COPY . .
