FROM rstudio/r-base:4.0-bullseye

WORKDIR /usr/local/src/myscripts

ENV RENV_VERSION 0.14.0
# RUN rm /etc/apt/apt.conf.d/default
RUN apt-get update -y && apt-get install -y dpkg-dev zlib1g-dev libssl-dev libffi-dev curl libcurl4-openssl-dev procps


RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"
RUN R -e "remotes::install_version('synapser', version = '0.11.7', repos = c('http://ran.synapse.org', 'http://cran.fhcrc.org'))"

ENV PYTHON /usr/local/lib/R/site-library/PythonEmbedInR/bin/python3.6


COPY . .

RUN R -e "renv::restore()"