FROM r-base:4.0.0

ENV RENV_VERSION 0.14.0

RUN rm /etc/apt/apt.conf.d/default
RUN apt-get update -y
RUN apt-get install -y dpkg-dev zlib1g-dev libssl-dev libffi-dev
RUN apt-get install -y curl libcurl4-openssl-dev
RUN R -e "install.packages('synapser', repos=c('http://ran.synapse.org', 'http://cran.fhcrc.org'))"

ENV PYTHON /usr/local/lib/R/site-library/PythonEmbedInR/bin/python3.6

RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts

RUN R -e 'renv::restore()'
