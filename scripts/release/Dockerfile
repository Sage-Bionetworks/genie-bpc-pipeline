FROM r-base:4.0.0

WORKDIR /usr/local/src/myscripts

ENV RENV_VERSION 0.14.0
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 0E98404D386FA1D9 6ED0E7B82643E131

RUN rm /etc/apt/apt.conf.d/default
RUN apt-get update -y && apt-get install -y dpkg-dev zlib1g-dev libssl-dev libffi-dev curl libcurl4-openssl-dev procps

RUN R -e "install.packages('synapser', repos=c('http://ran.synapse.org', 'http://cran.fhcrc.org'))"

ENV PYTHON /usr/local/lib/R/site-library/PythonEmbedInR/bin/python3.6

RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

COPY . .

RUN R -e "renv::restore()"
