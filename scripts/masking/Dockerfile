FROM rocker/r-base:4.2.2

WORKDIR /usr/local/src/myscripts

ENV RENV_VERSION 1.0.7

RUN rm /etc/apt/apt.conf.d/default
RUN apt-get update && \
    apt-get -y install libpng-dev \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

#RUN apt-get update -y
#RUN apt-get install -y dpkg-dev zlib1g-dev libssl-dev libffi-dev
# procps is required for nextflow tower
#RUN apt-get install -y curl libcurl4-openssl-dev procps
RUN R -e "install.packages('synapser', repos=c('http://ran.synapse.org', 'http://cran.fhcrc.org'))"
#ENV PYTHON /usr/local/lib/R/site-library/PythonEmbedInR/bin/python3.6

RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

COPY . .

RUN wget https://github.com/jgm/pandoc/releases/download/1.19.2.1/pandoc-1.19.2.1-1-amd64.deb
RUN dpkg -i pandoc-1.19.2.1-1-amd64.deb

RUN R -e "renv::restore()"
