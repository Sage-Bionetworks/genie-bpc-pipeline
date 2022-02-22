FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends build-essential r-base r-cran-randomforest python3.9 python3-pip python3-setuptools python3-dev

WORKDIR /root/bpc

COPY . .

RUN pip3 install pandas==1.3.5
RUN Rscript -e "renv::restore()"

CMD python3 tests/test.py && Rscript tests/test.R

