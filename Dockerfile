FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends build-essential r-base r-cran-randomforest python3.9 python3-pip python3-setuptools python3-dev

WORKDIR /bpc

COPY requirements.txt /bpc/requirements.txt
RUN pip3 install -r requirements.txt

COPY . /bpc

RUN Rscript -e "renv::restore()"

CMD python3 tests/test.py && Rscript tests/test.R

