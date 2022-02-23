FROM python:3.8

WORKDIR /root/bpc

COPY . .

RUN pip install -r requirements.txt

