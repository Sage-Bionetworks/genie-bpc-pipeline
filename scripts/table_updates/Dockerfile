FROM python:3.8

WORKDIR /root/scripts

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "update_data_table.py"]
