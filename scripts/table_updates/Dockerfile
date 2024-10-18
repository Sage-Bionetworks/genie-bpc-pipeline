FROM python:3.9

WORKDIR /root/scripts

COPY . .

RUN pip install -r requirements.txt

# Commented out ENTRYPOINT for NF tower
# ENTRYPOINT ["python", "update_data_table.py"]
