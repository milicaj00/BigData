FROM python:3.10.2

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY . /app
CMD [ "sh", "-c", "python producer.py"]
