FROM python:3.11
RUN apt-get update -y
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]