FROM python:3.11
RUN apt-get update -y
COPY requirements.txt /
RUN pip install -r /requirements.txt
COPY . /app
WORKDIR /app
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]