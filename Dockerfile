FROM python:3.11
RUN apt-get update -y
#RUN apt-get install -y python-pip python-dev build-essential
COPY . /app
WORKDIR /app
RUN chmod +x /app/entrypoint.sh
RUN pip install -r requirements.txt
ENTRYPOINT ["/app/entrypoint.sh"]