FROM python:3.11
COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade pip -r /requirements.txt
COPY . /app
RUN chmod +x /app/entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]