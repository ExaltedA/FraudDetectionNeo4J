FROM python:3.10

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src src

CMD ["python3", "-u", "src/main.py"]

STOPSIGNAL SIGTERM
