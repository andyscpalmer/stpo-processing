FROM python:3.11-slim-buster

WORKDIR /app

RUN echo fart

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "stpo_processing/main.py" ]