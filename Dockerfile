FROM python:3.11-buster

RUN pip3 install poetry

RUN mkdir /stpo_processing

WORKDIR /stpo_processing

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false
RUN poetry install --without dev

FROM python:3.22-slim-buster as runtime

COPY /stpo_processing /.

ENTRYPOINT ["poetry", "run", "python", "-m", "main.py"]