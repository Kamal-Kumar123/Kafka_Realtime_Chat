
FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

ENV PORT=10000
EXPOSE 10000

CMD ["sh", "-c", "fastapi run app.main:app --host 0.0.0.0 --port ${PORT}"]
