
FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

ENV PORT=10000
ENV PYTHONPATH=/code/app
EXPOSE 10000

CMD ["sh", "-c", "fastapi run /code/app/main.py --host 0.0.0.0 --port ${PORT}"]
