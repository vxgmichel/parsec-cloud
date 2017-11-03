FROM python:3.6-alpine

RUN apk add --no-cache \
    postgresql-dev gcc musl-dev libffi-dev openssl-dev linux-headers readline

ADD . /code
WORKDIR /code

RUN pip install -r requirements.txt
RUN python setup.py install

EXPOSE 8000

ENTRYPOINT ["/code/entrypoint.sh"]
