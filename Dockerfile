FROM nexus.pgk.ru/custom-images/python:3.9-base as requirements-stage

WORKDIR /tmp

RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
FROM nexus.pgk.ru/custom-images/python:3.9-oracle

# pass these variables when building image inside gitlab-ci to override local .env file
ARG PROJECT_NAME
ENV PROJECT_NAME=$PROJECT_NAME

ARG PROJECT_VERSION
ENV PROJECT_VERSION=$PROJECT_VERSION

ARG PROJECT_ENVIRONMENT
ENV PROJECT_ENVIRONMENT=$PROJECT_ENVIRONMENT

ARG DB_HOST
ENV DB_HOST=$DB_HOST

ARG DB_USER
ENV DB_USER=$DB_USER

ARG DB_PASS
ENV DB_PASS=$DB_PASS

ARG DB_NAME
ENV DB_NAME=$DB_NAME

ARG REDIS
ENV REDIS=$REDIS

ARG REDIS_HOST
ENV REDIS_HOST=$REDIS_HOST

WORKDIR /code

RUN echo "" > /etc/apt/sources.list

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        unzip \
        gcc \
        libsasl2-dev \
        python-dev \
        libaio1 \
        libldap2-dev \
        libssl-dev \
    # Cleaning up temporary files and packages
    && apt-get autoremove -y \
    && apt-get autoclean -y \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


COPY --from=requirements-stage /tmp/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN pip install --no-cache-dir --upgrade PyYAML
RUN pip install --no-cache-dir --upgrade python-ldap
RUN pip install --no-cache-dir --upgrade jinja2

# FROM gitlab.pgkweb.ru:4567/oks/custom-images/python:base
#
# RUN apt-get update && apt-get install -y build-essential libldap2-dev libsasl2-dev slapd ldap-utils tox \
#     lcov valgrind

COPY ./deploy/.env.copy /code/.env
COPY ./deploy/gunicorn_conf.py /code/
COPY ./app /code/app
COPY ./config /code/config
COPY ./static /code/static
COPY ./templates /code/templates

CMD ["gunicorn", "app.main:app", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn_conf.py"]
