FROM python:3.8

ARG DATA_PATH=/data

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN groupadd -r runner && useradd --no-log-init -r -g runner runner

RUN mkdir -p $DATA_PATH
RUN chown -R runner:runner $DATA_PATH

WORKDIR /workdir
USER runner:runner
COPY --chown=runner:runner . .