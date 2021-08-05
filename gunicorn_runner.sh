#!/bin/sh
gunicorn --bind 0.0.0.0:$1 ocdskingfisherprocess.docker_wsgi --timeout 240 -c gunicorn_docker.py