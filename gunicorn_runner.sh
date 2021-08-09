#!/bin/sh
gunicorn --bind 0.0.0.0:$1 ocdskingfisherprocess.docker_wsgi --timeout 980 -c gunicorn_docker.py