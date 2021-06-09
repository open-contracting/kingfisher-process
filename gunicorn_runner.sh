#!/bin/sh
gunicorn --bind 0.0.0.0:8000 ocdskingfisherprocess.amy01_wsgi -c gunicorn_amy01.conf.py