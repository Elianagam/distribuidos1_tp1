FROM python:3.9.7-slim

RUN pip install --upgrade pip && pip3 install pytz

COPY app /
COPY common /common
CMD [ "python", "./main.py" ]