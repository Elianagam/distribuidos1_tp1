FROM python:3.8
COPY app /
COPY common /common
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir filelock

CMD [ "python", "./main.py" ]