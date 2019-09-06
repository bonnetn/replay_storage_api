FROM python:3.7-alpine3.10

EXPOSE 8888
ENV STORAGE_PATH /tmp

WORKDIR /replay_storage

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD main.py .
CMD python3 main.py
