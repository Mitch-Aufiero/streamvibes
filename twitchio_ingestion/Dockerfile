FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install twitchio kafka-python requests

CMD [ "python", "./main.py" ]