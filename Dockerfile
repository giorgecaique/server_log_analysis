FROM python:3-alpine

ADD . /app

RUN pip install pandas

WORKDIR /app

CMD ["python", "main.py"]