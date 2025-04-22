FROM python:3.11
WORKDIR /app

COPY app/ .
COPY .env .

RUN pip install -r requirements.txt

CMD ["python", "-u", "main.py"]
