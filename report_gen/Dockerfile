FROM python:3.13
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt /
RUN pip install -r requirements.txt
WORKDIR /app
ADD ./code /app/
