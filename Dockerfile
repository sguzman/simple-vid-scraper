FROM python:3.7-alpine

COPY . /app
WORKDIR /app
RUN apk update && \
 apk add postgresql-libs && \
 apk add --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

ENTRYPOINT ["python3", "src/main.py"]