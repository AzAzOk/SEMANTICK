FROM python:3.11-slim

WORKDIR /app

RUN sed -i 's|http://|https://|g' /etc/apt/sources.list.d/debian.sources

COPY *.deb /tmp/debs/

RUN dpkg -i /tmp/debs/libssl1.1_*.deb || true && \
    dpkg -i /tmp/debs/openssl_*.deb || true && \
    dpkg -i /tmp/debs/ca-certificates_*.deb || true

COPY app ./app
COPY .env ./

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8443

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8443"]