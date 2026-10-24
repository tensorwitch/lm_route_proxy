FROM python:3.12-slim

WORKDIR /app

COPY docker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY core.py routing.py settings.json ./

RUN useradd --create-home --uid 10001 lmproxy \
 && mkdir -p /app/_temp_ \
 && chown -R lmproxy:lmproxy /app
USER lmproxy

EXPOSE 9354

STOPSIGNAL SIGTERM
CMD ["python", "core.py"]
