FROM apify/actor-python:3.11

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install --with-deps chromium

COPY . ./

CMD ["python", "main.py"]
