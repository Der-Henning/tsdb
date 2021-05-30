FROM python:3.9
COPY requirements.txt .

RUN pip install --user -r requirements.txt

RUN python -m nltk.downloader vader_lexicon

WORKDIR /app

COPY ./src .

HEALTHCHECK --interval=30s --timeout=30s --start-period=10s --retries=3 CMD python healthcheck.py

CMD [ "python", "-u", "./server.py" ] 
