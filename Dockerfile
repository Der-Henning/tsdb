FROM python:3.9
COPY requirements.txt .

RUN pip install --user -r requirements.txt

WORKDIR /app

RUN python import nltk && python nltk.download('vader_lexicon')

COPY ./src .

HEALTHCHECK --interval=30s --timeout=30s --start-period=10s --retries=3 CMD python healthcheck.py

CMD [ "python", "-u", "./server.py" ] 
