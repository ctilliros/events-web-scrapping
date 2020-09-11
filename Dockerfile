FROM python:3.8
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update
RUN apt-get install -yq firefox-esr
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.24.0/geckodriver-v0.24.0-linux32.tar.gz
RUN tar -xvzf geckodriver-v0.24.0-linux32.tar.gz
RUN rm geckodriver-v0.24.0-linux32.tar.gz
RUN chmod +x geckodriver
RUN cp geckodriver /usr/local/bin/
RUN pip3 install -r requirements.txt
COPY . .
CMD ["python","code.py"]