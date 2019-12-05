FROM ubuntu:latest
RUN apt update
RUN apt install python3 -y
RUN apt install python3-pip -y
RUN apt install libpq-dev -y
RUN apt-get install graphviz -y
WORKDIR /
COPY . /
RUN pip3 install -r requirements.txt
CMD ["python3","run.py"]
EXPOSE 5000/tcp
