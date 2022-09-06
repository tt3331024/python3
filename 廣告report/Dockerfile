# Use the official Python image.
# https://hub.docker.com/_/python
FROM python:3.8
# FROM nginx
MAINTAINER hotaiconnected Jerry.Ko

# RUN apt-get install -y vim

# Create workdir and copy files.
WORKDIR /app 
COPY . /app 

# Install python dependencies.
RUN pip install -r requirements.txt

# EXPOSE 5001

CMD python main.py

