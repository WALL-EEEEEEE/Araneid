FROM python:3.7
COPY . apps/araneid
RUN  pip3 install -e apps/araneid 
RUN  araneid