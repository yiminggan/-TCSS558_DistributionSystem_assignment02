FROM ubuntu
RUN apt-get update
# RUN apt-get install -y default-jre
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y net-tools
RUN pip3 install Pyro4
#RUN apt-get install -y telnet
#RUN apt-get install -y ufw
COPY Server.py /
COPY runserver.sh /
COPY PyroServer.py /
ENTRYPOINT ["/runserver.sh"]
CMD ["300"]


