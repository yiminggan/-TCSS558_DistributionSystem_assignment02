FROM ubuntu
RUN apt-get update
# RUN apt-get install -y default-jre
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y net-tools
RUN pip3 install Pyro4
COPY Client.py /
COPY entrypoint_client.sh /
COPY bigtest_rc.sh /
COPY bigtest_tc.sh /
COPY bigtest_uc.sh /
RUN chmod 777 bigtest_rc.sh bigtest_tc.sh bigtest_uc.sh
ENTRYPOINT ["/entrypoint_client.sh"]
CMD ["300"]


