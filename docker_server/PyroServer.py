import Pyro4
import sys
import time
from Server import operation

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class node_requst():
    def __init__(self):
        self.data = {}
        self.command = []
        self.path = ""
        self.server = type('server', (object,), {'conti':True})
    
    def put_values(self, command, json_path):
        self.command = command.split(' ')
        self.path = json_path
    
    def operation(self):
        msg = operation(self.command, self.path, self.server)
        #msg = self.command.upper()
        return msg

if __name__=="__main__":
    portocol = sys.argv[1]
    PORT = int(sys.argv[2])
    
    daemon = Pyro4.Daemon(host='', port=PORT)
    request = node_requst()
    def clean_up():
        return request.server.conti
    daemon.register(request, objectId='node.request')
    #print("open server at {}".format(PORT))
    daemon.requestLoop(loopCondition=clean_up)
    daemon.close()