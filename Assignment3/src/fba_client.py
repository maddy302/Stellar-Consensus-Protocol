#reference https://twistedmatrix.com/documents/current/core/howto/udp.html
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
import time
class MulticastPingClient(DatagramProtocol):
    nodeName = ""
    def __init__(self, nodeName):
        self.nodeName = nodeName

    def startProtocol(self):
        # Join the multicast address, so we can receive replies:
        self.transport.joinGroup("225.0.0.5")
        # Send to 225.0.0.5:9999 - all listeners on the multicast address
        # (including us) will receive this message.
        # Messages in order:
        # 1. foo:$10
        # 2. bar:$30
        # 3. foo:$20
        # 4. bar:$20
        # 5. foo:$30
        # 6. bar:$10
        msg_list = [{"foo":"$10"},{"bar":"$30"},{"foo":"$20"},{"bar":"$20"},{"foo":"$30"},{"bar":"$10"} ]

        for dict_temp in msg_list:
            name = list(dict_temp.keys())
            msg = "initial|||"+str(nodeName)+"|||"+name[0]+"|||"+str(dict_temp[name[0]][1:])+"|||"
            msg = str.encode(msg)
            #print(msg)
            self.transport.write(msg, ("225.0.0.5", 3000))
            time.sleep(5)
        
        # msg = "Meesage from client " + str(self.nodeName)
        # #initial|||<node name/port>|||data_key|||data_value
        # msg = "initial|||"+str(nodeName)+"|||foo|||10|||"

        # msg = str.encode(msg)
        # print(msg)
        # self.transport.write(msg, ("225.0.0.5", 3000))

        # time.sleep(5)
        # msg2 = "initial|||"+str(nodeName)+"|||foo|||20|||"
        # msg2 = str.encode(msg2)
        # self.transport.write(msg2, ("225.0.0.5", 3000))


    def datagramReceived(self, datagram, address):
        data = repr(datagram)
        data = data[2:-1]
        data_arr = data.split("|||")
        if(data_arr[0] == "commit"):
            print("Datagram %s received from %s" % (repr(datagram) , data_arr[1]))

nodeName = sys.argv[1]
#nodeName = 3000
reactor.listenMulticast(3000, MulticastPingClient(nodeName), listenMultiple=True)
reactor.run()