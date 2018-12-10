#reference https://twistedmatrix.com/documents/current/core/howto/udp.html
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor


class abc(DatagramProtocol):

    def startProtocol(self):
        self.transport.setTTL(5)
        self.transport.joinGroup("225.0.0.5")
    
    def datagramReceived(self, datagram, address):
        print("Datagram %s received from %s" % (repr(datagram), repr(address)))
        print(datagram)
        if datagram==b"Client1: Hello" or datagram == "Client1: Hello":
            self.transport.write(b"Server receives; Hello",address)


reactor.listenMulticast(9999, abc(),listenMultiple=True)
reactor.run()