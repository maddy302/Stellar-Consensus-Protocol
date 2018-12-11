#reference https://twistedmatrix.com/documents/current/core/howto/udp.html
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import pickledb
import sys

#udp_server.py 3000 225.0.0.5
quorum_dict = {"q1" : "225.0.0.5"
                }
channel_nb = 3000
quorum_list = list()

class Message_N:
    id = ""
    data = ""
    commited = "N"
    prepared = "N"
    pre_prepared = "N"
    from_node=""
    #node_list_in_quorum = set()


    def __init__(self, id,data, commit, prepare, pre_prepare, from_node):
        self.id = id
        self.data = data
        self.commited = commit
        self.prepared = prepare
        self.pre_prepared = pre_prepare
        self.from_node = from_node

    # def _changeToPrepared(from_node):
    #     self.prepared_set.add(from_node)
    #     if(len(self.prepared_set)>= (int((2/3)*len(quorum_list)) + 1)):
    #         self.commited = "N"
    #         self.prepared = "Y"
    #         self.pre_prepared = "N"

    # def _changeToPrePrepared(from_node):
        
    #     self.pre_prepared_set.add(from_node)
    #     if(len(self.pre_prepared_set)>= (int((2/3)*len(quorum_list)) + 1)):
    #         self.commited = "N"
    #         self.prepared = "N"
    #         self.pre_prepared = "Y"

    # def _changeToCommitted():
    #     self.commited_set.add(from_node)
    #     if(len(self.))
    #     self.commited = "Y"
    #     self.prepared = "N"
    #     self.pre_prepared = "N"

class Multicast(DatagramProtocol):
    node_name = 0
    quorum_list = ""
    data_node_dict = {}
    quorum_pre_prepared_data = {} #{data_key : {server: data}}
    quorum_prepared_data = {} #{data_key : {server: data}}
    quorum_commit_data = {} #{data_key : {server: data}}
    node_list_in_quorum = set()
    db=""
    def __init__(self, node_name, *quorum):
        self.db = pickledb.load('assinment'+str(node_name)+'.db', False)
        self.node_name = node_name
        #self.node_type = node_type
        # for x in quorum:
        #     self.quorum_list.append(x)
        self.quorum_list = quorum[0]
        self.node_list_in_quorum.add(node_name)

    def startProtocol(self):
        self.transport.setTTL(5)
        #i = 0
        for x in self.quorum_list:
            self.transport.joinGroup(x)
            msg = "Node "+ str(self.node_name) + " of quorum has joined"
            #self.transport.write(b"Hello boys", (x, channel_nb))
            #self.transport.write(msg.encode('utf-8'), (x, channel_nb))
            self.transport.write(str.encode("Node Joined|||%s|||of quorum has joined" % (self.node_name)), (x, channel_nb))
    

    def datagramReceived(self, datagram, address):
        #print("%s received from %s " % (repr(datagram), repr(address)))
        data = repr(datagram)
        #print(type(data))
        #print(type(data))
        #print(data)
        data = data[2:-1]
        #print(type(data))
        #print(data)
        
        data_arr = data.split("|||") #initial|||<node name/port>|||data_key|||data_value
        #print("data_arrrr")
        #print(data_arr)
        from_node = data_arr[1]
        if(data_arr[0] == "Node Joined" and from_node!= self.node_name):
            # self.node_list_in_quorum.add(from_node)
            # print("Node %s added to node_list_in_quorum" , from_node)
            if(from_node != self.node_name and from_node not in self.node_list_in_quorum):
                self.node_list_in_quorum.add(from_node)
                print("Node %s added to node_list_in_quorum" , from_node)
                self.transport.write(str.encode("Node Joined|||%s|||introducing own node to others in quorum" % (self.node_name)), ("225.0.0.5", channel_nb))
        if data_arr[0] == "initial" and self.node_name== from_node :
            message_dict = self.data_node_dict.get(from_node)
            if not message_dict:
                message_dict = {data_arr[2] : Message_N(data_arr[2],data_arr[3],'N','N','Y',address)}
                self.data_node_dict[from_node] = message_dict
            else:
                message_dict[data_arr[2]] = Message_N(data_arr[2],data_arr[3],'N','N','Y',address)
                self.data_node_dict[from_node] = message_dict
            print("Initialize method")
            print(self.data_node_dict)
            #msg = "pre_prepare|||"+str(self.node_name)+"|||"+str(data_arr[1])+"|||"+str(data_arr[2])
            print("Boradcasting the data the was received from client")
            self.transport.write(str.encode("pre_prepare|||%s|||%s|||%s" % (self.node_name,data_arr[2], data_arr[3])), ("225.0.0.5",channel_nb))


        #if(data_arr[0] == "pre_prepare"  and  from_node!= self.node_name):
        if(data_arr[0] == "pre_prepare"  ):
            self.pre_prepareData(data_arr[2],data_arr[3], from_node)
            self.transport.write(str.encode("ballot|||%s|||%s|||%s" % (self.node_name,data_arr[2], data_arr[3])), ("225.0.0.5",channel_nb))
        
        #if(data_arr[0] == "prepare"  and  from_node!= self.node_name):
        if(data_arr[0] == "ballot" ):    
            print("data inside ballot")
            #print(data_arr)
            self.prepareData(data_arr[2],data_arr[3],from_node)
    
    def pre_prepareData(self,key_data, value_data , from_node):
        x = Message_N(key_data, value_data,"N","N","Y",from_node)
        print("Inside pre_prepareData")
        #temp_dict ={}
        temp_dict = self.quorum_pre_prepared_data.get(key_data)
        #print("temp_dict")
        #print(temp_dict)
        #print(type(temp_dict))
        if temp_dict == None:
           temp_dict= {}
           temp_dict[from_node] =  x
        else:
            #temp_dict = {}
            #print("Inside else ")
            #print(type(temp_dict))
            temp_dict[from_node] =  x
                    
        self.quorum_pre_prepared_data[key_data] = temp_dict
        print("\n quorum_pre_prepared_data \n")
        print(self.quorum_pre_prepared_data)
        #then write to the multicast your pre prepared data

    def prepareData(self,key_data, value_data , from_node):
        x = Message_N(key_data, value_data,"N","Y","N",from_node)
        print("Inside ballot")
        temp_dict = self.quorum_prepared_data.get(key_data)
        if temp_dict==None:
           temp_dict= {}
           temp_dict[from_node] = x
        else:
            #temp_dict = {}
            temp_dict[from_node] =  x
                    
        self.quorum_prepared_data[key_data] = temp_dict
        print("\n quorum_prepared_data\n")
        print(self.quorum_prepared_data)
        #print("node_list_in_quorum number of nodes in quorum")
        print(self.node_list_in_quorum)
        if len(self.quorum_prepared_data[key_data].keys()) == len(self.node_list_in_quorum):
            print("Going into consensus\n")
            self.deriveConsensus(key_data, value_data)
        
        #then write to the multicast your prepared data


    def deriveConsensus(self,key_data, value_data):
        target = int((2/3)*len(self.node_list_in_quorum)) + 1
        #target = 4
        temp_data = self.quorum_prepared_data[key_data]
        data_validate = {}
        temp_max = 0
        temp_max_data = ""
        for node in temp_data.keys():
            data_new = temp_data[node].data
            if data_new in data_validate:
                temp_int = data_validate[data_new]
                temp_int = temp_int + 1
                data_validate[data_new] = temp_int
                if temp_int > temp_max:
                    temp_max = temp_int
                    temp_max_data = data_new
            else:
                data_validate[data_new] = 1
                temp_int = 1
                if temp_int > temp_max:
                    temp_max = temp_int
                    temp_max_data = data_new



        if(temp_max >= target):
            self.transport.write(str.encode("commit|||%s|||%s|||%s" % (self.node_name,key_data, value_data)), ("225.0.0.5",channel_nb))
            self.commitData(key_data, value_data)
        else:
            print("consensus failed")

    def commitData(self, key, value):
        value = int(value)
        if self.db.get(key) != False:
            x = self.db.get(key)
            x = x + value
            self.db.set(key, x)
        else:
            self.db.set(key, value)
        print("Data commited")
        self.db.dump()
        self.quorum_prepared_data={}
        self.quorum_commit_data = {}
        self.quorum_pre_prepared_data = {}

if __name__ == "__main__":
    node_name = sys.argv[1]
    #node_type = sys.argv[2] #type rogue or ideal
    #quorum_list_main = sys.argv[2]
    #channel_nb = 3000
    quorum_list_main = ["225.0.0.5"]
    reactor.listenMulticast(int(channel_nb), Multicast(node_name,quorum_list_main), listenMultiple=True)
    reactor.run()