class ConsistentHashMap:

    def __init__(self, nservers=3, nslots=512, nvirtual=9, requestHashfn=None, vserverHashfn=None, probing='linear'):
        self.nservers = nservers
        self.nslots = nslots
        self.nvirtual = nvirtual
        self.requestHashfn = requestHashfn
        if self.requestHashfn is None:
            self.requestHashfn = self.default_requestHashfn
        self.vserverHashfn = vserverHashfn
        if self.vserverHashfn is None:
            self.vserverHashfn = self.default_vserverHashfn

        if probing == 'linear':
            self.probe = self.linear_probe
        elif probing == 'quadratic':
            self.probe = self.quadratic_probe

        self.slot_to_server = {}
        self.server_to_slots = {}
    
    def default_requestHashfn(self, requestId) -> int:
        return (requestId*requestId+2*requestId+17)%self.nslots
    
    def default_vserverHashfn(self, serverId, virtualId) -> int:
        return (serverId*serverId+virtualId*virtualId+2*virtualId+25)%self.nslots
    
    def linear_probe(self, hashValue) -> int:
        while hashValue in self.slot_to_server:
            hashValue = (hashValue+1)%self.nslots
        return hashValue
    
    def quadratic_probe(self, hashValue) -> int:
        i = 1
        while hashValue in self.slot_to_server:
            hashValue = (hashValue+i*i)%self.nslots
            i += 1
        return hashValue

    def addServer(self, serverId) -> None:
        for virtualId in range(self.nvirtual):
            hashValue = self.vserverHashfn(serverId, virtualId)
            hashValue = self.probe(hashValue)
            self.slot_to_server[hashValue] = serverId
            if serverId not in self.server_to_slots:
                self.server_to_slots[serverId] = []
            self.server_to_slots[serverId].append(hashValue)


    def removeServer(self, serverId) -> None:
        if serverId not in self.server_to_slots:
            return
        for hashValue in self.server_to_slots[serverId]:
            del self.slot_to_server[hashValue]
        del self.server_to_slots[serverId]

    def getServer(self, requestId) -> int:
        if len(self.slot_to_server) == 0:
            return None
        hashValue = self.requestHashfn(requestId)
        while hashValue not in self.slot_to_server:
            hashValue = (hashValue+1)%self.nslots
        return self.slot_to_server[hashValue]


# testing purposes
# if __name__ == '__main__':

#     cMap = ConsistentHashMap(nservers=3, nslots=512, nvirtual=9, probing='quadratic')
#     for i in range(3):
#         cMap.addServer(i)

#     a = [0, 0, 0]
#     for i in range(512):
#         a[cMap.getServer(i)] += 1

#     print(a)

#     cMap.removeServer(1)
#     print(cMap.getServer(1))
#     print(cMap.getServer(2))

#     cMap.addServer(1)
#     print(cMap.getServer(1))
#     print(cMap.getServer(2))

#     cMap.removeServer(2)
#     print(cMap.getServer(1))
#     print(cMap.getServer(2))

#     cMap.addServer(2)
#     print(cMap.getServer(1))
