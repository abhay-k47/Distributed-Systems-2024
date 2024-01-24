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

        self.slot_to_server = [-1]*self.nslots
        self.server_to_slots = {}
    
    def default_requestHashfn(self, requestId) -> int:
        return (requestId*requestId+2*requestId+17)%self.nslots
    
    def default_vserverHashfn(self, serverId, virtualId) -> int:
        return (serverId*serverId+virtualId*virtualId+2*virtualId+25)%self.nslots
    
    def linear_probe(self, hashValue) -> int:
        while self.slot_to_server[hashValue] != -1:
            hashValue = (hashValue+1)%self.nslots
        return hashValue
    
    def quadratic_probe(self, hashValue) -> int:
        i = 1
        while self.slot_to_server[hashValue] != -1:
            hashValue = (hashValue+i*i)%self.nslots
            i += 1
        return hashValue

    def addServer(self, serverId) -> None:
        serverSlots = []
        for virtualId in range(self.nvirtual):
            hashValue = self.vserverHashfn(serverId, virtualId)
            hashValue = self.probe(hashValue)
            self.slot_to_server[hashValue] = serverId
            serverSlots.append(hashValue)
        self.server_to_slots[serverId] = serverSlots

    def removeServer(self, serverId) -> None:
        if serverId not in self.server_to_slots:
            return
        for slot in self.server_to_slots[serverId]:
            self.slot_to_server[slot] = -1
        del self.server_to_slots[serverId]

    def getServer(self, requestId) -> int:
        if len(self.server_to_slots) == 0:
            return -1
        hashValue = self.requestHashfn(requestId)
        while self.slot_to_server[hashValue] == -1:
            hashValue = (hashValue+1)%self.nslots
        return self.slot_to_server[hashValue]
