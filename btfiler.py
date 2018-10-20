from btpeer import *


PEERNAME = 'NAME'
LISTPEERS = 'LIST'
INSERTPEER = 'JOIN'
QUERY = 'QUER'
QRESPONSE = 'RESP'
FILEGET = 'FGET'
PEERQUIT = 'QUIT'
REPLY = 'REPL'
ERROR = 'ERRO'


class FilePeer(BTPeer):
    """
    Implement a file-sharing peer-to-peer entity based on the generic P2P network.
    """
    def __init__(self, maxpeers, serverhost, serverport):
        super().__init__(
            maxpeers=maxpeers,
            serverhost=serverhost,
            serverport=serverport,
            myid=':'.join([str(serverhost), str(serverport)])
        )
        self.files = {}  # available files: name --> peerid mapping
        self.router = self.__router
        handlers = {
            LISTPEERS: self.__handle_listpeers,
            INSERTPEER: self.__handle_insertpeer,
            PEERNAME: self.__handle_peername,
            QUERY: self.__handle_query,
            QRESPONSE: self.__handle_qresponse,
            FILEGET: self.__handle_fileget,
            PEERQUIT: self.__handle_quit,     
        }
        for key, value in handlers.items():
            self.addhandler(key, value)
        
    def __router(self, peerid):
        if peerid not in self.peers:
            return (None, None, None)
        else:
            return (peerid, *self.peers[peerid])
    
    def __handle_insertpeer(self, peerconn, data):
        self.peerlock.acquire()
        try:
            peerid, host, port = data.split()
            if self.maxpeer_searched():
                peerconn.senddata(ERROR, 'Join: too many peers')
                return None
            if peerid not in self.peers and peerid != self.myid:
                self.addpeer(peerid, host, port)
                peerconn.senddata(REPLY, 'Join: peer added: {}'.format(peerid))
            else:
                peerconn.senddata(ERROR, 'Join: peer already inserted {}'.format(peerid))
        except:
            peerconn.senddata(ERROR, 'Join: incorrect arguments')
        finally:
            self.peerlock.release()
    
    def __handle_listpeers(self, peerconn, data):
        self.peerlock.acquire()
        try:
            peerconn.senddata(REPLY, '{}'.format(self.number_of_peers()))
            for pid in self.peers:
                host, port = self.getpeer(pid)
                peerconn.senddata(REPLY, '{} {} {}'.format(pid, host, port))  # 多条回复
        finally:
            self.peerlock.release()
    
    def __handle_peername(self, peerconn, data):
        peerconn.senddata(REPLY, self.myid)
    
    def __handle_query(self, peerconn, data):
        try:
            peerid, key, ttl = data.split()  # ttl: search depth
            peerconn.senddata(REPLY, 'Query ACK: {}'.format(key))
        except:
            peerconn.senddata(ERROR, 'Query: incorrect arguments')
        t = threading.Thread(target=self.__process_query, args=(peerid, key, int(ttl)))
        t.start()
    
    def __process_query(self, peerid, key, ttl):
        for fname in self.files:
            if key in fname:
                fpeerid = self.files[fname]
                if fpeerid is None: fpeerid = self.myid
                host, port = peerid.split(':')
                msgdata = '{} {}'.format(fname, fpeerid)
                # 秒啊，peerid里直接包含了host和port，在这里不一定是直接邻居，只传id就可以有足够的信息进行连接
                # can't use sendtopeer here because peerid is not necessarily an immediate neighbor.
                self.connect_and_send(host, int(port), QRESPONSE, msgdata, pid=peerid)
        # will only reach here if key not found...
        # in which case propagate query to neighbors
        if ttl > 0:
            msgdata = '{} {} {}'.format(peerid, key, ttl-1)
            for nextpid in self.peers:
                self.send2peer(nextpid, QUERY, msgdata)
    
    def __handle_qresponse(self, peerconn, data):
        try:
            fname, fpeerid = data.split()
            if fname in self.files:
                pass  # Can't add duplicate file.
            else:
                self.files[fname] = fpeerid
        except:
            traceback.print_exc()
    
    def __handle_fileget(self, peerconn, data):
        fname = data
        if fname not in self.files:
            peerconn.senddata(ERROR, 'File not found.')
        else:
            try:
                with open(fname, 'r', encoding='utf-8') as f:
                    file_data = f.read()
            except:
                peerconn.senddata(ERROR, 'Error reading file.')
            else:
                peerconn.senddata(REPLY, file_data)

    def __handle_quit(self, peerconn, data):
        self.peerlock.acquire()
        try:
            peerid = data.strip()
            if str(peerid) in self.peers:
                msg = 'Quit: peer removed: {}'.format(peerid)
                peerconn.senddata(REPLY, msg)
                self.removepeer(peerid)
            else:
                msg = 'Quit: peer not found: {}'.format(peerid)
                peerconn.senddata(ERROR, msg)
        finally:
            self.peerlock.release()
    
    def buildpeers(self, host, port, hops=1):  # depth-first search
        if not self.maxpeer_searched() and hops > 0:
            peerid = None
            try:
                _, peerid = self.connect_and_send(host, port, PEERNAME, '')[0]
                msgdata = '{} {} {}'.format(self.myid, self.serverhost, self.serverport)
                resp = self.connect_and_send(host, port, INSERTPEER, msgdata)[0]
                if resp[0] == REPLY and peerid not in self.peers:
                    self.addpeer(peerid, host, port)
                    resp = self.connect_and_send(host, port, LISTPEERS, '', pid=peerid)
                    if len(resp) > 1:
                        resp.reverse()
                        resp.pop()
                        for item in resp:
                            nextpid, host, port = item[1].split()
                            if nextpid != self.myid:
                                self.buildpeers(host, port, hops-1)
            except:
                traceback.print_exc()
                self.removepeer(peerid)
    
    def add_local_file(self, filename):
        self.files[filename] = None
