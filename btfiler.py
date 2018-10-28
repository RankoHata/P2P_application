from btpeer import *


# PEERNAME = 'NAME'
# LISTPEERS = 'LIST'
# INSERTPEER = 'JOIN'
# QUERY = 'QUER'
# QRESPONSE = 'RESP'
# FILEGET = 'FGET'
# PEERQUIT = 'QUIT'
# REPLY = 'REPL'
# ERROR = 'ERRO'


class Signal(object):
    signal_name = None
    
    def handle_message(self, *arg, **kwargs):
        raise NotImplementedError


class REPLY(Signal):
    signal_name = 'REPLS'


class ERROR(Signal):
    signal_name = 'ERRO'


class PEERNAME(Signal):
    signal_name = 'NAME'
    
    def handle_message(self, btpeer, peerconn, data):
        peerconn.senddata(REPLY.signal_name, btpeer.myid)


class LISTPEERS(Signal):
    signal_name = 'LIST'
    
    def handle_message(self, btpeer, peerconn, data):
        btpeer.peerlock.acquire()
        try:
            peerconn.senddata(REPLY.signal_name, '{}'.format(btpeer.number_of_peers()))
            for pid in btpeer.peers:
                host, port = btpeer.getpeer(pid)
                peerconn.senddata(REPLY.signal_name, '{} {} {}'.format(pid, host, port))  # 多条回复
        finally:
            btpeer.peerlock.release()


class INSERTPEER(Signal):
    signal_name = 'JOIN'
    
    def handle_message(self, btpeer, peerconn, data):
        btpeer.peerlock.acquire()
        try:
            peerid, host, port = data.split()
            if btpeer.maxpeer_searched():
                peerconn.senddata(ERROR.signal_name, 'Join: too many peers')
                return None
            if peerid not in btpeer.peers and peerid != btpeer.myid:
                btpeer.addpeer(peerid, host, port)
                peerconn.senddata(REPLY.signal_name, 'Join: peer added: {}'.format(peerid))
            else:
                peerconn.senddata(ERROR.signal_name, 'Join: peer already inserted {}'.format(peerid))
        except:
            peerconn.senddata(ERROR.signal_name, 'Join: incorrect arguments')
        finally:
            btpeer.peerlock.release()


class QUERY(Signal):
    signal_name = 'QUER'
    
    def handle_message(self, btpeer, peerconn, data):
        try:
            peerid, key, ttl = data.split()  # ttl: search depth
            peerconn.senddata(REPLY.signal_name, 'Query ACK: {}'.format(key))
        except:
            peerconn.senddata(ERROR.signal_name, 'Query: incorrect arguments')
        t = threading.Thread(target=self.__process_query, args=(btpeer, peerid, key, int(ttl)))
        t.start()
    
    def __process_query(self, btpeer, peerid, key, ttl):
        for fname in btpeer.files:
            if key in fname:
                fpeerid = btpeer.files[fname]
                if fpeerid is None: fpeerid = btpeer.myid
                host, port = peerid.split(':')
                msgdata = '{} {}'.format(fname, fpeerid)
                # 妙啊，peerid里直接包含了host和port，在这里不一定是直接邻居，只传id就可以有足够的信息进行连接
                # can't use sendtopeer here because peerid is not necessarily an immediate neighbor.
                btpeer.connect_and_send(host, int(port), QRESPONSE.signal_name, msgdata, pid=peerid)
        # will only reach here if key not found...
        # in which case propagate query to neighbors
        if ttl > 0:
            msgdata = '{} {} {}'.format(peerid, key, ttl-1)
            for nextpid in btpeer.peers:
                btpeer.send2peer(nextpid, QUERY.signal_name, msgdata)


class QRESPONSE(Signal):
    signal_name = 'RESP'

    def handle_message(self, btpeer, peerconn, data):
        try:
            fname, fpeerid = data.split()
            if fname in btpeer.files:
                pass  # Can't add duplicate file.
            else:
                btpeer.files[fname] = fpeerid
        except:
            traceback.print_exc()


class FILEGET(Signal):
    signal_name = 'FGET'

    def handle_message(self, btpeer, peerconn, data):
        fname = data
        if fname not in btpeer.files:
            peerconn.senddata(ERROR.signal_name, 'File not found.')
        else:
            try:
                with open(fname, 'r', encoding='utf-8') as f:
                    file_data = f.read()
            except:
                peerconn.senddata(ERROR.signal_name, 'Error reading file.')
            else:
                peerconn.senddata(REPLY.signal_name, file_data)


class PEERQUIT(Signal):
    signal_name = 'QUIT'

    def handle_message(self, btpeer, peerconn, data):
        btpeer.peerlock.acquire()
        try:
            peerid = data.strip()
            if str(peerid) in btpeer.peers:
                msg = 'Quit: peer removed: {}'.format(peerid)
                peerconn.senddata(REPLY.signal_name, msg)
                btpeer.removepeer(peerid)
            else:
                msg = 'Quit: peer not found: {}'.format(peerid)
                peerconn.senddata(ERROR.signal_name, msg)
        finally:
            btpeer.peerlock.release()


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
        # handlers = {
        #     LISTPEERS: self.__handle_listpeers,
        #     INSERTPEER: self.__handle_insertpeer,
        #     PEERNAME: self.__handle_peername,
        #     QUERY: self.__handle_query,
        #     QRESPONSE: self.__handle_qresponse,
        #     FILEGET: self.__handle_fileget,
        #     PEERQUIT: self.__handle_quit,     
        # }
        handlers = {
            LISTPEERS.signal_name: LISTPEERS().handle_message,
            INSERTPEER.signal_name: INSERTPEER().handle_message,
            PEERNAME.signal_name: PEERNAME().handle_message,
            QUERY.signal_name: QUERY().handle_message,
            QRESPONSE.signal_name: QRESPONSE().handle_message,
            FILEGET.signal_name: FILEGET().handle_message,
            PEERQUIT.signal_name: PEERQUIT().handle_message,
        }
        for key, value in handlers.items():
            self.addhandler(key, value)
        
    def __router(self, peerid):
        if peerid not in self.peers:
            return (None, None, None)
        else:
            return (peerid, *self.peers[peerid])
    
    # def __handle_insertpeer(self, peerconn, data):
    #     self.peerlock.acquire()
    #     try:
    #         peerid, host, port = data.split()
    #         if self.maxpeer_searched():
    #             peerconn.senddata(ERROR, 'Join: too many peers')
    #             return None
    #         if peerid not in self.peers and peerid != self.myid:
    #             self.addpeer(peerid, host, port)
    #             peerconn.senddata(REPLY, 'Join: peer added: {}'.format(peerid))
    #         else:
    #             peerconn.senddata(ERROR, 'Join: peer already inserted {}'.format(peerid))
    #     except:
    #         peerconn.senddata(ERROR, 'Join: incorrect arguments')
    #     finally:
    #         self.peerlock.release()
    
    # def __handle_listpeers(self, peerconn, data):
    #     self.peerlock.acquire()
    #     try:
    #         peerconn.senddata(REPLY, '{}'.format(self.number_of_peers()))
    #         for pid in self.peers:
    #             host, port = self.getpeer(pid)
    #             peerconn.senddata(REPLY, '{} {} {}'.format(pid, host, port))  # 多条回复
    #     finally:
    #         self.peerlock.release()
    
    # def __handle_peername(self, peerconn, data):
    #     peerconn.senddata(REPLY, self.myid)
    
    # def __handle_query(self, peerconn, data):
    #     try:
    #         peerid, key, ttl = data.split()  # ttl: search depth
    #         peerconn.senddata(REPLY, 'Query ACK: {}'.format(key))
    #     except:
    #         peerconn.senddata(ERROR, 'Query: incorrect arguments')
    #     t = threading.Thread(target=self.__process_query, args=(peerid, key, int(ttl)))
    #     t.start()
    
    # def __process_query(self, peerid, key, ttl):
    #     for fname in self.files:
    #         if key in fname:
    #             fpeerid = self.files[fname]
    #             if fpeerid is None: fpeerid = self.myid
    #             host, port = peerid.split(':')
    #             msgdata = '{} {}'.format(fname, fpeerid)
    #             # 秒啊，peerid里直接包含了host和port，在这里不一定是直接邻居，只传id就可以有足够的信息进行连接
    #             # can't use sendtopeer here because peerid is not necessarily an immediate neighbor.
    #             self.connect_and_send(host, int(port), QRESPONSE, msgdata, pid=peerid)
    #     # will only reach here if key not found...
    #     # in which case propagate query to neighbors
    #     if ttl > 0:
    #         msgdata = '{} {} {}'.format(peerid, key, ttl-1)
    #         for nextpid in self.peers:
    #             self.send2peer(nextpid, QUERY, msgdata)
    
    # def __handle_qresponse(self, peerconn, data):
    #     try:
    #         fname, fpeerid = data.split()
    #         if fname in self.files:
    #             pass  # Can't add duplicate file.
    #         else:
    #             self.files[fname] = fpeerid
    #     except:
    #         traceback.print_exc()
    
    # def __handle_fileget(self, peerconn, data):
    #     fname = data
    #     if fname not in self.files:
    #         peerconn.senddata(ERROR, 'File not found.')
    #     else:
    #         try:
    #             with open(fname, 'r', encoding='utf-8') as f:
    #                 file_data = f.read()
    #         except:
    #             peerconn.senddata(ERROR, 'Error reading file.')
    #         else:
    #             peerconn.senddata(REPLY, file_data)

    def __handle_quit(self, peerconn, data):
        self.peerlock.acquire()
        try:
            peerid = data.strip()
            if str(peerid) in self.peers:
                msg = 'Quit: peer removed: {}'.format(peerid)
                peerconn.senddata(REPLY.signal_name, msg)
                self.removepeer(peerid)
            else:
                msg = 'Quit: peer not found: {}'.format(peerid)
                peerconn.senddata(ERROR.signal_name, msg)
        finally:
            self.peerlock.release()
    
    def buildpeers(self, host, port, hops=1):  # depth-first search
        if not self.maxpeer_searched() and hops > 0:
            peerid = None
            try:
                _, peerid = self.connect_and_send(host, port, PEERNAME.signal_name, '')[0]
                msgdata = '{} {} {}'.format(self.myid, self.serverhost, self.serverport)
                resp = self.connect_and_send(host, port, INSERTPEER.signal_name, msgdata)[0]
                if resp[0] == REPLY.signal_name and peerid not in self.peers:
                    self.addpeer(peerid, host, port)
                    resp = self.connect_and_send(host, port, LISTPEERS.signal_name, '', pid=peerid)
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
