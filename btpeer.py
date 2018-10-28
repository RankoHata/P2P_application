import socket
import struct
import threading
import time
import traceback
import logging

logging.basicConfig(level=logging.DEBUG)


class BTPeer(object):
    """ Implements the core functionality that might be used by a peer in a P2P networks. """
    def __init__(self, maxpeers, serverport, serverhost, myid=None, router=None, stabilizer=None):
        self.maxpeers = int(maxpeers)  # maxpeers may be set to -1 to allow unlimited number of peers.
        self.serverhost, self.serverport = serverhost, int(serverport)
        self.myid = myid if myid is not None else ':'.join([str(self.serverhost), str(self.serverport)])
        self.peerlock = threading.Lock()  # ensure proper access to peers list.
        self.peers = {}
        self.handlers = {}
        self.shutdown = False

        self.router = router
        """
        Register a routing function with this peer.
        The setup of routing is as follows:
            This peer maintains a list of other known peers (in self.peers).
            The routing function should take the name of a peer and 
            decide which of the known peers a message should be routed 
            to next in order to (hoepfully) reach the desired peer.
            The router function should return a tuple of three values: 
            (next-peer-id, host, port).
            If the message cannot be routed, the next-peer-id should be None.
        """
        self.stabilizer = stabilizer

    def __handlepeer(self, clientsock):
        """ Dispatches messages from the socket connection """
        host, port = clientsock.getpeername()
        print('handlepeer: host: {} port: {}'.format(host, port))
        peerconn = BTPeerConnection(None, host, port, clientsock)
        try:
            msgtype, msgdata = peerconn.recvdata()
            print('receive msg: msgtype: {}, msgdata: {}'.format(msgtype, msgdata))
            if msgtype in self.handlers:
                print('test.................{}'.format(msgdata))
                print(msgdata == '')
                # self.handlers[msgtype](peerconn, msgdata)
                self.handlers[msgtype](self, peerconn, msgdata)
        except KeyboardInterrupt:
            raise
        except:
            logging.error('Error in processing message.')
            traceback.print_exc()
        peerconn.close()
    
    def __runstabilizer(self, delay):
        while not self.shutdown:
            self.stabilizer()
            time.sleep(delay)
    
    def startstabilizer(self, delay):
        """
        Register and start a stabilizer function with this peer.
        The function will be activated every <delay> seconeds.
        """
        if callable(self.stabilizer):
            t = threading.Thread(target=self.__runstabilizer, args=(delay,))
            t.start()
    
    def addhandler(self, msgtype, handler):
        """
        Register the handle for the given message type with this peer.
        """
        self.handlers[msgtype] = handler
    
    def addpeer(self, peerid, host, port) -> bool:
        """ Add a peer name and host:port mapping to the known list of peers. """
        if peerid not in self.peers and (self.maxpeers == -1 or len(self.peers) < self.maxpeers):
            self.peers[peerid] = (host, int(port))
            return True
        else:
            return False
    
    def getpeer(self, peerid):
        """ Return the (host, port) tuple for the given peer name. """
        return self.peers.get(peerid, None)

    def removepeer(self, peerid):
        """ Remove peer information from the known list of peers. """
        if peerid in self.peers:
            del self.peers[peerid]
    
    def getpeerids(self):
        """ Return a list of all known peer id. """
        return list(self.peers.keys())
    
    def number_of_peers(self):
        """ Return the number of known peers. """
        return len(self.peers)
    
    def maxpeer_searched(self):
        """
        Return whether the maximum limit of peer has been added to the list 
        of known peers. Always return True if maxpeers is set to -1
        """
        assert self.maxpeers == -1 or len(self.peers) <= self.maxpeers
        return self.maxpeers > -1 and len(self.peers) == self.maxpeers
    
    def make_server_socket(self, port, backlog=5):
        """
        Construct and prepare a server socket listening on the given port.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))
        s.listen(backlog)
        return s
    
    def send2peer(self, peerid, msgtype, msgdata, waitreply=True):
        """
        send2peer( ... ) -> [(replytype, replydata), ... ]

        Send a message to the identified peer.
        In order to decide how to send the message, the router handler for 
        this peer will be called. If no router function has been registered, 
        it will not work. The router function should provide the next immediate 
        peer to whom the message should be forwarded. The peer's reply, if 
        it is expected, will be returned.
        """
        if self.router:
            nextpid, host, port = self.router(peerid)
            if nextpid is not None:
                return self.connect_and_send(host, port, msgtype, msgdata, pid=nextpid, waitreply=waitreply)
        return None
    
    def connect_and_send(self, host, port, msgtype, msgdata, pid=None, waitreply=True):
        """
        connect_and_send( ... ) -> [(replytype, replydata), ... ]
        Connect and send a message to the specified host:port.
        The host's reply, if expected, will be returned as a list of tuple.
        """
        msgreply = []
        try:
            peerconn = BTPeerConnection(pid, host, port)
            peerconn.senddata(msgtype, msgdata)
            if waitreply:
                onereply = peerconn.recvdata()
                while onereply != (None, None):  # loop receive message.
                    msgreply.append(onereply)
                    onereply = peerconn.recvdata()
                peerconn.close()
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()
        return msgreply

    def check_live_peers(self):
        """
        Attemp to ping all currently known peers in order to ensure that 
        they still active. Remove any from the peer list that do not reply.
        This function can be used as a simple stabilizer.
        """
        todelete = []
        for pid in self.peers:
            is_connected = False
            try:
                host, port = self.peers[pid]
                peerconn = BTPeerConnection(pid, host, port)
                peerconn.senddata('PING', '')
                is_connected = True  # 发送成功就认为是在连接
            except:
                todelete.append(pid)
                if is_connected:
                    peerconn.close()
        
        self.peerlock.acquire()
        try:
            for pid in todelete:
                if pid in self.peers:
                    del self.peers[pid]
        finally:
            self.peerlock.release()
    
    def main_loop(self):
        s = self.make_server_socket(self.serverport)
        # s.settimeout(3)  # 让下面的socket.accept()超时，进入下一次循环，否则程序会一直卡在accept那里, new: 现在没必要了，最外面将进程设置为守护状态
        while not self.shutdown:
            try:
                print('start accept....................')
                clientsock, clientaddr = s.accept()
                print('----main_loop-----: clientsock: {}, clientaddr: {}'.format(clientsock, clientaddr))
                clientsock.settimeout(None)
                t = threading.Thread(target=self.__handlepeer, args=(clientsock,))
                t.start()
            except KeyboardInterrupt:
                print('KeyboardInterrupt ......')
                self.shutdown = True
                break
            except:
                traceback.print_exc()
        s.close()


class BTPeerConnection(object):
    def __init__(self, peerid, host, port, sock=None):
        self.id = peerid
        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect((host, int(port)))
        else:
            self.s = sock
        self.sd = self.s.makefile('rw', 65536)

    def __makemsg(self, msgtype, msgdata):
        print(msgtype)
        msglen = len(msgdata)
        msg = struct.pack("!4sL%ds" % msglen, msgtype.encode('utf-8'), msglen, msgdata.encode('utf-8'))
        return msg
    
    def senddata(self, msgtype, msgdata) -> bool:
        """
        Send a message through a peer connection.
        Return True on success or Flase if there was an error.
        """
    
        try:
            msg = self.__makemsg(msgtype, msgdata)
            self.sd.write(msg.decode('utf-8'))
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()
            return False
        return True
    
    def recvdata(self):
        """
        recvdata() -> (msgtype, msgdata)
        Receive a message from a peer connection.
        Return (None, None) if there was any error.
        """
        try:
            msgtype = self.sd.read(4)
            if not msgtype: return (None, None)
            lenstr = self.sd.read(4)
            msglen = int(struct.unpack( "!L", lenstr.encode('utf-8') )[0])
            msg = ""
            while len(msg) != msglen:
                data = self.sd.read(min(2048, msglen - len(msg)))
                if not len(data):
                    break
                msg += data
            if len(msg) != msglen:
                return (None, None)
        except KeyboardInterrupt:
            raise
        except:
            logging.debug("Error receiving message.")
            traceback.print_exc()
            return (None, None)
        else:
            return (msgtype, msg)
    
    def close(self):
        """
        Close the peer connection. The send and recv methods will not work
        after call.
        """
        self.s.close()
    
    def __str__(self):
        return "BTPeerConnection: {}".format(self.id)


class MsgError(Exception):
    pass
