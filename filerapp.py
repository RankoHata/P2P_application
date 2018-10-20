"""
Module implementing simple application for a simple P2P network.
"""

import sys
import threading
import random
import tkinter as tk
from btfiler import *


class BTGui(tk.Frame):
    def __init__(self, serverhost, serverport, firstpeer=None, hops=2, maxpeers=5, master=None):
        tk.Frame.__init__(self, master)
        self.grid()
        self.createWidgets()
        self.master.title('File Sharing App - {}:{}'.format(serverhost, serverport))
        self.btpeer = FilePeer(maxpeers=maxpeers, serverhost=serverhost, serverport=serverport)
        self.bind("<Destroy>", self.__onDestroy)
        if firstpeer is not None:
            host, port = firstpeer.split(':')
            self.btpeer.buildpeers(host, int(port), hops=hops)
            self.update_peer_list()
        
        t = threading.Thread(target=self.btpeer.main_loop)
        t.start()
    
        self.btpeer.stabilizer = self.btpeer.check_live_peers
        self.btpeer.startstabilizer(3)
        self.after(3000, self.onTimer)

    def onTimer(self):
        self.onRefresh()
        self.after(3000, self.onTimer)
    
    def __onDestroy(self, event):
        self.btpeer.shutdown = True
    
    def update_peer_list(self):
        if self.peerList.size() > 0:
            self.peerList.delete(0, self.peerList.size() - 1)
        for p in self.btpeer.peers:
            self.peerList.insert(tk.END, p)
        
    def update_file_list(self):
        if self.fileList.size() > 0:
            self.fileList.delete(0, self.fileList.size() - 1)
        for f, p in self.btpeer.files.items():
            if p is None:
                p = '(local)'
            self.fileList.insert(tk.END, '{}:{}'.format(f, p))
    
    def createWidgets(self):
        fileFrame = tk.Frame(self)
        peerFrame = tk.Frame(self)

        rebuildFrame = tk.Frame(self)
        searchFrame = tk.Frame(self)
        addfileFrame = tk.Frame(self)
        pbFrame = tk.Frame(self)
    
        fileFrame.grid(row=0, column=0, sticky=tk.N+tk.S)
        peerFrame.grid(row=0, column=1, sticky=tk.N+tk.S)
        pbFrame.grid(row=2, column=1)
        addfileFrame.grid(row=3)
        searchFrame.grid(row=4)
        rebuildFrame.grid(row=3, column=1)

        tk.Label(fileFrame, text='Available Files').grid()
        tk.Label(peerFrame, text='Peer List').grid()

        fileListFrame = tk.Frame(fileFrame)
        fileListFrame.grid(row=1, column=0)
        fileScroll = tk.Scrollbar(fileListFrame, orient=tk.VERTICAL)
        fileScroll.grid(row=0, column=1, sticky=tk.N+tk.S)

        self.fileList = tk.Listbox(fileListFrame, height=5, yscrollcommand=fileScroll.set)

        self.fileList.grid(row=0, column=0, sticky=tk.N+tk.S)
        fileScroll["command"] = self.fileList.yview

        self.fetchButton = tk.Button(fileFrame, text='Fetch',
                            command=self.onFetch)
        self.fetchButton.grid()
        
        self.addfileEntry = tk.Entry(addfileFrame, width=25)
        self.addfileButton = tk.Button(addfileFrame, text='Add',
                            command=self.onAdd)
        self.addfileEntry.grid(row=0, column=0)
        self.addfileButton.grid(row=0, column=1)
        
        self.searchEntry = tk.Entry(searchFrame, width=25)
        self.searchButton = tk.Button(searchFrame, text='Search', 
                            command=self.onSearch)
        self.searchEntry.grid(row=0, column=0)
        self.searchButton.grid(row=0, column=1)
        
        peerListFrame = tk.Frame(peerFrame)
        peerListFrame.grid(row=1, column=0)
        peerScroll = tk.Scrollbar(peerListFrame, orient=tk.VERTICAL)
        peerScroll.grid(row=0, column=1, sticky=tk.N+tk.S)
        
        self.peerList = tk.Listbox(peerListFrame, height=5,
                            yscrollcommand=peerScroll.set)
        self.peerList.grid(row=0, column=0, sticky=tk.N+tk.S)
        peerScroll["command"] = self.peerList.yview
        
        self.removeButton = tk.Button(pbFrame, text='Remove',
                                    command=self.onRemove)
        self.refreshButton = tk.Button(pbFrame, text = 'Refresh', 
                                command=self.onRefresh)

        self.rebuildEntry = tk.Entry(rebuildFrame, width=25)
        self.rebuildButton = tk.Button(rebuildFrame, text = 'Rebuild', 
                                command=self.onRebuild)
        self.removeButton.grid(row=0, column=0)
        self.refreshButton.grid(row=0, column=1)
        self.rebuildEntry.grid(row=0, column=0)
        self.rebuildButton.grid(row=0, column=1)
    
    def onAdd(self):
        file = self.addfileEntry.get()
        filename = file.strip()
        if filename:
            self.btpeer.add_local_file(filename)
        self.addfileEntry.delete(0, len(file))
        self.update_file_list()
    
    def onSearch(self):
        key = self.searchEntry.get()
        self.searchEntry.delete(0, len(key))
        ttl = 4
        msgdata = '{} {} {}'.format(self.btpeer.myid, key, ttl)
        for p in self.btpeer.peers:
            self.btpeer.send2peer(p, QUERY, msgdata)
    
    def onFetch(self):
        sels = self.fileList.curselection()
        if len(sels) == 1:
            sel = self.fileList.get(sels[0]).split(':')
            if len(sel) > 2:  # fname:host:port
                fname, host, port = sel
                resp = self.btpeer.connect_and_send(host, port, FILEGET, fname)
                if len(resp) and resp[0][0] == REPLY:
                    with open(fname, 'w', encoding='utf-8') as f:
                        f.write(resp[0][1])
                    self.btpeer.files[fname] = None  # it's local now.

    def onRemove(self):
        sels = self.peerList.curselection()
        if len(sels) == 1:
            peerid = self.peerList.get(sels[0])
            self.btpeer.send2peer(peerid, PEERQUIT, self.btpeer.myid)
            self.btpeer.removepeer(peerid)

    def onRefresh(self):
        self.update_peer_list()
        self.update_file_list()
    
    def onRebuild(self):
        if not self.btpeer.maxpeer_searched():
            peerid = self.rebuildEntry.get()
            self.rebuildEntry.delete(0, len(peerid))
            peerid = peerid.strip()
            try:
                host, port = peerid.split(':')
                self.btpeer.buildpeers(host, port, hops=3)
            except:
                traceback.print_exc()


def main():
    if len(sys.argv) < 3:
        print('Syntax: server-host server-port max-peers first-peer-ip:first-peer-port')
        sys.exit(1)
    serverhost = sys.argv[1]
    serverport = int(sys.argv[2])
    maxpeers = 5
    peerid = None
    if len(sys.argv) >= 4:
        maxpeers = int(sys.argv[3])
    if len(sys.argv) >= 5:
        peerid = sys.argv[4]
    app = BTGui(
        serverhost=serverhost,
        serverport=serverport,
        maxpeers=maxpeers,
        firstpeer=peerid,
    )
    app.mainloop()


if __name__ == '__main__':
    main()
