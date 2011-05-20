#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os
import socket
import sys
import ConfigParser

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

def getConnection():
    config = ConfigParser.ConfigParser()
    config.read("../conf/pla-client.cfg")

    HOST = config.get('server', 'host')
    PORT = int(config.get('server', 'port'))

    data = " ".join(sys.argv[1:])
    
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to server and send data
    sock.connect((HOST, PORT))
    return sock

def requestUser(args):
    sock = getConnection()
    sock.send("CREATE " + " ".join(args) + "\n")
    
    # Receive data from the server and shut down
    received = sock.recv(1024)
    sock.close()

    print "Received: %s." % received
    tokens = data.split()
    if (tokens[0] == "OK"):
        # new user created, time to store credentials and import a .pladata folder
        [user, pawd, user_repo] = tokens[1:]
        config = ConfigParser.ConfigParser()
        config.read(os.path.join(pla.plaDirectory, 'conf', 'pla-client.cfg'))

        config.set('client', 'user', user)
        config.set('client', 'pass', pawd)
        svn_repo = config.get('server', 'host')
        svn_repo_url = url + user_repo
        
        os.mkdir('/tmp/.pladata')
        svn_client = pysvn.Client()
        svn_client.import_('/tmp/.pladata', repo_url)


def removeUser(args):
    sock = getConnection()
    sock.send("REMOVE " + " ".join(args) + "\n")
    
    # Receive data from the server and shut down
    received = sock.recv(1024)
    sock.close()

    print "Received: %s." % received


if __name__ == "__main__":
    if (sys.argv[1] == "CREATE"):
        requestUser(sys.argv[2:])
    elif (sys.argv[1] == "REMOVE"):
        removeUser(sys.argv[2:])
