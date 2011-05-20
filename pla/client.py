#!/usr/bin/python
# - *- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os
import pysvn
import socket
import sys
import ConfigParser

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

def ssl_server_trust_prompt(trust_dict):
    return True, 1, True

def get_login(realm, username, may_save):
    config = ConfigParser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-client.cfg')))

    user = config.get('client', 'user')
    pawd = config.get('client', 'pass')
    print "svn with credentials: {0} {1}".format(user, pawd)
    return True, user, pawd, True

def getConnection():
    config = ConfigParser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-client.cfg')))

    HOST = config.get('server', 'host')
    PORT = int(config.get('server', 'port'))

    data = " ".join(sys.argv[1:])
    
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to server and send data
    sock.connect((HOST, PORT))
    return sock

def requestUser(args):

    # check that a PLAworkspace folder doesnt exist
    if (os.path.exists(os.path.expanduser('~/.plaworkspace'))):
        print "ERROR: User already has a PLA workspace folder."
        return

    sock = getConnection()
    sock.send("CREATE " + " ".join(args) + "\n")
    
    # Receive data from the server and shut down
    received = sock.recv(1024)
    sock.close()

    print "Received: %s." % received
    tokens = received.split()
    if (tokens[0] == "OK"):
        [user, pawd, user_repo] = tokens[1:]

        # new user created, time to store credentials and import a .pladata folder
        config = ConfigParser.ConfigParser()
        conf_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-client.cfg'))
        config.read(conf_file)

        config.set('client', 'user', user)
        config.set('client', 'pass', pawd)
        svn_repo = config.get('server', 'host')

        # save the user credentials in configuration file
        with open(conf_file, 'wb') as configfile:
            config.write(configfile)

        # finally, checkout the user repository to have a local reference
        svn_client = pysvn.Client()
        svn_client.callback_get_login = get_login
        svn_client.callback_ssl_server_trust_prompt = ssl_server_trust_prompt
        svn_client.checkout(user_repo, os.path.expanduser('~/.plaworkspace'))
        

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
