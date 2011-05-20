#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import crypt
import datetime
import os
import random
import sys
import time
import uuid
import ConfigParser
import SocketServer

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

class TCPHandler(SocketServer.BaseRequestHandler):
    """
    RequestHandler class for PLA server.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        data = self.request.recv(1024).strip()
        tokens = data.split()
        command = tokens[0]
        args = tokens[1:]
        
        # output the command received
        ts = datetime.datetime.fromtimestamp(time.time()).isoformat(' ')
        print "[{0}] {1} - {2}".format(ts, self.client_address[0], data)


        self.config = ConfigParser.ConfigParser()
    	self.config.read(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-server.cfg'))

        # call the method according to received command
        if (command == "CREATE"):
            resp = self.createUser(args[0], args[1])
        elif (command == "REMOVE"):
            resp = self.removeUser(args[0], args[1], args[2])
        else:
            resp = "ERROR Invalid-command"

        # send back the response from the function
        self.request.send(resp)

    def createUser(self, group, token):
        # create a new user in the subversion repository
        # 0. validate group
        try:
            stored_token = self.config.get('groups', group)
            if (stored_token != token):
                return "ERROR Invalid-token"
        except ConfigParser.NoOptionError:
            return "ERROR Invalid-group"

        # 1. generate user
        user = str(uuid.uuid4())
        pawd = str(random.randint(1000000, 9999999))

        # 2. add it to credentials list
        user_file = self.config.get('svn', 'user_file')
        os.system("htpasswd -b {0} {1} {2}".format(user_file, user, pawd))

        acl_file = self.config.get('svn', 'acl_file')
        svn_repo = self.config.get('svn', 'repo')
        svn_url = self.config.get('svn', 'url')

        acl_config = ConfigParser.ConfigParser()
        acl_config.read(acl_file)
        user_repo = svn_repo + ":/" + group + "/" + user
        user_url = svn_url + group + "/" + user + "/"
        acl_config.add_section(user_repo)
        acl_config.set(user_repo, user, 'rw');
        with open(acl_file, 'wb') as conffile:            
            acl_config.write(conffile)

	# 3. create user repository
	admin_user = self.config.get('svn', 'admin_user')
	admin_pass = self.config.get('svn', 'admin_pass')

	os.mkdir('/tmp/.pladata')
	os.system("svn --username {0} --password {1} import -m 'Folder for user {2}' /tmp/.pladata {3}/.pladata".format(admin_user, admin_pass, user, user_url))	
	os.rmdir('/tmp/.pladata')

        # 4. reload svn / apache?
        os.system("/etc/init.d/apache2 reload")

        # output format: OK <user> <password> <user repository URL>
        return "OK {0} {1} {2}".format(user, pawd, user_url)

    def removeUser(self, group, user, pawd):
        # remove a user from the subversion repository
        # 0. validate user credentials
        user_file = self.config.get('svn', 'user_file')
        lines = [l.rstrip().split(':', 1) for l in file(user_file).readlines()]
        lines = [l for l in lines if l[0] == user]
        if not lines:
            return "ERROR Invalid-user"
        hashedPassword = lines[0][1]
        if hashedPassword != crypt.crypt(pawd, hashedPassword[:2]):
            return "ERROR Invalid-password"

        # 1. remove user privileges, but leave id in list
        acl_file = self.config.get('svn', 'acl_file')
        svn_repo = self.config.get('svn', 'repo')
        acl_config = ConfigParser.ConfigParser()
        acl_config.read(acl_file)
        user_repo = svn_repo + "/" + group + "/" + user
        acl_config.remove_option(user_repo, user);
        with open(acl_file, 'wb') as conffile:            
            acl_config.write(conffile)

        # 2. reload svn / apache?
        os.system("/etc/init.d/apache2 reload")
        
        # output format: OK
        return "OK"
        

if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-server.cfg'))
    HOST = config.get('server', 'host')
    PORT = int(config.get('server', 'port'))

    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((HOST, PORT), TCPHandler)

    print "Starting PLA-SVN server on {0}:{1}...".format(HOST, PORT)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
