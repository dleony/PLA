#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#

import ConfigParser
import os
import ztreamy

def get_config(file_name):
    config = ConfigParser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', file_name)))
    return config

class Server():
    def __init__(self, config):

        # Create stream server
        port = config.get('ztreamy', 'port')
        self.server = ztreamy.StreamServer(port)

        # Create main stream: PLA
        pla_stream = ztreamy.Stream(config.get('ztreamy', 'stream'), allow_publish=True)
        self.server.add_stream(pla_stream)

    def run(self):
        try:
            print 'Starting PLA ztreamy server'
            self.server.start(loop=True)
            print 'Server off'
        except KeyboardInterrupt:
            pass
        finally:
            self.server.stop()
            print 'Stopped PLA server'

def rec(self, event):
    print(str(event))


class Client():
    def __init__(self, config):
        stream = [config.get('ztreamy', 'stream')]
        self.client = ztreamy.Client(stream, event_callback=rec, error_callback=self.error)

    def received(self, event):
        print(str(event))

    def error(self, message, http_error=None):
        if http_error is not None:
            print('Error: ' + message + ': ' + str(http_error))
        else:
            print('Error: ' + message)

    def run(self):
        try:
            print 'Starting PLA ztreamy consumer'
            self.client.start(loop=True)
            print 'Client off'
        except KeyboardInterrupt:
            pass
        finally:
            self.client.stop()
            print 'Stopped PLA ztreamy consumer'


def server_child():
    server_conf = get_config('pla-server.cfg')
    server = Server(server_conf)
    server.run()


def client_child():
    client_conf = get_config('pla-client.cfg')
    client = Client(client_conf)
    client.run()


def main():
    newpid = os.fork()
    if newpid == 0:
        client_child()
    else:
        server_child()


if __name__ == "__main__":
    main()
