#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#

import ConfigParser
import os
import ztreamy
from rdflib import Graph, Namespace, Literal


def get_config(file_name):
    config = ConfigParser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', file_name)))
    return config


class EventPublisher():
    def __init__(self):
        self.config = get_config('pla-client.cfg')
        self.source_id = self.config.get('ztreamy', 'source_id')
        if (self.source_id is None or self.source_id == ''):
            self.source_id = self.create_source()

        stream = self.config.get('ztreamy', 'stream')
        self.publisher = ztreamy.EventPublisher(stream)

    def create_source(self):
        source_id = ztreamy.random_id()
        self.config.set('ztreamy', 'source_id', source_id)

        # save the source_id in configuration file
        conf_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'pla-client.cfg'))
        with open(conf_file, 'wb') as configfile:
            self.config.write(configfile)
        return source_id

    def publish_event(self, event):
        self.publisher.publish(event)

    def publish_fields(self, application, data):
        graph = Graph()
        ns = Namespace('http://flautin.it.uc3m.es/gastt/')
        graph.add((ns[application], ns['outputs'], Literal(data)))
        event = ztreamy.RDFEvent(self.source_id, 'text/n3', graph)

