#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#

# Import conditionally either regular xml support or lxml if present

import datetime, hashlib

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

_elements = {}

# Event enumeration
class EventTypes:
    SessionStart, SessionEnd, OtherCommand, Gcc, Gdb, Kate, \
        Kdevelop, Valgrind = range(8)

def eventName(value):
    if value == EventTypes.SessionStart:
        return 'SessionStart'
    elif value == EventTypes.SessionEnd:
        return 'SessionEnd'
    elif value == EventTypes.OtherCommand:
        return 'OtherCommand'
    elif value == EventTypes.Gcc:
        return 'Gcc'
    elif value == EventTypes.Gdb:
        return 'Gdb'
    elif value == EventTypes.Kate:
        return 'Kate'
    elif value == EventTypes.Kdevelop:
        return 'Kdevelop'
    elif value == EventTypes.Valgrind:
        return 'Valgrind'

def main():
    """
    Functions in this file create the XML entities to store the events
    """
    
    print main.__doc__

    personProfile = createPersonProfile('personid', 'idattrib')
    print (etree.tostring(personProfile, pretty_print = True))

    entity = createEntity(personProfile = personProfile)
    print (etree.tostring(entity, pretty_print = True))

    context = createContext(task = 'taskid', location = 'locationid',
                            environment = 'envit', session = 'sessionid')
    print (etree.tostring(context, pretty_print = True))

    event = createEvent(EventTypes.SessionStart,
                        datetime.datetime(2010, 06, 30, 13, 14, 59),
                        identifier = 'eventid',
                        name = 'eventname',
                        contextList = [context],
                        entityList = [entity])
    print (etree.tostring(event, pretty_print = True))

    thetree = etree.ElementTree(event)
    thetree.write('crap.xml', encoding = 'unicode', xml_declaration = True,
                  method = 'xml', pretty_print = True)

def createEvent(evType, tstamp, until = None, contextList = [], entityList = []):
    """
    Create an XML element representing an event. Returns the XML object

    It expects:
    evType: Enum
    tstamp: datetime object
    until : datetime object
    contextList: List of context elements
    entityList: List of entity elements
    """

    result = etree.Element('event')

    result.attrib['type'] = eventName(evType)
    if tstamp == None:
        tstamp = datetime.datetime.now()
    result.attrib['datetime'] = tstamp.strftime('%Y-%m-%d %H:%M:%S')

    for el in contextList + entityList:
        result.append(el)

    # Create the ID
    m = hashlib.sha1()
    m.update(etree.tostring(result))
    result.attrib['id'] = m.hexdigest()

    return result

def createEntity(device = None, application = None, itemVersion = None, 
                 personProfile = None):
    """
    Create an XML element representing an entity. Returns the XML object.
    It expects:
    - device: string
    - application: string
    - itemVersion: string
    - personProfile: string
    """

    result = etree.Element('entity')

    if device != None:
        etree.SubElement(result, 'device', {'id': device})
    if application != None:
        etree.SubElement(result, 'application', {'id': application })
    if itemVersion != None:
        etree.SubElement(result, 'itemVersion', {'id': itemVersion })
    if personProfile != None:
        etree.SubElement(result, 'personProfile', {'id': personProfile })
    
    return result

def createContext(task = None, location = None, environment = None, 
                  session = None):
    """
    Create an XML element representing a context. Returns the XML object. It
    expects:

    - task: string
    - location: string
    - environment: string
    - session: sessionId
    """
    
    result = etree.Element('context')

    if task != None:
        etree.SubElement(result, 'task', {'id': task})
    if location != None:
        etree.SubElement(result, 'location', {'id': location })
    if environment != None:
        etree.SubElement(result, 'environment', {'id': environment })
    if session != None:
        etree.SubElement(result, 'session', {'id': session })
    
    return result


def createPersonProfile(name, personId):
    """
    Create an XML element representing a peson profile attached to a person. It
    excpects:

    name: 'vmwork', 
    personId: User name
    """

    return lookupElement(etree.Element('personProfile', \
                                           {'name' : name, 
                                            'personId' : personId}))

def createPerson(name):
    """
    Create an XML element representing a person. It
    excpects:

    name: user name
    """

    return lookupElement(etree.Element('person', {'name' : name}))

def createItemVersion(itemId):
    """
    Create an XML element representing an item version. It expects
    
    itemId: id of an item
    """

    return lookupElement(etree.Element('itemVersion', {'itemId': itemId}))

def createItem(attribs, elems = [], text = None):
    """
    Create an XML element representing an item. It expects
    
    name: string
    """
    
    result = etree.Element('item', attribs)
    
    # If list of elems is given, attach it
    if elems != []:
        for el in elems:
            result.append(el)

    # If CDATA is given, attach it
    if text != None:
        result.text = text

    return lookupElement(result)

def createSession(name, tstart, tstop):
    """
    Create an XML element representing a session. Returns the XML object. It
    expects:

    - name: string
    - tstart: datetime.datetime
    - tstop: datetime.datetime
    """

    result = etree.Element('session')
    if name != None:
        result.attrib['name'] = str(name)
    if tstart != None:
        result.attrib['tstart'] = tstart.strftime('%Y-%m-%d %H:%M:%S')
    if tstop != None:
        result.attrib['tstop'] = tstop.strftime('%Y-%m-%d %H:%M:%S')

    return lookupElement(result)

def lookupElement(el):
    """
    Given an XML element, computes its SHA1 and searchs for it in the global
    dictionary _elements. If found, it is returned. If not found, the SHA1 is
    added as id attribute, the element is added to the dictionary and the ID is
    returned.
    """

    # Obtain the ID
    m = hashlib.sha1()
    m.update(etree.tostring(el))

    uniqueID = m.hexdigest()
    lookup = _elements.get(uniqueID)

    # Miss in the dictionary, create element
    if lookup == None:
        el.attrib['id'] = uniqueID
        _elements[uniqueID] = el

    return uniqueID
    
def insertElements(table, elements):
    """
    Given a list of elements, insert them in the given dictionary with the id
    attribute.
    """

    for el in elements:
        table[el.get('id')] = el

    return table

def writeElements():
    """
    Dump all the elements in the dictionary in a fixed XML file in the current
    directory.
    """
    
    global _elements

    NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    TREE = '{%s}' % NS
    NSMAP = {None: NS}
    root = etree.Element(TREE + 'rdf', nsmap = {None: NS})
    root.append(etree.Comment('File automatically generated by PLA'))

    for el in _elements.values():
        root.append(el)

    toWrite = etree.ElementTree(root)
    toWrite.write('elements.xml', encoding = 'utf-8', xml_declaration = True,
                  method = 'xml', pretty_print = True)
    
    
if __name__ == "__main__":
    main()
