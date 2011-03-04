#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#

# Import conditionally either regular xml support or lxml if present

import datetime, hashlib, rfc822, xml.sax.saxutils, sys

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

_elements = {}

# Event enumeration
class EventTypes:
    SessionBegin, SessionEnd, BashCommand, GccMsg, VisitURL = range(5)

def eventName(value):
    if value == EventTypes.SessionBegin:
        return 'SessionBegin'
    elif value == EventTypes.SessionEnd:
        return 'SessionEnd'
    elif value == EventTypes.BashCommand:
        return 'bashcommand'
    elif value == EventTypes.GccMsg:
        return 'GccMsg'
    elif value == EventTypes.VisitURL:
        return 'VisitURL'

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

    event = createEvent(EventTypes.SessionBegin,
                        datetime.datetime(2010, 06, 30, 13, 14, 59),
                        identifier = 'eventid',
                        name = 'eventname',
                        contextList = [context],
                        entityList = [entity])
    print (etree.tostring(event, pretty_print = True))

    thetree = etree.ElementTree(event)
    thetree.write('crap.xml', encoding = 'unicode', xml_declaration = True,
                  method = 'xml', pretty_print = True)

def createEvent(evType, tstamp, name = None, contextList = [], 
                entityList = []):
    """
    Create an XML element representing an event. Returns the XML object

    It expects:
    evType: Enum
    tstamp: datetime object
    name : string
    contextList: List of context elements
    entityList: List of entity elements
    """

    result = etree.Element('event')

    result.attrib['type'] = eventName(evType)
    if tstamp == None:
        tstamp = datetime.datetime.now()
    result.attrib['datetime'] = rfc822.formatdate(rfc822.mktime_tz(rfc822.parsedate_tz(tstamp.strftime("%a, %d %b %Y %H:%M:%S"))))
    if name != None:
        result.attrib['name'] = name

    for el in  entityList + contextList:
        result.append(el)

    # Create the ID
    m = hashlib.sha1()
    m.update(etree.tostring(result))
    result.attrib['id'] = m.hexdigest()

    return result

def createEntityAppDevice(name, text, role = None):
    """
    Create an XML element representing an entity. Returns the XML object.

    It expects:
    - name: string with the name of the entity ('application' or 'device')
    - text: name of the application or the device to be used. It will be used as
    the name attribute for a newly create element. 
    - role: string to be added as role attribute
    """

    result = etree.Element(name)
    result.attrib['refId'] = lookupElement(etree.Element(name, {'name': text}))

    if role != None:
        result.attrib['role'] = role

    return result

# def createEntity(device = None, application = None, itemVersion = None, 
#                  item = None, personProfile = None):
#     """
#     Create an XML element representing an entity. Returns the XML object.
#     It expects:
#     - device: string
#     - application: string
#     - itemVersion: string
#     - personProfile: string
#     """

#     result = etree.Element('entity')

#     if device != None:
#         etree.SubElement(result, 'device', {'id': device})
#     if application != None:
#         etree.SubElement(result, 'application', {'id': application })
#     if itemVersion != None:
#         etree.SubElement(result, 'itemVersion', {'id': itemVersion })
#     if item != None:
#         etree.SubElement(result, 'item', {'id': item })
#     if personProfile != None:
#         etree.SubElement(result, 'personProfile', {'id': personProfile })
    
#     return result

def createContext(result = None, task = None, location = None, 
                  environment = None, session = None, role = None):
    """
    Create an XML element representing a context. Returns the XML object. It
    expects:

    - task: string
    - location: string
    - environment: string
    - session: sessionId
    - role: string to be added as role attribute to the elements

    If currentContext is not None, it is extended with the appropriate
    elements.
    """
    
    if result == None:
        result = etree.Element('context')

    children = []
    if task != None:
        children.append(etree.Element('task', {'refId': task}))
    if location != None:
        children.append(etree.Element('location', {'refId': location }))
    if environment != None:
        children.append(etree.Element('environment', {'refId': environment }))
    if session != None:
        children.append(etree.Element('session', {'refId': session }))
    
    # Add the role attribute if given
    if role != None:
        for element in children:
            element.attrib['role'] = role
        
    result.extend(children)

    return result


def createPersonProfile(name, personId):
    """
    Create an XML element representing a peson profile attached to a person. It
    excpects:

    name: 'vmwork', 
    personId: User name
    """

    return etree.Element('personProfile', {'name' : name, 
                                           'personId' : personId})

def createPerson(name):
    """
    Create an XML element representing a person. It
    excpects:

    name: user name
    """

    return lookupElement(etree.Element('person', {'name' : name}))

def createItemVersion(itemId, role = None, attribs = None, elems = [], 
                      text = None):
    """
    Create an XML element representing an item version. It expects
    
    itemId: id of an item
    """

    result = None
    # If itemId is given, create a new version
    if itemId != None:
        result = etree.Element('itemVersion', {'itemId': itemId})
    else:
        # Create the item and then the version
        result = etree.Element('itemVersion',
                               {'itemId' : 
                                createItem(attribs, elems, text)})

    # If a role attribute is given, insert it in the result
    if role != None:
        result.attrib['role'] = role

    return result

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
        try:
            result.text = xml.sax.saxutils.escape(text)
        except ValueError, e:
            # A ValueError I think it means that is not unicode
            # result.text = xml.sax.saxutils.escape(unicode(text, 'utf-8'))
            result.text = xml.sax.saxutils.escape(text.encode('utf-8'))

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
        result.attrib['begin'] = rfc822.formatdate(rfc822.mktime_tz(rfc822.parsedate_tz(tstart.strftime("%a, %d %b %Y %H:%M:%S"))))
    if tstop != None:
        result.attrib['end'] = rfc822.formatdate(rfc822.mktime_tz(rfc822.parsedate_tz(tstop.strftime("%a, %d %b %Y %H:%M:%S"))))

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
    fileName = 'elements.xml'
    toWrite.write(fileName, encoding = 'utf-8', xml_declaration = True,
                  method = 'xml', pretty_print = True)
    
    # Reset the global variable to produce the next file
    _elements = {}
    return fileName
    
if __name__ == "__main__":
    main()
