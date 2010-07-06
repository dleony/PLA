#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#

import os, sys, re, codecs, locale
from lxml import etree

import PLACamOutput

sys.stdin = codecs.getreader(locale.getpreferredencoding())(sys.stdin)

#
# Regular expressions used to detect compiler messages
# 
# Errors taken from 
# An Introduction to GCC - for the GNU compilers gcc and g++
# by Brian J. Gough, foreword by Richard M. Stallman
# ISBN 0954161793
#
_msgs = [\
    # syntax error
    ('syntax_error', re.compile(u'parse error before \`')), 

    # Weird non-terminated constant
    ('unterminated_string', 
     re.compile(u'unterminated string or character constant')),
    
    # Wrong type for assignment or argument
    ('no_cast', 
     re.compile(u'warning: .+ makes integer from pointer ' + \
                    u'without a cast$')),

    # Incomplete structure
    ('incomplete_struct',
     re.compile(u'dereferencing pointer to incomplete type')),

    # Return missing
    ('missing_return',
     re.compile(u'warning: control reaches end of non-void function')),

    # Unused variable
    ('unused_var', 
     re.compile(u'warning: unused variable \`(?P<variable>[^\']+)')),

    # Variable is not declared
    ('undeclared_var',
     re.compile(u'error: \‘(?P<variable>[^\’])+\’ undeclared ' + \
                    u'\(first use in this function\)')),

    # Function implicitly declared
    ('implicit_function',
     re.compile(u'warning: implicit declaration of function ' + \
                    u'\‘(?P<function>[^\’]+)\’')),

    # Parameter of the wrong type
    ('unexpected_argument',
     re.compile(u'note: expected  \‘(?P<typeA>[^\’]+)\’ ' + \
                    u'but argument is of type \‘(?P<typeB>[^\’]+)\’')),

    # Return with no value
    ('return_no_value',
     re.compile(u'warning: \‘return\’ with no value, in function ' + \
                    u'returning non-void')),

    # The main is missing
    ('undefined_main',
     re.compile(u'undefined reference to \`main\'')),

    # Error when linking a nonexisting function
    ('link_undefined',
     re.compile(u'undefined reference to ' + u'\‘(?P<function>[^\’]+)\’'))
    ]
    
def main(): 
    """
    File containing functions to detect and manipulate GCC messages
    """
    if len(sys.argv) == 1:
        filterGccMsgs('', None, sys.stdin)
    else:
        filterGccMsgs('', None, codecs.open(sys.argv[1], 'r', 'utf-8'))

def filterGccMsgs(sessionId, dateEvent, text):
    """
    Given a set of lines produced by the compiler, create events from them. This
    is a catalog of regular expressions to catch some common mistakes.

    <event type="gccmsg" datetime=DATE BEGIN>
      <entity>
        <application>gcc</application>
        <item>TEXT</item>
      </entity>
      <context>
        <session id="GCC EVENT ID"/>
      </context>
    </event>

    <item id="msg>
     One message by the compiler
    </item>
    """

    global _msgs

    result = []

    # Parse line by line and detect patterns
    counter = 1
    for line in text:
        index = next((a for a in range(len(_msgs)) if _msgs[a][1].search(line)), 
                     None)
        if index == None:
            # Line does not match with anything at all
            counter += 1
            continue

        msgItem = PLACamOutput.createItem(None, text = line[:-1])

        entity = PLACamOutput.createEntity(application = 'gcc', item = msgItem)

        context = PLACamOutput.createContext(session = sessionId)

        result.append(PLACamOutput.createEvent(PLACamOutput.EventTypes.GccMsg,
                                                dateEvent, 
                                                name = str(counter),
                                                entityList = [entity],
                                                contextList = [context]))
        counter += 1

    print result
                      
    return result

if __name__ == "__main__":
    main()
