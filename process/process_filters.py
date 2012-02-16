#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Functions to process filter functions
# 
import sys, locale, codecs, os

import rule_manager

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

def initialize_filter(module_name):
    """
    Gets two options from the given dictionary, the filter file and the filter
    function. Imports the file and if a function with name "initialize_"
    followed by filter_function is found, it is executed. Modifies the
    dictionary so that filter_function points to the function instead of the
    name.  """

    filter_file = rule_manager.get_property(None, module_name, 'filter_file')
    function = None
    if filter_file != '':
        filter_function = rule_manager.get_property(None, module_name,
                                                    'filter_function')

        (head, tail) = os.path.split(filter_file)

        # Add the source directory to the path to fetch python modules
        sys.path.insert(0, head)

        try:
            module = __import__(tail, fromlist=[])
        except ImportError, e:
            print >> sys.stderr, 'Unable to import file', tail
            print str(e)
            sys.exit(1)

        # If the file of the import is not what is expected, notify and
        # terminate.
        if not module.__file__.startswith(head):
            print >> sys.stderr, 'Collision when importing', filter_file
            sys.exit(1)
           
        # Fetch the initialization function, and if found, execute it
        function = None
        try:
            function = getattr(sys.modules[tail], 
                               'initialize_' + filter_function)
        except AttributeError, e:
            pass
        if function != None:
            function()

        # Fetch the filter function and leave it assigned  in the dictionary
        try:
            function = getattr(sys.modules[tail], filter_function)
        except AttributeError, e:
            print >> sys.stderr, 'Function', filter_function, 'not found'
            print str(e)
            sys.exit(1)

    return function

