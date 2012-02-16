#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import sys, locale, codecs, getopt, os, ldap

import rule_manager

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

# Fix the input encoding when redirecting stdin
if sys.stdin.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdin = sw(sys.stdin)

#
# Module prefix
#
module_prefix = 'ldap_lookup'

#
# Configuration parameters for this module
#
config_params = {
    'uri': '',              # URI where the server is located
    'base': '',             # Base ou=*,c=*
    'fields': 'uid cn mail' # Fields to search
    }

ldap_obj = None

debug = 0

def initialize(module_name):
    """
    Initialize the ldap_obj.
    """

    global ldap_obj
    global debug

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    uri = rule_manager.get_property(None, module_name, 'uri')
    if uri == '':
        # Nothing to do
        return

    try:
        ldap_obj = ldap.initialize(uri)
    except:
        print >> sys.stderr, 'LDAP exception when initializing'
        sys.exit(1)


    print >> sys.stderr, 'LDAP object initialized successfully'

def execute(module_name):
    """
    Bogus function to perform the required task, but this module is an auxiliary
    one, thus, no task to execute.
    """
    return

def get(lookup_name):
    """
    Looks up the given name in the directory and returns synonyms
    """

    global module_prefix
    global ldap_obj
    
    # If the object has not been initialized, terminate
    if ldap_obj == None:
        return None

    base = rule_manager.get_property(None, module_prefix, 'base')
    attr_list = rule_manager.get_property(None, module_prefix, 
                                          'fields').split()

    expr = reduce(lambda x, y: '(|(' + y + '=' + lookup_name + ')' \
                      + x + ')', attr_list, '')

    try:
        l = ldap_obj.search_s(base, ldap.SCOPE_SUBTREE, expr, 
                              map(lambda x: str(x), attr_list))
    except:
        # Something went wrong, punt.
        return None

    # If empty result or more than one record, ignore
    if l == [] or len(l) != 1:
        return None

    # Return the dictionary with the attributes of the first element
    return l[0][1]

def main():
    """
    """

    global config_params
    global module_prefix

    config_params['uri'] = 'ldap://ldap.uc3m.es'
    config_params['base'] = 'ou=Gente,o=Universidad Carlos III,c=es'

    rule_manager.options = rule_manager.initial_config({})
    section_name = module_prefix
    try:
        configparser.options.add_section(section_name)
    except ConfigParser.DuplicateSectionError:
        pass

    for (vn, vv) in config_params.items():
        rule_manager.set_property(None, section_name, vn, vv,
                                  createRule = True, createOption = True)

    initialize(module_prefix)

    print get('abel')

if __name__ == "__main__":
    main()
