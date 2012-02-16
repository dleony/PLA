#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to filter out staff events
# 

import sys, locale, codecs, getopt, os 

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

def initialize_filter_moodle():
    pass

def filter_moodle(fields):
    """
    Filter out moodle events
    """
    
    return fields

def initialize_filter_apache():
    pass

def filter_apache(fields):
    """
    Filter out apache events
    """
    
    # Authentication failure are not processed
    if fields[5] != '200' and fields[5] != '404':
        return None

    # Process only URLs that terminate in .html
    url = fields[4].split()[1]

    # 404 that don't have the _bogus.html substring are also rejected
    if fields[5] == '404' and url.find('.html_bogus.html') == -1:
        return None

    # Hit on an HTML page 
    if (url.endswith('.html')) or (url == '/') or (url.find('.html?') != -1):
        return fields
    
    return None

def initialize_filter_firefox():
    pass

def filter_firefox(fields):
    """
    Filter out moodle events
    """
    
    # Get the URL
    url = fields[2]

    # Ignore those pointing to files or the course web site (got those wih
    # Apache)
    if url.startswith('file:///') or \
            url.startswith('https://labas.it.uc3m.es/'):
        return None

    return fields

def initialize_filter_gcc():
    pass

def filter_gcc(fields):
    """
    Filter out gcc events
    """

    # Drop compilation to dump the type of machine
    if len(fields) > 7 and (fields[7] == '-dumpmachine' or
                            fields[7] == '-xc++'):
        return None
    
    return fields
