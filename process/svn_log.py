#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import sys, locale, codecs, getopt, os, glob, re, datetime

#
# Type of event detected
#
# svn_commit
#   - user: userid
#   - application: svn
#   - comment: max 256 chars with the comment.

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

#
# Module prefix
#
module_prefix = 'svn_log'

#
# Configuration parameters for this module
#
config_params = {
    'urls': '',            # repositories to process
    'filter_file': '',     # File containing a function to filter events
    'filter_function': '', # Function to use to filter
    'from_date': '',       # Date from which to process events
    'until_date': ''       # Date until which to process events
    }

#
# Module prefix
#
module_prefix = 'svn_log'

def initialize(module_name):
    """
    Initialization function. Must be here always.

    """

    global filter_function
    global debug
    
    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    filter_function = process_filters.initialize_filter(module_name)

    return

def execute(module_name):
    """
    Given a list of files with firefox logs, process all of them. 

    Process the files containing the events. Return True if no error is
    detected.

    [type: 'svn_commit', 
     date_time: dtime,
     user: anonymize(user),
     application: svn,
     comment: (max 256 chars)]
    """

    pass

