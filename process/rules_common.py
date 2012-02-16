#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Functions that are common to all the rules
#
# 
import sys, locale, codecs, os, glob, datetime

import rule_manager, detect_new_files

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

def file_len(fname):
    """
    Calculate the file length
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def window_dates(module_name):
    """
    Given a module name, it obtains from the global rule_manager object the
    value of the variables 'from_date' and 'until_date'. Translates them to
    datetime.datetime objects and returns the pair (from_date, until_date) as
    result.
    """

    # Translate the date from text to datetime
    from_date = rule_manager.get_property(None, module_name, 'from_date')
    if from_date == '':
        from_date = datetime.datetime.min
    else:
        from_date = datetime.datetime.strptime(from_date, '%Y/%m/%d %H:%M:%S')

    until_date = rule_manager.get_property(None, module_name, 'until_date')
    if until_date == '':
        until_date = datetime.datetime.max
    else:
        until_date = datetime.datetime.strptime(until_date, '%Y/%m/%d %H:%M:%S')

    return (from_date, until_date)

def files_to_process(module_name):
    """
    Given a module name, obtains from the global rule manager the value of the
    "files" variable, computes the total number of lines, and those that need to
    be processed to print a tick in stdout. Returns:

    ([list of files], total_lines, mark_lines)
    
    """

    # Expand wildcards in file names
    files = sum([glob.glob(x) for x in \
                     rule_manager.get_property(None, module_name, 
                                               'files').split()], 
                [])
    # Fetch value to see if the cache for modified files is enabled
    file_modification_cache = \
        rule_manager.get_property(None, module_name,
                                  'file_modification_cache')
    # If modified files cache enabled, filter out those that were not modified
    if file_modification_cache != '':
        new_files = []
        for x in files:
            file_annotation = detect_new_files.needs_processing(None, x)
            if file_annotation == None:
                print >> sys.stderr, 'File', x, 'not modified. Skipping'
            else:
                new_files.append((x, file_annotation[1:]))
        files = new_files
    else:
        files = [(x, ['1970-01-01 00:00:00']) for x in files]

    # Count the total number of lines in the files
    total_lines = sum(map(lambda x: file_len(x[0]), files))
    mark_lines = total_lines / 40 + 1
  
    return (files, total_lines, mark_lines)
