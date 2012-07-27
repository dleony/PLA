#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to process a CSV file with events downloaded from Moodle
# 
import sys, locale, codecs, getopt, os, glob, datetime, re

import detect_new_files, rule_manager, rules_common, event_output, anonymize
import process_filters
from lxml import etree

#
# See update_events and event_output for the structure of the events
#

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
module_prefix = 'moodle_log'

#
# Configuration parameters for this module
#
config_params = {
    'files': '',                            # Files to process
    'filter_file': '',                      # File containing a function to filter events
    'filter_function': '',                  # Function to use to filter
    'event_file_type': 'csv',  # Format of the files with events: csv or html
    'remap_pairs': '',         # Comma separated list of pairs regexp, label to
                               # rename events.
    'datetime_format': '%%d %%B %%Y, %%H:%%M %%p' # Format to parse the date in Moodle log files
    }

# A list of pairs (regexp, label) to rename event names
remap_pairs = []

filter_function = None
    
debug = 0

html_parser = etree.HTMLParser()

xpath_get_table = etree.XPath('//table[contains(@class, "logtable")]/tr')

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
    Given a list of files with Moodle logs, process all of them. Some lines
    contain spurious 0d in the middle. They are removed.

    [('name', 'lms_' + eventtype), 
     ('datetime', datetime),
     ('user', anonymize(user)),
     ('application', 'moodle'),
     ('community', Community ID),
     ('ip', IP),
     ('resource', fields[5])]
            
    """

    global remap_pairs

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Get the files to process, lines and mark lines
    (files, total_lines, mark_lines) = \
        rules_common.files_to_process(module_name)

    # Get the type of file to process
    event_file_type = rule_manager.get_property(None, module_name, 
                                                'event_file_type')
    event_file_type = event_file_type.lower().strip()

    if event_file_type != 'csv' and event_file_type != 'html':
        print >> sys.stderr, 'Incorrect value for option event_file_type'
        print >> sys.stderr, 'Only "csv" or "html" allowed'
        sys.exit(2)
        
    # Get the remap_pairs evaluated from the options
    remap_pairs = eval('[' + rule_manager.get_property(None, module_name,
                                                       'remap_pairs') + \
                           ']')
    remap_pairs = [(re.compile(x), y) for (x, y) in remap_pairs]

    datetime_fmt = rule_manager.get_property(None, module_name, 
                                             'datetime_format')

    print >> sys.stderr, 'Processing', len(files), 'files'

    # Loop over all the given args
    total_counter = 0
    for file_annotation in sorted(files):

        # Get the file name and (if it exists, the date of the last event)
        filename = file_annotation[0]
        last_event = datetime.datetime.min
        if len(file_annotation[1]) != 0:
            last_event = datetime.datetime.strptime(file_annotation[1][0], 
                                                        '%Y-%m-%d %H:%M:%S')

        if event_file_type == 'csv':
            total_counter = process_csv_file(module_name, 
                                             filename, 
                                             mark_lines, 
                                             total_counter,
                                             last_event, 
                                             from_date, 
                                             until_date,
                                             datetime_fmt)
        else:
            total_counter = process_html_file(module_name, 
                                              filename, 
                                              mark_lines, 
                                              total_counter,
                                              last_event, 
                                              from_date, 
                                              until_date,
                                              datetime_fmt)
    print >> sys.stderr

def process_csv_file(module_name, filename, mark_lines, total_counter, 
                     last_event, from_date, until_date, datetime_fmt):
    """
    Receives the following parameters:
    - module_name: to record the modification of the file.
    - filename: file to process in CSV format
    - mark_lines: the number of lines to process to print out a mark
    - total_counter: total number of lines to be processed
    - last_event: the last event processed by this function in this file
    - from_date - to_date: the date limits to process events

    Returns the total_counter updated with the processed lines.
    
    Operations:
    - Open the file
    - Loop over each line 
      - Mark a line if needed
      - Split the line into fields
      - Check if the date/time of the event is allowed
      - Store the new_last_event
      - Dump the event 
    - Close the file
    - Update the info in the detect_new_files

    """

    global filter_function

    new_last_event = last_event

    data_in = codecs.open(filename, 'r', encoding = 'utf8', errors = 'replace')
    old = ''
    counter = 0
    for line in data_in:
        counter += 1
        total_counter += 1
        if total_counter % mark_lines == 0:
            print >> sys.stderr, '+',
            sys.stderr.flush()

        line = line[:-1]
        
        # Detect, accumulate \x0D to be removed
        if line[-1] == '\x0D':
            old = old + line[:-1]
            continue

        # If there is something in old, dump it
        if old != '':
            old = ''

        # Check the number of fields and skip lines without 6 fields
        fields = line.split('\t')
        if len(fields) != 6:
            continue

        # Dump event and remember the last one
        new_last_event = check_data_and_dump_event(fields,
                                                   datetime_fmt,
                                                   last_event,
                                                   new_last_event,
                                                   from_date, 
                                                   until_date)

    data_in.close()
    detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

    return total_counter

def process_html_file(module_name, filename, mark_lines, total_counter, 
                      last_event, from_date, until_date, datetime_fmt):
    """
    Receives the following parameters:
    - module_name: to record the file modification
    - filename: file to process in CSV format
    - mark_lines: the number of lines to process to print out a mark
    - total_counter: total number of lines to be processed
    - last_event: the last event processed by this function in this file
    - from_date - to_date: the date limits to process events

    Returns the total_counter updated with the processed lines.
    
    Operations:
    - Open the file
    - Parse the file
    - Obtain the table element
    - Loop over each table line
      - Mark a line if needed
      - Split the line into fields
      - Check if the date/time of the event is allowed
      - Store the new_last_event
      - Dump the event 
    - Close the file
    - Update the info in the detect_new_files

    """

    global html_parser
    global xpath_get_table

    new_last_event = last_event

    try:
        tree = etree.parse(filename, html_parser)
    except etree.XMLSyntaxError, e:
        print >> sys.stderr, 'Error while parsing', filename
        print str(e)
        sys.exit(1)

    # Get the table rows except the first one that is the header
    table_rows = xpath_get_table(tree.getroot())[1:]
    
    if len(table_rows) == 0:
        # If no rows, empty table
        return total_counter

    # Rows have the format:
    # Date, IP address, User name, Action, Information
    
    # Try to fetch the course title from the breadcrumbs. Detect a link that
    # terminates in /course/view.php?id=???. Get the text in this link.
    el = tree.getroot().xpath('//div[@class = "breadcrumb"]/ul/li/a')
    el = [x for x in el if x.get('href').find('/course/view.php?id=') >= 0]
    if el == []:
        course_name = 'Not calculated'
    else:
        course_name = el[0].text

    # Loop over all the rows
    for row_elem in table_rows:
        # print(etree.tostring(row_elem, pretty_print=True))
        date_raw_string = row_elem[0].text.partition(' ')[2]
        ip_raw_string = row_elem[1][0].text
        user_name = row_elem[2][0].text
        # Isolate the user ID from the href attribute of the <a> element in the
        # third column
        user_id = row_elem[2][0].get('href')
        user_id = user_id.partition('?')[2]
        user_id = user_id.partition('&')[0]
        user_id = user_id.split('=')[1]
        event_name = row_elem[3].text.strip().replace(' ', '_')
        if event_name == '':
            event_name = row_elem[3][0].text.strip().replace(' ', '_')
        resource = row_elem[4].text

        fields = (course_name, date_raw_string, ip_raw_string, user_id,
                  event_name, resource)

        # Dump event and remember the last one
        new_last_event = check_data_and_dump_event(fields,
                                                   datetime_fmt,
                                                   last_event,
                                                   new_last_event,
                                                   from_date, 
                                                   until_date)

    detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

        
    return total_counter

def check_data_and_dump_event(fields, datetime_fmt, last_event, new_last_event, 
                              from_date, until_date):
    """
    Given a tuple with fields, check for some properties and dump the event. The
    field must have:
    
    - Course name
    - Date time
    - IP
    - Username
    - Event name
    - Resource
    """

    global filter_function

    # Translate date time of the event
    try:
        dtime = datetime.datetime.strptime(fields[1].strip(), 
                                           datetime_fmt)
    except ValueError, v:
        print >> sys.stderr, 'Skipping line due to date', fields[1].strip()
        return new_last_event

    if dtime <= last_event:
        # Event is older than what has been recorded in the
        # detect_new_files. skip
        return new_last_event

    if dtime < from_date or dtime > until_date:
        # Ignore event because it is outside the window
        return new_last_event

    # Obtain the event type and apply the remap_pairs
    event_type = 'lms_' + fields[4].replace(' ', '_')
    # Apply the mapping
    event_type = next((y for (x, y) in remap_pairs 
                       if x.search(item_name) != None), event_type)
    
    # If there is a filter function and returns None, skip this event
    if filter_function != None and filter_function(fields) == None:
        return new_last_event
    
    # Record the event with the highest datetime.
    if dtime > new_last_event:
        new_last_event = dtime

    # Create the event data structure
    event = (event_type, dtime, 
             anonymize.find_or_encode_string(fields[3]),
             [('application', 'moodle'), ('community', fields[0]), 
              ('ip', fields[2]), ('resource', fields[5])])
    
    try:
        event_output.out(event)
    except Exception, e:
        print 'Exception while processing', filename, ':', counter
        print str(e)
        sys.exit(1)

    return new_last_event

def main():
    """
    Script that given a file with CSV Moodle events, it process them and
    prepares to be dumped.

    script [options] logfile logfile ...

    Options:

    -d Boolean to turn in debugging info

    Example:

    script -d srcFolder/f*

    The type of lines considered by this script are of the form

    Course_Id Date IP NAME EVENT Information
    
    Some stats (operation, events, eventrelatedentity, relatedentity, time)

    Moodle import: 52755, 263775, 6777 8m 34s
    Moodle re-import 13m 42s

    """

    global config_params
    global module_prefix
    global debug

    #######################################################################
    #
    # OPTIONS
    #
    #######################################################################

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d", [])
    except getopt.GetoptError, e:
        print >> sys.stderr, 'Incorrect option.'
        print >> sys.stderr, main.__doc__
        sys.exit(2)

    # Parse the options
    for optstr, value in opts:
        # Debug option
        if optstr == "-d":
            config_params['debug'] = '1'
            debug = True

    # Check that there are additional arguments
    if len(args) == 0:
        print >> sys.stderr, 'Script needs additional parameters'
        sys.exit(1)

    if debug:
        print >> sys.stderr, 'Options: ', args

    #######################################################################
    #
    # MAIN PROCESSING
    #
    #######################################################################
    incorrect_file = next((x for x in args if not(os.path.isfile(x))), None)
    if incorrect_file != None:
        print >> sys.stderr, 'File', incorrect_file, 'not found'
        sys.exit(1)

    config_params['files'] = ' '.join(args)

    rule_manager.options = rule_manager.initial_config({})
    section_name = module_prefix
    try:
        rule_manager.options.add_section(section_name)
    except ConfigParser.DuplicateSectionError:
        pass

    for (vn, vv) in config_params.items():
        rule_manager.set_property(None, section_name, vn, vv,
                                  createRule = True, createOption = True)

    initialize(module_prefix)

    execute(module_prefix)

# Execution as script
if __name__ == "__main__":
    main()
