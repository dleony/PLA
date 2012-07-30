#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to process a CSV file with events downloaded from Apache derived from
# the interaction with embedded questions.
# 
import sys, locale, codecs, getopt, os, glob, re, datetime

import detect_new_files, rules_common, rule_manager, event_output, anonymize
import process_filters

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
module_prefix = 'embeddedq_log'

#
# Configuration parameters for this module
#
config_params = {
    'debug': '0',              # Debug flag
    'files': '',               # Files to process
    'filter_file': '',         # File containing a function to filter events
    'filter_function': '',     # Function to use to filter
    'remap_pairs': '',         # Comma separated list of pairs regexp, label to
                               # group the events by blocks.
    'mark_word': '_bogus.html' # Mark used to detect URL in logs
    }

# Regular expression to split the URL in an apache event into pieces
clf_re = re.compile(r'(\d+\.\d+\.\d+\.\d+)\s+(\S+)\s+(.+)\s+\[(.+)\] "(.+)" (\d+) (.*)')

# Regular expressions to detect if a URL is an answer submission or a show event.
submit_result_re = \
    re.compile('_bogus\.html\?form=(?P<formname>[^&]+)&answers=(?P<answerstr>[^&]+)')
show_result_re = \
    re.compile('_bogus\.html\?form=(?P<formname>[^&]+)&showIt=(?P<answerstr>[^&]+)')

# A list of pairs (regexp, label) to translate URL to a meaningful label
remap_pairs = []

filter_function = None

debug = 0

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
    Given a list of files with Apache logs, process all of them that contain the
    word mark word and produce the following events:

    [('name', 'embedded_question_correct'),
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', browser?),
     ('url', URL), 
     ('ip', IP),
     ('block_id', id)]
            
    [('name', 'embedded_question_incorrect'),
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', browser?),
     ('url', URL), 
     ('ip', IP),
     ('block_id', id)]
            
    [('name', 'embedded_question_blank'),
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', browser?),
     ('url', URL), 
     ('ip', IP),
     ('block_id', id)]
            
    [('name', 'embedded_question_show'),
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', browser?),
     ('url', URL), 
     ('ip', IP),
     ('block_id', id)]
            
    """

    global clf_re
    global filter_function
    global remap_pairs

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Get the files to process, lines and mark lines
    (files, total_lines, mark_lines) = \
        rules_common.files_to_process(module_name)

    # Get the remap_pairs evaluated from the options
    remap_pairs = eval('[' + rule_manager.get_property(None, module_name,
                                                       'remap_pairs') + \
                           ']')
    remap_pairs = [(re.compile(x), y) for (x, y) in remap_pairs]

    # Fetch the word to detect an embeddedq to use it later
    mark_word = rule_manager.get_property(None, module_name, 'mark_word')

    # Loop over all the given args
    total_counter = 0
    for file_annotation in files:
        # Get the file name and (if it exists, the date of the last event)
        filename = file_annotation[0]
        last_event = datetime.datetime.min
        if len(file_annotation[1]) != 0:
            last_event = datetime.datetime.strptime(file_annotation[1][0], 
                                                        '%Y-%m-%d %H:%M:%S')
        new_last_event = last_event

        data_in = codecs.open(filename, 'r', encoding = 'utf8', 
                              errors = 'replace')
        old = ''
        counter = 0
        for line in data_in:

            counter += 1
            total_counter += 1
            
            if total_counter % mark_lines == 0:
                print >> sys.stderr, '+',
                sys.stderr.flush()

            line = line[:-1]
            
            fields = clf_re.match(line).groups()

            if fields[2] == '':
                raise ValueError('Empty string' + line)

            # Translate date time of the event
            dtime = datetime.datetime.strptime(fields[3].strip()[:-6], 
                                               '%d/%b/%Y:%H:%M:%S')

            if dtime <= last_event:
                # Event is older than what has been recorded in the
                # detect_new_files. skip
                continue

            if dtime < from_date or dtime > until_date:
                # Ignore event because it is outside the given window
                continue
            
            # Split the url to match and see if it has the mark word
            (method, url, protocol) = fields[4].split()

            # Only 404 that have the mark word substring are accepted
            if fields[5] != '404' or url.find(mark_word) == -1:
                continue

            # If there is a filter function and returns None, skip this event
            if filter_function != None and filter_function(fields) == None:
                continue

            # Record the event with the highest datetime.
            if dtime > new_last_event:
                new_last_event = dtime

            # At this point we have an event of an embedded question.

            event_pairs = process_log_line(url, mark_word)
            
            for (event_suffix, question_id) in event_pairs:
                event = ('embedded_question_' + event_suffix, 
                         dtime,
                         anonymize.find_or_encode_string(fields[2]),
                         [('application', 'unknown'), 
                          ('url', url),
                          ('ip', fields[0]),
                          ('question_id', question_id)])
                
                event_output.out(event)

        data_in.close()
        detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

    print >> sys.stderr

def process_log_line(url_body, mark_word):
    """
    Receives a URL that contains the mark_word followed by a set of parameters
    stating the block from where the question was obtained and the encoded
    results for the answers.

    Examples of URLs are: 

    /INT/inclass_en.html_bogus.html?form=project_score&answers=11C

    Returns a list of tuples of the form: ([outcome], [id]) where [outcome] is
    one of _correct, _incorrect, _blank, _show, and [id] is the block id.
    
    """

    global submit_result_re
    global show_result_re
    global remap_pairs

    show_line = False

    # See if line contains question results or "show results"
    match_obj = submit_result_re.search(url_body)
    if match_obj == None:
        match_obj = show_result_re.search(url_body)
        show_line = True

    # If no match was obtained, bomb out
    if match_obj == None:
        raise ValueError('Impossible URL in process_log_line:', url_body)

    form_name = match_obj.group('formname')
    answer_str = match_obj.group('answerstr')

    if not(show_line) and ((len(answer_str) % 3) != 0):
        raise ValueError('URL with incorrect format:', url_body)
                
    url = url_body.split('?')[0].replace('_bogus.html', '')
    # Get rid off the first slash
    if url[0] == '/':
        url = url[1:]
    item_name = url + form_name
    item_name = item_name.replace('/', '_')
    item_name = item_name.replace('_es.html', '_')
    item_name = item_name.replace('_en.html', '_')
    item_name = item_name.replace('.html', '_')
    if item_name[0] == '.':
        item_name = item_name[1:]

    # See if the question belongs to a block.
    block_name = next((y for (x, y) in remap_pairs 
                       if x.search(item_name) != None), None)

    # If it is a show line, return the "show" and the block name
    if show_line:
        if block_name == None:
            block_name = item_name
        return [('show', block_name)]

    # It is not a show line but a set of answers to (potentially) various
    # questions 
    
    # create list with groups of three characters from answer_str
    answers = []
    for index in map(lambda x: 3 * x, range(len(answer_str)/3)):
        answers.append(answer_str[index:index+3])

    # Process each triplet in the list [abc] where
    # a is the question number within the form
    # b is the answer marked (could be zero)
    # c is C/I/0 if answer is correct, incorrect or not answered
    result = []
    for item in answers:
        # Each answer is a different item

        # The id needs the question index if block_name is None (no compaction)
        if block_name == None:
            question_id = item_name + '_' + item[0]
        else:
            question_id = block_name

        if item[2] == 'C':
            outcome = 'correct'
        elif item[2] == 'I':
            outcome = 'incorrect'
        elif item[2] == '0':
            outcome = 'blank'
        else:
            # There should be anything else other than C, I, or 0
            raise ValueError('Incorrect value in answer string:', outcome)
        result.append((outcome, question_id))
    # End of for each item in answers
    return result

def main():
    """
    Script that given a file with CSV Apache events, it process them and
    prepares to be dumped.

    script [options] logfile logfile ...

    Options:

    -d Boolean to turn in debugging info

    Example:

    script -d srcFolder/f*

    The type of lines considered by this script are of the form (Common Log Format)

    
    IP - USER [DATETIME] "GET /CSS/as.css HTTP/1.1" 200 13089

    Some stats (operations, events, eventrelatedentity, relatedentity, time)

    Import from scratch: 101267, 303801, 7531 2m 19s
    Re-import same data:   -       -       -  
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
            debug = 1

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
