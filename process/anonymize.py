#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to compute tf-idf for a set of documents
#
import sys, locale, codecs, getopt, os, hashlib, getpass, atexit
import rule_manager, ldap_lookup

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
module_prefix = 'anonymize'

#
# Configuration parameters for this module
#
config_params = {
    'file': '', # File to read/update the anonymized strings
    'min_length': '9',
    'passwd': '' # Password used to anonymize
    }

anonymize_map = {}

debug = 0

def to_unicode(obj, encoding = 'utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj

def initialize(module_name = None):
    """
    Read an anonymize map from a file. Lines are comma separated pairs of name,
    sha256 key.
    """

    global anonymize_map
    global module_prefix
    global debug

    if module_name == None:
        module_name = module_prefix

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    # Get values from config
    map_file = rule_manager.get_property(None, module_name, 'file')
    passwd = rule_manager.get_property(None, module_name, 'passwd')
    min_length = int(rule_manager.get_property(None, module_name,
                                               'min_length'))

    # Load the content in the dictionary
    load_data(map_file)

def load_data(map_file):
    """
    Given a map file, loads its data in the global dictionary anonymize_map
    """
    global anonymize_map

    # File must be given and exist, then load its content
    if map_file != '' and os.path.isfile(map_file):
        dataIn = codecs.open(map_file, 'r', 'utf-8')
        for line in dataIn:
            # Drop the newline at the end
            line = line[:-1]

            # Skip empty lines and those starting with #
            if len(line) == 0 or line[0] == '#':
                continue

            fields = line.split(',')

            # If the line does not have at least two values, exception
            if len(fields) < 2:
                raise ValueError('Incorrect line ' + line)

            anonymize_map[fields[0]] = fields[1]
        dataIn.close()

    # Program the update_map_file when the application terminates
    atexit.register(update_map_file, map_file = map_file)

def execute(module_name):
    """
    Bogus function to perform the required task, but this module is an auxiliary
    one, thus, no task to execute.
    """
    return

def update_map_file(map_file):
    """
    Write the given anonymize map to a file. Lines are comma separated pairs of
    key, name.
    """

    global anonymize_map

    # If no file is given, nothing to do
    if map_file == '':
        return

    # Open data, loop over elements in the dictionary and write the file
    dataOut = codecs.open(map_file, 'wU', 'utf-8')
    for key, value in sorted(anonymize_map.items()):
        dataOut.write(key + ',' + value + '\n')
    dataOut.close()

def find_string(value):
    """
    Given a string, see if it is included in the anonymize_map
    """

    global anonymize_map
    return anonymize_map.get(value)

def find_or_encode_string(value, synonyms = None):
    """
    Given a string, obtains its sha256 digest with the password stored in the
    module. The map anonymize_map is updated. The strings included in the
    synonyms are also added to the map with the same key.
    """

    global anonymize_map
    global module_prefix

    # Remove leading and trailing whitespace
    value = value.strip()

    # See if any of the IDs is in the
    digest = find_string(value)
    if digest != None:
        # Hit, return
        return digest

    passwd = rule_manager.get_property(None, module_prefix, 'passwd')
    min_length = int(rule_manager.get_property(None, module_prefix,
                                               'min_length'))

    # String not present. Encode, store and return
    digest = hashlib.sha256((value + passwd).encode('utf-8')).hexdigest()

    while anonymize_map.get(digest[0:min_length]):
        min_length += 1

    # Decide the final key
    digest = digest[0:min_length]

    # Look up the given values in LDAP and add to the collection of synonyms
    other_ids = set([value])
    ldap_dict = ldap_lookup.get(value)
    if ldap_dict != None:
        other_ids = other_ids.union(set(map(lambda x: x[0].decode('utf-8'),
                                            ldap_dict.values())))

    if synonyms != None:
        other_ids = other_ids.union(set(synonyms))

    # Other_ids has now all possible synonyms for the given value. See if any of
    # them is in the table.
    old_digest = next((anonymize_map.get(x) for x in other_ids \
                           if anonymize_map.get(x) != None), None)

    # If one synonym was found, take that as the digest.
    if old_digest != None:
        digest = old_digest

    # Propagate the digest to the rest of synonyms
    for other_id in other_ids:
        anonymize_map[other_id] = digest

    return digest

def main():
    """
    Get a list of NIAs and create an encrypted mapping. The idea is to create a
    procedure that is difficult to reverse, and keep it as secret as possible.

    Procedure:

    Take the NIA, concatenate it with a password, apply sha256 and take the
    smallest prefix of the resulting hexdigest to be able to differentiate all
    the nia in the population.

    The NIAs are assumed to be the first field of a CSV line give by stdin. Dump
    the mapping to stdout in CSV format.

    script [options]

    Options:

    -d Boolean to turn in debugging info

    -m mapfile File with comma separated map of pairs (key,name)

    Example:

    sed -e '1d' user_group.csv | awk -F, '{ printf("%s,%s\n", $1, $3, $2); } | \
    python -m mapfile Anonymize.py

    """

    global config_params
    global anonymize_map
    global debug

    #######################################################################
    #
    # OPTIONS
    #
    #######################################################################
    map_file = None

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "dm:", [])
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
        elif optstr == "-m":
            map_file = value

    # Check that there are additional arguments
    if len(args) != 0:
        print >> sys.stderr, 'Script does not accept parameters'
        sys.exit(1)

    if debug:
        print >> sys.stderr, 'Options: ', args

    #######################################################################
    #
    # MAIN PROCESSING
    #
    #######################################################################

    passwd = getpass.getpass()

    config_params['file'] = map_file
    config_params['passwd'] = passwd

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

    # Loop for every line in stdin
    for line in sys.stdin:
        line = to_unicode(line)
        line = line[:-1]
        # Skip empty lines
        if len(line) == 0:
            continue

        fields = line.split(',')
        nia = fields[0]
        find_or_encode_string(nia, fields[1:])

    if len(anonymize_map) == 0:
        print >> sys.stderr, 'No data to anonymize'
        sys.exit(1)

    # maxlength = max([len(b) for a, b in anonymize_map.items()])

    # length = 0
    # shortanonymize_map = set([])
    # while len(shortanonymize_map) != len(anonymize_map) and length < maxlength:
    #     length += 1
    #     shortanonymize_map = set([])

    #     for nia, hash_digest in anonymize_map.items():
    #         shortanonymize_map.add(hash_digest[:length])

    # result = {}
    # for nia, hash_digest in anonymize_map.items():
    #     result[nia] = (hash_digest, hash_digest[:length])

    # for nia, item in sorted(anonymize_map.items()):
    #     print nia + ',' + item


    # update_map_file(map_file)

# Execution as script
if __name__ == "__main__":
    main()
