#!/usr/bin/env python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of PLA: Personal Learing Analytics

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor
# Boston, MA  02110-1301, USA.
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
# This file implements a rule manager based on the Microsoft INI format. The
# manager parses the files in the INI format using the ConfigParser
# objects. With this basic functionality, the follogin enhancements have been
# implemented:
#
#   1.- The list of rules [name] in the INI file are returned in the same order
#   in which they appear in the file as result. This is to allow the execution
#   of these rules in the same order.  
#
#   2.- Allow the [template] rule with a single "files" variable that forces the
#   given files to be included in that file location. This allows writing rules
#   in different files and merge them with this rule. Circular template
#   inclusions are properly detected.
#
#   3.- The rules are defined hierarchically using the character '.' in their
#   name. Thus, if var = 3 in rule, and rule.subrule has no value assigned to
#   'var', when requesting the value of variable var in rule.subrule, the value
#   3 is returned. In other words, the character '.' serves a similar role as
#   '/' in file system paths, and a variable is valued in the specified level,
#   and its predecessors. These allows the definition of variables that apply to
#   a set of rules. The function get_property takes into account this hierarchy
#   when looking up a variable.
#
#   4.- Each rule may have an "alias" variable to define a synonym of the rule
#   name. Alias are expanded recursively and, circular definitions are properly
#   detected.
#
#   5.- The module keeps a global variable "options" in case one single rule
#   manager is needed in an application. This rule manager is assumed to be the
#   default one and is used when the functions receive a "None" parameter as
#   manager.


import sys, os, re, datetime, ConfigParser, StringIO, ordereddict, atexit
import codecs

# Dictionary to store the ConfigParsers for a set of files. The pairs stored are
# of the form (filename, ConfigParser.RawConfigParser({},
# orderedict.OrderedDict))
_config_parsers_created = {}

_given_definiton_rule_name = "__GIVEN_DEFINITIONS_RULE_NAME__"

options = None

def get_config_parser(fileName):
    """
    Given a set of files, returns the resulting ConfigParser object after being
    parsed.
    """

    global _config_parsers_created

    config = _config_parsers_created.get(fileName)
    if config != None:
        # Hit in the cache, return
        return config

    # Parse the file with a raw parser
    config = ConfigParser.RawConfigParser({}, ordereddict.OrderedDict)

    try:
        config.readfp(codecs.open(fileName, "r", "utf8"))
    except Exception, msg:
        print >> sys.stderr, 'Error while parsing', fileName
        print >> sys.stderr, str(msg)
        sys.exit(1)

    _config_parsers_created[fileName] = config
    return config

def flush_config_parsers():
    """
    Delete all the stored parsers
    """
    global _config_parsers_created

    _config_parsers_created = {}
    return

#
# Flush all the created config parsers.
#
atexit.register(flush_config_parsers);

def load_config_file(config, filename, aliasDict, includeChain = None):
    """
    Function that receives a set of config options (ConfigParser) and a
    filename. Parses the file, makes sure all the new config options are present
    in the first config and adds them to it. The parsing may require, through
    some options, the inclusion of additional files. The includeChain is a list
    of files to detect circual includes (and to notify the path to a missing
    file)

    Returns a pair (set of files finally loaded, list of rules detected)
    """
    
    global options

    # If no config is given, use the local to the module
    if config == None:
        config = options

    # Cannot use empty dictionary as default value in parameter as it
    # accumulates the values.
    if includeChain == None:
        includeChain = []

    # If the file to be processed has been processed already, we are in a
    # "template" chain, terminate
    if os.path.abspath(filename) in includeChain:
        commonPrefix = os.path.commonprefix(includeChain)
        print >> sys.stderr, 'Circular_include:'
        print >> sys.stderr, 'Prefix:', commonPrefix
        print >> sys.stderr, 'Files:', 
        print >> sys.stderr, ' '.join(map(lambda x: x.replace(commonPrefix, 
                                                              '', 1), 
                                          includeChain))
        sys.exit(1)

    # Insert the filename in the includeChain
    includeChain.append(os.path.normpath(filename))

    # If file not found write also the include stack
    if not os.path.isfile(filename):
        print >> sys.stderr, 'Cannot open file', filename
        if includeChain[:-1] != []:
            print >> sys.stderr, 'included from'
            print >> sys.stderr, '  ' + '\n  '.join(includeChain[:-1])
        sys.exit(1)

    # Get the ConfigParser for the input file
    newOptions = get_config_parser(filename)

    # Move defaults to the original config passing them to a [DEFAULT] rule
    defaultsIO = StringIO.StringIO()
    defaultsIO.write('[DEFAULT]\n')
    for (on, ov) in newOptions.defaults().items():
        defaultsIO.write(on + ' = ' + ov + '\n')
    defaultsIO.seek(0)
    try:
        config.readfp(defaultsIO)
    except ConfigParser.ParsingError, msg:
        print >> sys.stderr, 'Error while parsing', filename
        print >> sys.stderr, str(msg)
        defaultsIO.close()
        sys.exit(1)

    # Move all options to the given config but checking if they are legal
    result = (set([filename]), [])
    for sname in newOptions.sections():
        # Get the prefix
        sprefix = sname.split('.')[0]

        # Treat the special case of a template rule that needs to be expanded
        if sprefix == 'template':
            (a, b) = treat_template(config, filename, newOptions, sname,
                                   aliasDict, includeChain)
            # Incorporate results
            result[0].update(a)
            result[1].extend(b)
            continue

        # Apply the alias expansion
        try:
            unaliased = expand_alias(sname, aliasDict)
        except SyntaxError, e:
            print >> sys.stderr, 'Incorrect alias expression'
	    print >> sys.stderr, str(e)
            sys.exit(1)

        # Get the prefix again
        sprefix = unaliased.split('.')[0]

        # Process all the new options (check if legal, add them, etc)
        for (oname, ovalue) in newOptions.items(sname):
            # Treat the special case of option with +name or name+
            prepend = False
            append = False
            if oname[0] == '+' and oname[-1] != '+':
                prepend = True
                oname = oname[1:]
            elif oname[0] != '+' and oname[-1] == '+':
                append = True
                oname = oname[:-1]

            # If not present in the default options, terminate
            if not config.has_option(sprefix, oname) and \
                    newOptions.defaults().get(oname) == None:
                optionName = sname + '.' + oname
                print >> sys.stderr, 'Incorrect option', optionName, 
                print >> sys.stderr, 'in file', filename
                sys.exit(1)

            # Set the values considering the cases of append or prepend
            try:
                if prepend:
                    ovalue = ' '.join([ovalue, get_property(config, unaliased,
                                                              oname)])
                elif append:
                    ovalue = ' '.join([get_property(config, unaliased, oname),
                                       ovalue])
            except ConfigParser.NoOptionError:
                print >> sys.stderr, 'Error while parsing', filename
                print >> sys.stderr, 'Incorrect option', sname + '.' + oname
                sys.exit(1)
            finalValue = set_property(config, unaliased, oname, ovalue,
                                     fileName = filename)

            # Consider the special case of the alias option
            if oname == 'alias':
                for keyValue in finalValue.split():
                    aliasDict[keyValue] = sname
        # Add the original rule ot the result
        result[1].append(sname)

    return result

def dump(config, pad = None, sections = None):
    """
    Function to print out the content of a config object
    """

    global options

    # If no config is given, use the local to the module
    if config == None:
        config = options

    if sections == None:
        sections = config.sections()
    print sections

    if pad == None:
	pad = ''

    try:
        for sname in sections:
            if sname == _given_definiton_rule_name:
                continue
            print sname
            for (oname, vname) in config.items(sname):
                print pad, '  ', oname, '=', config.get(sname, oname)
    except ConfigParser.InterpolationDepthError, e:
        print >> sys.stderr, 'Interpolation Depth error'
        print >> sys.stderr, str(e)
        sys.exit(1)

def treat_template(config, filename, newOptions, sname, aliasDict, includeChain):
    """
    Process template and parse the required files.

    - Config what you have so far
    - filename file where the template is found
    - NewOptions is the new config
    - sname is the rule name where the template was detected
    - includeChain are the files that are included

    Returns the pair (set of files processed, list of rules detected)
    """

    global options

    # If no config is given, use the local to the module
    if config == None:
        config = options

    # Get the pairs in the template rule that are not in the defaults
    # dictionary
    fileItem = [(a, b) for (a, b) in newOptions.items(sname)
                if not a in newOptions.defaults()]

    # There must be a single option with name 'files'
    if len(fileItem) != 1:
        print >> sys.stderr, 'Error in template rule in', filename
        sys.exit(1)

    # The option must have name files
    if fileItem[0][0] != 'files':
        print >> sys.stderr, 'Incorrect option', fileItem[0][0],
        print >> sys.stderr, 'in file', filename
        sys.exit(1)

    # Add template rule to the given config to evaluate the files assignment
    templateFiles = set_property(config, sname, 'files', fileItem[0][1],
                                fileName = filename,
                                createRule = True,
                                createOption = True).split()

    # Remove rule from the original config:
    config.remove_section(sname)

    # Process the template files recursively!
    result = (set([]), [])
    for fname in templateFiles:
        # Get the full path of the template
        if os.path.isabs(fname):
            templateFile = fname
        else:
            templateFile = \
            	os.path.abspath(os.path.join(os.path.dirname(filename), fname))

        (a, b) = load_config_file(config, 
                                  os.path.normpath(templateFile), 
                                  aliasDict, includeChain)
        result[0].update(a)
        result[1].extend(b)
    return result

def expand_alias(rule, aliasDict):
    """
    Given a rule a.b.c, apply the alias values contained in the given
    dictionary. The values are applied hierarchically. That is, the alias is
    looked up starting with the whole rule and then removing the suffixes one
    by one. Once an alias is found, it is applied and the process is repeated
    with the new rule. Loops in the alias expansion are detected.
    """

    # Split the rule in two halfs to separate suffixes
    head = rule
    oldHead = None

    # Set to store the applied rules
    appliedAliases = set([])

    # Keep looping while changes are detected
    while head != oldHead:

        # Remember the current value
        oldHead = head;

        tail = ''
        while head != '':
            # Look up the rule
            newHead = aliasDict.get(head)
            if newHead != None:
                # HIT: Apply the alias
                if newHead in appliedAliases:
                    # A loop in the alias expansion has been detected. Bomb out
                    print >> sys.stderr, 'Circular alias detected'
                    print >> sys.stderr, ' '.join(appliedAliases)
                    sys.exit(1)
                # Propagate the change and remember it
                appliedAliases.add(head)
                head = newHead
                # Get out of the inner loop
                break
            # No alias has been applied, re-apply with a shorter prefix and pass
            # the suffix to the tail
            (head, sep, pr) = head.rpartition('.')
            tail = sep + pr + tail

        # If there is a tail, attach it to the new head
        head += tail

    return head

def get_property(config, rule, option):
    """
    Function that given a rule name of the form 'a.b.c.d' and an option name,
    gets the value obtained from the given config. The procedure works
    hierarchically. It first checks for the option value in the given rule,
    and if not found, it keeps asking for the values in the rules obtained by
    dropping the last suffix (from the last dot until the end of the rule
    name.
    """

    global _given_definiton_rule_name
    global options

    # If no config is given, use the local to the module
    if config == None:
        config = options

    # If the rule is a.b.c, loop asking if we have the option a.b.c.option,
    # then a.b.option, and then a.option.
    partialRule = rule
    while partialRule != '':
        if config.has_option(_given_definiton_rule_name,
                             partialRule + '.' + option):
            result = config.get(partialRule, option)
            return result
        partialRule = partialRule.rpartition('.')[0]

    # If no hit so far, need to make one more test to see if the value is in the
    # DEFAULT rule
    try:
        result = config.get(rule, option)
        # Yes, the value is stored as in  DEFAULT
        return result
    except ConfigParser.InterpolationMissingOptionError, e:
        print >> sys.stderr, 'Incorrect reference to var', option
        sys.exit(1)
    except ConfigParser.NoSectionError:
        print >> sys.stderr, 'Unknown rule', rule
        sys.exit(1)

    # Weird case, bomb out to notify error
    raise ConfigParser.NoOptionError

def set_property(config, rule, option, value, fileName = None,
                createRule = None, createOption = None):
    """
    Function that sets the given value for the rule.option in the given
    config and returns the resulting value (after interpolation). createRule
    and createOption is the boolean allowing the creation of both.
    """

    global options

    # If no config is given, use the local to the module
    if config == None:
        config = options

    # Obtain the rule prefix
    rulePrefix = rule.split('.')[0]

    # Check if the rule is allowed,
    if (not createRule) and (not config.has_section(rulePrefix)):
        # Rule prefix does not exist in config, and creation is not allowed
        raise ValueError('Rule ' + rulePrefix + ' not allowed.')

    # Create the rule if needed
    if not config.has_section(rule):
        config.add_section(rule)

    # See if the option is already present in the config
    try:
        optionPresent = True
        get_property(config, rule, option)
    except ConfigParser.NoOptionError:
        optionPresent = False

    # Check if option is allowed
    if (not createOption) and (not optionPresent):
        # Option does not exist in config, and creation is not allowed
        raise ValueError('Option ' + option + ' not allowed.')

    # Insert the option
    config.set(rule, option, value)

    # Register the rule.option also in the __GIVEN_RULE_NAME
    config.set(_given_definiton_rule_name,
               rule + '.' + option, value)

    # Get the option just inserted to verify interpolation errors
    try:
        finalValue = config.get(rule, option)
    except (ConfigParser.InterpolationDepthError,
            ConfigParser.InterpolationMissingOptionError), e:
        if fileName != None:
            print >> sys.stderr, 'Parse error in file', fileName
        print >> sys.stderr, 'Incorrect reference to variable', value
        sys.exit(3)

    return finalValue

def initial_config(configDefaults):
    """
    Given a dictionary with a set of pairs (name, value), return a ConfigParser
    in which all these values are stored in a special rule with a special name
    to be treated as default values. The reason for not using the DEFAULT rule
    of ConfigParser is because there is no way to know if a rule.name has a
    value that appeared explicitly in a config file, or it derives from the
    DEFAULT. However, both rules are kept (DEFAULTS and the internal) because
    interpolations may require values in DEFAULS"""

    global _given_definiton_rule_name

    result = ConfigParser.SafeConfigParser(configDefaults,
                                           ordereddict.OrderedDict)

    # Add the special rule for the defaults
    result.add_section(_given_definiton_rule_name)

    return result

