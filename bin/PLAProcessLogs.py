#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, sys, tarfile, getopt, pysvn, re, shutil, subprocess, datetime, glob
import codecs

from lxml import etree

import PLACamOutput, PLAGccMessages

_rdfNS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

# Directory in user HOME containing the instrumented commands
plaDirectory = os.path.expanduser('~/.pladata')

# Default options
_debug = False
_givenUserSet = set([])

# Regular expression to filter log files
_logFileFilter = re.compile('.+\.tgz')

def main():
    """

    <script> [-d] [-u username] [-t toolname] svnDirectory

    Process a directory under subversion control containing log files.

    -d    Turns on debugging.

    -u username Select only this username (it might appear several times)
 
    The svnDirectory should be .pladata where the *.tgz files are stored.

    """

    global _debug
    global _logFileFilter
    global _givenUserSet

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   "du:t:")
    except getopt.GetoptError, e:
        print e.msg
        print main.__doc__
        sys.exit(2)

    # Process the arguments
    for optstr, value in opts:
        if optstr == "-d":
            _debug = True
        elif optstr == '-u':
            _givenUserSet.add(value)

    dbg('Checking if the argument is correctly given')

    # If no argument is given terminate.
    if len(args) == 0:
        print 'Script needs at least a directory'
        print main.__doc__
        sys.exit(2)

    # If any of the directories does not exist, complain
    incorrectDir = next((dn for dn in args \
                             if (not os.path.exists(dn)) or \
                             (not os.path.isdir(dn))), \
                            None)
    if incorrectDir != None:
        print 'Incorrect argument: ' + incorrectDir
        print main.__doc__
        sys.exit(2)

    # Remember the initial dir
    initialDir = os.getcwd()

    # Loop over the given dirs
    createdFiles = []
    for dirName in args:

        # Change to the given directory
        dbg('Changing current dir to ' + str(dirName))
        os.chdir(dirName)

        # Expand the content of the tar files
        expandTarFiles()

        # Calculate the users to process
        filterUsers()

        dbg('Processing users: ' + ', '.join(_givenUserSet))

        # If no user is given terminate
        if _givenUserSet == set([]):
            print 'No users to process.'
            sys.exit(2)

        for userName in _givenUserSet:
            # Obtain user sessions
            sessions = obtainSessions(userName)
            # sessions.sort(key = lambda x: x[1][0])

            dbg('Detected ' + str(len(sessions)) + ' sessions.')

            # Classify all the events for the user
            toolProcess(userName, sessions)

            # Write data into session files
            createdFiles.extend(writeSessionFile(userName, sessions))

        # Dump the addittional elements stored in the Cam Output
        createdFiles.append(PLACamOutput.writeElements())

        # Create a RDF file including all the generated files
        writeMasterRDFFile(createdFiles)

        # Restore initialDir
        os.chdir(initialDir)

def expandTarFiles():
    """
    Traverse the data directory and expand all the tar files
    """

    # Make sure the directory to leave processed data is there
    processedDataDir = '.processedData'
    if not os.path.exists(processedDataDir):
        dbg(' Created dir ' + processedDataDir)
        os.mkdir(processedDataDir)

    # If there is something called processDataDir other than a dir, notify
    if not os.path.isdir(processedDataDir):
        dbg(' File ' + processedDataDir + ' is on the way. Remove it')
        sys.exit(2)

    # Fetch the svn client
    svnClient = pysvn.Client()
    svnClient.exception_style = 1

    try:
        dbg('Obtaining SVN info from ' + os.getcwd())
        entryInfo = svnClient.info(os.getcwd())
    except pysvn.ClientError, e:
        print 'Directory is not under Subversion control'
        print main.__doc__
        sys.exit(2)
        
    # Get list of files to process
    filesToProcess = [os.path.basename(a[0]['repos_path']) \
                          for a in svnClient.list('.', recurse = False) \
                          if _logFileFilter.match(a[0]['repos_path'])]
    dbg('Files to process: ' + str(filesToProcess))

    # Process each detected tar file
    for tarFileName in filesToProcess:
        if not os.path.exists(os.path.join(processedDataDir, tarFileName)):
            dbg('Begin process file: ' + tarFileName)
        else:
            # Tar file has been processed alrady
            dbg('Skip file: ' + tarFileName)
            continue

        logMsg = svnClient.log(tarFileName, limit = 1)
        # If the file has no logs, skip its processing
        if logMsg == []:
            dbg(' No logs found for file. Skipping')
            continue
        userName = logMsg[0]['author']
        dbg(' User = ' + userName)

        # If the directory does not exist, create it
        userDir = os.path.join('users', userName)
        if not os.path.exists(userDir):
            dbg(' Created dir ' + userDir)
            os.makedirs(userDir)

        # If there is something called userName other than a dir, notify
        if not os.path.isdir(userDir):
            dbg(' File ' + userDir + ' is on the way. Remove. Skipping')
            
        # Ready to unpack the file
        dbg(' Unpacking ' + tarFileName + ' in user ' + userDir)
        tarFile = tarfile.open(tarFileName, mode = 'r')

        # Extract only those tar members with legal filenames
        members = [m for m in tarFile.getmembers() \
                       if (not m.name.startswith('/')) and \
                          (m.name.find('../') == -1)]

        try:
            tarFile.extractall(userDir, members)
        except:
            # Something went wrong, maintain this tar file
            continue

        # Remove the tar file from SVN and set it aside
        shutil.copy(tarFileName, os.path.join(processedDataDir, tarFileName))
        
    return

def obtainSessions(userDir):
    """
    Obtain the sessions for the given user
    """

    dbg('Obtaining Sessions for ' + userDir)

    # Obtain the the filees with the "last" data
    lastFiles = glob.glob(os.path.join(userDir, 'home', 'teleco', '.lastrc') + 
                          '_[0-9]*_[0-9]*')

    # Process all lines in all files
    sessionLines = set([])
    for sessionfile in lastFiles:

        # Detect spurious files left by other tools
        if not re.match('.+\.lastrc_[0-9]+_[0-9]+$', sessionfile):
            dbg('Discarding file ' + sessionfile)
            continue

        # Loop for every line in session file
        dataIn = codecs.open(sessionfile, 'r', 'utf-8')
        for line in dataIn:

            # Skip lines with no data
            if line.startswith('reboot') or line == '\n' or \
                    line.startswith('wtmp') or \
                    (line.find('no logout') != -1) or \
                    (line.find('still logged') != -1) or \
                    (line.find('down') != -1):
                continue
            
            # Add it to the set (repeated lines are ignored)
            sessionLines.add(line[:-1].strip())
        dataIn.close()

    # Proceed to parse all lines and create session data
    return parseLastFile(sessionLines, userDir)

def toolProcess(userName, sessions):
    """
    Process the files for each tool
    """

    toolProcessBash(userName, sessions)

    toolProcessGcc(userName, sessions)

    toolProcessGdb(userName, sessions)

    toolProcessValgrind(userName, sessions)

    toolProcessFirefox(userName, sessions)

    toolProcessLog('kate', userName, sessions)

    toolProcessLog('kdevelop', userName, sessions)

def toolProcessBash(userName, sessions):
    """
    Given the user and the session files, move the events in the bash file to
    the corresponding session

    <event type="bashcommand" datetime="date/time when executed" id="...">
      <application refId="id of the command"/>
      <personProfile personId="..." name="vmwork"/>
      <itemVersion refId="to the version" role="command"/>
      <context>
        <session refId="session Id"/>
      </context>
    </event>

    <application name="the command name" id="..."/>
    <person name="..." id="..."/>
    <item id="...">The entire command line</item>
    """
    
    # Commands that even though the appear as bash, they require special
    # processing thus, they are processed somewhere else.
    skipCommands = set(['gcc', 'valgrind', 'gdb', 'kate', 'kdevelop'])

    # Obtain the the files with the bash history data
    dataDir = os.path.join(userName, 'home', 'teleco')
    historyFiles = glob.glob(os.path.join(dataDir, '.bash_history') + 
                             '_[0-9]*_[0-9][0-9][0-9][0-9][0-9]')
    historyFiles.sort()
    
    dbg('Processing bash data files')

    # Loop over all the bash files
    for historyFileName in historyFiles:
        dbg('  ' + historyFileName)
        dataFile = codecs.open(historyFileName, 'r', 'utf-8')

        stamp = datetime.datetime.fromtimestamp(os.stat(historyFileName).st_mtime)
        counter = 0
        # Loop over all the lines
        for line in dataFile:

            # Detect and skip empty lines
            if line == '\n':
                continue

            # Detect timestamp
            if line.startswith('#'):
                stamp = datetime.datetime.fromtimestamp(float(line[1:]))
                continue
            
            # Chop the command line to find out if it is one of the special
            # commands: gcc, valgrind, gdb, kate, kdevelop. If so, skip the
            # processing because it is done in other specific function.
            fields = line.split()
            if os.path.basename(fields[0]) in skipCommands:
                dbg(' Skipping cmd ' + os.path.basename(fields[0]))
                continue

            # Find to which file this session belongs
            index = locateEventInSession(sessions, stamp)
            
            # Create the appropriate event and insert it in the proper bucket of
            # the sessions list.
            counter = counter + 1
            person = PLACamOutput.createPerson(os.path.basename(userName))
            
            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.BashCommand, stamp,
                entityList = [\
                    PLACamOutput.createPersonProfile('vmwork', person),
                    PLACamOutput.createEntityAppDevice('application',
                                                       os.path.basename(fields[0])),
                    PLACamOutput.createItemVersion(None, role = 'command', 
                                                   text = line[:-1])
                    ],
                contextList = [\
                    PLACamOutput.createContext(session = sessions[index][3])
                    ])

            # Add the event to the session data structure
            sessions[index][2][event.get('id')] = event
        
        dbg('  Added ' + str(counter) + ' new events.')
        dataFile.close()

def toolProcessGcc(userName, sessions):
    """
    Given the events logged by the compiler, create the event with the following
    structure:

    <event type="bashcommand" datetime="date/time when executed" id="...">
      <application refId="id of Gcc"/>
      <personProfile personId="..." name="vmwork"/>
      <itemVersion role="command" refId="item with the command" role="command"/>
      <itemVersion role="message" refId="item with msg"/>
      ...
      <itemVersion role="message" refId="item with msg"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <application name="gcc" id="..."/>
    <person name="..." id="..."/>
    <item id="...">The entire command</item>
    <item id="...">error msg</item>
    ...
    <item id="...">error msg</item>
    """

    # Obtain the files with the gcc history data
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           'gcc')
    dataFiles = glob.glob(os.path.join(dataDir, 'gcc') + '_[0-9]*_[0-9]*')
    dataFiles.sort()
    
    dbg('Processing gcc data files')

    # Loop over all the gcc files
    for dataFileName in dataFiles:
        dbg(' ' + dataFileName)
        dataFile = codecs.open(dataFileName, 'r', 'utf-8')

        counter = 0
        errorText = ''
        outputText = ''
        inError = False
        inOutput = False
        dateEvent = None
        command = None
        lineNumber = 1
        # Loop over all the lines in the file
        for line in dataFile:
            
            # Beginning of log (and error message)
            if re.match('^\-BEGIN .+$', line):
                # Beginning of log
                inError = True
                inOutput = False
                fields = line.split()
                dateEvent = datetime.datetime.strptime(' '.join(fields[4:6]),
                                                       '%Y-%m-%d %H:%M:%S')
                command = ' '.join(fields[6:])

                # Find to which file this session belongs
                index = locateEventInSession(sessions, dateEvent)

                dbg(' Detected begin of GCC session')
                continue

            # Beginning of output message
            if re.match('^\-O .+$', line):
                inError = False
                inOutput = True
                continue

            if re.match('^\-END$', line):

                dbg(' Detected end of GCC session')

                # Create the event element
                person = PLACamOutput.createPerson(os.path.basename(userName))
                
                # Entities in the event (personProfile, application and item as
                # command 
                entityList = [\
                    PLACamOutput.createPersonProfile('vmwork', person),
                    PLACamOutput.createEntityAppDevice('application', 'gcc'),
                    PLACamOutput.createItemVersion(None, role = 'command', 
                                                   text = command)
                    ]
                
                # Extend the entities with the error messages
                entityList.extend(PLAGccMessages.filterGccMsgs(errorText + \
                                                                   outputText))
                # Create the event
                event = PLACamOutput.createEvent(
                    PLACamOutput.EventTypes.BashCommand, 
                    dateEvent,
                    entityList = entityList,
                    contextList = [\
                        PLACamOutput.createContext(session = sessions[index][3])
                        ])

                # And add the event to the session
                sessions[index][2][event.get('id')] = event


                # Reset all flags to keep parsing
                errorText = ''
                outputText = ''
                inError = False
                inOutput = False
                dateEvent = None
                command = None

                continue

            # Regular line in the middle of the log file
            if inError:
                errorText += line
            elif inOutput:
                outputText += line
            else:
                print 'line ' + str(lineNumber) + ' inconsistent in ' + \
                    dataFileName
                sys.exit(2)

def toolProcessGdb(userName, sessions):
    """
    Given the events logged by the debugger, create the following events:
    
    <event type="SessionBegin" datetime="begin time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'gdb'"/>
      <itemVersion role="commands" refId="item with the commands"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <event type="SessionEnd" datetime="End time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'gdb'"/>
      <itemVersion role="commands" refId="item with the commands"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <item id="commands">All the commands, one per line</item>
    <session name="GDB_session_id of sesionBegin" 
             id="..." begin="begin date/time" 
             end="end date/time"/>
    """

    # Obtain the files with the gcc history data
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           'gdb')
    dataFiles = glob.glob(os.path.join(dataDir, 'gdb') + '_[0-9]*_[0-9]*')
    dataFiles.sort()
    
    dbg('Processing gdb data files')

    # Loop over all the gcc files
    for dataFileName in dataFiles:
        dbg(' ' + dataFileName)
        dataFile = codecs.open(dataFileName, 'r', 'utf-8')

        outputText = ''
        dateEvent = None
        command = None
        # Loop over all the lines in the file
        for line in dataFile:

            # Skip the empty lines
            if line == '\n':
                continue

            # Beginning of log
            if re.match('^\-BEGIN .+$', line):
                fields = line.split()
                dateEvent = datetime.datetime.strptime(' '.join(fields[1:3]),
                                                       '%Y-%m-%d %H:%M:%S')
                dateEnd = datetime.datetime.strptime(' '.join(fields[3:5]),
                                                       '%Y-%m-%d %H:%M:%S')
                command = ' '.join(fields[5:])

                # Find to which file this session belongs
                index = locateEventInSession(sessions, dateEvent)

                dbg(' Detected begin of GDB session')
                continue

            # End of log
            if re.match('^\-END$', line):
                dbg(' Detected end of GDB session')

                # Create the dbg session element with begin/end date/times
                sessionElement = \
                    PLACamOutput.createSession('GDB_' + sessions[index][3],
                                               dateEvent, dateEnd)

                # Create the person element to be inserted in the SessionBegin
                person = PLACamOutput.createPerson(os.path.basename(userName))

                # Create a context with two session elements distinguished by a
                # role
                context = \
                    PLACamOutput.createContext(session = sessionElement)

                # Create the SessionBegin and SessionEnd events
                eventBegin = PLACamOutput.createEvent(
                    PLACamOutput.EventTypes.SessionBegin, dateEvent,
                    entityList = [
                        PLACamOutput.createPersonProfile('vmwork', person),
                        PLACamOutput.createEntityAppDevice('application',
                                                           'gdb'),
                        PLACamOutput.createItemVersion(None, role="commands",
                                                       text = outputText)
                        ],
                    contextList = [context])
                    
                # Create a duplicate of the context to be inserted in the end
                # event as well
                context = \
                    PLACamOutput.createContext(session = sessionElement)

                eventEnd = PLACamOutput.createEvent(
                    PLACamOutput.EventTypes.SessionEnd, dateEnd,
                    entityList = [
                        PLACamOutput.createPersonProfile('vmwork', person),
                        PLACamOutput.createEntityAppDevice('application', 'gdb'),
                        PLACamOutput.createItemVersion(None, role="commands",
                                                       text = outputText)
                        ],
                    contextList = [context])
                    

                # And add the two events to the tuple
                sessions[index][2][eventBegin.get('id')] = eventBegin
                sessions[index][2][eventEnd.get('id')] = eventEnd

                # Reset values
                dateEvent = None
                command = None
                outputText = ''
                continue

            # Regular line
            outputText += line

def toolProcessValgrind(userName, sessions):
    """
    Given the events logged by valgrind, create the following elements
    
    <event type="SessionBegin" datetime="begin time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'valgrind'"/>
      <itemVersion role="messages" refId="item with the messages"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <event type="SessionEnd" datetime="End time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'valgrind'"/>
      <itemVersion role="commands" refId="item with the messages"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <item id="...">All the commands, one per line</item>
    <session name="Valgrind_session_id of sessionBegin" 
             id="..." begin="begin date/time" 
             end="end date/time"/>
    """

    # Obtain the files with the gcc history data
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           'valgrind')
    dataFiles = glob.glob(os.path.join(dataDir, 'valgrind') + '_[0-9]*_[0-9]*')
    dataFiles.sort()
    
    dbg('Processing valgrind data files')

    # Loop over all the gcc files
    for dataFileName in dataFiles:
        dbg(' ' + dataFileName)
        dataFile = codecs.open(dataFileName, 'r', 'utf-8')

        outputText = ''
        dateEvent = None
        command = None
        # Loop over all the lines in the file
        for line in dataFile:
            
            # Skip the empty lines
            if line == '\n':
                continue

            # Beginning of log
            if re.match('^\-BEGIN .+$', line):
                fields = line.split()
                dateEvent = datetime.datetime.strptime(' '.join(fields[1:3]),
                                                       '%Y-%m-%d %H:%M:%S')
                dateEnd = datetime.datetime.strptime(' '.join(fields[3:5]),
                                                       '%Y-%m-%d %H:%M:%S')
                command = ' '.join(fields[5:])

                # Find to which file this session belongs
                index = locateEventInSession(sessions, dateEvent)

                dbg(' Detected begin of Valgrind session')
                continue

            # End of log
            if re.match('^\-END [0-9]+', line):
                dbg(' Detected end of Valgrind session')

                # Create the dbg session element with begin/end date/times
                sessionElement = \
                    PLACamOutput.createSession('Valgrind_' + sessions[index][3],
                                               dateEvent, dateEnd)

                # Create the person element to be inserted in the SessionBegin
                person = PLACamOutput.createPerson(os.path.basename(userName))

                # Create a context with two session elements distinguished by a
                # role
                context = \
                    PLACamOutput.createContext(session = sessionElement)
                # Create the SessionBegin and SessionEnd events
                eventBegin = PLACamOutput.createEvent(
                    PLACamOutput.EventTypes.SessionBegin, dateEvent,
                    entityList = [
                        PLACamOutput.createPersonProfile('vmwork', person),
                        PLACamOutput.createEntityAppDevice('application',
                                                           'valgrind'),
                        PLACamOutput.createItemVersion(None, role="messages",
                                                       text = outputText)
                        ],
                    contextList = [context])
                    
                # Create a duplicate of the context to be inserted in the end
                # event as well
                context = \
                    PLACamOutput.createContext(session = sessionElement)

                eventEnd = PLACamOutput.createEvent(
                    PLACamOutput.EventTypes.SessionEnd, dateEnd,
                    entityList = [
                        PLACamOutput.createPersonProfile('vmwork', person),
                        PLACamOutput.createEntityAppDevice('application', 
                                                           'valgrind'),
                        PLACamOutput.createItemVersion(None, role="commands",
                                                       text = outputText)
                        ],
                    contextList = [context])
                    
                # And add the two events to the tuple
                sessions[index][2][eventBegin.get('id')] = eventBegin
                sessions[index][2][eventEnd.get('id')] = eventEnd

                # Reset values
                dateEvent = None
                command = None
                outputText = ''
                continue

            # Regular line
            outputText += line

def toolProcessFirefox(userName, sessions):
    """
    Given the user and the session files, parse the events in the firefox file
    and add them to the corresponding session

    <event type="VisitURL" datetime="date/time" id="...">
      <application refId="Id pointing to 'firefox'/>
      <personProfile personId="..." name="vmwork"/>
      <itemVersion refId="item with the URL"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <item id="...">The URL</item>
    """
    
    # Obtain the the files with the bash history data
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           'firefox')
    dataFiles = glob.glob(os.path.join(dataDir, 'firefox') + \
                              '_[0-9]*_[0-9][0-9][0-9][0-9][0-9]')
    dataFiles.sort()
    
    dbg('Processing firefox data files')

    # Loop over all the bash files
    for dataFileName in dataFiles:
        dbg('  ' + dataFileName)
        dataFile = codecs.open(dataFileName, 'r', 'utf-8')

        counter = 0
        # Loop over all the lines
        for line in dataFile:
            
            fields = line.split()
            dateEvent = datetime.datetime.strptime(' '. join(fields[0:2]),
                                                   '%Y-%m-%d %H:%M:%S')
            url = ' '.join(fields[2:])

            # Find to which file this session belongs
            index = locateEventInSession(sessions, dateEvent)

            # Create the appropriate event and insert it in the proper bucket of
            # the sessions list.
            counter = counter + 1
            person = PLACamOutput.createPerson(os.path.basename(userName))


            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.VisitURL, dateEvent,
                entityList = [\
                    PLACamOutput.createPersonProfile('vmwork', person),
                    PLACamOutput.createEntityAppDevice('application', 'firefox'),
                    PLACamOutput.createItemVersion(None, text = url)
                    ],
                contextList = [\
                    PLACamOutput.createContext(session = sessions[index][3])
                    ])

            # Add the event to the session data structure
            sessions[index][2][event.get('id')] = event
        
        dbg('  Added ' + str(counter) + ' new commands.')
        dataFile.close()

def toolProcessLog(prefix, userName, sessions):
    """
    Given an event logged with a single line such as :

    status DATE BEGIN DATE END COMMAND 

    Example:
    0 2010-06-29 13:12:01 2010-06-29 13:12:21 'command'
    
    Create the following elements

    <event type="SessionBegin" datetime="begin time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to the given prefix"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <event type="SessionEnd" datetime="End time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to the given prefix"/>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <session name="PREFIX_session_id of sesionBegin" 
             id="..." begin="begin date/time" 
             end="end date/time"/>

    """
    
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           prefix)
    # Obtain the the files with the bash history data
    dataFiles = glob.glob(os.path.join(dataDir, prefix) + '_[0-9]*_[0-9]*')
    dataFiles.sort()
    
    dbg('Processing ' + prefix + ' data files')

    # Loop over all the bash files
    for dataFileName in dataFiles:
        dbg('  ' + dataFileName)
        dataFile = codecs.open(dataFileName, 'r', 'utf-8')

        counter = 0
        # Loop over all the lines
        for line in dataFile:

            # Skip the empty lines
            if line == '\n':
                continue

            # Detect dates
            fields = line.split()
            dateBegin = datetime.datetime.strptime(' '.join(fields[1:3]),
                                                     '%Y-%m-%d %H:%M:%S')
            dateEnd = datetime.datetime.strptime(' '.join(fields[3:5]), 
                                                     '%Y-%m-%d %H:%M:%S')

            # Find to which file this session belongs
            index = locateEventInSession(sessions, dateBegin, dateEnd)

            # If the session is not detected discard command
            if index == None:
                print 'Discarding command ' + line[:-1]
                continue
            
            counter = counter + 1
            # Create the session element with begin/end date/times
            sessionElement = PLACamOutput.createSession(prefix + '_'
                                                        + sessions[index][3],
                                                        dateBegin, dateEnd)
            
            # Create the person element to be inserted in the SessionBegin
            person = PLACamOutput.createPerson(os.path.basename(userName))
            
            # Create a context with two session elements distinguished by a
            # role
            context = PLACamOutput.createContext(session = sessionElement)

            # Create the SessionBegin and SessionEnd events
            eventBegin = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.SessionBegin, dateBegin,
                entityList = [
                    PLACamOutput.createPersonProfile('vmwork', person),
                    PLACamOutput.createEntityAppDevice('application', 
                                                       prefix)
                    ],
                contextList = [context])
            
            # Create a duplicate of the context to be inserted in the end
            # event as well
            context = PLACamOutput.createContext(session = sessionElement)
                
            eventEnd = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.SessionEnd, dateEnd,
                entityList = [
                    PLACamOutput.createPersonProfile('vmwork', person),
                    PLACamOutput.createEntityAppDevice('application', 
                                                       prefix)
                    ],
                contextList = [context])
            
            # And add the two events to the tuple
            sessions[index][2][eventBegin.get('id')] = eventBegin
            sessions[index][2][eventEnd.get('id')] = eventEnd

        dbg('  Added ' + str(counter) + ' new commands.')
        dataFile.close()

def parseLastFile(sessionLines, userDir):
    """
    Given a set of lines from the last command parse its content and creates as
    many session tuples as detected.

    Returns a list of tuples with the following structure:

    (filename, [date from, date two], Dict[id] = element, session element)

    - filename is the file where the events of the session will be stored
    - [date from, date two] is the begin/end session times
    - Dict[id] is a dictionary of the event elements in the session
    - session is the element capturing the session information

    The idea is to parse these lines, populate the empty set to contain all the
    events to be added to the file, and add them in one shot (open, parse,
    write, close) in a single call afterwards.

    Elements created:

    <event type="SessionBegin" datetime="begin time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'virtualbox'"/>
      <context>
        <session refId="session id"/>
      </context>
    </event>

    <event type="SessionEnd" datetime="begin time" id="...">
      <personProfile personId="..." name="vmwork"/>
      <application refId="id pointing to 'virtualbox'"/>
      <context>
        <session refId="session id"/>
      </context>
    </event>

    <session name="File name" id="..." begin="begin date/time" 
             end="end date/time"/>
    """

    dbg(' Parsing sessionlines')
    
    # Open and loop over each line in the data file
    sessions = []
    for line in sessionLines:

        # Skip lines with no data
        if line.startswith('reboot') or line == '\n' or \
                line.startswith('wtmp') or \
                (line.find('down') != -1):
            continue
        
        # Split line in fields
        fields = line.split()
        
        # Obtain the start and end dates for the session
        try:
            sessionDates = extractDates(fields)
        except ValueError, e:
            # The values were incorrect, terminate
            print 'Incorrect format detected in "last" line:'
            print line[:-1]
            continue

        # If the parsing failed, notify
        if sessionDates == None:
            print ' Unable to get dates in line: ' + line[:-1]
            continue

        # Skipping lines with no second date nor "still logged" message
        if sessionDates[1] == None and (line.find('still logged') == -1):
            continue

        # Skip the sessions that have an empty duration
        if sessionDates[0] == sessionDates[1]:
            dbg(' Skipping empty session')
            continue

        # Skip the sessions with reversed dates
        if sessionDates[0] > sessionDates[1]:
            dbg(' Skipping reversed session')
            continue

        # Decide the name of the session file
        nameStart = sessionDates[0].strftime('%Y%m%d%H%M%S') 
        nameEnd = 'StillLogged.xml'
        if sessionDates[1] != None:
            nameEnd = sessionDates[1].strftime('%Y%m%d%H%M%S' + '.xml')

        # Create the data structure with the basic session information
        sessionDataFileName = nameStart + '_' + nameEnd

        # Session element to be referenced
        sessionElement = \
            PLACamOutput.createSession(getSessionIdFromName(sessionDataFileName),
                                       sessionDates[0],
                                       sessionDates[1])
        
        # Session events containing session begin, session end
        person = PLACamOutput.createPerson(os.path.basename(userDir))

        eventStart = PLACamOutput.createEvent(
            PLACamOutput.EventTypes.SessionBegin, sessionDates[0],
            entityList = [
                PLACamOutput.createPersonProfile('vmwork', person),
                PLACamOutput.createEntityAppDevice('application', 'virtualbox')
                ],
            contextList = [PLACamOutput.createContext(session = sessionElement)])

        eventEnd = PLACamOutput.createEvent(
            PLACamOutput.EventTypes.SessionEnd, sessionDates[1],
            entityList = [
                PLACamOutput.createPersonProfile('vmwork', person),
                PLACamOutput.createEntityAppDevice('application', 'virtualbox')
                ],
            contextList = [PLACamOutput.createContext(session = sessionElement)])

        # Insert the data for the session
        sessions.append((sessionDataFileName, sessionDates, 
                         PLACamOutput.insertElements({},
                                                     [eventStart, 
                                                      eventEnd]), sessionElement))

    # Add an extra entry to catch events that do not fall in any of the sessions
    sessions.append(('EventsInNoSession.xml', [None, None], {}, None))

    return sessions

def writeSessionFile(userDir, sessions):
    """
    For each session in the list, create a file for those with non empty data
    """

    global _rdfNS

    dbg('Writing session files')

    # Create the "Sessions" directory to store the sessions
    sessionDir = os.path.join(userDir, 'sessions')
    if not os.path.exists(sessionDir):
        dbg(' Created ' + sessionDir)
        os.makedirs(sessionDir)

    # Start from scratch
    dbg('Removing session files in the directory')
    map(os.remove, glob.glob(os.path.join(sessionDir, '*.xml')))

    # Loop over all the session
    fileNames = []
    for session in sessions:

        if len(session[2]) == 0:
            dbg('Empty session ' + session[0])
            continue

        dbg('Session file ' + session[0] + ' with ' + \
                str(len(session[2])) + ' events')

        root = createRootElement('rdf', _rdfNS)
        root.append(etree.Comment('File automatically generated by PLA'))

        # Add each event to the root
        eventList = session[2].values()
        eventList.sort(key = lambda x: x.get('datetime'))
        for elem in eventList:
            root.append(elem)

        toWrite = etree.ElementTree(root)
        fileName = os.path.join(sessionDir, session[0])
        toWrite.write(fileName, encoding='utf-8', xml_declaration = True, 
                      method = 'xml', pretty_print = True)
        fileNames.append(fileName)

    return fileNames

def writeMasterRDFFile(createdFiles):
    """
    Given a set of RDF file names each of them containing a set of events,
    create a file which includes all these events.
    """

    global _rdfNS
    
    dbg('Writing master file')

    # Xinclude namespace 
    XITREE = '{http://www.w3.org/2001/XInclude}'

    # Create the root element
    root = createRootElement('rdf', _rdfNS,
                             [('xi', 'http://www.w3.org/2001/XInclude')])
    root.append(etree.Comment('File automatically generated by PLA'))

    # Loop over all the created files
    for filename in createdFiles:
        root.append(etree.Element(XITREE + 'include',
                                  {'href' : filename,
                                   'xpointer' : 'xpointer(/*/node())'}))

    toWrite = etree.ElementTree(root)
    fileName = 'master.xml'
    toWrite.write(fileName, encoding='utf-8', xml_declaration = True, 
                  method = 'xml', pretty_print = True)
        
        
def extractDates(fields):
    """
    Given a line of the output generated by "last -F" splitted into fields,
    extract the two dates that are included. The second one might not be there.
    """
    
    dates = []
    for index in range(0, len(fields)):
        if fields[index] in set(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', \
                                     'Sun']):
            dates.append(datetime.datetime.strptime(\
                    ' '.join(fields[index:index + 5]), \
                        '%a %b %d %H:%M:%S %Y'))

    # If length is not 2, stick a None at the end
    if len(dates) == 1:
        dates.append(None)

    return dates

def filterUsers():
    """
    Search for directories in the "users" folder and take the intersection
    between them and the value of _givenUserSet. If this last var is empty, take
    the union of both sets, otherwise, the intersection. The global variable is
    then used as a restrictor.
    """

    global _givenUserSet

    # Create the list of user directories
    userDirs = set([])
    for uDir in os.listdir('users'):
        name = os.path.join('users', uDir)
        if os.path.isdir(name):
            userDirs.add(name)

    # Take union or intersection depending on value of global var
    if _givenUserSet == set([]):
        _givenUserSet = userDirs
    else:
        _givenUserSet = set(map(lambda x: os.path.join('users', x),
                                _givenUserSet))
        intersect = _givenUserSet & userDirs
        if intersect != _givenUserSet:
            print 'Unknown users: ' + ' '.join(_givenUserSet - userDirs)
            _givenUserSet = intersect
    return

def createRootElement(rootname, nsUrl, otherNSUrl = []):
    """
    Creates the root element of an XML file. 

    rootname: name of the root element
    nsUrl: The URL of the main namespace
    otherNSUrl: list of pairs (prefix, url) for additional namespaces
    """
    TREE = '{%s}' % nsUrl
    NSMAP = {None: nsUrl}
    
    # Add the additional namespaces to the dictionary
    for (pr, url) in otherNSUrl:
        NSMAP[pr] = url

    return etree.Element(TREE + rootname, nsmap = NSMAP)
    
    
def getSessionIdFromName(name):
    """
    Given the name of a file containing a session, guess the Id of that session
    """

    return 'SessionBegin_' + name.split('_')[0]


def locateEventInSession(sessions, stamp, tend = None):
    """
    Given a time stamp and a list of tuples (sessionfile, [dateBegin, dateEnd],
    .....) decide in which of the [dateBegin, dateEnd] can be included. The
    first one is returned.
    """
    if tend == None:
        tend = stamp
    
    return next((i for i in xrange(len(sessions)) \
                     if (sessions[i][1][0] == None or \
                             sessions[i][1][0] <= stamp) and \
                     (sessions[i][1][1] == None or \
                          sessions[i][1][1] >= tend)), None)

def dbg(msg):
    """
    Function that prints the given message (with a prefix) if global var _debug
    is true
    """
    global _debug
    
    if _debug:
        print 'dbg: ' + msg

if __name__ == "__main__":
    main()
