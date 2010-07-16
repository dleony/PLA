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

    <event id = ??
           datetime = from the file>
      <entity>
        <application>bash</application>
        <personProfile id="vmwork"/>
        <item type="bash"> command </item>
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>
    """
    
    # Obtain the the files with the bash history data
    dataDir = os.path.join(userName, 'home', 'teleco')
    historyFiles = glob.glob(os.path.join(dataDir, '.bash_history') + 
                             '_[0-9]*_[0-9]*')
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
            # Detect timestamp
            if line.startswith('#'):
                stamp = datetime.datetime.fromtimestamp(float(line[1:]))
                continue

            # Find to which file this session belongs
            index = locateEventInSession(sessions, stamp)
            
            # Create the appropriate event and insert it in the proper bucket of
            # the sessions list.
            counter = counter + 1
            person = PLACamOutput.createPerson(os.path.basename(userName))
            profile = PLACamOutput.createPersonProfile('vmwork', person)
            
            item = PLACamOutput.createItem(None, text = line[:-1])
            itemV = PLACamOutput.createItemVersion(item)
            
            entity = PLACamOutput.createEntity(personProfile = profile,
                                               application = 'bash',
                                               itemVersion = itemV)
            
            context = PLACamOutput.createContext(session = sessions[index][3])

            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.OtherCommand, stamp,
                entityList = [entity], contextList = [context])

            # Add the event to the session data structure
            sessions[index][2][event.get('id')] = event
        
        dbg('  Added ' + str(counter) + ' new commands.')
        dataFile.close()

def toolProcessGcc(userName, sessions):
    """
    Given the events logged by the compiler, create the appropriate event
    
    <event type='Gcc' datetime=DATE BEGIN>
      <entity>
        <application>gcc-output</application>
        <personProfile id="vmwork" personId="REF TO USER"/>
        <item id="??">
      </entity>
      <entity>
        <application>gcc-error</application>
        <personProfile id="vmwork" personId="REF TO USER"/>
        <item id="??">
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <event type="gccmsg" datetime=DATE BEGIN>
      <entity>
        <application>gcc</application>
        <item>TEXT</item>
      </entity>
      <context>
        <session id="GCC EVENT ID"/>
      </context>
    </event>
    <item id="a">
    All the text in the command output
    </item>
    <item id="b">
    All the text in the command error
    </item>
    <item id="msg>
     One message by the compiler
    </item>
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
                # Create the different event elements
                person = PLACamOutput.createPerson(os.path.basename(userName))
                profile = PLACamOutput.createPersonProfile('vmwork', person)

                # Create the entity stating that the command executed
                commandItem = PLACamOutput.createItemVersion(None, 
                                                             text = command)
                
                entities = [PLACamOutput.createEntity(personProfile = profile,
                                                      application = 'gcc',
                                                      itemVersion = commandItem)]

                # Create the two possible entities with output and error msgs
                if errorText != '':
                    errorItem = PLACamOutput.createItemVersion(None, 
                                                               text = errorText)
                    entities.append(\
                        PLACamOutput.createEntity(personProfile = profile,
                                                  application = 'gcc-error',
                                                  itemVersion = errorItem))
                outputEntity = []
                if outputText != '':
                    outputItem = \
                        PLACamOutput.createItemVersion(None, text = outputText)
                    entities.append(\
                        PLACamOutput.createEntity(personProfile = profile,
                                                  application = 'gcc-output',
                                                  itemVersion = outputItem))

                # The context
                context = PLACamOutput.createContext(session = sessions[index][3])

                # And the event
                event = PLACamOutput.createEvent(PLACamOutput.EventTypes.Gcc, \
                                                     dateEvent, \
                                                     entityList = entities, \
                                                     contextList = [context])
                # And add the event to the session
                sessions[index][2][event.get('id')] = event
                
                # Add the events derived from processing the output/error msgs
                for msgevent in PLAGccMessages.filterGccMsgs(event.get('id'), dateEvent,
                                                            errorText + outputText):
                    sessions[index][2][msgevent.get('id')] = msgevent

                # Reset all flags
                errorText = ''
                outputText = ''
                inError = False
                inOutput = False
                dateEvent = None
                command = None

                continue

            # Regular line
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
    Given the events logged by the debugger, create the appropriate events
    
    <event type='Gdb' datetime=DATE BEGIN>
      <entity>
        <application>gdb</application>
        <personProfile id="vmwork" personId="REF TO USER"/>
      </entity>
      <entity>
        <application>gdb-cmds</application>
        <item id="cmds" />
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <item id="commands">
    All the commands, one per line
    </item>
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
            
            # Beginning of log
            if re.match('^\-BEGIN .+$', line):
                fields = line.split()
                dateEvent = datetime.datetime.strptime(' '.join(fields[1:3]),
                                                       '%Y-%m-%d %H:%M:%S')
                command = ' '.join(fields[3:])

                # Find to which file this session belongs
                index = locateEventInSession(sessions, dateEvent)

                dbg(' Detected begin of GDB session')
                continue

            # End of log
            if re.match('^\-END$', line):
                dbg(' Detected end of GDB session')
                # Create the different event elements
                person = PLACamOutput.createPerson(os.path.basename(userName))
                profile = PLACamOutput.createPersonProfile('vmwork', person)

                # Create the entity stating that the command executed
                commandItem = PLACamOutput.createItemVersion(None, 
                                                             text = command)

                entities = [PLACamOutput.createEntity(personProfile = profile,
                                                      application = 'gdb',
                                                      itemVersion = commandItem)]
                
                gdbcmds = PLACamOutput.createItemVersion(None, text = outputText)
                entities.append(\
                    PLACamOutput.createEntity(personProfile = profile,
                                              application = 'gdb-cmds',
                                              itemVersion = gdbcmds))

                # The context
                context = PLACamOutput.createContext(session = sessions[index][3])

                # And the event
                event = PLACamOutput.createEvent(PLACamOutput.EventTypes.Gdb, \
                                                     dateEvent, \
                                                     entityList = entities, \
                                                     contextList = [context])
                # And add the event to the session
                sessions[index][2][event.get('id')] = event
                
                # Reset values
                dateEvent = None
                command = None
                outputText = ''
                continue

            # Regular line
            outputText += line

def toolProcessValgrind(userName, sessions):
    """
    Given the events logged by valgrind, create the appropriate events
    
    <event type='valgrind' datetime=DATE BEGIN until= DATE END>
      <entity>
        <application>valgrind</application>
        <personProfile id="vmwork" personId="REF TO USER"/>
      </entity>
      <entity>
        <application>gdb</application>
        <item ref="valgrind messages"/>
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>

    <item id="valgrind messages">
    All the commands, one per line
    </item>
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
                # Create the different event elements
                person = PLACamOutput.createPerson(os.path.basename(userName))
                profile = PLACamOutput.createPersonProfile('vmwork', person)

                # Create the entity stating that the command executed
                commandItem = PLACamOutput.createItemVersion(None, 
                                                             text = command)

                entities = [PLACamOutput.createEntity(personProfile = profile,
                                                      application = 'vagrind',
                                                      itemVersion = commandItem)]
                
                valgrindmsgs = PLACamOutput.createItemVersion(None, 
                                                              text = outputText)
                entities.append(\
                    PLACamOutput.createEntity(personProfile = profile,
                                              application = 'valgrind-msgs',
                                              itemVersion = valgrindmsgs))

                # The context
                context = PLACamOutput.createContext(session = sessions[index][3])

                # And the event
                event = PLACamOutput.createEvent(PLACamOutput.EventTypes.Valgrind, \
                                                     dateEvent, \
                                                     entityList = entities, \
                                                     contextList = [context])
                # And add the event to the session
                sessions[index][2][event.get('id')] = event
                
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

    <event id = ??
           datetime = given>
      <entity>
        <application>firefox</application>
        <personProfile id="vmwork"/>
        <item type="firefox-visit">url</item>
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>
    """
    
    # Obtain the the files with the bash history data
    dataDir = os.path.join(userName, 'home', 'teleco', '.pladata', 'tools', 
                           'firefox')
    dataFiles = glob.glob(os.path.join(dataDir, 'firefox') + '_[0-9]*_[0-9]*')
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
            profile = PLACamOutput.createPersonProfile('vmwork', person)
            
            itemElement = PLACamOutput.createItemVersion(None, text = url)
            
            entity = PLACamOutput.createEntity(personProfile = profile,
                                               application = 'bash',
                                               itemVersion = itemElement)
            
            context = PLACamOutput.createContext(session = sessions[index][3])

            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.Firefox, dateEvent,
                entityList = [entity], contextList = [context])

            # Add the event to the session data structure
            sessions[index][2][event.get('id')] = event
        
        dbg('  Added ' + str(counter) + ' new commands.')
        dataFile.close()

def toolProcessLog(prefix, userName, sessions):
    """
    Given an event logged as:

    status DATE BEGIN DATE END COMMAND 
    Example:
    0 2010-06-29 13:12:01 2010-06-29 13:12:21 'command'
    
    Create the structure

    <event type='prefix' datetime=DATE BEGIN until=DATE END>
      <entity>
        <application>prefix</application>
        <personProfile id="vmwork" personId="REF TO USER"/>
      </entity>
      <context>
        <session id="session Id"/>
      </context>
    </event>
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
            person = PLACamOutput.createPerson(os.path.basename(userName))
            profile = PLACamOutput.createPersonProfile('vmwork', person)
            
            itemV = PLACamOutput.createItemVersion(None, 
                                                   text = ' '.join(fields[5:-1]))
            
            entity = PLACamOutput.createEntity(personProfile = profile,
                                               application = prefix,
                                               itemVersion = itemV)
            
            context = PLACamOutput.createContext(
                session = sessions[index][3])
            
            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.OtherCommand, dateBegin,
                until = dateEnd,
                entityList = [entity], contextList = [context])
            
            # Add the event to the session data structure
            sessions[index][2][event.get('id')] = event
        
        dbg('  Added ' + str(counter) + ' new commands.')
        dataFile.close()

def parseLastFile(sessionLines, userDir):
    """
    Given a set of lines from the last command parse its content and creates as
    many session files at the same level as detected.

    Return a list of tuples with the following structure:

    (filename, [date from, date two], Dict[id] = element, session element)

    The idea is to parse these lines, populate the empty set to contain all the
    events to be added to the file, and add them in one shot (open, parse,
    write, close) in a single call afterwards.
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

        # Session events containing session begin, session end
        person = PLACamOutput.createPerson(os.path.basename(userDir))
        profile = PLACamOutput.createPersonProfile('vmwork', 
                                                   person)
        entity = PLACamOutput.createEntity(personProfile = profile)

        sessionElement = \
            PLACamOutput.createSession(getSessionIdFromName(sessionDataFileName),
                                       sessionDates[0],
                                       sessionDates[1])
        
        context = PLACamOutput.createContext(session = sessionElement)

        eventStart = PLACamOutput.createEvent(
            PLACamOutput.EventTypes.SessionStart, sessionDates[0],
            entityList = [entity],
            contextList = [context])

        eventEnd = PLACamOutput.createEvent(
            PLACamOutput.EventTypes.SessionEnd, sessionDates[1],
            contextList = [context])

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

    return 'SessionStart_' + name.split('_')[0]


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
