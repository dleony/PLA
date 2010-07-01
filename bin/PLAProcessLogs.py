#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, sys, tarfile, getopt, pysvn, re, shutil, subprocess, datetime, glob

from lxml import etree

import PLACamOutput

# Directory in user HOME containing the instrumented commands
plaDirectory = os.path.expanduser('~/.pladata')

# Default options
_debug = False
_givenUserSet = set([])
_givenToolSet = set(['bash', 'last', 'gcc', 'gdb', 'valgrind', 'kate', \
                         'kdevelop'])

# Regular expression to filter log files
_logFileFilter = re.compile('.+\.tgz')

def main():
    """

    <script> [-d] [-u username] [-t toolname] svnDirectory

    Process a directory under subversion control containing log files.

    -d    Turns on debugging.

    -u username Select only this username (it might appear several times)
 
    -t toolname Select only this tool

    The svnDirectory should be .pladata where the *.tgz files are stored.

    """

    global _debug
    global _logFileFilter
    global _givenUserSet
    global _givenToolSet

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
        elif optstr == '-t':
            _givenToolSet.add(value)

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
            writeSessionFile(userName, sessions)

        # Dump the addittional elements stored in the Cam Output
        PLACamOutput.writeElements()

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
    sessionDir = os.path.join(userDir, 'sessions')

    # Create the "Sessions" directory to store the sessions
    if not os.path.exists(sessionDir):
        dbg(' Created ' + sessionDir)
        os.makedirs(sessionDir)

    # Obtain the the filees with the "last" data
    lastFiles = glob.glob(os.path.join(userDir, 'home', 'teleco', '.lastrc') + 
                          '_[0-9]*_[0-9]*')
    lastFiles.sort()
                     
    # Create a duplicate of the first file which is the output of last directly
    mainLastFile = os.path.join(sessionDir, 'last')
    shutil.copy(lastFiles.pop(0), mainLastFile)

    # Apply the remaining files as patches
    lastFiles.sort()
    for file in lastFiles:
        try:
            command = ['patch', '-s', mainLastFile, file]
            dbg(' Patching with ' + ' '.join(command))
            patchCmd = subprocess.Popen(command)
        except:
            print('Failed to apply patch ' + file)
            sys.exit(1)
            
        # Wait for the patch to be applied
        patchCmd.wait()
            
        if patchCmd.returncode != 0:
            print('Patch returned status ' + patchCmd.returncode)
            sys.exit(1)

    # LAST file produced, proceed to parse and create session files
    return parseLastFile(mainLastFile, userDir)

def toolProcess(userName, sessions):
    """
    Process the files for each tool
    """

    toolProcessBash(userName, sessions)

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
        dataFile = open(historyFileName, 'r')

        stamp = datetime.datetime.fromtimestamp(os.stat(historyFileName).st_mtime)
        counter = 0
        # Loop over all the lines
        for line in dataFile:
            # Detect timestamp
            if line.startswith('#'):
                stamp = datetime.datetime.fromtimestamp(float(line[1:]))
                continue

            # Find to which file this session belongs
            index = next((i for i in xrange(len(sessions)) \
                              if sessions[i][1][0] < stamp and \
                              (sessions[i][1][1] == None or \
                                   sessions[i][1][1] > stamp)), None)
            
            # If the command does not belong to any session, dumpt it
            if index == None:
                print 'Discarding command ' + line[:-1]
                continue

            counter = counter + 1
            person = PLACamOutput.createPerson(os.path.basename(userName))
            profile = PLACamOutput.createPersonProfile('vmwork', person)
            
            item = PLACamOutput.createItem(None, text = line[:-1])
            itemV = PLACamOutput.createItemVersion(item)
            
            entity = PLACamOutput.createEntity(personProfile = profile,
                                               application = 'bash',
                                               itemVersion = itemV)
            
            context = PLACamOutput.createContext(
                session = sessions[index][3])

            event = PLACamOutput.createEvent(
                PLACamOutput.EventTypes.OtherCommand, stamp,
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
        dataFile = open(dataFileName, 'r')


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
            index = next((i for i in xrange(len(sessions)) \
                              if sessions[i][1][0] < dateBegin and \
                              (sessions[i][1][1] == None or \
                                   sessions[i][1][1] > dateEnd)), None)
            
            # If the session is not detected discard command
            if index == None:
                print 'Discarding command ' + line[:-1]
                continue

            counter = counter + 1
            person = PLACamOutput.createPerson(os.path.basename(userName))
            profile = PLACamOutput.createPersonProfile('vmwork', person)
            
            item = PLACamOutput.createItem(None, 
                                           text = ' '.join(fields[5:-1]))
            itemV = PLACamOutput.createItemVersion(item)
            
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

def parseLastFile(lastFile, userDir):
    """
    Given the file containing the dump by the last command parse its content and
    creates as many session files at the same level as detected.

    Return a list of tuples with the following structure:

    (filename, [date from, date two], Dict[id] = element)

    The idea is to parse the log files, populate the empty set to contain all
    the events to be added to the file, and add them in one shot (open, parse,
    write, close)
    """
    dbg(' Parsing ' + lastFile)
    
    # Open and loop over each line in the data file
    dataIn = open(lastFile, 'r')
    dates = []
    for line in dataIn:

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
        if sessionDates[1] == None and (line.find('still logged') != -1):
            continue

        # Skip the sessions that have an empty duration
        if sessionDates[0] == sessionDates[1]:
            dbg(' Skipping empty session')
            continue

        # Decide the name of the session file
        nameStart = sessionDates[0].strftime('%Y%m%d%H%M%S') 
        nameEnd = 'StillLogged.xml'
        if sessionDates[1] != None:
            nameEnd = sessionDates[1].strftime('%Y%m%d%H%M%S' + '.xml')

        # Create the data structure with the basic session information
        sessionDataFileName = os.path.join(os.path.dirname(lastFile),
                                           nameStart + '_' + nameEnd)

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
        dates.append((sessionDataFileName, sessionDates, 
                      PLACamOutput.insertElements({},
                                                  [eventStart, 
                                                   eventEnd]), sessionElement))
    dataIn.close()

    return dates

def writeSessionFile(userName, sessions):
    """
    For each session in the list, create a file for those with non empty data
    """
    
    dbg('Writing session files')

    # Remove those files that terminate in StillLogged because they contain
    # temporary data
    toRemove = glob.glob(os.path.join(userName, 'sessions', '*StillLogged'))
    if toRemove != []:
        dbg('Removing ' + ', '.join(toRemove))
        map(os.remove, toRemove)

    # Loop over all the session
    for session in sessions:

        dbg('Write session ' + session[0] + ' with ' + \
                str(len(session[2])) + ' events')

        NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
        TREE = '{%s}' % NS
        NSMAP = {None: NS}
        root = etree.Element(TREE + 'rdf', nsmap = {None: NS})
        root.append(etree.Comment('File automatically generated by PLA'))

        # Add each event to the root
        eventList = session[2].values()
        eventList.sort(key = lambda x: x.get('datetime'))
        for elem in eventList:
            root.append(elem)

        toWrite = etree.ElementTree(root)
        toWrite.write(session[0], encoding='utf-8', xml_declaration = True, 
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

def getSessionIdFromName(name):
    """
    Given the name of a file containing a session, guess the Id of that session
    """

    return 'SessionStart_' + name.split('_')[0]

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
