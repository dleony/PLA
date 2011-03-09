#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
# Based on bash.py script
#
import sys, os

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

dataDir = os.path.join(pla.plaDirectory, 'tools', 'iwatch')
dataFile = os.path.expanduser('~/.iwatchlog')

def main(): 
    """
    Script to simply return the iwatch log file and reset its content
    """
    pass

if __name__ == "__main__":
    main()
