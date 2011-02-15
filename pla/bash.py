#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import sys, os

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

dataDir = os.path.join(pla.plaDirectory, 'tools', 'bash')
dataFile = os.path.expanduser('~/.bash_history')

def main(): 
    """
    Script to simply return the history file and reset its content
    """
    pass

if __name__ == "__main__":
    main()
