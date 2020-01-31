import os
import sys
save = sys.stdout

def off():
  sys.stdout = open( os.devnull, 'w' )

def on():
  sys.stdout = save
