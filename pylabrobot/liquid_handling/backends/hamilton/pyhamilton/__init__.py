"""
Pyhamilton
"""
import os
import shutil
from os.path import dirname, join, abspath
PACKAGE_PATH = abspath(dirname(__file__))
from .interface import *
from .oemerr import *

this_file_dir = os.path.dirname(os.path.abspath(__file__))
PACKAGE_DIR = os.path.abspath(os.path.join(this_file_dir))

