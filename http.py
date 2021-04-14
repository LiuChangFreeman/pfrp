# -*- coding: utf-8 -*-
from __future__ import print_function
import time
import datetime
import logging
import os
import socket
import select

path_current = os.path.split(os.path.realpath(__file__))[0]