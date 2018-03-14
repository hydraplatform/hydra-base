#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#

import logging
import logging.config
from . import config
import os
import ctypes
import inspect

def init(level=None):
 #   if level is None:
 #       level = config.get('DEFAULT', 'log_level')

 #   if os.name == "nt":
 #       logging.addLevelName( logging.INFO, logging.getLevelName(logging.INFO))
 #       logging.addLevelName( logging.DEBUG, logging.getLevelName(logging.DEBUG))
 #       logging.addLevelName( logging.WARNING, logging.getLevelName(logging.WARNING))
 #       logging.addLevelName( logging.ERROR, logging.getLevelName(logging.ERROR))
 #       logging.addLevelName( logging.CRITICAL, logging.getLevelName(logging.CRITICAL))
 #       logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=level)
 #       return

 #   if level is None:
 #       level = config.get('DEFAULT', 'log_level')


 #   logging.addLevelName( logging.INFO, "\033[0;m%s\033[0;m" % logging.getLevelName(logging.INFO))
 #   logging.addLevelName( logging.DEBUG, "\033[0;32m%s\033[0;32m" % logging.getLevelName(logging.DEBUG))
 #   logging.addLevelName( logging.WARNING, "\033[0;33m%s\033[0;33m" % logging.getLevelName(logging.WARNING))
 #   logging.addLevelName( logging.ERROR, "\033[0;31m%s\033[0;31m" % logging.getLevelName(logging.ERROR))
 #   logging.addLevelName( logging.CRITICAL, "\033[0;35m%s\033[0;35m" % logging.getLevelName(logging.CRITICAL))

 #   logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s\033[0m', level=level)
    #Log to a file with the same name as that calling 'import logging'
    #ex: server.py logs to server.log, ImportCSV.py logs to ImportCSV.log
    #All log files should be located in the same location.

    try:
        calling_file = inspect.stack()[-1][0].f_globals['__file__']
        calling_file = os.path.split(calling_file)[1]

        log_file = "%s.log" % calling_file.split('.')[0]
        log_base_path = config.get('logging_conf', 'log_file_dir', '.')

        if not os.path.isdir(log_base_path):
            os.makedirs(log_base_path)

        log_loc = os.path.expanduser(os.path.join(log_base_path, log_file))

    except:
        log_file = None
        log_loc  = None
        log_base_path = None

    use_default = False
    try:
        config_file = os.path.expanduser(config.get('logging_conf', 'log_config_path', '.'))
        #check the config file exists...
        if os.path.isfile(config_file) and log_base_path is not None:
            logging.config.fileConfig(config_file)
            logger = logging.getLogger()
            handler = logging.FileHandler(os.path.join(log_base_path, log_file),"a")
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(process)s %(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            use_default=False
            logging.debug("Using logging config at %s", config_file)
        else:
            logging.debug("No logging config file found. Using default settings.")
            use_default = True
    except Exception as e:
        logging.critical("Error finding logging conf file: %s", e)
        use_default = True

    if use_default is True:
        logging_conf_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'hydraFormatter': {
                'format': '%(process)s %(asctime)s - %(levelname)s - %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level':'DEBUG',
                'class':'hydra_base.hydra_logging.ColorizingStreamHandler',
                'formatter' : 'hydraFormatter',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'INFO',
                'propagate': True
            },
        }
        }

        if log_loc is not None:
            logging_conf_dict['handlers']['file'] = {
                'level':'DEBUG',
                'class':'logging.FileHandler',
                'formatter' : 'hydraFormatter',
                'filename'  : log_loc,
                'mode'      : 'a',
            }

        logging.config.dictConfig(logging_conf_dict)

def shutdown():
	logging.shutdown()

class ColorizingStreamHandler(logging.StreamHandler):
#
#Thanks to Vinay Sajip
#(http://plumberjack.blogspot.co.uk/2010/12/colorizing-logging-output-in-terminals.html)
##

    @property
    def is_tty(self):
        isatty = getattr(self.stream, 'isatty', None)
        return isatty and isatty()

    def emit(self, record):
        try:
            message = self.format(record)
            stream = self.stream
            if not self.is_tty:
                stream.write("%s : %s"%(os.getpid(),message))
            else:
                self.output_colorized(message)
            stream.write(getattr(self, 'terminator', '\n'))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def format(self, record):
        message = super(logging.StreamHandler, self).format(record)

        if self.is_tty:
            # Don't colorize any traceback
            parts = message.split('\n', 1)
            parts[0] = self.colorize(parts[0], record)
            message = '\n'.join(parts)
        return message

    # color names to indices
    color_map = {
        'black': 0,
        'red': 1,
        'green': 2,
        'yellow': 3,
        'blue': 4,
        'magenta': 5,
        'cyan': 6,
        'white': 7,
    }

    #levels to (background, foreground, bold/intense)
    if os.name == 'nt':
        level_map = {
            logging.DEBUG: (None, 'blue', True),
            logging.INFO: (None, 'white', False),
            logging.WARNING: (None, 'yellow', True),
            logging.ERROR: (None, 'red', True),
            logging.CRITICAL: ('red', 'white', True),
        }
    else:
        level_map = {
            logging.DEBUG: (None, 'cyan', False),
            logging.INFO: (None, 'white', False),
            logging.WARNING: (None, 'yellow', False),
            logging.ERROR: (None, 'red', False),
            logging.CRITICAL: ('red', 'white', True),
        }
    csi = '\x1b['
    reset = '\x1b[0m'

    def colorize(self, message, record):
        if record.levelno in self.level_map:
            bg, fg, bold = self.level_map[record.levelno]
            params = []
            if bg in self.color_map:
                params.append(str(self.color_map[bg] + 40))
            if fg in self.color_map:
                params.append(str(self.color_map[fg] + 30))
            if bold:
                params.append('1')
            if params:
                message = ''.join((self.csi, ';'.join(params),
                                   'm', message, self.reset))
        return message

    if os.name != 'nt':
        def output_colorized(self, message):
            self.stream.write(message)
    else:
        import re
        ansi_esc = re.compile(r'\x1b\[((\d+)(;(\d+))*)m')

        nt_color_map = {
            0: 0x00,    # black
            1: 0x04,    # red
            2: 0x02,    # green
            3: 0x06,    # yellow
            4: 0x01,    # blue
            5: 0x05,    # magenta
            6: 0x03,    # cyan
            7: 0x07,    # white
        }

        def output_colorized(self, message):
            parts = self.ansi_esc.split(message)
            write = self.stream.write
            h = None
            fd = getattr(self.stream, 'fileno', None)
            if fd is not None:
                fd = fd()
                if fd in (1, 2): # stdout or stderr
                    h = ctypes.windll.kernel32.GetStdHandle(-10 - fd)
            while parts:
                text = parts.pop(0)
                if text:
                    write(text)
                if len(parts) > 4:
                    params = parts[0]
                    parts = parts[4:]
                    if h is not None:
                        params = [int(p) for p in params.split(';')]
                        color = 0
                        for p in params:
                            if 40 <= p <= 47:
                                color |= self.nt_color_map[p - 40] << 4
                            elif 30 <= p <= 37:
                                color |= self.nt_color_map[p - 30]
                            elif p == 1:
                                color |= 0x08 # foreground intensity on
                            elif p == 0: # reset to default color
                                color = 0x07
                            else:
                                pass # error condition ignored
                        ctypes.windll.kernel32.SetConsoleTextAttribute(h, color)
