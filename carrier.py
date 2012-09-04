#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import errno
import sys
import optparse
import string
import smtplib
from datetime import date
import subprocess
from thread_pool import ThreadPool
from application_lock import ApplicationLock
import cloudfiles
import zipfile
import hashlib


"""
Email alert using GMail
"""
EMAIL		= ''
SUBJECT		= 'Carrier Report ' + str(date.today())
FROM_EMAIL	= '@gmail.com'
USERNAME	= ''
PASSWORD	= ''


"""
END configuration options
"""


class logBuffer:

    def __init__(self):
        self.content = []

    def write(self, string):
        self.content.append(string)


emaillog = logBuffer()


"""
  Create MD5 checksum

  @param	_file		Path + filename of file to be processed.
"""
def md5Checksum(_file):	
    md5 = hashlib.md5()
    file = open(_file, "rb")
    while True:
        data = file.read(8192)
        if not data:
            break
        md5.update(data)
    file.close()
    return md5.hexdigest()


""" 
  Parse variables passed to the program 
"""
def parseOptions():
    usage = 'usage: %prog [options]'
    description = 'A work in progress.'
    parser = optparse.OptionParser(usage=usage, description=description)
    parser.add_option(
        '-i',
        '--ignore',
        action='store',
        dest='ignore',
        default=['_broken'],
        help='List of directories that should be ignored.'
        )
    parser.add_option(
        '-p',
        '--patron',
        action='store',
        dest='patron',
        default=['RIP_Patron', 'RIP_to_TIFF'],
        help='List of directories that should be sent to patron_bundle process.'
        )
    parser.add_option(
        '-z',
        '--patronzip',
        action='store',
        dest='patronzip',
        default='/tmp',
        help='Location to store patron zip files.  default: /tmp'
        )
    parser.add_option(  
        '-s',
        '--source',
        action='store',
        dest='source',
        default='',
        help='Source directory to be processed: Absolute or relative path.'
        )
    parser.add_option(
        '-t',
        '--threads',
        action='store',
        dest='threads',
        default=4,
        help='Set number of threads to execute at once.   default  = 4'
        )
    return parser.parse_args()


"""
  Send an email using the appropriate global configuration

  @param      _message         Message to be sent via email
"""
def sendEmail(_message):
    body = string.join(('From: %s' % FROM_EMAIL, 'To: %s' % EMAIL,
                        'Subject: %s' % SUBJECT, '', _message), '\r\n')
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(USERNAME,PASSWORD)

    # server.set_debuglevel(1)

    server.sendmail(FROM_EMAIL, EMAIL, body)
    server.quit()


"""
  Iterate through source directory tree and execute sub routines based on what is found

  @param	_source		Source directory (can include subdirectories)
  @param	_ignore		Directories to ignore when processing
  @param	_patron		Directories to be sent to patron_bundle function
  @param	_patron_zip	Location to store patron zip files
  @param	_threads	Number of threads to launch
"""
def iterate(_source,_ignore,_patron,_patron_zip,_threads):
    print 'Descend into ' + _source
 
    t = ThreadPool(_threads)

    for (root, dirs, files) in os.walk(_source):
	t.add_task(patron_bundle,_patron,_patron_zip,root)
    t.await_completion()


"""
  Make directory if it doesn't exist

  @param        _dir    Directory to be tested and created
"""
def makeDir(_dir):
    try:
        os.makedirs(_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
            print >>emaillog, 'Unable to create ' + _dir
            sendEmail(''.join(emaillog.content))
            sys.exit()

 
"""
  If defined as a patron directory, zip up all secondary level batch files and save.

  @param        _patron         Directories to be sent to patron_bundle function 
  @param	_patron_zip	Where to save zip files
  @param	_root		Path to test
"""
def patron_bundle(_patron,_patron_zip,_root):
    if not os.path.isdir(_patron_zip):
        print _patron_zip + ' does not exist.  Creating...'
        makeDir(_patron_zip)

    for patdir in _patron:
        # don't load parent directories
        if patdir + '/' in _root:
	    batch =  os.path.split(_root)[-1]
	    print batch
	    zf = zipfile.ZipFile( os.path.join(_patron_zip,batch) + ".zip", "w")
            for root,dirs,files in os.walk(_root):
                for file in files:
                  # Only grab *.tifs
		    if file.endswith('.tif'):
		      # Write package as batch/file
			zf.write(os.path.join(_root, file),os.path.join(batch,file))
		      # Create batch/file.md5 checksum document
	                zf.writestr(os.path.join(batch,"md5",file) + ".md5", md5Checksum(os.path.join(_root, file)))
	    zf.close()


def main():
  
  # Fetch those options
  
    (options, args) = parseOptions()

  # Test source exists
   
    if os.path.isdir(options.source):

      # Iterate through options.source

        iterate(options.source,options.ignore,options.patron,options.patronzip,int(options.threads)) 

      # Test if errors exist, email if true

        if emaillog.content:
            sendEmail(''.join(emaillog.content))


if __name__ == '__main__':
    applock = ApplicationLock ('/tmp/carrier.lock')
    if (applock.lock()):
        main()
        applock.unlock()
    else:
        print ('Unable to obtain lock, exiting')
