How to run the server natively on Windows Using python. 
=======================================================

The soap server runs in python (2.7), and requires some external
libraries. The following lists all the istallations required to get
the server up and running natively on Windows.

First, you must install all the appropriate libraries, as described below.
You will need to command prompt to do this.

To open the command prommpt, click the 'start button', type 'cmd' and hit enter.

Running the server and client
-----------------------------
Open two command promots, and in both, navigate to the directory containing both the server and test python files.

To run the server, in the first command prompt, type "python server.py"
To run the test client, in the second command promot, type "python test.py"

Creating the windows executable using cx_Freeze (deprecated)
------------------------------------------------------------
To create a windows executable, in the same directory as the server and test,
type ``python setup.py build`` This will create or update the
build/exe.win23-2.7 directory.

Thei server executable can be run by double-clicking on the server.bat file in
the server directory from a windows explorer window.

Building windows execuatable using Pyinstaller
----------------------------------------------

Make sure you have installed pip (:ref:`pip`).

Install pyinstaller (if you haven't done so already)::

    pip install pyinstaller

Build the server::

    pyinstaller server.spec

The server can now be run in the commandline::

    dist\server\server.exe

Installing Python
-----------------
Several installations of python are available.
The one we recommend is PythonXY_

.. _PythonXY: http://code.google.com/p/pythonxy/wiki/Downloads

Some of the libraries may require you to install them from source. This means
running a setup python file in a zip file you have downloaded. To ensure you can unzip the files correctly, make sure you have an unzip program installed like `PeaZip <http://peazip.sourceforge.net/>`_

For the moment, we recommend installing the 32 bit version of python, even
if your operating system is 64 bit. This ensures maximum compatability, particularly
if you need to build the server as an exe.

.. _pip:

Additional Libraries
--------------------
First off, you need to install Pip. This is a tool which downloads and installs python libraries for you.

- Navigate to pythonlibs_
- Search for Pip
- Download and install this exe: pip-2.4.1.win32-py2.7.exe

.. _pythonlibs: http://www.lfd.uci.edu/~gohlke/pythonlibs/

To use Pip, open the command prompt.

To check if Pip is working, type 'pip'. It should print out some documentation.


Spyne
******
Spyne is the SOAP server library.

*edit*: The spyne library has a small bug, which needs to be fixed in order
to work with plugins. The fixed version is available here. To install it, you
need to download the zip file on the right hand side of this page: https://github.com/knoxsp/spyne

Open up this zip file somewhere and navigate to this directory in the command line.
Then type `python setup.py install` or read the Read me.

If this does not work and you want to install the base version of spyne, do the
following:

In the command prompt, type:

`pip install spyne`

Should this fail, you must install it from source. Don't worry. It's easy.
The source is available at: www.spyne.io

- Click on `Download` at the top.
- This will download a file called something like: spyne-2.10.9.tar.gz
- Once done, unzip the file.
  (If you are using peazip, right-click on the downloaded spyne file and click PeaZIp > ExtractHere.
  This will create a new file to which you can navigate. Perform the same procedure on the tar 
  file in the new directory. This will again create a new directory containing the source.)
- Navigate to the file containing the source. Something like:

- `cd Downloads\spyne-2.10.9.tar\dist\spyne-2.10.9\spyne`

- `python setup.py install`

The screen should fill with logging and after about 5 seconds, you are done.

Suds
****
Suds is the soap client library. This is not needed for the server to run, but is needed to build the server executable as the test is built into this process.

first try:
`pip install suds`

If this fails, you must install it from source. This process is identical to that of installing spyne.

- Download the SUDS source at: https://fedorahosted.org/releases/s/u/suds/python-suds-0.4.tar.gz

- Once downloaded navigate to the file and unzip it (See above on how to do this).

- Open up a command prompt

- Navigate to the file containing the source. Something like:
- cd Downloads\python-suds-0.4.tar\python-suds-0.4

- Type `python setup.py install`

The screen should fill with logging and after about 5 seconds, you are done.


Bcrypt
*******
Try:
`pip install bcrypt`

If you see some errors referring to 'vcvarsall.bat', then download and install the executable, located here:

https://bitbucket.org/alexandrul/py-bcrypt/downloads/

If the file extention ends with a `.whl`, then do the following:
`pip install wheel`
`wheel install py-bcrypt_my_version.whl`

MySql Connector
***************
Navigate to: http://dev.mysql.com/downloads/connector/python/

Click on 'Download' of the MSI installer appropriate for your windows installation.

SqlAlchemy
**********
Try:
`pip install sqlalchemy`

We use sqlalchemy to provide database connections and manage the database communication.  

zope.sqlalchemy
***************
Try:
`pip install zope.sqlalchemy`

The aim of this package is to unify the plethora of existing packages integrating 
SQLAlchemy with Zope's transaction management. As such it seeks only to provide 
a data manager and makes no attempt to define a zopeish way to configure engines.

Pandas
******
Try:
`pip install pandas`

Pandas allows us to manipulate and store timeseries and arrays in a very efficient and flexible way.

winpath
********
Try:
`pip install winpath`

winpath is a python module that retrieves the names of common Windows folders.

CherryPy
*********
Try:
`pip install cherrypy`

CherryPy allows developers to build web applications in much the same way they would build any other object-oriented Python program.

python-dateutil
****************
Try: 
`pip install python-dateutil`

The dateutil module provides powerful extensions to the standard datetime module.

lxml
****
`pip install lxml`
Lxml is an efficient library for parsing XML content.

pywin32:
********
If you plan on building the server into an exe, you will need this:
http://sourceforge.net/projects/pywin32/
