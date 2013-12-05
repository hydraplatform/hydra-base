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

Creating the windows executable
-------------------------------
To create a windows executable, in the same directory as the server and test, type "python setup.py install" This will create or update the dist/exe.win23-2.7 directory.

Thei server executable can be run by double-clicking on the server.bat file in the server directory from a windows explorer window.

Installing Python
------
Several installations of python are available.
The one we recommend is PythonXY:
http://code.google.com/p/pythonxy/wiki/Downloads

Some of the libraries may require you to install them from source. This means
running a setup python file in a zip file you have downloaded. To ensure you can unzip the files correctly, make sure you have an unzip program installed like:

PeaZip: http://peazip.sourceforge.net/


Additional Libraries
--------------------
First off, you need to install Pip. This is a tool which downloads and installs python libraries for you.

- Navigate to: http://www.lfd.uci.edu/~gohlke/pythonlibs/
- Search for Pip
- Download and install this exe: pip-2.4.1.win32-py2.7.exe

To use Pip, open the command prompt.

To check if Pip is working, type 'pip'. It should print out some documentation.


Spyne
******
Spyne is the SOAP server library.

In the command prompt, type:

pip install spyne


Should this fail, you must install it from source. Don't worry. It's easy.
The source is available at: www.spyne.io

- Click on 'Download' at the top.
- This will download a file called something like: spyne-2.10.9.tar.gz
- Once done, unzip the file.
(If you are using peazip, right-click on the downloaded spyne file and click PeaZIp > ExtractHere. This will create a new file to which you can navigate. Perform the same procedure on the tar file in the new directory. This will again create a new directory containing the source.)

- Navigate to the file containing the source. Something like:
- cd Downloads\spyne-2.10.9.tar\dist\spyne-2.10.9\spyne

- Type python setup.py install

The screen should fill with logging and after about 5 seconds, you are done.

Suds
****
Suds is the soap client library. This is not needed for the server to run, but is needed to build the server executable as the test is built into this process.

first try:
pip install suds

If this fails, you must install it from source. This process is identical to that of installing spyne.

- Download the SUDS source at: https://fedorahosted.org/releases/s/u/suds/python-suds-0.4.tar.gz

- Once downloaded navigate to the file and unzip it.

  If you are using peazip, right-click on the downloaded spyne file and click PeaZIp > ExtractHere. This will create a new file to which you can navigate. Perform the same procedure on the tar file in the new directory. This will again create a new directory containing the source.

- Open up a command prompt

- Navigate to the file containing the source. Something like:
- cd Downloads\python-suds-0.4.tar\python-suds-0.4

- Type python setup.py install

The screen should fill with logging and after about 5 seconds, you are done.


Bcrypt
*******
Try:
pip install bcrypt.

If you see some errors referring to 'vcvarsall.bat', then download and install the executable, located here:

https://bitbucket.org/alexandrul/py-bcrypt/downloads/py-bcrypt-0.3.post1.win32-py2.7.exe

MySql Connector
***************
Navigate to: http://dev.mysql.com/downloads/connector/python/

Click on 'Download' of the MSI installer appropriate for your windows installation.
