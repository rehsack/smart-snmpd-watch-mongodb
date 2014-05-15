Smart SNMP Daemon Plugin for watching MongoDB
=============================================

http://www.smart-snmpd.org/ (once we do it ...)

Description
-----------

It's a tiny plugin for smart-snmpd which regulary watches mongodb and allows
passive checks at client site (eg. Nagios).


License
-------

The Smart SNMP Daemon MongoDB watcher is distributed under the Apache 2.0 License.
Prerequisites may have different licenses, but according to
http://www.opensource.org/licenses/alphabetical their licenses are
compatible.


Building and Installation
-------------------------

Place the wanted mongodb-client into src/mongo and invoke make MONGO\_PW=yourmongodbpw


Running
-------

See OPERATION and EXTENDING documents distributed with smart-snmpd package.


Platform specific notes
-----------------------

Although if not explicitely developed for Unices only, it's not expected
that the Smart SNMP Daemon runs without modifications on Windows.  The
Windows support of libstatgrab needs to be massively improved to reach
this, too.

Similar situation for VMS.


Problems?
---------

Please let us know when you have any problems via users mailing list at
smart-snmpd-users@smart-snmpd.org.

To help you make the best use of the smart-snmpd-users mailing list, and
any other lists or forums you may use, I strongly recommend that you read
"How To Ask Questions The Smart Way" by Eric Raymond:
http://www.catb.org/~esr/faqs/smart-questions.html.

If you think you've found a bug then please also read "How to Report
Bugs Effectively" by Simon Tatham:
http://www.chiark.greenend.org.uk/~sgtatham/bugs.html.

