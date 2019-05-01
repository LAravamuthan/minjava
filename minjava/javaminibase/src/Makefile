#This Makefile is used to create assignments.

#the following macros should be updated according to
#the assignment to be generated

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class
JDKPATH = /usr/lib/jvm/java-8-oracle
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap btree iterator index
##############  update the above for each assignment in making

ASSIGN=/home/kira/D_Drive/CSE_510_DBMSI/DBMSI/project/minjava/javaminibase/src
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

IMAGELINK=$(PACKAGEINDEX)/images
PACKAGEINDEX=$(ASSIGN)/javadoc

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files.  Apply to most assignments.
db: 
	make -C global
	make -C chainexception
	make -C btree
	make -C intervaltree
	make -C index
	make -C iterator
	make -C bufmgr
	make -C diskmgr
	make -C heap


	
doc:
	$(JAVADOC) $(DOCFILES)

test: 
	 cd tests; make xmltest

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE

