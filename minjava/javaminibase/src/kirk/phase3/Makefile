# modify the paths appropriately
# general.asu.edu setting
#JDKPATH = /usr/lib/jvm/java

JDKPATH = /usr
LIBPATH = .:..:../*

CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap btree iterator index

##############  update the above for each assignment in making

## general.asu.edu settings
#ASSIGN_PRE=/mnt/c/Users
#ASSIGN_USER=/Kirk Manegold/Documents/GitHub/CSE510-Phase3
#ASSIGN_SRC=/minjava/javaminibase/src

## Ubuntu on win10 settings
ASSIGN_PRE=/afs/asu.edu/users
ASSIGN_USER=/m/a/n/manegold/cse510/CSE510-Phase
ASSIGN_SRC=/minjava/javaminibase/src

ASSIGN=$(ASSIGN_PRE)$(ASSIGN_USER)$(ASSIGN_SRC)
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

IMAGELINK=$(PACKAGEINDEX)/images
PACKAGEINDEX=$(ASSIGN)/javadoc

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files
db:
	make -C ../global
	make -C ../chainexception
	make -C ../btree
	make -C ../bufmgr
	make -C ../diskmgr
	make -C ../heap
	make -C ../index
	make -C ../iterator

doc:
	$(JAVADOC) $(DOCFILES)

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE
	\rm -f ../*/*.class *~ \#* core $(JARFILES) TRACE

# build XMLDriver
XMLDriver:XMLDriver.java
	$(JAVAC) XMLDriver.java

# make xmlload DB=test.db XML=xml_sample_data.xml
xmldriver: XMLDriver
	$(JAVA) phase3.XMLDriver $(DB) $(XML)

# build XMLoad
XMLLoad:XMLQuery.java
	$(JAVAC) XMLLoad.java

# make xmlload DB=test.db XML=xml_sample_data.xml
xmlload: XMLLoad
	$(JAVA) phase3.XMLLoad $(DB) $(XML)

# build XMLQuery
XMLQuery:XMLQuery.java
	$(JAVAC) XMLQuery.java

# make xmlload DB=test.db Q=Query01.txt
xmlquery: XMLQuery
	$(JAVA) phase3.XMLQuery $(DB) $(Q)

qtest:
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query01.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query02.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query03.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query04.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query05.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query06.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query07.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query08.txt
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query09.txt

qt:
	$(JAVA) phase3.XMLQuery XML_sample_data.db Query06.txt

