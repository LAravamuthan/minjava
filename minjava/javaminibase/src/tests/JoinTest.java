package tests;
//originally from : joins.C

import btree.BTreeFile;
import btree.IntegerKey;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
*/





//Creating a new class to test interval joins. 
class IntervalTest{
  public IntervalType interval;		//an interval type
  public String id;

  public IntervalTest(IntervalType _interval, String _id, int _level){
   interval = _interval;
   id = _id;
 }
}

class JoinsDriverJT implements GlobalConst {
  
  private boolean OK = true;
  private boolean FAIL = false;
  private Vector sailors;
  private Vector boats;
  private Vector reserves;
  
/* New constructor for interval joins testing */
  private Vector intervals;

  /* 
     Constructor
  */
  public JoinsDriverJT() {
    
    //build Sailor, Boats, Reserves table
    sailors  = new Vector();
    boats    = new Vector();
    reserves = new Vector();
    intervals = new Vector();

    IntervalType A = new IntervalType();
    A.assign(1,10,1);
    IntervalType B = new IntervalType();
    B.assign(2,7,2);
    IntervalType C = new IntervalType();
    C.assign(3,4,3);
    IntervalType D = new IntervalType();
    D.assign(5,6,3);
    IntervalType E = new IntervalType();
    E.assign(8,9,2);

    intervals.addElement(A);
    intervals.addElement(B);
    intervals.addElement(C);
    intervals.addElement(D);
    intervals.addElement(E);


    sailors.addElement(new Sailor(53, "Bob Holloway",       9, 53.6));
    sailors.addElement(new Sailor(54, "Susan Horowitz",     1, 34.2));
    sailors.addElement(new Sailor(57, "Yannis Ioannidis",   8, 40.2));
    sailors.addElement(new Sailor(59, "Deborah Joseph",    10, 39.8));
    sailors.addElement(new Sailor(61, "Landwebber",         8, 56.7));
    sailors.addElement(new Sailor(63, "James Larus",        9, 30.3));
    sailors.addElement(new Sailor(64, "Barton Miller",      5, 43.7));
    sailors.addElement(new Sailor(67, "David Parter",       1, 99.9));   
    sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.addElement(new Sailor(71, "Guri Sohi",         10, 42.1));
    sailors.addElement(new Sailor(73, "Prasoon Tiwari",     8, 39.2));
    sailors.addElement(new Sailor(39, "Anne Condon",        3, 30.3));
    sailors.addElement(new Sailor(47, "Charles Fischer",    6, 46.3));
    sailors.addElement(new Sailor(49, "James Goodman",      4, 50.3));
    sailors.addElement(new Sailor(50, "Mark Hill",          5, 35.2));
    sailors.addElement(new Sailor(75, "Mary Vernon",        7, 43.1));
    sailors.addElement(new Sailor(79, "David Wood",         3, 39.2));
    sailors.addElement(new Sailor(84, "Mark Smucker",       9, 25.3));
    sailors.addElement(new Sailor(87, "Martin Reames",     10, 24.1));
    sailors.addElement(new Sailor(10, "Mike Carey",         9, 40.3));
    sailors.addElement(new Sailor(21, "David Dewitt",      10, 47.2));
    sailors.addElement(new Sailor(29, "Tom Reps",           7, 39.1));
    sailors.addElement(new Sailor(31, "Jeff Naughton",      5, 35.0));
    sailors.addElement(new Sailor(35, "Miron Livny",        7, 37.6));
    sailors.addElement(new Sailor(37, "Marv Solomon",      10, 48.9));

    boats.addElement(new Boats(1, "Onion",      "white"));
    boats.addElement(new Boats(2, "Buckey",     "red"  ));
    boats.addElement(new Boats(3, "Enterprise", "blue" ));
    boats.addElement(new Boats(4, "Voyager",    "green"));
    boats.addElement(new Boats(5, "Wisconsin",  "red"  ));
 
    reserves.addElement(new Reserves(10, 1, "05/10/95"));
    reserves.addElement(new Reserves(21, 1, "05/11/95"));
    reserves.addElement(new Reserves(10, 2, "05/11/95"));
    reserves.addElement(new Reserves(31, 1, "05/12/95"));
    reserves.addElement(new Reserves(10, 3, "05/13/95"));
    reserves.addElement(new Reserves(69, 4, "05/12/95"));
    reserves.addElement(new Reserves(69, 5, "05/14/95"));
    reserves.addElement(new Reserves(21, 5, "05/16/95"));
    reserves.addElement(new Reserves(57, 2, "05/10/95"));
    reserves.addElement(new Reserves(35, 3, "05/15/95"));

    boolean status = OK;
    int numsailors = 25;
    int numsailors_attrs = 4;
    int numreserves = 10;
    int numreserves_attrs = 3;
    int numboats = 5;
    int numboats_attrs = 3;
    
    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }

   
    /*
    ExtendedSystemDefs extSysDef = 
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
    
    // creating the sailors relation
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

    //SOS
    short [] Ssizes = new short [1];
    Ssizes[0] = 30; //first elt. is 30
    
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 4,Stypes, Ssizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    int size = t.size();
    
    // inserting the tuple into file "sailors"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numsailors; i++) {
      try {
	t.setIntFld(1, ((Sailor)sailors.elementAt(i)).sid);
	t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
	t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
	t.setFloFld(4, (float)((Sailor)sailors.elementAt(i)).age);
      }
      catch (Exception e) {
	System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the boats relation
    AttrType [] Btypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
    };
    
    short  []  Bsizes = new short[2];
    Bsizes[0] = 30;
    Bsizes[1] = 20;
    t = new Tuple();
    try {
      t.setHdr((short) 3,Btypes, Bsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file "boats"
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("boats.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 3, Btypes, Bsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    
    for (int i=0; i<numboats; i++) {
      try {
	t.setIntFld(1, ((Boats)boats.elementAt(i)).bid);
	t.setStrFld(2, ((Boats)boats.elementAt(i)).bname);
	t.setStrFld(3, ((Boats)boats.elementAt(i)).color);
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for boats");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the boats relation
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrString);

    short [] Rsizes = new short [1];
    Rsizes[0] = 15; 
    t = new Tuple();
    try {
      t.setHdr((short) 3,Rtypes, Rsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file "boats"
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("reserves.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 3, Rtypes, Rsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numreserves; i++) {
      try {
	t.setIntFld(1, ((Reserves)reserves.elementAt(i)).sid);
	t.setIntFld(2, ((Reserves)reserves.elementAt(i)).bid);
	t.setStrFld(3, ((Reserves)reserves.elementAt(i)).date);

      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }      
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for reserves");
      Runtime.getRuntime().exit(1);
    }
    
    

//Implemented as part of Minibase changes. Check if records with interval type data can be successfully inserted first. 
    AttrType [] Itypes = new AttrType[3];
    Itypes[0] = new AttrType(AttrType.attrInterval);
    Itypes[1] = new AttrType(AttrType.attrString);
 //   Itypes[2] = new AttrType(AttrType.attrInteger);

    //SOS
    short [] Isizes = new short [1];
    Isizes[0] = 1;	//maximum allowed size
    t = new Tuple();
    try {
      t.setHdr((short) 2, Itypes, Isizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() for intervals relation ***");
      status = FAIL;
      e.printStackTrace();
    }

    size = t.size();
    
    f = null;
    Heapfile f2;
    f2 = null;
    RID rid2 = null;

    try{
	f = new Heapfile("intervals.in");
        f2 = new Heapfile("intervals2.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor for intervals relation ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 2, Itypes, Isizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() for intervals relation***");
      status = FAIL;
      e.printStackTrace();
    }


    int numintervals = intervals.size();
    for(int i = 0 ; i < numintervals ; i++){
      try{
	  t.setIntervalFld(1, (IntervalType) intervals.elementAt(i));
	  t.setStrFld(2, "A");
//int a = ((IntervalTest)intervals.elementAt(i)).level;
//         System.out.println("id = " + a); 
//	  t.setIntFld(3, ((IntervalTest)intervals.elementAt(i)).level);
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() for intervals relation ***");
	status = FAIL;
	e.printStackTrace();
      }      
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
        if(i > 0 && i < 6)
        	rid2 = f2.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() for the intervals relation***");
	status = FAIL;
	e.printStackTrace();
      }   
    }

   if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for intervals");
      Runtime.getRuntime().exit(1);
    }
 
  System.out.println("Records have been inserted into interval table successfully!!");   
 }
  
  public boolean runTests() {
    
    Disclaimer();
/*
    Query1();   
    Query2();
    Query3();
    
   
    Query4();
    Query5();
    System.out.println("Should start query 6 now");
    Query6();
*/
//     Query7();
//     Query8();
    test7();
    System.out.println("Finished test 7 for testing interval sorting" + "\n");
    System.out.println ("Finished joins testing"+"\n");
    return true;
  }


  
  /* Added 7th test for testing sort function on interval type. */
  private boolean test7()
  {
    int SORTPGNUM = 12;
    boolean status = OK;
    intervalType[] data = new intervalType[10];
    int numintervals = 5;

    for(int i = 0 ; i < numintervals ; i++)
      data[i] = new intervalType();

   data[0].assign(1,10,1);
   data[1].assign(2,7,2);
   data[2].assign(8,9,3);
   data[3].assign(3,4,4);
   data[4].assign(5,6,5);
//   data[5].assign(6,11,6);

 //  int intervalobjsize = instrumentation.getObjectSize(data[0]); 
   short[] attrSize = new short[1];
   short intervalobjsize = 12;		//2 integers, 4 bytes each.

   short strsizes[] = new short[1];
   strsizes[0] = 0;				//no strings in this relation. 
 
   attrSize[0] = intervalobjsize;
//   attrSize[1] = intervalobjsize;

   AttrType[] attrType = new AttrType[1];
   attrType[0] = new AttrType(AttrType.attrInterval);
//   attrType[1] = new AttrType(AttrType.attrInterval);
  
   TupleOrder[] order = new TupleOrder[1];
   order[0] = new TupleOrder(TupleOrder.Ascending);
 //  order[1] = new TupleOrder(TupleOrder.Descending);
  
    // create a tuple of appropriate size
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 1, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

     int size = t.size();
    
    // Create unsorted data file "test7.in"
    
  
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("test8.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 1, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    for(int i = 0 ; i < numintervals ; i++)
    {
	try{
	   t.setIntervalFld(1,data[i]);	
//           System.out.println(data[i].getStart() + " " + data[i].getEnd() + " " + data[i].getLevel());
	}
        catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try{
        AttrType[] DTypes = {new AttrType(AttrType.attrInterval)};
	System.out.println("Inserting :  ");
        t.print(DTypes);
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }

   /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions

   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */
    FldSpec[] projlist = new FldSpec[1];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
//    projlist[1] = new FldSpec(rel, 2);
    
    FileScan fscan = null;
    
    try {
      fscan = new FileScan("test8.in", attrType, strsizes, (short)1 , (short)1, projlist, null);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    // Sort "test7.in"
     /** 
   * Class constructor, take information about the tuples, and set up 
   * the sorting
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */ 
    Sort sort = null;
    try {
      sort = new Sort(attrType, (short) 1, strsizes, fscan, 1, order[0], intervalobjsize, SORTPGNUM);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
  
    int count = 0;
    t = null;
     
    AttrType[]  RTypes = {new AttrType(AttrType.attrInterval)};	//get the types for the result tuple. 
    System.out.println("\n\n******* Started printing the result tuples ********\n\n");
    try {
      while( (t = sort.get_next()) != null ){
      	t.print(RTypes);
//      intervaltype curr = t.getIntervalFld(1);
//      System.out.println("start = " + curr.s + " end = " + curr.e);
      	count++;
	}
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }
    
    System.out.println("--------------------Test 7 completed successfully!---------------------------------");
    System.out.println("Total number of tuples accessed : " + count);   
    return true;
  }

  private void Query1_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[1].next  = null;
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
    expr[1].operand2.integer = 1;
    expr[2] = null;
  }

  private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr[1] = null;
 
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);   
    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    expr2[1].operand2.string = "red";
 
    expr2[2] = null;
  }

  private void Query3_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    expr[1] = null;
  }

  private CondExpr[] Query5_CondExpr() {
    CondExpr [] expr2 = new CondExpr[3];
    expr2[0] = new CondExpr();
    
   
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr2[1] = new CondExpr();
    expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
   
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
    expr2[1].type2 = new AttrType(AttrType.attrReal);
    expr2[1].operand2.real = (float)40.0;
    

    expr2[1].next = new CondExpr();
    expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
    expr2[1].next.next = null;
    expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
    expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
    expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
    expr2[1].next.operand2.integer = 7;
 
    expr2[2] = null;
    return expr2;
  }

  private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
   
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].next  = null;
    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand2.integer = 7;
 
    expr[2] = null;
 
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr2[1].next = null;
    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand2.string = "red";
 
    expr2[2] = null;
  }
  
  private void Query7_CondExpr(CondExpr[] expr)
  {
    expr[0].next = null;
    expr[0].flag = 0;
    expr[0].op = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    expr[1] = null;
  }

  public void Query1() {
    
    System.out.print("**********************Query1 strating *********************\n");
    boolean status = OK;
    
    // Sailors, Boats, Reserves Queries.
    System.out.print ("Query: Find the names of sailors who have reserved "
		      + "boat number 1.\n"
		      + "       and print out the date of reservation.\n\n"
		      + "  SELECT S.sname, R.date\n"
		      + "  FROM   Sailors S, Reserves R\n"
		      + "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");
    
    System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");
 
    CondExpr[] outFilter = new CondExpr[3];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
    outFilter[2] = new CondExpr();
 
    Query1_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30
    
    FldSpec [] Sprojection = new FldSpec[4];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    CondExpr [] selects = new CondExpr [1];
    selects = null;
    
 
    FileScan am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrString);

    short [] Rsizes = new short[1];
    Rsizes[0] = 15; 
    FldSpec [] Rprojection = new FldSpec[3];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
 
    FileScan am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short) 3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
   
    
    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

    AttrType [] jtype = new AttrType[2];
    jtype[0] = new AttrType (AttrType.attrString);
    jtype[1] = new AttrType (AttrType.attrString);
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4, 
			 1, 4, 
			 10,
			 am, am2, 
			 false, false, ascending,
			 outFilter, proj_list, 2);
    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***"); 
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

   
 
    QueryCheck qcheck1 = new QueryCheck(1);
 
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);

        qcheck1.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }
    
    qcheck1.report(1);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
  }
  
  public void Query2() {
    System.out.print("**********************Query2 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print 
      ("Query: Find the names of sailors who have reserved "
       + "a red boat\n"
       + "       and return them in alphabetical order.\n\n"
       + "  SELECT   S.sname\n"
       + "  FROM     Sailors S, Boats B, Reserves R\n"
       + "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
       + "  ORDER BY S.sname\n"
       + "Plan used:\n"
       + " Sort (Pi(sname) (Sigma(B.color='red')  "
       + "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
       + "(Tests File scan, Index scan ,Projection,  index selection,\n "
       + "sort and simple nested-loop join.)\n\n");
    
    // Build Index first
    IndexType b_index = new IndexType (IndexType.B_Index);

   
    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }
    
    


    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    CondExpr [] outFilter2 = new CondExpr[3];
    outFilter2[0] = new CondExpr();
    outFilter2[1] = new CondExpr();
    outFilter2[2] = new CondExpr();

    Query2_CondExpr(outFilter, outFilter2);
    Tuple t = new Tuple();
    t = null;

    AttrType [] Stypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrReal)
    };

    AttrType [] Stypes2 = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
    };

    short []   Ssizes = new short[1];
    Ssizes[0] = 30;
    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
    };

    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 15;
    AttrType [] Btypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
    };

    short  []  Bsizes = new short[2];
    Bsizes[0] =30;
    Bsizes[1] =20;
    AttrType [] Jtypes = {
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
    };

    short  []  Jsizes = new short[1];
    Jsizes[0] = 30;
    AttrType [] JJtype = {
      new AttrType(AttrType.attrString), 
    };

    short [] JJsize = new short[1];
    JJsize[0] = 30;
    FldSpec []  proj1 = {
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.innerRel), 2)
    }; // S.sname, R.bid

    FldSpec [] proj2  = {
       new FldSpec(new RelSpec(RelSpec.outer), 1)
    };
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
 
    CondExpr [] selects = new CondExpr[1];
    selects[0] = null;
    
    
    //IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am = null;
   

    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
        Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID rid = new RID();
    int key =0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);
      
      try {
	key = tt.getIntFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    
    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print ("After Building btree index on sailors.sid.\n\n");
    try {
      am = new IndexScan ( b_index, "sailors.in",
			   "BTreeIndex", Stypes, Ssizes, 4, 2,
			   Sprojection, null, 1, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Stypes2, 2, Ssizes,
				  Rtypes, 3, Rsizes,
				  10,
				  am, "reserves.in",
				  outFilter, null, proj1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

     NestedLoopsJoins nlj2 = null ; 
    try {
      nlj2 = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				   Btypes, 3, Bsizes,
				   10,
				   nlj, "boats.in",
				   outFilter2, null, proj2, 1);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;
    try {
      sort_names = new Sort (JJtype,(short)1, JJsize,
			     (iterator.Iterator) nlj2, 1, ascending, JJsize[0], 10);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    
    QueryCheck qcheck2 = new QueryCheck(2);
    
   
    t = null;
    try {
      while ((t = sort_names.get_next()) != null) {
        t.print(JJtype);
        qcheck2.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    qcheck2.report(2);

    System.out.println ("\n"); 
    try {
      sort_names.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);
      }
  }
  

   public void Query3() {
    System.out.print("**********************Query3 strating *********************\n"); 
    boolean status = OK;

        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ( "Query: Find the names of sailors who have reserved a boat.\n\n"
	+ "  SELECT S.sname\n"
	+ "  FROM   Sailors S, Reserves R\n"
	+ "  WHERE  S.sid = R.sid\n\n"
	+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");
    
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck3 = new QueryCheck(3);
 
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck3.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
       Runtime.getRuntime().exit(1);
    }
 
 
    qcheck3.report(3);
 
    System.out.println ("\n"); 
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
  }

   public void Query4() {
     System.out.print("**********************Query4 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of sailors who have reserved a boat\n"
       + "       and print each name once.\n\n"
       + "  SELECT DISTINCT S.sname\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid\n\n"
       + "(Tests FileScan, Projection, Sort-Merge Join and "
       + "Duplication elimination.)\n\n");
 
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    short  []  jsizes    = new short[1];
    jsizes[0] = 30;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
    
   

    DuplElim ed = null;
    try {
      ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck4 = new QueryCheck(4);

    
    t = null;
 
    try {
      while ((t = ed.get_next()) != null) {
        t.print(jtype);
        qcheck4.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace(); 
      Runtime.getRuntime().exit(1);
      }
    
    qcheck4.report(4);
    try {
      ed.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
   System.out.println ("\n");  
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
 }

   public void Query5() {
   System.out.print("**********************Query5 strating *********************\n");  
    boolean status = OK;
        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of old sailors or sailors with "
       + "a rating less\n       than 7, who have reserved a boat, "
       + "(perhaps to increase the\n       amount they have to "
       + "pay to make a reservation).\n\n"
       + "  SELECT S.sname, S.rating, S.age\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
       + "(Tests FileScan, Multiple Selection, Projection, "
       + "and Sort-Merge Join.)\n\n");

   
    CondExpr [] outFilter;
    outFilter = Query5_CondExpr();
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] = 15;

    FldSpec [] Sprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
    
    CondExpr[] selects = new CondExpr [1];
    selects[0] = null;
 
    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    FldSpec [] Rprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3)
    };
  
    AttrType [] jtype     = { 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrReal)
    };


    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
			 (short)3, (short)3,
			 Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 3);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

    QueryCheck qcheck5 = new QueryCheck(5);
    //Tuple t = new Tuple();
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck5.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    qcheck5.report(5);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error close for sortmerge");
      Runtime.getRuntime().exit(1);
    }
    System.out.println("This is the end of query 5!");
 }

  public void Query6()
    {
      System.out.print("**********************Query6 strating *********************\n");
      boolean status = OK;
      // Sailors, Boats, Reserves Queries.
      System.out.print( "Query: Find the names of sailors with a rating greater than 7\n"
			+ "  who have reserved a red boat, and print them out in sorted order.\n\n"
			+ "  SELECT   S.sname\n"
			+ "  FROM     Sailors S, Boats B, Reserves R\n"
			+ "  WHERE    S.sid = R.sid AND S.rating > 7 AND R.bid = B.bid \n"
			+ "           AND B.color = 'red'\n"
			+ "  ORDER BY S.name\n\n"
			
			+ "Plan used:\n"
			+" Sort(Pi(sname) (Sigma(B.color='red')  |><|  Pi(sname, bid) (Sigma(S.rating > 7)  |><|  R)))\n\n"
			
			+ "(Tests FileScan, Multiple Selection, Projection,sort and nested-loop join.)\n\n");
      
      CondExpr [] outFilter  = new CondExpr[3];
      outFilter[0] = new CondExpr();
      outFilter[1] = new CondExpr();
      outFilter[2] = new CondExpr();
      CondExpr [] outFilter2 = new CondExpr[3];
      outFilter2[0] = new CondExpr();
      outFilter2[1] = new CondExpr();
      outFilter2[2] = new CondExpr();
      
      Query6_CondExpr(outFilter, outFilter2);
      Tuple t = new Tuple();
      t = null;
      
      AttrType [] Stypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrReal)
      };
      
      
      
      short []   Ssizes = new short[1];
      Ssizes[0] = 30;
      AttrType [] Rtypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
      };
      
      short  []  Rsizes = new short[1] ;
      Rsizes[0] = 15;
      AttrType [] Btypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrString), 
      };
      
      short  []  Bsizes = new short[2];
      Bsizes[0] =30;
      Bsizes[1] =20;
      
      
      AttrType [] Jtypes = {
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
      };
      
      short  []  Jsizes = new short[1];
      Jsizes[0] = 30;
      AttrType [] JJtype = {
	new AttrType(AttrType.attrString), 
      };
      
      short [] JJsize = new short[1];
      JJsize[0] = 30; 
      
      
      
      FldSpec []  proj1 = {
	new FldSpec(new RelSpec(RelSpec.outer), 2),
	new FldSpec(new RelSpec(RelSpec.innerRel), 2)
      }; // S.sname, R.bid
      
      FldSpec [] proj2  = {
	new FldSpec(new RelSpec(RelSpec.outer), 1)
      };
      
      FldSpec [] Sprojection = {
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.outer), 3),
        new FldSpec(new RelSpec(RelSpec.outer), 4)
      };
      
      
      
      
      
      FileScan am = null;
      try {
	am  = new FileScan("sailors.in", Stypes, Ssizes, 
			   (short)4, (short)4,
			   Sprojection, null);
      }
      catch (Exception e) {
	status = FAIL;
	System.err.println (""+e);
	e.printStackTrace();
      }
      
      if (status != OK) {
	//bail out
	
	System.err.println ("*** Error setting up scan for sailors");
	Runtime.getRuntime().exit(1);
      }
      
  
      
      NestedLoopsJoins inl = null;
      try {
	inl = new NestedLoopsJoins (Stypes, 4, Ssizes,
				    Rtypes, 3, Rsizes,
				    10,
				  am, "reserves.in",
				    outFilter, null, proj1, 2);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
     
      System.out.print( "After nested loop join S.sid|><|R.sid.\n");
	
      NestedLoopsJoins nlj = null;
      try {
	nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				    Btypes, 3, Bsizes,
				    10,
				    inl, "boats.in",
				    outFilter2, null, proj2, 1);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
      
      System.out.print( "After nested loop join R.bid|><|B.bid AND B.color=red.\n");
      
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
      Sort sort_names = null;
      try {
	sort_names = new Sort (JJtype,(short)1, JJsize,
			       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for sorting");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }
      
      
      System.out.print( "After sorting the output tuples.\n");
   
      
      QueryCheck qcheck6 = new QueryCheck(6);
      
      try {
	while ((t =sort_names.get_next()) !=null) {
	  t.print(JJtype);
	  qcheck6.Check(t);
	}
      }catch (Exception e) {
	System.err.println ("*** Error preparing for get_next tuple");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }
      
      qcheck6.report(6);
      
      System.out.println ("\n"); 
      try {
	sort_names.close();
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      if (status != OK) {
	//bail out
	System.out.println("There is some problem with Query 6!");
	Runtime.getRuntime().exit(1);
      }
      System.out.println("Query 6 has completed. ");
    }
 
/* New Query for testing interval based joins */   
    private void Query7() 
    {
 
    System.out.print("**********************Query7 strating *********************\n");
      boolean status = OK;
      // Sailors, Boats, Reserves Queries.
      System.out.print( "Query: Test that joins based on intervals work."
			+ "  SELECT   A.id, B.id, B.level\n"
			+ "  FROM     IntervalTest A, IntervalTest B\n"
			+ "  WHERE    A.interval > B.interval \n"
			+ " All we are doing is testing if the > operator will work for the join. "
			+ "(Tests FileScan, Multiple Selection, Projection,sort and nested-loop join.)\n\n");
      
     CondExpr [] outFilter  = new CondExpr[2];		//create 2 conditional expressions. The last oe is always null to indicate end. 
     outFilter[0] = new CondExpr();
     outFilter[1] = new CondExpr();
     Query7_CondExpr(outFilter);			//outfilter will vary

      AttrType [] Atypes = {
	new AttrType(AttrType.attrInterval), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger)
      };
      short []   Asizes = new short[1];
      Asizes[0] = 10;

      FldSpec [] Aprojection = {				//3 fields which we want to display in the projection list. 
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.outer), 3)
      };
      
      AttrType [] Btypes = {
	new AttrType(AttrType.attrInterval), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger)
      };
      short []   Bsizes = new short[1];
      Bsizes[0] = 10;

      FldSpec [] Bprojection = {				//3 fields which we want to display in the projection list. 
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.outer), 3)
      };
      
          
      FileScan am = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns. 
      try {
	am  = new FileScan("intervals.in", Atypes, Asizes, 
			   (short)3, (short)3,
			   Aprojection, null);
      }
       catch (Exception e) {
	status = FAIL;
	System.err.println (""+e);
	e.printStackTrace();
      }
   
      Tuple test = new Tuple();
/*
      AttrType[] attrs = new AttrType[]{ new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger)};
      try{
        for(int i = 0 ; i < 3 ; i++)
        {
	  test = am.get_next();
          test.print(attrs);
        }
      }
      catch(IOException | InvalidTypeException e) {
	System.err.println ("*** Error outputting tuples ");
	System.err.println (""+e);
     }
*/

      if (status != OK) {
	//bail out
	System.err.println ("*** Error setting up scan for intervals");
	Runtime.getRuntime().exit(1);
      }
      
       FldSpec [] proj1  = {
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.innerRel), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.innerRel),2),
        new FldSpec(new RelSpec(RelSpec.innerRel),3)
      };

      System.out.println("Outfilter[0] = " + outFilter[0]);
      System.out.println(outFilter[0].type1);
      System.out.println(outFilter[0].type2);


      NestedLoopsJoins nlj = null;
      NestedLoopsJoins nlj2 = null;

      try {
	nlj = new NestedLoopsJoins (Atypes, 3, Asizes,
				    Btypes, 3, Bsizes,
				    10,
				  am, "intervals.in",
				    outFilter, null, proj1, 5);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
     
       
       if(nlj != null)
	  System.out.println("Nested loop join object created successfully!!");

       AttrType [] Rtype = {
	new AttrType(AttrType.attrInterval),
	new AttrType(AttrType.attrInterval),
	new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrString),
	new AttrType(AttrType.attrInteger) 
      };

       Tuple t = new Tuple();		//create a result array which contains all tuples satifying join condition
       Tuple res[] = new Tuple[30];

       for(int i = 0 ; i < 30 ; i++)
          res[i] =null;
       int count = 0;


       nlj2 = nlj;

       try {
	while ((t = nlj.get_next()) !=null) {
          res[count++] = t;
	  System.out.println("THE CONDITION IS TRUE FOR THIS TUPLE : PRINTING IT NOW" );
	  t.print(Rtype);
	}
      }catch (Exception e) {
	System.err.println ("*** Error preparing for get_next tuple");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }

/* Seeing if causing get_next() once will cause a second invocation of get_next to fail as well. */
      
      System.out.println("Now trying for the nlj2 object : " + nlj2);
       try {
	while ((t = nlj2.get_next()) !=null) {
	  System.out.println("THE CONDITION IS TRUE FOR THIS TUPLE : PRINTING IT NOW" );
	  t.print(Rtype);
	}
      }catch (Exception e) {
	System.err.println ("*** Error preparing for get_next tuple");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }


      for(int i = 0 ; i < count ; i++)
      {
        try{
        res[i].print(Rtype);
       }
       catch(IOException ie){
       	 ie.printStackTrace();
       }
      }	
      System.out.println("No of tuples read : " + count);
      System.out.println("All tuples printed out successfully!");
}

  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }

/*
Query to test for sort-merge join 
*/
/* Two conditions : One is interval should be greater and next is level should be greater. */
private void Query8_CondExpr(CondExpr[] expr)
{
    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1); 

/*
    expr[1].next  = null;
    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrSymbol);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
*/
    expr[1] = null;
}

public void Query8() {
    
    System.out.print("**********************Query 8 starting *********************\n");
    boolean status = OK;
    
    // Sailors, Boats, Reserves Queries.
    System.out.print (" Query to test for Sort-Merge join ");
 
    CondExpr[] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr(); 
//    outFilter[1] = new CondExpr();
    Query8_CondExpr(outFilter);			//condition filter. 
 
    Tuple t = new Tuple();
    
    //Attribute types which are present in the relation. 
    AttrType [] Stypes = new AttrType[3];				
    Stypes[0] = new AttrType (AttrType.attrInterval);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
   
    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 1; 
    
    FldSpec [] Sprojection = new FldSpec[3];					/* We are only projecting the interval and string values out now. */
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);	
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

  
    FileScan am = null;
    try {
      am  = new FileScan("intervals.in", Stypes, Ssizes, 
				  (short)3, (short)3,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for intervals");
      Runtime.getRuntime().exit(1);
    }
    
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInterval);
    Rtypes[1] = new AttrType (AttrType.attrString);
    Rtypes[2] = new AttrType (AttrType.attrInteger);

    short [] Rsizes = new short[1];
    Rsizes[0] = 1; 

    FldSpec [] Rprojection = new FldSpec[3];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
 
    FileScan am2 = null;
    try {
      am2 = new FileScan("intervals2.in", Rtypes, Rsizes, 
				  (short)3, (short) 3,
				  Rprojection, null);
    }    
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
  
/*
    AttrType[] attrs = {new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger)};
    try{
      while((t = am2.get_next()) != null)
        t.print(attrs);
    }
    catch(Exception e){
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for intervals");
      Runtime.getRuntime().exit(1);
    }
*/   
 
    FldSpec [] proj_list = new FldSpec[6];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);	//project all the 6 columns. 
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
    proj_list[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
    proj_list[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

   
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 3, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 8, 
			 1, 8, 
			 10,
			 am, am2, 
			 false, false, ascending,
			 outFilter, proj_list, 6);
    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***"); 
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

    AttrType [] jtype = new AttrType[6];

    jtype[0] = new AttrType (AttrType.attrInterval);
    jtype[1] = new AttrType (AttrType.attrString);
    jtype[2] = new AttrType (AttrType.attrInteger);
    jtype[3] = new AttrType (AttrType.attrInterval);
    jtype[4] = new AttrType (AttrType.attrString);
    jtype[5] = new AttrType (AttrType.attrInteger);
    
    t = null;
    System.out.println("----------JOIN COMPLETED SUCCESSFULLY. PLEASE FIND RESULTS BELOW------------");
    int count = 0;

    try {
      System.out.println("Tuple " + count + ":");
      while ((t = sm.get_next()) != null) {
        System.out.println("printing tuple : ");
        t.print(jtype);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }
   
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
}

/*
A
|
B
*/



//read the data table
public FileScan readtable(String key, int pos)	//read in the initial heapfile and return pointer. 
{
    boolean status = OK;  
    String nodes[] = {"A","B","C","D","E"};

    System.out.println("key = " + key);

    AttrType [] Itypes = new AttrType[2];
    Itypes[0] = new AttrType(AttrType.attrInterval);
    Itypes[1] = new AttrType(AttrType.attrString);
   
    FldSpec[] Sprojection = new FldSpec[2];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer),2);

//Create the filter for select expression. 
    CondExpr[] selectFilter = new CondExpr[2];
    selectFilter[0] = new CondExpr();
    selectFilter[0].next = null;
    selectFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
    selectFilter[0].type1 = new AttrType(AttrType.attrSymbol);		//S.interval > R.interval
    selectFilter[0].type2 = new AttrType(AttrType.attrString);
    selectFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),pos); 
    selectFilter[0].operand2.string = key; 

    selectFilter[1] = null;

    //SOS
    short [] Isizes = new short [1];
    Isizes[0] = 1;	//maximum allowed size
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 2, Itypes, Isizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() for intervals relation ***");
      status = FAIL;
      e.printStackTrace();
    }

    int size = t.size();
    
    RID rid = null;
    Heapfile f = null;
    Heapfile f2 = null;

    try{
	f = new Heapfile("intervalstest.in");
	f2 = new Heapfile("datatable.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor for intervals relation ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 2, Itypes, Isizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() for intervals relation***");
      status = FAIL;
      e.printStackTrace();
    }

    AttrType[] attr = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
    int numintervals = intervals.size();
    for(int i = 0 ; i < numintervals ; i++){
      try{
	  t.setIntervalFld(1, (IntervalType) intervals.elementAt(i));   //Interval String <>
	  t.setStrFld(2, nodes[i]); 
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() for intervals relation ***");
	status = FAIL;
	e.printStackTrace();
      }      
      
      RID rid2;
      try {
	rid = f.insertRecord(t.getTupleByteArray());
	rid2 = f2.insertRecord(t.getTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() for the intervals relation***");
	status = FAIL;
	e.printStackTrace();
      }   
   
   if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for intervals");
      Runtime.getRuntime().exit(1);
    }
  }

   FileScan am = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns. 
      try {
	am  = new FileScan("intervalstest.in", Itypes, Isizes, 
			   (short)2, (short)2,
			   Sprojection, selectFilter);
      }
       catch (Exception e) {
	status = FAIL;
	System.err.println (""+e);
	e.printStackTrace();
      }

  System.out.println("Records have been inserted into interval table successfully!!");
  return am;
}

//Create the conditional expression for the join query. 
void createjoinquery_condexpr(CondExpr[] expr, String rel, int intervalpos1, int intervalpos2)
{
    System.out.println("In create join query conditional expression");
    expr[0].next = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);		//S.interval > R.interval
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),intervalpos1); 
    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),intervalpos2); 
    System.out.println("rel = "  + rel);
    if(rel.compareTo("AD") == 0)
    {
       expr[0].ad = 1;
       System.out.println("setting ad to 1");
    }
    else
    {
       System.out.println("Setting pc to 1"); 
       expr[0].pc = 1; 
    }
   expr[1] = null;
}

/*
a
|
b

T:  interval id
     1,3     A
*/

//only need one iterator as nested loop join 
/*
@Parameters
FileScan am : the left table file scan object
intervalpos1 : the index of the interval in first table
intervalpos2 : the index of interval in second table 
attr : attribute types of outer table
rel: relationship between the nodes (PC/AD) ? 
num_attrs: number of attributes of outer table. 
pos: The position of the column in outer table, on which select must be applied.
*/
/* First read parent table. Then perform one level of join. 


res, childvalue, intervalpos[parent],1,Dtypes,num_attrs,rel
*/
public iterator.Iterator createjoinquery(FileScan am, String child , int intervalpos1, int intervalpos2, AttrType[] attr, String rel, int num_attrs) throws JoinsException
{

boolean status = OK;

CondExpr[] outFilter = new CondExpr[2];		//will give 3 
outFilter[0] = new CondExpr();
outFilter[1] = new CondExpr();

//define conditions for join query
createjoinquery_condexpr(outFilter, rel, intervalpos1, intervalpos2);
System.out.println("pc = " + outFilter[0].pc + " ad = " + outFilter[0].ad);
    
System.out.println("conditional expression created");

//count how many strings are coming in from the input table being passed as am. 
int num_strs = 0;
for(int i = 0 ; i < num_attrs ; i++)
{
   if(attr[i].attrType == AttrType.attrString)
     num_strs++;
}

AttrType[] Stypes = new AttrType[num_attrs];				
for(int i = 0 ; i < num_attrs ; i++)
 Stypes[i] = new AttrType(attr[i].attrType);				

short [] Ssizes = new short[num_strs];
for(int i = 0 ; i < num_strs ; i++)
	Ssizes[i] = 10; 		//Let's keep string size as 10 bytes. 
    
/*Code for filescan using select*/

/* Attribute types for the big intervals.in relation */
AttrType [] Rtypes = new AttrType[2];				/*Here he's defining all the data types for the sailors relation*/
Rtypes[0] = new AttrType(AttrType.attrInterval);
Rtypes[1] = new AttrType(AttrType.attrString);

//SOS
short [] Rsizes = new short[1];
Rsizes[0] = 10; 				//right hand side data table has one column which is string. 

FldSpec[] RProjection = new FldSpec[2];
RProjection[0] =  new FldSpec(new RelSpec(RelSpec.outer), 1); 
RProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

FileScan table = null;	

//System.out.println("pos = " + pos);

CondExpr[] selectFilter = new CondExpr[2];
selectFilter[0] = new CondExpr();
selectFilter[0].next = null;
selectFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
selectFilter[0].type1 = new AttrType(AttrType.attrSymbol);		//The equality condition on main table. 
selectFilter[0].type2 = new AttrType(AttrType.attrString);
selectFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);   //pos = 2 for String. 
selectFilter[0].operand2.string = child; //child value for which we have to filter from the right table; 
System.out.println("child being searched = " + child);
selectFilter[1] = null;
		
try {
      System.out.println("In try block...trying to create a table pointer");
      table  = new FileScan("datatable.in", Rtypes, Rsizes, (short)2, (short)2, RProjection, /*selectFilter*/null);          //read the child node values from right data table. 
}
catch (Exception e) {
      System.out.println("Whoops!");
      status = FAIL;
      System.err.println (""+e);
}


System.out.println("table pointer created!");
Tuple t1 = new Tuple();
	AttrType[] ptypes = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
	try{
	while( (t1 = table.get_next()) != null)
	{
	   System.out.println("Wow, look data!");
	   t1.print(ptypes);
        }
       }
	catch(Exception e){
}
/*Code for filescan based on select condition ends*/


/*Code to get the nested loop join iterator */

//Deciding the schema for the final output table. 
FldSpec [] Projection = new FldSpec[num_attrs+2];				

for(int i = 0 ; i < num_attrs ; i++)
  Projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
Projection[num_attrs] =  new FldSpec(new RelSpec(RelSpec.innerRel), 1);
Projection[num_attrs+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

if(outFilter == null)
	System.out.println("outfilter is null");


NestedLoopsJoins nlj = null;
try {
      nlj = new NestedLoopsJoins (Stypes, num_attrs, Ssizes,
				  Rtypes, 2, Rsizes,
				  50,
				  /*table*/ am, "intervalstest.in",                      //table contains all As, the other table contains all decendants of A.
				  outFilter, null, Projection, num_attrs+2);
}
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
      status = FAIL;
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan");
      Runtime.getRuntime().exit(1);
    }

/*
Tuple t = new Tuple();
t.setHdr(short(num_attrs+2), )
*/

return (iterator.Iterator)nlj;			//returns the nested loop join object constructed. 
}


/* 
GENERATE A DYNAMIC QUERY GIVEN A GRAPH. 
Target file to work on : intervalstest.in

Plan is as follows : 
1. Call the readtable function to read in the root node. 
2. for every element extracted from the queue, process its PC and AD matrix. Create table according to the connditions (PC/AD), one join at a time. 
*/





/*Reads data from an iterator, stores it in heap file. */
public FileScan readdata (iterator.Iterator am,short num_attrs,AttrType[] attr_types)
{
	Heapfile f = null;
	try{
	  f = new Heapfile("intmdt2.in");
	}
	catch(Exception e){
	  e.printStackTrace();
	}
	
	Tuple t = new Tuple();			//list of attribute types, no of string sizes. 
	short n = num_attrs;	

	int num_strs = 0;			//counting number of strings in this table. 
	for(int i = 0 ; i < num_attrs ; i++)
	   if(attr_types[i].attrType == AttrType.attrString)
		num_strs++;
	
	short[] s_sizes = new short[num_strs];
	for(int i = 0 ; i < num_strs ; i++)
		s_sizes[i] = 10;			//keeping string size as 10 bytes for all strings stored on system. 	
	
        try {
      		t.setHdr((short) n,attr_types, s_sizes);
    	}
    	catch (Exception e) {
      		System.err.println("*** error in Tuple.setHdr() ***");
      		e.printStackTrace();
    	}

	int size = t.size();
	t = new Tuple(size);
	RID rid;

	System.out.println("printing attr types in readdata");
	for(int i = 0 ; i < num_attrs ; i++)
	{			
	        System.out.println(attr_types[i].toString());
	}	

	try{
	   while((t = am.get_next()) != null){
		  try {
			t.print(attr_types);
			rid = f.insertRecord(t.getTupleByteArray());
      		  }
      		catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			e.printStackTrace();
      		}      
    	    }
	}
	catch(Exception e){
	}

	System.out.println("n = " + n);
	System.out.println("data inserted into intmdt.in");

	FldSpec[] SProjection = new FldSpec[num_attrs];
	for(int i = 0 ; i < num_attrs ; i++)
		SProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

	FileScan scan = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns. 
      	try {
		scan  = new FileScan("intmdt2.in", attr_types, s_sizes, (short)n, (short)n, SProjection, null);	//first just check if one table is properly written to or not. 
      	}
       catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
      }

      System.out.println("Records have been inserted into interval table successfully!!");
      return scan;
}







/* Given a graph, generate a query plan from it. */
  public void generate_query(ArrayList<Integer>[] graph,String[] keys, int[][] PC, int[][] AD, int n) throws JoinsException,IOException,IndexException         //0 1 2 3 4 5 (in case of nodes uptil 5)
  {

	System.out.println("Graph size = " + n);
	System.out.println("Graph is : ");

	for(int i = 1 ; i <= 5 ; i++)
        {
           for(int j = 0 ; j < graph[i].size() ; j++)
             System.out.print(graph[i].get(j) + " " );
           System.out.println();
        }

        Queue<Integer> q = new LinkedList<>();
	q.add(1);						//1 is given as root node. 
 
	System.out.println("Queue is : ");
        while(q.isEmpty() == false)
        {
	    int element = q.remove();
	    System.out.println(element + " " );			//print elmeents of queue one by one. 
	    for(int i = 0 ; i < graph[element].size() ; i++)
		q.add(graph[element].get(i));
        }

        int parent = 1;
	String rootkey = keys[1];						//get string label of root node. 
	System.out.println("Read table function being called...");

	FileScan root = readtable(rootkey,2);				//read the root node value and store it in a table. 
	
	System.out.println("In generate query : Filescan done successfully!");

/*
	Tuple t1 = new Tuple();
	AttrType[] ptypes = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
	try{
	while( (t1 = root.get_next()) != null)
	{
	   System.out.println("Wow, look data!");
	   t1.print(ptypes);
        }
       }
	catch(Exception e){
	}
*/

	String rel;
	short num_attrs = 2;			           			//store column count in final result table.. 
	AttrType[] Dtypes = new AttrType[50];
	Dtypes[0] = new AttrType(AttrType.attrInterval);
	Dtypes[1] = new AttrType(AttrType.attrString);
	int end = 2;

	int[] intervalpos = new int[n+1];					//store the position of interval field for key k.(from 1 to n).
	intervalpos[1] = 1;							//the interval for first node is at position 1. 
	iterator.Iterator res = null;							//store the file pointer to resulting table. 
	FileScan f = null;

	boolean first = true;
	for(int child = 0 ; child < graph[parent].size() ; child++)		//go through all child nodes in the graph. 	
        {
		int childnode = graph[parent].get(child);
		System.out.println("Reached here parent = " + keys[parent] + " child = " + keys[childnode]);
		if(PC[parent][childnode] == 1)
			rel = "PC";
		else if(AD[parent][childnode] == 1)
			rel = "AD";		
		else
			continue;

		System.out.println("Processing for parent = " + keys[parent] + " child = " + keys[childnode]);
		String childvalue = keys[childnode];					//get the string key of child node.
		if(first == true) 
		{
			res = createjoinquery(root, childvalue, intervalpos[parent],1,Dtypes,rel,num_attrs);
			first = false;
		}
		else
		{
			res = createjoinquery(f, childvalue, intervalpos[parent],1,Dtypes,rel,num_attrs);
		}

		System.out.println("Join done successfully");

/* Adding the two new columns for the new table. */
		Dtypes[end] = new AttrType(AttrType.attrInterval);
		intervalpos[child] = end+1;
		end++;						//store the position of interval column for the child node. 
		Dtypes[end] = new AttrType(AttrType.attrString);
		end++;	
		num_attrs += 2; 

		AttrType[] types = new AttrType[num_attrs];
		for(int i = 0 ; i < num_attrs ; i++)
		{			
			types[i] = Dtypes[i];
			System.out.println(types[i].toString());
		}	
		Tuple t = new Tuple();
		try{
	 	    while( (t = res.get_next()) != null)
	  	   {
			System.out.println("This tuple is part of result!--");
			t.print(types);
	           }
	       }
		catch(Exception e){ }	

/* The res iterator is created now. Just store all the tuples from the res pointer in a heapfile, and obtain a pointer to that heap file. */
		f = readdata(res,num_attrs,types);
	}

//Examine the results obtained 
	System.out.println("here now");
	Tuple t = new Tuple();
	AttrType[] types = new AttrType[num_attrs];
	for(int i = 0 ; i < num_attrs ; i++)
		types[i] = Dtypes[i];

	try{
	  while( (t = f.get_next()) != null)
	  {
		System.out.println("This tuple is part of result!--");
		t.print(types);
	  }
	}
	catch(Exception e){

	}
  }

/*
@Parameters
FileScan am : the left table file scan object
child : the key of the child. 
intervalpos1 : the index of the interval in first table
intervalpos2 : the index of interval in second table 
attr : attribute types of outer table
rel: relationship between the nodes (PC/AD) ? 
num_attrs: number of attributes of outer table. 
pos: The position of the column in outer table, on which select must be applied.
*/






  public void genqueryplan(ArrayList<Integer>[] graph, String[] keys, int[][] PC, int[][] AD) throws JoinsException, IndexException, IOException	
  {

	int p = 1;
        int c = graph[p].get(0);			//fetch the first child.
	for(int i = 1 ; i <= 5 ; i++)
        {
           for(int j = 0 ; j < graph[i].size() ; j++)
             System.out.print(graph[i].get(j) + " " );
           System.out.println();
        }

	String parent = keys[p];
	String child = keys[c];
        for(int i = 1 ; i <= 5 ; i++)
           System.out.println(keys[i] + " ");

	System.out.println("parent = " + parent + " child = " + child);

	String rel = "";

	if(PC[p][c] == 1)
	  rel = "PC";
        else if(AD[p][c] == 1)
          rel = "AD";

	System.out.println("rel = " + rel);
	int intervalpos1 = 1;
        int intervalpos2 = 1;

	FileScan data;
	iterator.Iterator nlj, a2;
	int num_attrs = 2;        //for the initial table. 

        AttrType[] attr = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
	
        data = readtable(parent, 2);		//the node is at position 2.
	if(data !=  null)
          System.out.println("able to read data successfully!"); 	
        
	AttrType[] DTypes = {new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString),new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString)}; 
        Tuple t = new Tuple();

	System.out.println("Printing data from table and checking if valid");
        try{
           while( (t = data.get_next()) != null)
           {
                System.out.println("valid tuple! : ");
		t.print(DTypes);
           }
        }
	catch(Exception e){
		e.printStackTrace();
        }		


	nlj = createjoinquery(data, child, intervalpos1, intervalpos2, attr, rel, num_attrs);       //Takes as input,
        System.out.println("Query created successfully!"); 
	
   
        try{
           while( (t = nlj.get_next()) != null)
           {
		System.out.println("\n\nValid tuple!");
		t.print(DTypes);
           }
        }
	catch(Exception e){
		e.printStackTrace();
        }
   
  }

} //end of class



@SuppressWarnings("ALL")
public class JoinTest
{

public static ArrayList<Integer>[] graph;
public static String[] keys;
public static int[][] PC;
public static int[][] AD;
public static int n;
public static int tagcount;

public static ArrayList<Integer>[] getgraph() throws IOException {
		// TODO Auto-generated method stub

		String path = System.getProperty("user.dir") + "/patterntree";			//a A -> C (AD)
		System.out.println(path);
		File file = new File(path);

                BufferedReader br = null;
		int n,i,count,tagcount;
                i = n = count = tagcount = 0;
		String line;

		try{
			br = new BufferedReader(new FileReader(file));
		   	line = br.readLine();
			n = Integer.parseInt(line);
			keys = new String[n+1];
			i = 0;
			count = n;
		}
		 catch(IOException ioe){
			ioe.printStackTrace();
		      }

 		String[] tagmap = new String[count];		//store the mapping from node number to tag. 

		//get the tags from the next few lines. 
		while(count >= 1)
		{
		      String tag = "";
                      try{
			tag = br.readLine();
		      }
                      catch(IOException ioe){
			ioe.printStackTrace();
		      }
			tagcount++;				//keep count of no of tags. 
			keys[tagcount] = tag;
			count--;
			System.out.println(tag);
		}
		
		PC = new int[n+1][n+1];			//parent child matrix. 
		AD = new int[n+1][n+1];
		
		for(i = 0 ; i <= n ; i++)
			for(int j = 0 ; j <= n ; j++)
				AD[i][j] = PC[i][j] = 0;
		
		System.out.println("Starting next loop now : \n");
	
		graph = new ArrayList[n+1];
		for(i = 0 ; i <= n ; i++)
			graph[i] = new ArrayList<Integer>();
		
		int desc_count[] = new int[n+1];
		for(i = 0 ; i <= n ; i++)
			desc_count[i] = 0;			//count no of descendants for each node. 
		
		while((line = br.readLine()) != null)
		{
			String tokens[] = line.split(" ");
			int a = Integer.parseInt(tokens[0]); //1 2 PC
			int d = Integer.parseInt(tokens[1]);
			String rel = tokens[2];
	//		System.out.println(tokens[0] + " " +  tokens[1] +  " " + rel);
			if(rel == "AD")
				AD[a][d] = 1;
			else
				PC[a][d] = 1;
			graph[a].add(d);
			desc_count[a]++;
		}
		
		int root = 1;
	
		for(i = 0 ; i <= n ; i++)
		{
			System.out.println(graph[i]);
		}		
	
	boolean visited[] = new boolean[n+1];
	for(i = 1 ; i <= n ; i++)
		visited[i] = false;	
	br.close();
        return graph;
/*
	List<Integer> pathtillnow = new ArrayList<Integer>();
	dfs(graph,1,pathtillnow,1);			//perform a dfs on graph to find all root to leaf paths in this tree
	System.out.println(prefix);
*/
   }



  public static void main(String argv[]) throws IOException, JoinsException
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriverJT jjoin = new JoinsDriverJT();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }


//CODE BELOW IS FOR CREATING PATTERN TREE. REFER THESE FUNCTIONS :
    try {
    graph = getgraph();
    }
    catch(IOException ioe){
	ioe.printStackTrace();
    }
    try{
	System.out.println("n = " + n);
	jjoin.generate_query(graph, keys, PC, AD,n+1);
    }
    catch(Exception e){

    }
  }
}

