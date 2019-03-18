package tests;
//originally from : joins.C

import global.*;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import iterator.*;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


/**
 Here is the implementation for the tests. There are N tests performed.
 We start off by showing that each operator works on its own.
 Then more complicated trees are constructed.
 As a nice feature, we allow the user to specify a selection condition.
 We also allow the user to hardwire trees together.
 */





//Creating a new class to test interval joins.
class IntervalTest1{
    public IntervalType interval;		//an interval type
    public String id;

    public IntervalTest1(IntervalType _interval, String _id, int _level){
        interval = _interval;
        id = _id;
    }
}

@SuppressWarnings("ALL")
class JoinsDriverJT1 implements GlobalConst {


    public static String[] str = new String[400000];
    public static int i = 0;
    public static int l = 0;
    public static int s = 0;
    public static int e = 0;
    public static int[] level = new int[400000];
    public static int[] start = new int[400000];
    public static int[] end = new int[400000];

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private Vector boats;
    private Vector reserves;
    private List<XmlData> xmlDataList = new ArrayList<XmlData>();

    /* New constructor for interval joins testing */
    private Vector intervals;

    /*
       Constructor
    */
    public JoinsDriverJT1() {

        //build Sailor, Boats, Reserves table
        sailors  = new Vector();
        boats    = new Vector();
        reserves = new Vector();
        intervals = new Vector();

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

        SystemDefs sysdef = new SystemDefs( dbpath, 100000, NUMBUF, "Clock" );

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
        AttrType [] Itypes = new AttrType[2];
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
        Heapfile f2 = null;
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


    public boolean runTests() throws FileNotFoundException, XMLStreamException {

        Disclaimer();


        //test7();
        //testSelect();
        testSortMerge();
        //Query7();

        System.out.println("Finished test 7 for testing interval sorting" + "\n");
        System.out.println ("Finished joins testing"+"\n");
        return true;
    }



    /* Added 7th test for testing sort function on interval type. */
    private boolean test7()
    {
        int SORTPGNUM = 12;
        boolean status = OK;
        IntervalType[] data = new IntervalType[10];
        int numintervals = 6;

        for(int i = 0 ; i < numintervals ; i++)
            data[i] = new IntervalType();



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

    private void sortMege_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
        expr[0].pc = 1;
        expr[1] = null;
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


    private void testSelect(){
        int SORTPGNUM = 12;



        boolean status = OK;
        short[] attrSize = new short[1];
        short intervalobjsize = 12;		//2 integers, 4 bytes each.


        short strsizes[] = new short[1];
        strsizes[0] = 5;				//no strings in this relation.
        attrSize[0] = 17;

//   attrSize[1] = intervalobjsize;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInterval);


        TupleOrder[] order = new TupleOrder[1];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        //  order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2, attrType, attrSize);
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
            f = new Heapfile("selectTest.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for(int i = 0 ; i < xmlDataList.size() ; i++)
        {
            try{
                t.setStrFld(1,xmlDataList.get(i).getTag());
                t.setIntervalFld(2,xmlDataList.get(i).getIt());
//           System.out.println(data[i].getStart() + " " + data[i].getEnd() + " " + data[i].getLevel());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try{
                AttrType[] DTypes = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};
                System.out.println("Inserting :  ");
                t.print(DTypes);
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        rel = new RelSpec(RelSpec.outer);
        projlist[1] = new FldSpec(rel, 2);

        CondExpr[] selectFilter = new CondExpr[2];
        selectFilter[0] = new CondExpr();

        selectFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
        selectFilter[0].type1 = new AttrType(AttrType.attrSymbol);		//The equality condition on main table.
        selectFilter[0].type2 = new AttrType(AttrType.attrString);
        selectFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        selectFilter[0].operand2.string = "A";
        selectFilter[0].next = new CondExpr();
        selectFilter[0].next.next = null;
        selectFilter[0].next.op    = new AttrOperator(AttrOperator.aopEQ);
        selectFilter[0].next.type1 = new AttrType(AttrType.attrSymbol);		//The equality condition on main table.
        selectFilter[0].next.type2 = new AttrType(AttrType.attrString);
        selectFilter[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        selectFilter[0].next.operand2.string = "B";
        selectFilter[1] = null;

        FileScan fscan = null;

        try {
            fscan = new FileScan("selectTest.in", attrType, attrSize, (short)2 , (short)2, projlist, selectFilter);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }



        int count = 0;
        t = null;

        AttrType[]  RTypes = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};	//get the types for the result tuple.
        System.out.println("\n\n******* Started printing the result tuples ********\n\n");
        try {
            while( (t = fscan.get_next()) != null ){
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

        System.out.println("--------------------Select Test completed successfully!---------------------------------");
        System.out.println("Total number of tuples accessed : " + count);
    }

    private void testSortMerge(){
        boolean status = OK;
        short[] attrSize = new short[1];
        short intervalobjsize = 12;		//2 integers, 4 bytes each.


        short strsizes[] = new short[1];
        strsizes[0] = 5;				//no strings in this relation.
        attrSize[0] = 17;

//   attrSize[1] = intervalobjsize;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInterval);


        TupleOrder[] order = new TupleOrder[1];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        //  order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test7.in"


        RID             rid;
        RID rid2;
        Heapfile        f = null;
        Heapfile        f2 = null;
        try {
            f = new Heapfile("sortMergeXml.in");
            f2 = new Heapfile("sortMergeXml1.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for(int i = 0 ; i < xmlDataList.size() ; i++)
        {
            try{
                t.setStrFld(1,xmlDataList.get(i).getTag());
                t.setIntervalFld(2,xmlDataList.get(i).getIt());
//           System.out.println(data[i].getStart() + " " + data[i].getEnd() + " " + data[i].getLevel());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try{
                AttrType[] DTypes = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};
                System.out.println("Inserting :  ");
                t.print(DTypes);
                rid = f.insertRecord(t.returnTupleByteArray());
                rid2 = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }


        System.out.print("**********************SortMerge xmlData strating *********************\n");


        // Sailors, Boats, Reserves Queries.
        System.out.print ("Query: Find the names of sailors who have reserved "
                + "boat number 1.\n"
                + "       and print out the date of reservation.\n\n"
                + "  SELECT S.sname, R.date\n"
                + "  FROM   Sailors S, Reserves R\n"
                + "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");

        System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();


        sortMege_CondExpr(outFilter);

        t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrString);
        Stypes[1] = new AttrType (AttrType.attrInterval);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 17; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        CondExpr [] selects = new CondExpr [1];
        selects = null;


        FileScan am = null;
        try {
            am  = new FileScan("sortMergeXml.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Sprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for XMLDATA");
            Runtime.getRuntime().exit(1);
        }

        AttrType [] Rtypes = new AttrType[2];
        Rtypes[0] = new AttrType (AttrType.attrString);
        Rtypes[1] = new AttrType (AttrType.attrInterval);


        short [] Rsizes = new short[1];
        Rsizes[0] = 17;
        FldSpec [] Rprojection = new FldSpec[2];
        Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FileScan am2 = null;
        try {
            am2 = new FileScan("sortMergeXml1.in", Rtypes, Rsizes,
                    (short)2, (short) 2,
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


        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

        AttrType [] jtype = new AttrType[4];
        jtype[0] = new AttrType (AttrType.attrString);
        jtype[1] = new AttrType (AttrType.attrInterval);
        jtype[2] = new AttrType (AttrType.attrString);
        jtype[3] = new AttrType (AttrType.attrInterval);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge sm = null;
        try {
            sm = new SortMerge(Stypes, 2, Ssizes,
                    Rtypes, 2, Rsizes,
                    2, 1,
                    2, 1,
                    30,
                    am, am2,
                    false, false, ascending,
                    outFilter, proj_list, 4);
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

        t = null;

        try {
            while ((t = sm.get_next()) != null) {
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
                new AttrType(AttrType.attrString)
        };
        short []   Asizes = new short[1];
        Asizes[0] = 1;

        FldSpec [] Aprojection = {				//3 fields which we want to display in the projection list.
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)
        };

        AttrType [] Btypes = {
                new AttrType(AttrType.attrInterval),
                new AttrType(AttrType.attrString)
        };
        short []   Bsizes = new short[1];
        Bsizes[0] = 1;

        FldSpec [] Bprojection = {				//3 fields which we want to display in the projection list.
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)
        };


        FileScan am = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns.
        try {
            am  = new FileScan("intervals.in", Atypes, Asizes,
                    (short)2,(short)2,
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
                new FldSpec(new RelSpec(RelSpec.innerRel),2)
        };

        System.out.println("Outfilter[0] = " + outFilter[0]);
        System.out.println(outFilter[0].type1);
        System.out.println(outFilter[0].type2);


        NestedLoopsJoins nlj = null;
        NestedLoopsJoins nlj2 = null;

        try {
            nlj = new NestedLoopsJoins (Atypes, 2, Asizes,
                    Btypes, 2, Bsizes,
                    30,
                    am, "intervals.in",
                    outFilter, null, proj1, 4);
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
                new AttrType(AttrType.attrString)
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
            e.printStackTrace();
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



    //read the data table
    public FileScan readtable(String key, int pos)	//read in the initial heapfile and return pointer.
    {
        boolean status = OK;
        String nodes[] = {"A","B","C","D","E"};

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
        try{
            f = new Heapfile("intervals.in");
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
                t.setIntervalFld(1, (IntervalType) intervals.elementAt(i));   //Interval String <>
                t.setStrFld(2, nodes[i]);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() for intervals relation ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
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
            am  = new FileScan("intervals.in", Itypes, Isizes,
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
        expr[0].next = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);		//S.interval > R.interval
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),intervalpos1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),intervalpos2);

        if(rel == "AD")
            expr[0].ad = 1;
        else
            expr[0].pc = 1;

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
    public iterator.Iterator createjoinquery(FileScan am, String child , int intervalpos1, int intervalpos2, AttrType[] attr, String rel, int num_attrs, int pos) throws JoinsException
    {

        boolean status = OK;

        CondExpr[] outFilter = new CondExpr[2];		//will give 3
        outFilter[0] = new CondExpr();

//define conditions for join query
        createjoinquery_condexpr(outFilter, rel, intervalpos1, intervalpos2);

        System.out.println("conditional expression created");

        AttrType[] Stypes = new AttrType[num_attrs];
        for(int i = 0 ; i < num_attrs ; i++)
            Stypes[i] = new AttrType(attr[i].attrType);

        short [] Ssizes = new short[1];
        Ssizes[0] = 1; //first elt. is 30

        /*Code for filescan using select*/

        /* Attribute types for the big intervals.in relation */
        AttrType [] Rtypes = new AttrType[2];				/*Here he's defining all the data types for the sailors relation*/
        Rtypes[0] = new AttrType(AttrType.attrInterval);
        Rtypes[1] = new AttrType(AttrType.attrString);

//SOS
        short [] Rsizes = new short[1];
        Rsizes[0] = 1;

        FldSpec[] RProjection = new FldSpec[2];
        RProjection[0] =  new FldSpec(new RelSpec(RelSpec.outer), 1);
        RProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FileScan table = null;

        CondExpr[] selectFilter = new CondExpr[1];
        selectFilter[0] = new CondExpr();
        selectFilter[0].next = null;
        selectFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
        selectFilter[0].type1 = new AttrType(AttrType.attrSymbol);		//The equality condition on main table.
        selectFilter[0].type2 = new AttrType(AttrType.attrString);
        selectFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),pos);
        selectFilter[0].operand2.string = child;

        try {
            table  = new FileScan("intervals.in", Rtypes, Rsizes, (short)2, (short)2, RProjection, selectFilter);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        try{
            Tuple t = new Tuple();
            AttrType[] Dtypes = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
            System.out.println("Scanned tuples = " );
            while((t = table.get_next()) != null)
                t.print(Dtypes);
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
                    table, "intervals.in",
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
            System.err.println ("*** Error setting up scan for sailors");
            Runtime.getRuntime().exit(1);
        }

/*
Tuple t = new Tuple();
t.setHdr(short(num_attrs+2), )
*/

        return (iterator.Iterator)nlj;			//returns the nested loop join object constructed.
    }

    /* First read parent table. Then perform one level of join. */
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

        int intervalpos1 = 1;
        int intervalpos2 = 1;

        FileScan data;
        iterator.Iterator a1, a2;
        int num_attrs = 2;        //for the initial table.

        AttrType[] attr = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};

        data = readtable(parent, 2);		//the node is at position 2.
        if(data !=  null)
            System.out.println("able to read data successfully!");

        AttrType[] DTypes = {new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString)};
        Tuple t = new Tuple();
/*
        try{
           while( (t = data.get_next()) != null)
		t.print(DTypes);
        }
	catch(Exception e){
		e.printStackTrace();
        }
*/

        a2 = createjoinquery(data, child, intervalpos1, intervalpos2, attr, rel, num_attrs, 2);       //Takes as input,
        System.out.println("Query created successfully!");

   /*
        try{
           while( (t = a2.get_next()) != null)
		t.print(DTypes);
        }
	catch(Exception e){
		e.printStackTrace();
        }
   */
    }

} //end of class



@SuppressWarnings("ALL")
public class JoinTestFinal
{

    public static ArrayList<Integer>[] graph;
    public static String[] keys;
    public static int[][] PC;
    public static int[][] AD;




    public static ArrayList<Integer>[] getgraph() throws IOException {
        // TODO Auto-generated method stub

        String path = System.getProperty("user.dir") + "/data";
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
            keys[tagcount] = tag;
            tagcount++;
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




    public static void main(String argv[]) throws IOException, JoinsException, XMLStreamException {
        boolean sortstatus;
        //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
        //JavabaseDB.openDB("/tmp/nwangdb", 5000);

        JoinsDriverJT1 jjoin = new JoinsDriverJT1();

        String [] temp = new String[2];



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
            jjoin.genqueryplan(graph, keys, PC, AD);
        }
        catch(Exception e){

        }

    }
}

