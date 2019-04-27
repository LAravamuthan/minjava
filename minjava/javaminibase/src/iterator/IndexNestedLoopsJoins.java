package iterator;


import btree.*;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
/**
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class IndexNestedLoopsJoins  extends Iterator
{
    private RID rid;
    private RID outerscanrid;
    private AttrType      _in1[],  _in2[];
    private   int        in1_len, in2_len;
    private IntervalTFileScan inner;            //the index object to iterate through inner relation
    private   short t2_str_sizescopy[];
    private short[] t1_str_sizes;
    private short[] t2_str_sizes;
    private   CondExpr OutputFilter[], RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean     done;         // Is the join complete
    private boolean  get_from_outer;                 // if TRUE, a tuple is got from outer
    private   Tuple     outer_tuple, inner_tuple;
    private   Tuple     Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private   Heapfile  outerfile,innerfile;
    private Scan outer;
    private int joinfieldno1;
    private int joinfieldno2;
    private IntervalTreeFile intervalfile;
    int tuplestoview = 1;                   //we will look at the fields for


    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param in2  Array containing field types of S
     *@param len_in2  # of columns in S
     *@param  t2_str_sizes shows the length of the string fields.
     *@param amt_of_mem  IN PAGES
     *@param outFilter   select expressions
     *@param rightFilter reference to filter applied on right i/p
     *@param proj_list shows what input fields go where in the output tuple
     *@param n_out_flds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */

    /*
    * Ony difference is outer file fields will come after inner file fields. This is because inner file contains the table on the left, and outerfile contains the table on the right
    * */
    public IndexNestedLoopsJoins( AttrType    in1[],
                                int     len_in1,
                                short   t1_str_sizes[],
                                AttrType    in2[],
                                int     len_in2,
                                short   t2_str_sizes[],
                                int     amt_of_mem,
                                IntervalTreeFile  intervalfile,     //an interval tree file using which index scan object will be created.
                                Heapfile outerfile,         //the heap file for the given relation.
                                  Heapfile innerfile,      //heap file for inner relation.
                                CondExpr outFilter[],
                                  CondExpr rightFilter[],       //the filters to be applied to right hand side table.
                                FldSpec   proj_list[],         //The list of fields to be projected.
                                int        n_out_flds,
                                  int joinfieldno1,
                                  int joinfieldno2
    ) throws IOException,NestedLoopException
    {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        this.joinfieldno1 = joinfieldno1;
        this.joinfieldno2 = joinfieldno2;
        this.intervalfile = intervalfile;
        outerscanrid = new RID();
        this.t1_str_sizes = t1_str_sizes;
        this.t2_str_sizes = t2_str_sizes;
        this.innerfile = innerfile;

        try{
            outer = outerfile.openScan();            //open up a scan on outer heap file.
        }
        catch(Exception e){
            e.printStackTrace();
        }

        t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Tuple();
        outer_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;

        System.out.println("Attribute types for in1 : ");
        for(int i = 0 ; i < in1.length ; i++)
            System.out.print(in1[i].toString() + " ");

        System.out.println("Attribute types for in2 : ");
        for(int i = 0 ; i < in1.length ; i++)
            System.out.print(in2[i].toString() + " ");
        System.out.println("No of outfields = " + nOutFlds);


        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in2, len_in2, in1, len_in1,
                    t2_str_sizes, t1_str_sizes,
                    proj_list, nOutFlds);
        }catch (TupleUtilsException e){
            throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        }
    }

    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions

     */
    public Tuple get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        // This is a DUMBEST form of a join, not making use of any key information...


        intervaltype outerinterval = null;
        intervaltype innerinterval = null;

        if (done)
            return null;

        do
        {
            // If get_from_outer is true, Get a tuple from the outer, create a new
            //index on the inner relation, based on the interval value fetched from outer.
            //Use that to get tuples from inner relation, one by one
            // If a get_next on the outer returns DONE?, then the nested loops
            //join is done too.

            if (get_from_outer == true)
            {
                outer_tuple = outer.getNext(outerscanrid);
                //if there are no more tuples left in outer relation.
                if(outer_tuple == null)
                {
                    done = true;
                    if (inner != null){
                        inner = null;
                    }
                    return null;
                }

                try {
                    outer_tuple.setHdr((short)in1_len, _in1, t1_str_sizes);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                /*
                if(outer_tuple != null)
                    System.out.println("outer has data");
                */

                get_from_outer = false;
                try {
       //             System.out.println("Reached inside try...");
                    int condition = 0;               //before or equal.
                    intervaltype target = new intervaltype();
         //           System.out.println("Outer tuple currently looked at : ");
          //          outer_tuple.print(_in1);
          //          System.out.println("Getting interval field : ");
          //          System.out.println("Join field no for outer is joinfieldno2 : " + joinfieldno1);
          //          System.out.println("Attribute types for outer : ");
          //          for(int i = 0 ; i < _in1.length ; i++)
           //             System.out.printf(_in1[i].toString() + " ");
       //             System.out.println();
 //                   outer_tuple.print(_in1);
                    intervaltype inter = outer_tuple.getIntervalFld(joinfieldno1);           //get the interval value stored at joinfieldno1, which is the field for the outer tuple.
                    int keyS = inter.get_s();
                    int keyE = inter.get_e();
          //          System.out.println("keyS = " + keyS + " KeyE = " + keyE);
                    target.assign(keyS,keyE);
                    IntervalKey targetkey = new IntervalKey(target);
                    inner = intervalfile.new_scan(targetkey, condition);            //based on the start value of the interval column of outer tuple, create an index on inner relation.
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }
            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            RID rid = new RID();
            KeyDataEntry entry;
            int innertuples = 0;
            while ((entry = inner.get_next()) != null)
            {
                rid = ((LeafData) entry.data).getData();                //get the record using the RID
                inner_tuple = innerfile.getRecord(rid);             //based on the RID fetched from the index, get the row from inner file.
                inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizes);  //set headers for inner tuple.

         //       System.out.println("page no : " + rid.pageNo + " slot no: " + rid.slotNo);
                outerinterval = outer_tuple.getIntervalFld(joinfieldno1);
                innerinterval = inner_tuple.getIntervalFld(joinfieldno2);
                int innerS = innerinterval.get_s();
                int innerE = innerinterval.get_e();
                int outerS = outerinterval.get_s();
                int outerE = outerinterval.get_e();

                /*
                if(tuplestoview > 0){
                    System.out.println("outer tuple is : ");
                    outer_tuple.print(_in1);
                    System.out.println("tuple number : " + innertuples + " inner tuple is : ");
                    inner_tuple.print(_in2);
                    innertuples++;
                }
                */

        //        System.out.println("Inner tuple is : ");
//                inner_tuple.print(_in2);                                                      //inner is the parent interval, outer is the child interval.
                if (outerS >= innerS && outerE <= innerE)                                       //the outer interval(child) must be contained in inner(parent)       // PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true
                {
         //           System.out.println("Rightfilter eval is true...");
                    /*
                    if (PredEval.Eval(OutputFilter, inner_tuple, outer_tuple, _in2, _in1) == true)      //outer contains the descendant tuples, inner contains ancestor tuples. My inner should contain my outer.
                    {
         //               System.out.println("OutputFilter Eval is true...Joining tuples");
                        // Apply a projection on the outer and inner tuples.
                     */
                        Projection.Join(inner_tuple, _in2,
                                outer_tuple, _in1,
                                Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                }
            }
            //}

//              if(tuplestoview > 0)
  //               System.out.println("No of inner tuples viewed = " + innertuples);
  //            tuplestoview--;
            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag) {
            try {
                outer.closescan();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}







