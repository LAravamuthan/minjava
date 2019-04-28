package tests;
import global.*;
import heap.Scan;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import java.util.ArrayList;
import java.util.List;

public class SortByFld {

    public FldSpec[] GetProjections(int outer, int inner, List<Integer> ltfieldtoomit)
    {
        FldSpec[] projections = new FldSpec[outer+inner-(3*ltfieldtoomit.size())]; //outer = 18, inner = 9.
        RelSpec rel_out = new RelSpec(RelSpec.outer);
        RelSpec rel_in = new RelSpec(RelSpec.innerRel);

        int len = (inner/3)-ltfieldtoomit.size();     //which fields should I keep from the inner table?
        //	System.out.println("In GetProjections : outer = " + outer + " inner = " + inner + " fields to keep size = " + len);

        System.out.println("outer = " + outer + " inner = " + inner + " ltfieldtoomit " + len);
        for(int i=0;i<outer;i++)
        {
            projections[i] = new FldSpec(rel_out, i+1);			//for outer table.
        }


        int[] fieldstokeep = new int[len];
        int k = 0;									//if there are any fields to keep from inner table, then add those fields to the projections list.

        if(len > 0) {
            for (int i = 1; i <= (inner / 3); i++) {
                System.out.println("i = " + i);
                if (ltfieldtoomit.indexOf(i) == -1) {
                    System.out.println("Adding " + i + " to fields to keep at position : " + k);
                    fieldstokeep[k] = i;
                    k += 1;
                }
            }

            k = outer;
            for (int i = 0; i < fieldstokeep.length; i++) {
                int pos = 3 * (fieldstokeep[i] - 1) + 1;
                for (int p = 0; p < 3; p++) {
                    projections[k] = new FldSpec(rel_in, pos);
                    k += 1;
                    pos += 1;
                }
            }
        }

        System.out.println("Done with GetProjections..");
        return projections;
    }


    void SortByFld(NodeContext context, int node)
    {
        System.out.println("Entered the sort function...");
        System.out.println("name of file : " + context.getNodeHeapFileName());
        AttrType[] attrtypes = context.getTupleAtrTypes();
        short[] strsizes = context.getTupleStringSizes();
        short numcolumns = (short)attrtypes.length;
        int sortfldlen = 8;            //the length of interval field.
        TupleOrder sortorder = new TupleOrder(TupleOrder.Ascending);
        int npages = 10;
        int outer = numcolumns;
        List<Integer> fieldstoomit = new ArrayList<Integer>();

        FldSpec[] projections = GetProjections(outer, 0, fieldstoomit);
        int intervalindex = -1;

        FileScan fscan = null;
        Scan scanobj = null;

        try{
//            System.out.println("Heap file name is : " + context.getNodeHeapFileName());
            scanobj = context.getNodeHeapFile().openScan();
            if(scanobj == null) {
 //               System.out.println("scan object i null!");
            }
            else{
                System.out.println("Scan object not null :D");
            }
            RID rid = new RID();
            Tuple tuple = new Tuple();
            tuple.setHdr(numcolumns,attrtypes,strsizes);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        scanobj.closescan();           //done with the scan.
        int fieldtosort = 3*node + 2; //which field number to sort on?

        System.out.println("Will need to sort on the " + fieldtosort + " column ");
        boolean status;
        try {
            fscan = new FileScan(context.getNodeHeapFileName(), attrtypes, strsizes, numcolumns, numcolumns, projections, null);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }
   //     System.out.println("Filescan created successfully!");


        Sort sort = null;
        try {
            sort = new Sort(attrtypes, numcolumns, strsizes, fscan, fieldtosort, sortorder, sortfldlen, npages);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        Tuple tuple = new Tuple();

        try {
            tuple.setHdr(numcolumns, attrtypes, strsizes);
            int count = 0;
            while ((tuple = sort.get_next()) != null) {
                System.out.println("Tuple number : " + count);
                tuple.print(attrtypes);
                count++;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }

        fscan.close();
    }
}
