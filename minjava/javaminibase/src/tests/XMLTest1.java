package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;
import iterator.Iterator;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.charset.*;

import static tests.XMLTest.GetDistinctValues;
/*
This class keeps track of a heap file and its field after a join has been made between two tables
when we join two table we keep track of the fields which are present in the resultant table so when a new table is added we can eleminate the duplicate columns
that is why we keep track of the fields or maintain a list<Integer>
*/

@SuppressWarnings("Duplicates")
class XMLDriver1 implements GlobalConst {
    private BufferedReader reader;
    private XMLLineParser xmlobj;
    private Stack<XMLNode> stack = null;
    private int IntervalNo;
    private Tuple xmlDriverTuple = null;

    private Heapfile hpfile = null;
    private RID rid = null;
    private String hpfilename;
    public String[] tagnames;

    private AttrType[] Stypes = null;
    private short[] Ssizes = null;
    private int SizeofTuple;
    NodeContext GlobalMainTagPair;
    public int queryplancount = 0;

    public XMLDriver1(String fileName) {
        try {
            this.reader = new BufferedReader(new FileReader(fileName)); //initalize the file reader
            this.xmlobj = new XMLLineParser();    //initialize the class constructor
            this.stack = new Stack<XMLNode>(); //create a stack for pushing and popping the nodes of XML this will help in assigning proper interval values
            this.IntervalNo = 1;
            this.rid = null;
            this.hpfilename = "XMLtags.in";

            //Attrtypes , these are used for the sethdr function which helps set the offset to where the data is stored
            this.Stypes = new AttrType[3];
            this.Stypes[0] = new AttrType(AttrType.attrInteger);
            this.Stypes[1] = new AttrType(AttrType.attrInterval);
            this.Stypes[2] = new AttrType(AttrType.attrString);

            this.Ssizes = new short[1];
            this.Ssizes[0] = 10; //first elt. is 10

            String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.xmldb";
            String logpath = "/tmp/" + System.getProperty("user.name") + ".xmllog";
            SystemDefs sysdef = new SystemDefs(dbpath, 50000, MINIBASE_BUFFER_POOL_SIZE, "Clock");

            this.xmlDriverTuple = new Tuple();
            try {
                this.xmlDriverTuple.setHdr((short) 3, Stypes, Ssizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

            this.SizeofTuple = this.xmlDriverTuple.size();
            this.xmlDriverTuple = new Tuple(this.SizeofTuple);
            try {
                this.xmlDriverTuple.setHdr((short) 3, Stypes, Ssizes); //set the header for the tuple to be stored in heap file
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

            try {
                this.hpfile = new Heapfile(this.hpfilename);
            } catch (Exception e) {
                System.err.println("*** error in Heapfile constructor ***");
                e.printStackTrace();
            }

        } catch (IOException e) {
            System.out.println("Cannot load file");
        }
    }

    public AttrType[] GetAttrType() {
        return this.Stypes;
    }

    public short[] GetStrSizes() {
        return this.Ssizes;
    }

    public int GetTupleSize() {
        return this.SizeofTuple;
    }

    //push node on the stack , push when we get an opening node for a XML
    private void PushOnStack(String nodename) {
        XMLNode xmlNode;
        if (this.stack.empty()) {
            xmlNode = new XMLNode(-1, this.IntervalNo, nodename);        //if it is the root then parent value should be -1
        } else {
            xmlNode = new XMLNode(this.stack.peek().getStartOfInterval(), this.IntervalNo, nodename); //otherwise get everyone parent and store it
        }
        this.stack.push(xmlNode);
        this.IntervalNo += 1;
    }

    //pop a node from the stack, when you pop a node you get the end interval of the tag and you can now write it to the heap file
    private void PopFromStackWriteFile() throws IOException, EmptyStackException {
        XMLNode xmlNode = stack.pop();
        xmlNode.setEndOfInterval(this.IntervalNo);
        this.IntervalNo += 1;
        PushNodeToHeapFile(xmlNode);
    }

    //Write all the tags of an XML line into the heap file (either push it or pop it from the stack)
    private void WriteFileLbyL(List<String> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            String nname = nodes.get(i);
            if (nname.charAt(1) == '/')  //if node is an end node then pop it from stack
            {
                try {
                    PopFromStackWriteFile();
                } catch (IOException e) {
                    System.out.println("Write File Error");
                }
            } else {
                PushOnStack(nname.replaceAll("[/<>]", ""));
            }
        }
    }


    //When a node is popped from the stack it has to be pushed in the heap file (now we can actually store it in a heap file becuase we have all the
    //information of the tag, start , end parent and tag
    private void PushNodeToHeapFile(XMLNode xmlNode) {
        try {
            int parent = xmlNode.getParentStartId();
            intervaltype val = new intervaltype();
            String tag = xmlNode.getNodeString();

            val.assign(xmlNode.getStartOfInterval(), xmlNode.getEndOfInterval());

            xmlDriverTuple.setIntFld(1, parent);
            xmlDriverTuple.setIntervalFld(2, val);
            xmlDriverTuple.setStrFld(3, tag);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile.insertRecord() ***");
            e.printStackTrace();
        }

        try {
            this.rid = this.hpfile.insertRecord(this.xmlDriverTuple.returnTupleByteArray()); //store it in the heap file
            intervaltype temp = this.xmlDriverTuple.getIntervalFld(2);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile.insertRecord() ***");
            e.printStackTrace();
        }
    }


    //This function reads each line of an XML and passes the data to the parsing function and at the end it stores it in the heap file
    public NodeContext ReadFileLbyLStoreInHeapFile() {
        String line;
        List<String> parsedxml = null;
        NodeContext tgpr = null;

        try {
            int count = 0;
            while ((line = this.reader.readLine()) != null) {
                parsedxml = xmlobj.ParseXMLLine(line);
                WriteFileLbyL(parsedxml);
                count++;
            }
            this.reader.close();
            tgpr = new NodeContext(this.hpfile, this.rid, this.hpfilename, GetAttrType(), GetStrSizes(), GetTupleSize());

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            parsedxml.clear();
            this.stack.clear();
            return tgpr;
        }
    }


    //This function generates the FLdSpec array which give the projections. i.e after joining two table which colums to keep from the outer and inner table.
    //we keep all the column from the outer table and only omit those columns which are passed in the ltfieldtoomit list.
    //This generate the condexpr required to see the containment or equality criteria
    //ContainOrEquality == true check containment or else check equality

    public FldSpec[] GetProjections(int outer, int inner, List<Integer> ltfieldtoomit) {
        FldSpec[] projections = new FldSpec[outer + inner - (3 * ltfieldtoomit.size())];
        RelSpec rel_out = new RelSpec(RelSpec.outer);
        RelSpec rel_in = new RelSpec(RelSpec.innerRel);

        int[] fieldstokeep = new int[(inner / 3) - ltfieldtoomit.size()];
        int k = 0;
        for (int i = 1; i <= (inner / 3); i++) {
            if (ltfieldtoomit.indexOf(i) == -1) {
                fieldstokeep[k] = i;
                k += 1;
            }
        }
        for (int i = 0; i < outer; i++) {
            projections[i] = new FldSpec(rel_out, i + 1);
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
        return projections;
    }





    public CondExpr[] GenerateCondExpr(int opersymbfld1, int opersymbfld2, boolean ContainOrEquality) {
        CondExpr[] outFilter = new CondExpr[1];

        outFilter[0] = new CondExpr();
        outFilter[0].next = null;

        if (ContainOrEquality) {
            outFilter[0].op = new AttrOperator(AttrOperator.aopLT);            //if you need to check containment
        } else {
            outFilter[0].flag = 0;
            outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);        //if you need to check equality
        }

        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), opersymbfld1);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), opersymbfld2);

        return outFilter;
    }

    //This funtion joins the attr type of two table depending upon the projection specified
    public AttrType[] JoinAttrtype(AttrType[] tagattrtype1, AttrType[] tagattrtype2, int outer, FldSpec[] projections) {
        AttrType[] JoinedTagAttrtype = new AttrType[projections.length];
        System.arraycopy(tagattrtype1, 0, JoinedTagAttrtype, 0, outer);

        for (int i = outer; i < projections.length; i++) {
            JoinedTagAttrtype[i] = tagattrtype2[projections[i].offset - 1];
        }
        return JoinedTagAttrtype;
    }

    //This function joins the attrstrsizes of the two tables depending upon the field to omit list
    public short[] JoinAttrsize(short[] tagattrsize1, short[] tagattrsize2, List<Integer> fieldtoomit) {
        short[] JoinedTagsize = new short[tagattrsize1.length + tagattrsize2.length - fieldtoomit.size()];
        System.arraycopy(tagattrsize1, 0, JoinedTagsize, 0, tagattrsize1.length);
        int pos = 0;

        for (int i = tagattrsize1.length; i < JoinedTagsize.length; i++) {
            JoinedTagsize[i] = tagattrsize2[pos];
            pos += 1;
        }
        return JoinedTagsize;
    }

    //This function calculates the tuple size after it has been joined
    public int JoinedTupSize(AttrType[] JoinedTagAttrtype, short[] JoinedTagsize) {
        Tuple temptup = new Tuple();
        try {
            temptup.setHdr((short) JoinedTagAttrtype.length, JoinedTagAttrtype, JoinedTagsize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }
        return temptup.size();
    }

    //parentchildflag == true check parent child or else check ancester descendant
    //ContainOrEquality == true check containment or else check equality


    public NodeContext JoinTwoFields(NodeContext tag1, int joinfieldno1, NodeContext tag2, int joinfieldno2, List<Integer> ltfieldtoomit,
                                     boolean parentchildflag, boolean ContainOrEquality) {
        return JoinTwoFields(tag1, joinfieldno1, tag2, joinfieldno2, ltfieldtoomit, parentchildflag, ContainOrEquality, false);
    }


    public NodeContext JoinTwoFields(NodeContext tag1, int joinfieldno1, NodeContext tag2, int joinfieldno2, List<Integer> ltfieldtoomit,
                                     boolean parentchildflag, boolean ContainOrEquality, boolean indexToBeUsed) {


        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

        AttrType[] tagattrtype1 = tag1.getTupleAtrTypes();
        short[] tagattrsize1 = tag1.getTupleStringSizes();
        int tagtupsize1 = tag1.getNodeSizeofTuple();
        String taghpfilename1 = tag1.getNodeHeapFileName();
        int outer = tagattrtype1.length;

        AttrType[] tagattrtype2 = tag2.getTupleAtrTypes();
        short[] tagattrsize2 = tag2.getTupleStringSizes();
        int tagtupsize2 = tag2.getNodeSizeofTuple();
        String taghpfilename2 = tag2.getNodeHeapFileName();
        int inner = tagattrtype2.length;


        AttrType[] JoinedTagAttrtype = null;
        short[] JoinedTagsize = null;
        int JoinedTagTupSize = 0;

        byte[] array = new byte[10];
        new Random().nextBytes(array);

        String JoinedTaghpfilename = new String(array, Charset.forName("UTF-8"));
        Heapfile JoinedTaghpfile = null;

        RID JoinedTagRID = new RID();

        FldSpec[] projlist_tag1 = null, projlist_tag2 = null;
        List<Integer> ftoO1 = new ArrayList<Integer>();
        List<Integer> ftoO2 = ltfieldtoomit;
        projlist_tag1 = GetProjections(outer, 0, ftoO1);            //generate projections for the left table
        projlist_tag2 = GetProjections(outer, inner, ftoO2);        //generate projections for the right table


        CondExpr[] outFilter = null;
        outFilter = GenerateCondExpr(joinfieldno1, joinfieldno2, ContainOrEquality);    //generate condexpr for the two joining fields

        JoinedTagAttrtype = JoinAttrtype(tagattrtype1, tagattrtype2, outer, projlist_tag2);
        JoinedTagsize = JoinAttrsize(tagattrsize1, tagattrsize2, ltfieldtoomit);
        JoinedTagTupSize = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);

        Iterator fscan = null;


        if(tag1.getBtf() != null && indexToBeUsed){
            try {
                fscan = new IntervalIndexScan(new IndexType(IndexType.Interval), taghpfilename1, tag1.getIntervalTreeIndexString(),
                        tagattrtype1, tagattrsize1, tagattrtype1.length, projlist_tag1.length, projlist_tag1, null, 2, false);  //file scan pointer
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
            }
        }
        else if(taghpfilename1 != null){
            try {
                fscan = new FileScan(taghpfilename1, tagattrtype1, tagattrsize1, (short) outer, outer, projlist_tag1, null);  //file scan pointer
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
            }
        }


        Iterator nlj = null;
        try {
            if(fscan == null){
                fscan = tag1.getItr();
            }
            if(taghpfilename2 != null || tag1.getBtf() != null){
                nlj = new NestedLoopsJoins(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, 10, fscan, taghpfilename2, outFilter, null, projlist_tag2, outer + inner - (3 * ltfieldtoomit.size()));
            }else{
                nlj = new SortMerge(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, joinfieldno1, 8, joinfieldno2, 8, 10, fscan, tag2.getItr(), false, false,
                        ascending, outFilter, projlist_tag2, outer + inner - (3 * ltfieldtoomit.size()));
            }
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple temptup;
        int parent;
        intervaltype intval;

        NodeContext nodeContext = new NodeContext(JoinedTaghpfile, JoinedTagRID, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
        nodeContext.setItr(nlj);
        return nodeContext;
    }


    //Extract all the data from the heap file which match to the given tags in tagnames array and create new heap file for each of them
//basically this function creates new heap files where each individual heap file contains only a particular tag value


    public NodeContext[] ExtractTagToHeap(NodeContext tag_params, String[] tagnames) {

        System.out.println("In extracttagtoheap. Creating heap files and indexes for all the tags....");
        Heapfile heaptosearch = null;
        try {
            heaptosearch = new Heapfile(tag_params.getNodeHeapFileName());
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        RID ridtosearch = tag_params.getNodeRID();

        int tot_len = tagnames.length;
        Heapfile[] heaptostore = new Heapfile[tot_len];
        IntervalTreeFile[] intervalTreeFiles = new IntervalTreeFile[tot_len];
        RID[] ridtostore = new RID[tot_len];
        String[] hpfilenm = new String[tot_len];
        NodeContext[] tag_pars = new NodeContext[tot_len];
        for (int i = 0; i < tot_len; i++) {
            hpfilenm[i] = tagnames[i] + queryplancount + ".in";
        }
        boolean done = false;
        String tag;
        int sizetup;
        AttrType[] Stps = GetAttrType();
        short[] Sszs = GetStrSizes();
        int TupSize = GetTupleSize();
        Tuple tup = new Tuple(TupSize);
        try {
            tup.setHdr((short) 3, Stps, Sszs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < tot_len; i++) {
                heaptostore[i] = new Heapfile(hpfilenm[i]);
                intervalTreeFiles[i] = new IntervalTreeFile("IntervalTreeIndex" + tagnames[i] + queryplancount, AttrType.attrInterval, 8, 1);
            }
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        Scan scan = null;
        try {
            scan = heaptosearch.openScan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        while (!done) {
            try {
                tup = scan.getNext(ridtosearch);
                if (tup == null) {
                    done = true;
                    break;
                }
                tup.setHdr((short) 3, Stps, Sszs);
                tag = tup.getStrFld(3);

                for (int i = 0; i < tot_len; i++) {
                    if (tag.equals(tagnames[i])) {
                        try {
                            ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray()); //inset the data into a separate heap file
                            intervalTreeFiles[i].insert(new IntervalKey(tup.getIntervalFld(2)), ridtostore[i]);
                            break;
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < tot_len; i++) {
            NodeContext nodeContext = new NodeContext(heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize);
            nodeContext.setBtf(intervalTreeFiles[i]);
            nodeContext.setIntervalTreeIndexString("IntervalTreeIndex" + tagnames[i] + queryplancount);
            tag_pars[i] = nodeContext; //generate tag pars data type for each of the heap file
        }
        return tag_pars;
    }

    public void AddField(List<Integer> FieldTracker, int num) {
        if (FieldTracker.indexOf(num) == -1) {
            FieldTracker.add(num);
        }
    }

    //This gives which fields can be joined for resultant join and a new joined table
    public int[] GetFieldsToJoin(List<Integer> FieldTracker, int ftfld, int scfld) {
        int[] fields = new int[2];
        if (FieldTracker.indexOf(ftfld) > -1) {
            fields[0] = (3 * FieldTracker.indexOf(ftfld)) + 2;
            fields[1] = 2;
        } else if (FieldTracker.indexOf(scfld) > -1) {
            fields[0] = (3 * FieldTracker.indexOf(scfld)) + 2;
            fields[1] = 5;
        } else {
            fields[0] = -1;
            fields[1] = -1;
        }
        return fields;
    }

    //prepare the list of fields which are reuqired to be ommited from the second table , as in the common fields
    public List<Integer> GetMyOmitList(List<Integer> FieldTracker, List<Integer> ltfieldtoomit, int ftfld, int scflt) {
        ltfieldtoomit.clear();
        if (FieldTracker.indexOf(ftfld) > -1) {
            ltfieldtoomit.add(1);
        }
        if (FieldTracker.indexOf(scflt) > -1) {
            ltfieldtoomit.add(2);
        }
        return ltfieldtoomit;
    }

    //parentchildflag == true check parent child or else check ancester descendant
    //ContainOrEquality == true check containment or else check equality

    public TagparamField Query(NodeContext[] ExtractTags, String[] Joins) {
        return Query(ExtractTags, Joins, true);
    }

    public TagparamField Query(NodeContext[] ExtractTags, String[] Joins, boolean toBeIndexed) {
        NodeContext ResultTagPar = null;
        NodeContext TempTagPar = null;
        List<Integer> FieldTracker = new ArrayList<Integer>();
        List<Integer> ltfieldtoomit = new ArrayList<Integer>();

        for (int i = 0; i < Joins.length; i++) {
            String[] Jnfields = Joins[i].split(" ");
            int ftfld = Integer.parseInt(Jnfields[0]) - 1;
            int scfld = Integer.parseInt(Jnfields[1]) - 1;
            String typerel = Jnfields[2];

            boolean relflag = false;
            int[] fields = null;

            if (typerel.equals("PC")) {
                relflag = true;
            }

            //Note that initially field to omit list would be empty. This field to omit thing is mainly being done to create projections.
            TempTagPar = null;
            if (tagnames[ftfld].equalsIgnoreCase("*")) {                                                //not a star query
                TempTagPar = JoinTwoFields(GlobalMainTagPair, 2, ExtractTags[scfld], 2, ltfieldtoomit, relflag, true, toBeIndexed);
            } else if (tagnames[scfld].equalsIgnoreCase("*")) {
                System.out.println("Entered here for * based join");
                TempTagPar = JoinTwoFields(ExtractTags[ftfld], 2, GlobalMainTagPair, 2, ltfieldtoomit, relflag, true, toBeIndexed);
            } else {
                TempTagPar = JoinTwoFields(ExtractTags[ftfld], 2, ExtractTags[scfld], 2, ltfieldtoomit, relflag, true, toBeIndexed);
            }
            if (ResultTagPar != null) {
                fields = GetFieldsToJoin(FieldTracker, ftfld, scfld); //get which fields need to be joined
                ltfieldtoomit = GetMyOmitList(FieldTracker, ltfieldtoomit, ftfld, scfld); //get which fields need to be omitted
                boolean countonly = true;
                ResultTagPar = JoinTwoFields(ResultTagPar, fields[0], TempTagPar, fields[1], ltfieldtoomit, false, false);
                AddField(FieldTracker, ftfld);
                AddField(FieldTracker, scfld);
                ltfieldtoomit.clear();
            } else {
                ResultTagPar = TempTagPar;
                AddField(FieldTracker, ftfld);
                AddField(FieldTracker, scfld);
            }
        }
        return new TagparamField(ResultTagPar, FieldTracker);
    }

    public String[] GetJoins(String[] NumberofJoins, int st, int end) {
        String[] subjoins = new String[end - st];
        int k = 0;
        for (int i = st; i < end; i++) {
            subjoins[k] = NumberofJoins[i];
            k += 1;
        }
        return subjoins;
    }

    // This function joins the two tables obtained from two queires and finally gives you result which we print as query result obtained
    public TagparamField JoinQuery(TagparamField query1, TagparamField query2) {
        NodeContext tagpar1 = query1.GetTagParams();
        NodeContext tagpar2 = query2.GetTagParams();
        List<Integer> fldtrk1 = query1.GetFldtrk();
        List<Integer> fldtrk2 = query2.GetFldtrk();

        List<Integer> ltfieldtoomit = new ArrayList<Integer>();

        int[] fields = new int[2];

        for (int i = 0; i < fldtrk2.size(); i++) {
            if (fldtrk1.indexOf(fldtrk2.get(i)) > -1) {
                ltfieldtoomit.add(i + 1);
            }
        }

        for (int i = 0; i < fldtrk2.size(); i++) {
            if (fldtrk1.indexOf(fldtrk2.get(i)) > -1) {
                fields[0] = 3 * fldtrk1.indexOf(fldtrk2.get(i)) + 2;
                fields[1] = 3 * i + 2;
                break;
            }
        }


        NodeContext ResQueryJoin = JoinTwoFields(tagpar1, fields[0], tagpar2, fields[1], ltfieldtoomit, false, false);
        return new TagparamField(ResQueryJoin,  new ArrayList<Integer>());

    }

    //check which queries are possible and give out two such number which can be used to split the joins
    // so for each query we have a certain number of ancesteer descendant or parent child joins given we try to create split in the joins
    // and try to see whether the split creates will be able to form a successfull join on its own, when we get two such successful split values we
    //return the two number and those two number will be used to get our two query plans
    public int[] querypossible(String[] NumberofJoins) {
        int count = 0;

        List<Integer> firlot = new ArrayList<Integer>();
        List<Integer> seclot = new ArrayList<Integer>();
        int splitter = 1;
        int lenoflist = NumberofJoins.length;
        int[] splitlist = new int[]{-1, -1};
        boolean possiblesec = true;

        while (splitter < lenoflist) {
            String[] tempstr;
            for (int i = 0; i < splitter; i++) {
                tempstr = NumberofJoins[i].split(" ");
                firlot.add(Integer.parseInt(tempstr[0]));
                firlot.add(Integer.parseInt(tempstr[1]));
            }
            //if all the join given below are not possible then discard this candidate
            for (int i = splitter; i < lenoflist; i++) {
                tempstr = NumberofJoins[i].split(" ");
                if (seclot.size() != 0) {
                    if (seclot.indexOf(Integer.parseInt(tempstr[0])) == -1 && seclot.indexOf(Integer.parseInt(tempstr[1])) == -1) {
                        possiblesec = false;
                        break;
                    }
                }
                seclot.add(Integer.parseInt(tempstr[0]));
                seclot.add(Integer.parseInt(tempstr[1]));
            }
            if (possiblesec) {
                for (int i = 0; i < firlot.size(); i++) {
                    if (seclot.indexOf(firlot.get(i)) > -1) {
                        splitlist[count] = splitter;
                        count += 1;
                        break;
                    }
                }
            }
            splitter += 1;
            firlot.clear();
            seclot.clear();
            possiblesec = true;
            if (count == 1) //if we get two query plans then break (more query plans are also possible)
            {
                break;
            }
        }
        return splitlist;
    }

    //this just executes the three queries
    public TagparamField[] MakeQueryPlanner(NodeContext[] AllTags, String[] NumberofJoins, int noOfPlans) {
        TagparamField tf1 = null;
        TagparamField tf2 = null;

        TagparamField[] tgprarr = new TagparamField[noOfPlans];

        //PCounter.initialize();

        tgprarr[0] = Query(AllTags, NumberofJoins);  //query 1
        System.out.println("Query plan 1 Executed");
        System.out.println("reads : " +  PageCounter.getreads() + " writes  : " +  PageCounter.getwrites());

        if(noOfPlans > 1){
            tgprarr[1] = Query(AllTags, NumberofJoins);  //query plan 2
            System.out.println("Query plan 2 Executed");
            System.out.println("reads : " +  PageCounter.getreads() + " writes  : " +  PageCounter.getwrites());
        }

        if(noOfPlans > 2){
            int[] spliter = querypossible(NumberofJoins);

            //loop for second and the third query
            for (int i = 0; i < spliter.length; i++) {
                if (spliter[i] != -1) {
                    tf1 = Query(AllTags, GetJoins(NumberofJoins, 0, spliter[i]));
                    tf2 = Query(AllTags, GetJoins(NumberofJoins, spliter[i], NumberofJoins.length));
                    tgprarr[i + 2] = JoinQuery(tf1, tf2);
                    System.out.println("reads : " +  PageCounter.getreads() + " writes : " +  PageCounter.getwrites());
                    System.out.printf("Query Plan " +  (i + 2) + " Executed ");
                } else {
                    System.out.printf("Query plan : " + (i + 2) + " Not avaiable");
                }
            }
        }
        return tgprarr;
    }

    //Get the main heap file and the query file and fire queries
    public TagparamField[] ReadQueryAndExecute(NodeContext MainTagpair, String Queryfilename, int noOfPlans) {
        List<String> querylinelist = null;
        try {
            String line;
            querylinelist = new ArrayList<String>();
            BufferedReader queryreader = new BufferedReader(new FileReader(Queryfilename));
            while ((line = queryreader.readLine()) != null) {
                querylinelist.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int numberofnodes = Integer.parseInt(querylinelist.get(0));
        String[] searchtags = new String[numberofnodes];
        for (int i = 1; i <= numberofnodes; i++) {
            searchtags[i - 1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
        }
        this.tagnames = searchtags;

        String[] NumberofJoins = new String[querylinelist.size() - numberofnodes - 1];
        for (int i = 0; i < NumberofJoins.length; i++) {
            NumberofJoins[i] = querylinelist.get(numberofnodes + 1 + i);
        }
        NodeContext[] AllTags = ExtractTagToHeap(MainTagpair, searchtags);  //get all the heap file for each tag

        System.out.println("XML File Read");

        TagparamField[] finalresult = MakeQueryPlanner(AllTags, NumberofJoins, noOfPlans);
        return finalresult;
    }

    //scan a heap file and print all the values of each
    public void ScanHeapFile(NodeContext tgprms) {
        boolean done = false;
        int count_records = 0;

        Heapfile hpfl = null;
        try {
            hpfl = new Heapfile(tgprms.getNodeHeapFileName());
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        RID filerid = tgprms.getNodeRID();
        AttrType[] Atrtyps = tgprms.getTupleAtrTypes();
        short[] Strsizes = tgprms.getTupleStringSizes();
        int TupSize = tgprms.getNodeSizeofTuple();
        int numoffields = Atrtyps.length;
        Tuple temptup = null;
        Scan scan = null;
        try {
            scan = hpfl.openScan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        while (!done) {
            try {
                temptup = scan.getNext(filerid);
                if (temptup == null) {
                    done = true;
                    break;
                }
                temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
                temptup.print(Atrtyps);
                //System.out.println();
                count_records += 1;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total results found are: " + count_records);
    }


    public void printItr(NodeContext tgprms) {
        boolean done = false;
        int count_records = 0;

        AttrType[] Atrtyps = tgprms.getTupleAtrTypes();
        short[] Strsizes = tgprms.getTupleStringSizes();
        int TupSize = tgprms.getNodeSizeofTuple();
        int numoffields = Atrtyps.length;
        Tuple temptup = null;
        Iterator itr = tgprms.getItr();


        try {
            while ((temptup = itr.get_next()) != null) {
                temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
                temptup.print(Atrtyps);
                System.out.println("Tuple Size :" + temptup.size());
                count_records += 1;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Total results found are: "+count_records);
    }

    public int GetNumberOfNodes(String filename)
    {
        int numberofnodes = -1;
        try{
            BufferedReader br = new BufferedReader(new FileReader(filename));
            numberofnodes = Integer.parseInt(br.readLine());
            br.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return numberofnodes;
    }


    public void ScanHeapFile(NodeContext tgprms,boolean countonly)
    {
        boolean done = false;
        int count_records = 0;

        Heapfile hpfl = null;
        try {
            hpfl = new Heapfile(tgprms.getNodeHeapFileName());
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        RID filerid = tgprms.getNodeRID();
        AttrType[] Atrtyps = tgprms.getTupleAtrTypes();
        short[] Strsizes = tgprms.getTupleStringSizes();
        int TupSize = tgprms.getNodeSizeofTuple();
        int numoffields = Atrtyps.length;
        Tuple temptup = null;
        Scan scan = null;
        try
        {
            scan = hpfl.openScan();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        while (!done)
        {
            try
            {
                temptup = scan.getNext(filerid);
                if (temptup == null)
                {
                    done = true;
                    break;
                }
                temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
                if(countonly == false) {
                    temptup.print(Atrtyps);
                }
                //System.out.println();
                count_records += 1;
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        System.out.printf("Total records fetched = %s\n", count_records);
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
        TupleOrder sortorder1 = new TupleOrder(TupleOrder.Descending);
        int npages = 10;
        int outer = numcolumns;
        List<Integer> fieldstoomit = new ArrayList<Integer>();

        FldSpec[] projections = GetProjections(outer, 0, fieldstoomit);
        int intervalindex = -1;



        int fieldtosort = 3*node + 2; //which field number to sort on?



        Sort sort = null;
        try {
            sort = new Sort(attrtypes, numcolumns, strsizes, context.getItr(), fieldtosort, sortorder1, sortfldlen, npages);
        }
        catch (Exception e) {

            e.printStackTrace();
        }

        Tuple tuple = new Tuple();

        try {
            tuple.setHdr(numcolumns, attrtypes, strsizes);
            int count = 0;
            while ((tuple = sort.get_next()) != null) {
                //System.out.println("Tuple number : " + count);
                tuple.print(attrtypes);
                count++;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }


    //This function executes ONLY query plan 1, and instead of returning the NodeContext object, it returns the TagParam object which has the field tracker information.
    //This woud be needed for joining the two files, as we need to know where a given field is in the table.
    public TagparamField ExecuteQueryPlan1(NodeContext MainTagpair, String Queryfilename)
    {
        NodeContext QueryResult = null;
        List<String> querylinelist = null;
        try
        {
            String line;
            querylinelist = new ArrayList<String>();
            BufferedReader queryreader = new BufferedReader(new FileReader(Queryfilename));
            while ((line = queryreader.readLine()) != null)
            {
                querylinelist.add(line);
            }
        }

        catch(IOException e)
        {
            e.printStackTrace();
        }

        int numberofnodes = Integer.parseInt(querylinelist.get(0));

        //populate ancestor descendant matrix for a given pattern tree.
        String[] searchtags = new String[numberofnodes];
        for(int i=1;i<=numberofnodes;i++)
        {
            searchtags[i-1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
        }

        String[] NumberofJoins = new String[querylinelist.size()-numberofnodes-1];
        for(int i=0;i<NumberofJoins.length;i++)
        {
            NumberofJoins[i] = querylinelist.get(numberofnodes+1+i);
            String[] anc_desc = NumberofJoins[i].split(" ");
            int anc = Integer.parseInt(anc_desc[0])-1;
            int desc = Integer.parseInt(anc_desc[1])-1;
            System.out.println("Ancestor is " + anc + " It's descendant is : " + desc);
        }

        NodeContext[] AllTags = ExtractTagToHeap(MainTagpair, searchtags);  //get all the heap file for each tag
        System.out.println("File Parsing Completed");
        TagparamField tagparams = QueryPlan1(AllTags, NumberofJoins);
        return tagparams;
    }

    //This function just runs one query plan, instead of the 3 as seen in the MakeQueryPlanner
    public TagparamField QueryPlan1(NodeContext[] AllTags, String[] NumberofJoins)
    {
        TagparamField tf1 = null;
        tf1 = Query(AllTags, NumberofJoins);  //query 1
        System.out.println("Query 1 executed");
        System.out.printf("reads = %s writes = %s\n", PageCounter.getreads(), PageCounter.getwrites());
        return tf1;
    }




}
//parentchildflag == true check parent child or else check ancester descendant
//ContainOrEquality == true check containment or else check equality

@SuppressWarnings("Duplicates")
public class XMLTest1// implements  GlobalConst
{
    public static String inputPatternTreeFilesFolder = "/home/aravamuthan/Documents/codebase/minjava/javaminibase/src/tests/input_files/";
    public static String codeBaseSrcFolder = "/home/aravamuthan/Documents/codebase/minjava/javaminibase/src/";
    public static String codeBaseTestsFolder = "/home/aravamuthan/Documents/codebase/minjava/javaminibase/src/tests/";
    public static String codeBaseFolder = "/home/aravamuthan/Documents/codebase/minjava/javaminibase/";

    static TagparamField GetDistinctValues1(TagparamField table,int index){


        int fld = 3*table.GetFldtrk().indexOf(index) + 3;			//GET STRING FIELD. POSITION WITHIN ORIGINAL HEAP FILE,
        Iterator itr = table.GetTagParams().getItr();
        AttrType[] attrtypes = table.GetTagParams().getTupleAtrTypes();
        short[] strsizes = table.GetTagParams().getTupleStringSizes();
        short numflds = (short)attrtypes.length;
        int tuplesize = table.GetTagParams().getNodeSizeofTuple();
        String filename = table.GetTagParams().getNodeHeapFileName();

        FldSpec[] project = new FldSpec[1];
        project[0] = new FldSpec(new RelSpec(RelSpec.outer),fld);     //get string position at this field.

        Heapfile resfile = null;
        RID rid = new RID();

        Tuple tuple = new Tuple();
        TagparamField result = null;
        List<Integer> tracker = new ArrayList<Integer>();

        AttrType[] resultattrtypes = {new AttrType(AttrType.attrString)};
        short[] resultstrsizes = {10};

        HashSet<String> set = new HashSet<String>();

        try {
            resfile = new Heapfile("distinct.in");
            tuple.setHdr((short)1,resultattrtypes,resultstrsizes);
            while((tuple = itr.get_next()) != null) {
                String val = tuple.getStrFld(fld);
                if(set.contains(val) == false) {
                    set.add(val);
                    rid = resfile.insertRecord(tuple.returnTupleByteArray());
                }
            }
            NodeContext context = new NodeContext(resfile,rid,"distinct.in",resultattrtypes,resultstrsizes,1);
            result = new TagparamField(context,tracker);
        }
        catch(Exception e){
            e.printStackTrace();
        }

        return result;
    }


    public static void main(String [] argvs) {
        System.out.println("---------------------MENU------------------------");
        System.out.println("What would you like to do? ");
        System.out.println("1. Do a node join between two heap files based on node id : (NJ i j)");
        System.out.println("2. Do a tag join between two heap files based on node id : (TJ tag)");
        System.out.println("3. Sort a file based on a given tag : (SRT i)");
        System.out.println("4. Group a file based on a given tag : (GRP i)");
        System.out.println("Enter your choice : ");

        int choice = 0;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        try {
            choice = Integer.parseInt(br.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }


        String DataFileName = codeBaseFolder + "xml_sample_data.xml"; //initializing the data file name.
        XMLDriver1 xmldvr = new XMLDriver1(DataFileName);
        NodeContext	MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();
        xmldvr.GlobalMainTagPair = MainTagPair;

        if (choice == 1) {

            String file1, file2;
            int fld1, fld2;

            try{
                System.out.println("Enter the name of first pattern tree file (Please ensure its in the src/tests folder ): ");
                file1 = br.readLine();
                System.out.println("Enter the name of second pattern tree file (Please ensure its in the src/tests folder ): ");
                file2 = br.readLine();

                System.out.println("Enter node ID from first table : ");
                int node1 = Integer.parseInt(br.readLine())-1;
                System.out.println("Enter node ID from second table : ");
                String s = br.readLine();
                int node2 = Integer.parseInt(s) - 1;


                String filepath1 = inputPatternTreeFilesFolder + file1;
                String filepath2 = inputPatternTreeFilesFolder + file2;
                boolean countonly = false;

                TagparamField tagparams1, tagparams2;
                String[] tagnames1, tagnames2;						//the tag names MAY change every time a new query file is provided!!

                //get result table for pattern tree 1.
                int numberofnodes1 = xmldvr.GetNumberOfNodes(filepath1);
                int numberofnodes2 = xmldvr.GetNumberOfNodes(filepath2);

                System.out.println("Number of nodes 1 : " + numberofnodes1 + " Number of nodes 2:  " + numberofnodes2);

                tagparams1 = xmldvr.ReadQueryAndExecute(MainTagPair,filepath1, 1)[0];
                tagnames1 = xmldvr.tagnames;

                //get result table for pattern tree 2.
                xmldvr.queryplancount++;			//indicates we are starting query plan for second file.

                tagparams2 = xmldvr.ReadQueryAndExecute(MainTagPair,filepath2, 1)[0];
                tagnames2 = xmldvr.tagnames;

                //Now in the below code, we compute all the common nodes on which we will need to join, given a certain node.

                System.out.println();

                System.out.println("Query plans executed successfully!");

                NodeContext result = null;
                List<Integer> ltfieldtoomit  = new ArrayList<Integer>();

                System.out.println("Field to omit list will now be : ");
                for(int i = 0 ; i < ltfieldtoomit.size() ; i++)
                    System.out.print(ltfieldtoomit.get(i) + " ");
                System.out.println();


                List<Integer> fldtrk1 = tagparams1.GetFldtrk();
                List<Integer> fldtrk2 = tagparams2.GetFldtrk();
                int joinfield1 = 3*fldtrk1.indexOf(node1)+2;
                int joinfield2 = 3*fldtrk2.indexOf(node2)+2;
                System.out.println("join field 1 = " + joinfield1 + " join field 2 = " + joinfield2);
                result = xmldvr.JoinTwoFields(tagparams1.GetTagParams(),joinfield1,tagparams2.GetTagParams(),joinfield2,ltfieldtoomit,false,false);
                xmldvr.printItr(result);

            }
            catch(Exception e){
                e.printStackTrace();
            }

        }

        else if (choice == 2) {

            String file1, file2;
            int fld1, fld2;

            try{
                System.out.println("Enter the name of first pattern tree file (Please ensure its in the src/tests folder ): ");
                file1 = br.readLine();
                System.out.println("Enter the name of second pattern tree file (Please ensure its in the src/tests folder ): ");
                file2 = br.readLine();

                int node1, node2;
                System.out.println("Enter the first node ID : ");
                node1 = Integer.parseInt(br.readLine())-1;
                System.out.println("Enter the second node ID : ");
                node2 = Integer.parseInt(br.readLine())-1;

                String filepath1 = inputPatternTreeFilesFolder + file1;
                String filepath2 = inputPatternTreeFilesFolder + file2;

                TagparamField tagparams1, tagparams2;
                String[] tagnames1, tagnames2;
                tagparams1 = xmldvr.ReadQueryAndExecute(MainTagPair,filepath1, 1)[0];
                tagnames1 = xmldvr.tagnames;

                xmldvr.queryplancount++;			//indicates we are starting query plan for second file.

                tagparams2 = xmldvr.ReadQueryAndExecute(MainTagPair,filepath2, 1)[0];
                tagnames2 = xmldvr.tagnames;

                System.out.println("Join on Tag executed successfully!");

                //Join the two heap files obtained, based on the node given
                List<Integer> ltfieldtoomit = new ArrayList<Integer>();

                List<Integer> fldtrk1 = tagparams1.GetFldtrk();				//get the index where the node is stored in both files.
                List<Integer> fldtrk2 = tagparams2.GetFldtrk();
                int pos1 = 3*fldtrk1.indexOf(node1) + 3;				//string based join this time
                int pos2 = 3*fldtrk2.indexOf(node2) + 3;

                NodeContext result = xmldvr.JoinTwoFields(tagparams1.GetTagParams(),pos1,tagparams2.GetTagParams(),pos2,ltfieldtoomit,false,false);
                System.out.println("results from node based join are : \n");
                xmldvr.printItr(result);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }

        else if(choice == 3) {
            TagparamField result;
            try {
                System.out.println("Enter the name of pattern tree file (Please ensure its in the src/tests folder ): ");
                String file = br.readLine();
                int[][] ad1 = null;
                String filepath = inputPatternTreeFilesFolder+file;
                result = xmldvr.ReadQueryAndExecute(MainTagPair, filepath, 1)[0];
                System.out.println("Please enter the tag on which you want to sort");
                int node = Integer.parseInt(br.readLine())-1;
                xmldvr.SortByFld(result.GetTagParams(),node);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        else if(choice == 4){
            br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the name of pattern tree file (Please ensure its in the src/tests folder ): ");

            String file = "";
            try {
                file = br.readLine();
            }
            catch(IOException ie){
                ie.printStackTrace();
            }
            TagparamField result;
            String filepath =inputPatternTreeFilesFolder+file;
            result = xmldvr.ReadQueryAndExecute(MainTagPair, filepath, 1)[0];
            System.out.println("Please enter the node id on which to join");

            int node = -1;
            try {
                node = Integer.parseInt(br.readLine()) - 1;
            }
            catch(Exception e){
                e.printStackTrace();
            }

            TagparamField tagvalues = GetDistinctValues1(result,node);  //get the distinct values for this node.
            int pos = 3*result.GetFldtrk().indexOf(node) + 3;				//get the position of string column for given node.
            List<Integer> ltfieldtoomit = new ArrayList<Integer>();
            System.out.println("node = " + node + " pos = " + pos);
            NodeContext joinresult = xmldvr.JoinTwoFields(tagvalues.GetTagParams(), 1, result.GetTagParams(), pos, ltfieldtoomit, false, false);
            System.out.println("\n\nFinal results after the join....");
            xmldvr.printItr(joinresult);
        }

        else {
            br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Which pattern tree you want to use ");
            String filename = "";
            try {
                filename = br.readLine();
            }
            catch(Exception e){
                e.printStackTrace();
            }

            String QueryFileName = inputPatternTreeFilesFolder + filename;
            try {
                boolean countonly = false;

                TagparamField[] result;
                result = xmldvr.ReadQueryAndExecute(MainTagPair, QueryFileName, 3);
                System.out.println("results for query file 1 have been obtained");
                System.out.println(PageCounter.getreads());
                System.out.println(PageCounter.getwrites());
                xmldvr.queryplancount++;			//indicates we are starting query plan for second file.
                xmldvr.printItr(result[0].GetTagParams());
                System.out.println("Calling clean up...");
            }
            catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error encountered during XML tests:\n");
                Runtime.getRuntime().exit(1);
            }
        }
    }
}



