package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.charset.*;
/*
This class keeps track of a heap file and its field after a join has been made between two tables
when we join two table we keep track of the fields which are present in the resultant table so when a new table is added we can eleminate the duplicate columns
that is why we keep track of the fields or maintain a list<Integer> 
*/

class TagparamField {
    private NodeContext tgpr = null;
    private List<Integer> Fldtrk = null;

    public TagparamField(NodeContext tagpar, List<Integer> fldtrk) {
        this.tgpr = tagpar;
        this.Fldtrk = fldtrk;
    }

    public NodeContext GetTagParams() {
        return this.tgpr;
    }

    public List<Integer> GetFldtrk() {
        return this.Fldtrk;
    }
}


/*
This class contains parsing function which are responsible for parsing the XML into tags

*/


class XMLLineParser {
    public XMLLineParser() {
    }

    // This will extract all the tags from a XML line contained in < and > characters
    public List<String> ExtraxtContent(String line) {
        List<String> XMLTags = new ArrayList<String>();
        // String[] tag_values = line.trim();
        line = line.trim();
        int firstspace = line.indexOf(" ");
        if (firstspace < 0) {
            String justelem = line.length() > 5 ? line.substring(0, 5) : line; //trim if lenght is greater than 5 characters
            XMLTags.add("<" + justelem + ">");
            return XMLTags;
        }

        String justelem = line.substring(0, firstspace);
        justelem = justelem.length() > 5 ? justelem.substring(0, 5) : justelem;
        XMLTags.add("<" + justelem + ">");

        line = line.substring(firstspace + 1) + " ";
        String[] tag_values = line.split("\" ");

        for (int i = 0; i < tag_values.length; i++) {
            tag_values[i] = tag_values[i].trim();
            String[] tv = tag_values[i].split("=\"");
            String tg = tv[0].length() > 5 ? tv[0].substring(0, 5) : tv[0];
            String vl = tv[1].length() > 5 ? tv[1].substring(0, 5) : tv[1];
            XMLTags.add("<" + tg + ">");
            XMLTags.add("<" + vl + ">");
            XMLTags.add("</" + vl + ">");
            XMLTags.add("</" + tg + ">");
        }
        return XMLTags;
    }


    //This will parser an entire XML line and return a list of string which will contain the tags
    public List<String> ParseXMLLine(String line) {

        List<String> XMLTags;
        line = line.trim();
        int lnlt = line.length();
        int counter = lnlt - line.replace("<", "").length();

        if (counter == 1) {
            if (line.charAt(lnlt - 2) == '/') {
                XMLTags = ExtraxtContent(line.substring(1, lnlt - 2));
                int lenftag = XMLTags.get(0).length();
                XMLTags.add("</" + XMLTags.get(0).substring(1, lenftag - 1) + ">");
            } else {
                XMLTags = ExtraxtContent(line.replaceAll("[<>]", ""));
            }
            return XMLTags;
        } else {
            int brk_indx = line.indexOf(">");
            XMLTags = ExtraxtContent(line.substring(1, brk_indx));
            line = line.substring(brk_indx + 1);
            String vlu = line.substring(0, line.indexOf("<")).replaceAll("[/]", "");
            if (!vlu.equals("")) {
                vlu = vlu.length() > 5 ? vlu.substring(0, 5) : vlu;
                XMLTags.add("<" + vlu + ">");
                XMLTags.add("</" + vlu + ">");
            }
            int lenftag = XMLTags.get(0).length();
            XMLTags.add("</" + XMLTags.get(0).substring(1, lenftag - 1) + ">");
            return XMLTags;
        }
    }
}


@SuppressWarnings("Duplicates")
class XMLDriver implements GlobalConst {
    private BufferedReader reader;
    private XMLLineParser xmlobj;
    private Stack<XMLNode> stack = null;
    private int IntervalNo;
    private Tuple xmlDriverTuple = null;

    private Heapfile hpfile = null;
    private RID rid = null;
    private String hpfilename;

    private AttrType[] Stypes = null;
    private short[] Ssizes = null;
    private int SizeofTuple;

    public XMLDriver(String fileName) {
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

    public NodeContext JoinTwoFields(NodeContext tag1, int joinfieldno1, NodeContext tag2, int joinfieldno2, List<Integer> ltfieldtoomit, boolean parentchildflag, boolean ContainOrEquality) {

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


		System.out.println("Tag1 attribute types ");
		for(int  i = 0 ; i < tagattrtype1.length ; i++)
			System.out.printf(tagattrtype1[i].toString() + " ");         //what kind of fields are present in both tables?
		System.out.println("Tag2 attribute types ");
		for(int  i = 0 ; i < tagattrtype2.length ; i++)
			System.out.println(tagattrtype2[i].toString() + " ");         //what kind of fields are present in both tables?
        AttrType[] JoinedTagAttrtype = null;
        short[] JoinedTagsize = null;
        int JoinedTagTupSize = 0;

        byte[] array = new byte[10];
        new Random().nextBytes(array);

		String JoinedTaghpfilename = joinfile+filecount;//new String(array, Charset.forName("UTF-8"));
        Heapfile JoinedTaghpfile = null;
		System.out.println("Heapfile being constructed : " + JoinedTaghpfilename);

        try {
            JoinedTaghpfile = new Heapfile(JoinedTaghpfilename);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        RID JoinedTagRID = new RID();

        FldSpec[] projlist_tag1 = null, projlist_tag2 = null;
        List<Integer> ftoO1 = new ArrayList<Integer>();
        List<Integer> ftoO2 = ltfieldtoomit;
        projlist_tag1 = GetProjections(outer, 0, ftoO1);            //generate projections for the left table
        projlist_tag2 = GetProjections(outer, inner, ftoO2);        //generate projections for the right table

		/*
		for(int i=0;i<projlist_tag2.length;i++)
		{
			System.out.println(projlist_tag2[i].offset);
		}*/

        CondExpr[] outFilter = null;
        outFilter = GenerateCondExpr(joinfieldno1, joinfieldno2, ContainOrEquality);    //generate condexpr for the two joining fields

        JoinedTagAttrtype = JoinAttrtype(tagattrtype1, tagattrtype2, outer, projlist_tag2);
        JoinedTagsize = JoinAttrsize(tagattrsize1, tagattrsize2, ltfieldtoomit);
        JoinedTagTupSize = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);
	/*
		System.out.println("jere");
		System.out.printf("%s %s\n", joinfieldno1, joinfieldno2);
	for(int i=0;i<JoinedTagsize.length;i++)
		{
			System.out.println(JoinedTagsize[i]);
		}


		for(int i=0;i<JoinedTagAttrtype.length;i++)
		{
			System.out.println(JoinedTagAttrtype[i].toString());
		}*/

        FileScan fscan = null;
        try {
            fscan = new FileScan(taghpfilename1, tagattrtype1, tagattrsize1, (short) outer, outer, projlist_tag1, null);  //file scan pointer
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
        }

        NestedLoopsJoins nlj = null;
        try {
            //initalizing nested loop join consructor
            nlj = new NestedLoopsJoins(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, 10, fscan, taghpfilename2, outFilter, null, projlist_tag2, outer + inner - (3 * ltfieldtoomit.size()));
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple temptup;
        int parent;
        intervaltype intval;
        try {
            while ((temptup = nlj.get_next()) != null) {
                if (parentchildflag) {
                    if (temptup.getIntervalFld(2).get_s() == temptup.getIntFld(4)) {
                        JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());    //insert the joined record into a new heap file
                    }
                } else {
                    JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());  //get the tag RID of the resulting tuple
                }
            }
        } catch (Exception e) {
            System.err.println("*** Error preparing for get_next tuple");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
		filecount++;
        return new NodeContext(JoinedTaghpfile, JoinedTagRID, JoinedTaghpfilename, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
    }


    //Extract all the data from the heap file which match to the given tags in tagnames array and create new heap file for each of them
//basically this function creates new heap files where each individual heap file contains only a particular tag value

	void ScanFile(Heapfile file,AttrType[] attrs, short[] attrSizes){
		Scan scan = null;
		try {
			scan = file.openScan();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		Tuple t = new Tuple();
		try {
			while ((t = scan.getNext(rid)) != null) {
				t.setHdr((short)3, attrs, attrSizes);
				t.print(attrs);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
    public NodeContext[] ExtractTagToHeap(NodeContext tag_params, String[] tagnames) {

		System.out.println("In extracttagtoheap. Creating heap files and indexes for all the tags....");
        Heapfile heaptosearch = tag_params.getNodeHeapFile();
        RID ridtosearch = tag_params.getNodeRID();

        int tot_len = tagnames.length;

        Heapfile[] heaptostore = new Heapfile[tot_len];
        RID[] ridtostore = new RID[tot_len];
        String[] hpfilenm = new String[tot_len];


        NodeContext[] tag_pars = new NodeContext[tot_len];

		intervaltreefile = new IntervalTreeFile[tot_len];			//creating an interval tree object for each of the tags.
		int keyType= AttrType.attrInterval;
		try {
        for (int i = 0; i < tot_len; i++) {
            hpfilenm[i] = tagnames[i] + ".in";
				intervaltreefile[i] = new IntervalTreeFile(tagnames[i], keyType, 8, 1); //Create an interval tree file for each of the tags.
			}
		}
		catch(Exception e){
			e.printStackTrace();
        }

        boolean done = false;
        String tag;
        int sizetup;
        AttrType[] Stps = GetAttrType();
        short[] Sszs = GetStrSizes();
        int TupSize = GetTupleSize();

        Tuple tup = new Tuple(TupSize);
		try
		{
            tup.setHdr((short) 3, Stps, Sszs);
		}
		catch (Exception e)
		{
            e.printStackTrace();
        }

		try
		{
			for(int i=0;i<tot_len;i++)
			{
                heaptostore[i] = new Heapfile(hpfilenm[i]);
            }
		}
		catch (Exception e)
		{
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        Scan scan = null;
		try
		{
            scan = heaptosearch.openScan();
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
                tup = scan.getNext(ridtosearch);
				if (tup == null)
				{
                    done = true;
                    break;
                }
                tup.setHdr((short) 3, Stps, Sszs);
                tag = tup.getStrFld(3);

				for(int i=0;i<tot_len;i++)
				{
					if(tag.equals(tagnames[i]))  //A
					{
						try
						{
                            ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray()); //inset the data into a separate heap file
							if(i == 0 || i == 1) {
								tup.print(Stypes);
								System.out.println();
							}
							IntervalType keyint= tup.getIntervalFld(2);              //the second field is the interval in our data files.
							int keys = keyint.getS();
							int keye = keyint.getE();
							keyint.assign(keyint.getS(), keyint.getE());			//assign the interval values.
							rid = ridtostore[i];
							intervaltreefile[i].insert(new IntervalKey(keyint), rid); //nserting interval key and rid for each tag.
                            break;
						}
						catch (Exception e)
						{
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }
			}
			catch (Exception e)
			{
                e.printStackTrace();
            }
        }

		System.out.println("Done!");
		for(int i=0;i<tot_len;i++)
		{
			System.out.println("Looking at i = " + i + " tag : " + tagnames[i]);
            tag_pars[i] = new NodeContext(heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize); //generate tag pars data type for each of the heap file
        }
        return tag_pars;
    }

	public void AddField(List<Integer> FieldTracker, int num)
	{
		if(FieldTracker.indexOf(num) == -1)
		{
            FieldTracker.add(num);
        }
    }

    //This gives which fields can be joined for resultant join and a new joined table
	public int[] GetFieldsToJoin(List<Integer> FieldTracker, int ftfld, int scfld)
	{
		System.out.println("Field tracker is now : ");
		for(int i = 0 ; i < FieldTracker.size() ; i++)
			System.out.printf(FieldTracker.get(i) + " ");
        int[] fields = new int[2];
		if(FieldTracker.indexOf(ftfld) > -1)
		{
			System.out.println(ftfld + " is present in table : So we will join interval column at : " + (3*FieldTracker.indexOf(ftfld))+2);
            fields[0] = (3 * FieldTracker.indexOf(ftfld)) + 2;
            fields[1] = 2;
		}
		else if(FieldTracker.indexOf(scfld) > -1)
		{
			System.out.println(ftfld + " is present in table : So we will join interval column at : " + (3*FieldTracker.indexOf(scfld))+2);
            fields[0] = (3 * FieldTracker.indexOf(scfld)) + 2;
            fields[1] = 5;
		}
		else
		{
            fields[0] = -1;
            fields[1] = -1;
        }
        return fields;
    }

    //prepare the list of fields which are reuqired to be ommited from the second table , as in the common fields
	public List<Integer> GetMyOmitList(List<Integer> FieldTracker, List<Integer> ltfieldtoomit, int ftfld, int scflt)
	{
        ltfieldtoomit.clear();
		if(FieldTracker.indexOf(ftfld) > -1)
		{
            ltfieldtoomit.add(1);
        }
		if(FieldTracker.indexOf(scflt) > -1)
		{
            ltfieldtoomit.add(2);
        }
        return ltfieldtoomit;
	}


	//Given a NodeContext object for a certain file and join field, will create an index on that interval join field and return the file for use in index based joins.
	IntervalTreeFile CreateIntervalTreeFile(NodeContext tgprms, int joinfield)
	{
		boolean done = false;
		int count_records = 0;
		Heapfile hpfl       = tgprms.getNodeHeapFile();
		String heapfilename = tgprms.getNodeHeapFileName();
		RID filerid         = tgprms.getNodeRID();
		AttrType [] Atrtyps = tgprms.getTupleAtrTypes();
		short [] Strsizes   = tgprms.getTupleStringSizes();
		int TupSize         = tgprms.getNodeSizeofTuple();
		int numoffields     = Atrtyps.length;
		Tuple temptup = null;
		Scan scan = null;
		System.out.println("Creating interval tree file from file " + heapfilename);
		String filename = "intervalfile";      //we can give it one name, since we anyways destroy it after each time it has been used up.
		int keyType = AttrType.attrInterval;
		IntervalTreeFile intervaltree = null;
		try {
			intervaltree = new IntervalTreeFile(filename, keyType, 8, 1);
		}
		catch(Exception e){
			e.printStackTrace();
		}
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
				//		temptup.print(Atrtyps);
				count_records+=1;
//				System.out.println("Filerid pageno = " + filerid.pageNo + " slot no : " + filerid.slotNo);
				//inserting the given key into the corresponding heap file as well.
				IntervalType keyint= temptup.getIntervalFld(joinfield);              //the second field is the interval in our data files.
				int keys = keyint.getS();
				int keye = keyint.getE();
				keyint.assign(keyint.getS(), keyint.getE());			//assign the interval values.
				intervaltree.insert(new IntervalKey(keyint), filerid);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		System.out.println("Total records inserted into the interval tree file = %s\n" + count_records);
		return intervaltree;
	}




	//Test if you're able to join two fields using simple index nested loop join.
	//ftfld and scfld are needed in order to know which heap file we should access.
	//A variable temptagpairjoin introduced, which indicates if the join is between two base tag files/or in between complex tables.
	//Note that tag1 consists of larger table, with more columns. tag2 consists of the smaller table with 6 columns, 3 for each of the tags. We will create an index on that column.
	//Since ContainsOrEquality is false, it will do an Equality based join for above case, and thus index on smaller table will work.

	public NodeContext JoinTwoFieldsIndexJoin(NodeContext tag1, int joinfieldno1, NodeContext tag2, int joinfieldno2, List<Integer> ltfieldtoomit, boolean parentchildflag, boolean ContainOrEquality, int ftfld, int scfld, boolean temptagpairjoin)
	{

		/*
		 * Note, in our implementation, the second file, that is the one on the right is the outer file, whereas the first file, that is file on left is the inner file.
		 *
		 * */

		AttrType [] tagattrtype1  = tag1.getTupleAtrTypes();
		short    [] tagattrsize1  = tag1.getTupleStringSizes();
		int      tagtupsize1      = tag1.getNodeSizeofTuple();
		String   taghpfilename1   = tag1.getNodeHeapFileName();
		int      outer            = tagattrtype1.length;
		Heapfile outerfile = tag2.getNodeHeapFile();

		AttrType [] tagattrtype2  = tag2.getTupleAtrTypes();
		short    [] tagattrsize2  = tag2.getTupleStringSizes();
		int      tagtupsize2      = tag2.getNodeSizeofTuple();
		String   taghpfilename2   = tag2.getNodeHeapFileName();
		int      inner            = tagattrtype2.length;
		Heapfile innerfile = tag1.getNodeHeapFile();

/*
		System.out.println("Tag1 attribute types ");
		for(int  i = 0 ; i < tagattrtype1.length ; i++)
			System.out.printf(tagattrtype1[i].toString() + " ");         //what kind of fields are present in both tables?
		System.out.println("Tag2 attribute types ");
		for(int  i = 0 ; i < tagattrtype2.length ; i++)
			System.out.println(tagattrtype2[i].toString() + " ");         //what kind of fields are present in both tables?
*/

		AttrType[] JoinedTagAttrtype = null;
		short [] JoinedTagsize = null;
		int JoinedTagTupSize = 0;

		byte[] array = new byte[10];
		new Random().nextBytes(array);

		String JoinedTaghpfilename = indexjoinfile+"_"+indexfilecount;
		Heapfile JoinedTaghpfile = null;
		System.out.println("Heapfile being constructed using index join : " + JoinedTaghpfilename);

		try
		{
			JoinedTaghpfile = new Heapfile(JoinedTaghpfilename);
		}
		catch (Exception e)
		{
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		RID JoinedTagRID = new RID();

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> ftoO2 = ltfieldtoomit;
		projlist_tag1 = GetProjections(outer, 0, ftoO1);			//generate projections for the left table
		projlist_tag2 = GetProjections(outer, inner, ftoO2);		//generate projections for the right table

		/*
		for(int i=0;i<projlist_tag2.length;i++)
		{
			System.out.println(projlist_tag2[i].offset);
		}*/

		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(joinfieldno1, joinfieldno2, ContainOrEquality);	//generate condexpr for the two joining fields

		JoinedTagAttrtype = JoinAttrtype(tagattrtype1, tagattrtype2, outer, projlist_tag2);
		JoinedTagsize     = JoinAttrsize(tagattrsize1, tagattrsize2, ltfieldtoomit);
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);
	/*
		System.out.println("jere");
		System.out.printf("%s %s\n", joinfieldno1, joinfieldno2);
	for(int i=0;i<JoinedTagsize.length;i++)
		{
			System.out.println(JoinedTagsize[i]);
		}


		for(int i=0;i<JoinedTagAttrtype.length;i++)
		{
			System.out.println(JoinedTagAttrtype[i].toString());
		}*/


/*
		System.out.println("Printing out data for outer file: ");
		ScanFile(outerfile, tagattrtype1);
		System.out.println("Printing out data for inner file: ");
		ScanFile(innerfile, tagattrtype2);
*/
		IndexNestedLoopsJoins inlj = null;
		IntervalTreeFile intervalfile = null;
		try
		{
			//initalizing nested loop join consructor
			if(temptagpairjoin) {
				intervalfile = intervaltreefile[ftfld];   //if it is simply a join between two tag heap files, then get the interval file from the intervaltreefile array
			}
			else {
				intervalfile = CreateIntervalTreeFile(tag1, joinfieldno1);  //if it is a complex join, we will need to create an index on the inner file, which corresponds to tag1.
			}

//			for(int i = 0 ; i < projlist_tag2.length ; i++)
//				System.out.println(projlist_tag2[i].relation.key + " offset = " + projlist_tag2[i].offset);
			inlj = new IndexNestedLoopsJoins(tagattrtype2, inner, tagattrsize2, tagattrtype1, outer, tagattrsize1, 10, intervalfile, outerfile, innerfile, outFilter, null, projlist_tag2, outer+inner-(3*(ltfieldtoomit).size()), joinfieldno2, joinfieldno1);
		}
		catch (Exception e)
		{
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple temptup;
		int parent;
		IntervalType intval;

		try
		{
			while ((temptup=inlj.get_next()) !=null)
			{
				if(parentchildflag)
				{
					if(temptup.getIntervalFld(2).getS() == temptup.getIntFld(4))
					{
						//				System.out.println("Inserting folllowing record into the file");
						//				temptup.print(JoinedTagAttrtype);
						JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());	//insert the joined record into a new heap file
					}
				}
				else
				{
					//			System.out.println("Inserting folllowing record into the file");
					//			temptup.print(JoinedTagAttrtype);
					JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());  //get the tag RID of the resulting tuple
				}
			}
		}
		catch (Exception e)
		{
			System.err.println ("*** Error preparing for get_next tuple");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		System.out.println("The file has been created successfully using index nested loop join! :D Will print out what this file has later");
		indexfilecount++;

		if(temptagpairjoin == false) {
			try {
				System.out.println("This is a more complex join between two large tables. Hence, destroying the created interval file.");
				intervalfile.destroyFile();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new NodeContext(JoinedTaghpfile, JoinedTagRID, JoinedTaghpfilename, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
	}


	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality

	public TagparamField Query(NodeContext[] ExtractTags, String[] Joins)
	{
        NodeContext ResultTagPar = null;
        NodeContext TempTagPar = null;
		NodeContext IndexTempTagPar = null;
		NodeContext IndexResultTagPar = null;
        List<Integer> FieldTracker = new ArrayList<Integer>();
        List<Integer> ltfieldtoomit = new ArrayList<Integer>();

		for(int i=0;i<Joins.length;i++)
		{
            String[] Jnfields = Joins[i].split(" ");
            int ftfld = Integer.parseInt(Jnfields[0]) - 1;
            int scfld = Integer.parseInt(Jnfields[1]) - 1;
            String typerel = Jnfields[2];
			System.out.println("Processing join " + ftfld + "  " + scfld  + " " + typerel);
			if(ExtractTags[ftfld] == null)
				System.out.println("null for first field");
			if(ExtractTags[ftfld] == null)
				System.out.println("null for second field");
            boolean relflag = false;
            int[] fields = null;

			if(typerel.equals("PC"))
			{
                relflag = true;
				System.out.println("PC flag set");
            }

			TempTagPar = JoinTwoFieldsIndexJoin(ExtractTags[ftfld], 2, ExtractTags[scfld], 2, ltfieldtoomit, relflag, true, ftfld, scfld, true);
			System.out.println("Heapfile for TempTagPar (smaller table with 2 columns only) : " + TempTagPar.getNodeHeapFileName() + " created ");

			if(ResultTagPar != null)
			{
				System.out.println("Calling GetFieldsToJoin...");
                fields = GetFieldsToJoin(FieldTracker, ftfld, scfld); //get which fields need to be joined
				System.out.println("The fields to be joined are field[1] = " + fields[0] + " field[2] = " + fields[1]);
                ltfieldtoomit = GetMyOmitList(FieldTracker, ltfieldtoomit, ftfld, scfld); //get which fields need to be omitted
				boolean countonly = true;
				ResultTagPar = JoinTwoFieldsIndexJoin(ResultTagPar, fields[0], TempTagPar, fields[1], ltfieldtoomit, false, false, ftfld, scfld, false);
				System.out.println("Heapfile for ResultTagPar : " + ResultTagPar.getNodeHeapFileName() + " created ");
		//		ScanHeapFile(ResultTagPar,countonly);
		//		ScanHeapFile(TempTagPar,countonly);

//				ResultTagPar = JoinTwoFields(ResultTagPar, fields[0], TempTagPar, fields[1], ltfieldtoomit, false, false); //join the tags to a resultant table
				/*
				IntervalTreeFile intervalfile = CreateIntervalTreeFile(TempTagPar,fields[1]);
				System.out.println("File created successfullly! Deleting it now...");
				try {
					intervalfile.destroyFile();
				}
				catch(Exception e){
					e.printStackTrace();
				}
				*
				 */
/*
				System.out.println("No of records in ResultTagPar : " + ResultTagPar.getNodeHeapFileName());
*/
				AddField(FieldTracker, ftfld);       //takes care of this! only adds those fields where the index is -1, that is field is not in list. :D
				AddField(FieldTracker, scfld);
				System.out.println("Field tracker is now...");
				for(int j = 0 ; j < FieldTracker.size() ; j++)
					System.out.println(FieldTracker.get(j) + " ");
				ltfieldtoomit.clear();
			}
			else
			{
				ResultTagPar = TempTagPar;
				System.out.println("Heapfile for ResultTagPar : " + ResultTagPar.getNodeHeapFileName() + " created ");
				AddField(FieldTracker, ftfld);
				AddField(FieldTracker, scfld);
			}
		}
		return new TagparamField(ResultTagPar, FieldTracker);
	}

	public String[] GetJoins(String[] NumberofJoins, int st, int end)
	{
        String[] subjoins = new String[end - st];
        int k = 0;
		for(int i=st;i<end;i++)
		{
            subjoins[k] = NumberofJoins[i];
            k += 1;
        }
        return subjoins;
    }

    // This function joins the two tables obtained from two queires and finally gives you result which we print as query result obtained
	public NodeContext JoinQuery(TagparamField query1,TagparamField query2)
	{
        NodeContext tagpar1 = query1.GetTagParams();
        NodeContext tagpar2 = query2.GetTagParams();
        List<Integer> fldtrk1 = query1.GetFldtrk();
        List<Integer> fldtrk2 = query2.GetFldtrk();

        List<Integer> ltfieldtoomit = new ArrayList<Integer>();

        int[] fields = new int[2];

		for(int i=0;i<fldtrk2.size();i++)
		{
			if(fldtrk1.indexOf(fldtrk2.get(i)) > -1)
			{
                ltfieldtoomit.add(i + 1);
            }
        }

		for(int i=0;i<fldtrk2.size();i++)
		{
			if(fldtrk1.indexOf(fldtrk2.get(i)) > -1)
			{
                fields[0] = 3 * fldtrk1.indexOf(fldtrk2.get(i)) + 2;
                fields[1] = 3 * i + 2;
                break;
            }
        }


        NodeContext ResQueryJoin = JoinTwoFields(tagpar1, fields[0], tagpar2, fields[1], ltfieldtoomit, false, false);
        return ResQueryJoin;

    }

    //check which queries are possible and give out two such number which can be used to split the joins
    // so for each query we have a certain number of ancesteer descendant or parent child joins given we try to create split in the joins
    // and try to see whether the split creates will be able to form a successfull join on its own, when we get two such successful split values we
    //return the two number and those two number will be used to get our two query plans
	public int[] querypossible(String[] NumberofJoins)  
	{
        int count = 0;

        List<Integer> firlot = new ArrayList<Integer>();
        List<Integer> seclot = new ArrayList<Integer>();
        int splitter = 1;
        int lenoflist = NumberofJoins.length;
        int[] splitlist = new int[]{-1, -1};
        boolean possiblesec = true;

		while(splitter < lenoflist)
		{
            String[] tempstr;
			for(int i=0;i<splitter;i++)
			{
                tempstr = NumberofJoins[i].split(" ");
                firlot.add(Integer.parseInt(tempstr[0]));
                firlot.add(Integer.parseInt(tempstr[1]));
            }
            //if all the join given below are not possible then discard this candidate
			for(int i=splitter;i<lenoflist;i++)
			{
                tempstr = NumberofJoins[i].split(" ");
				if(seclot.size()!=0)
				{
					if(seclot.indexOf(Integer.parseInt(tempstr[0])) == -1  &&  seclot.indexOf(Integer.parseInt(tempstr[1])) == -1)
					{
                        possiblesec = false;
                        break;
                    }
                }
                seclot.add(Integer.parseInt(tempstr[0]));
                seclot.add(Integer.parseInt(tempstr[1]));
            }
			if(possiblesec)
			{
				for(int i=0;i<firlot.size();i++)
				{
					if(seclot.indexOf(firlot.get(i)) > -1)
					{
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
            if (count == 2) //if we get two query plans then break (more query plans are also possible)
            {
                break;
            }
        }
        return splitlist;
    }

    //this just executes the three queries
	public NodeContext[] MakeQueryPlanner(NodeContext[] AllTags, String[] NumberofJoins)
	{
        TagparamField tf1 = null;
        TagparamField tf2 = null;

        NodeContext[] tgprarr = new NodeContext[3];

        //PCounter.initialize();

        tgprarr[0] = Query(AllTags, NumberofJoins).GetTagParams();  //query 1
        System.out.println("Query plan 1 Executed");
        System.out.println("reads : " +  PageCounter.getreads() + " writes  : " +  PageCounter.getwrites());

        int[] spliter = querypossible(NumberofJoins);

        //loop for second and the third query
        for (int i = 0; i < spliter.length; i++) {
            if (spliter[i] != -1) {
                tf1 = Query(AllTags, GetJoins(NumberofJoins, 0, spliter[i]));
                tf2 = Query(AllTags, GetJoins(NumberofJoins, spliter[i], NumberofJoins.length));
                tgprarr[i + 1] = JoinQuery(tf1, tf2);

                System.out.println("reads : " +  PageCounter.getreads() + " writes : " +  PageCounter.getwrites());
                System.out.printf("Query Plan " +  (i + 2) + " Executed ");

            } else {
                System.out.printf("Query plan : " + (i + 2) + " Not avaiable");
            }

        }

        return tgprarr;
    }

    //Get the main heap file and the query file and fire queries
	public NodeContext[] ReadQueryAndExecute(NodeContext MainTagpair, String Queryfilename)
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
        String[] searchtags = new String[numberofnodes];
		for(int i=1;i<=numberofnodes;i++)
		{
            searchtags[i - 1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
        }

        String[] NumberofJoins = new String[querylinelist.size() - numberofnodes - 1];
		for(int i=0;i<NumberofJoins.length;i++)
		{
            NumberofJoins[i] = querylinelist.get(numberofnodes + 1 + i);
        }
        NodeContext[] AllTags = ExtractTagToHeap(MainTagpair, searchtags);  //get all the heap file for each tag

        System.out.println("XML File Read");
/*		try
		{
			MainTagpair.GetHeapFile().deleteFile();			
		}
		catch(Exception e)
		{
			e.printStackTrace();		
		}*/


        //ReverseArray(NumberofJoins);
        NodeContext[] finalresult = MakeQueryPlanner(AllTags, NumberofJoins);
        //QueryResult = Query(AllTags, NumberofJoins);
        return finalresult;
    }

    //scan a heap file and print all the values of each
	public void ScanHeapFile(NodeContext tgprms,boolean countonly)
	{
        boolean done = false;
        int count_records = 0;
        Heapfile hpfl = tgprms.getNodeHeapFile();
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

	public void cleanup()
	{
		try {
			for (int i = 0; i < indexfilecount; i++) {
				Heapfile hf = new Heapfile(indexjoinfile+"_"+indexfilecount);
				hf.deleteFile();
			}
			for(int i = 0 ; i < filecount ; i++){
				Heapfile hf = new Heapfile(joinfile+filecount);
				hf.deleteFile();
			}
			for(int i = 0 ; i < intervaltreefile.length ; i++){
				intervaltreefile[i].destroyFile();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}


}
  	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality

public class XMLTest// implements  GlobalConst
{
	public static void main(String [] argvs) 
	{
		String DataFileName = System.getProperty("user.dir") + "/tests/xml_sample_data.xml";
		String QueryFileName = System.getProperty("user.dir") + "/tests/queryfile.txt";
		try
		{
            XMLDriver xmldvr = new XMLDriver(DataFileName);

            NodeContext MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();


            NodeContext[] qresult = xmldvr.ReadQueryAndExecute(MainTagPair, QueryFileName);
			boolean countonly = false;
			for(int i=0 ; i < qresult.length ; i++)
			{
				System.out.println("results for query plan : " + i+1);
				if(qresult[i] != null)
				{
					xmldvr.ScanHeapFile(qresult[i],countonly);  //print all the heap file query results
                }

            }
            System.out.println(PageCounter.getreads());
            System.out.println(PageCounter.getwrites());
			System.out.println("Calling clean up...");
			xmldvr.cleanup();
		}
		catch (Exception e) 
		{
            e.printStackTrace();
            System.err.println("Error encountered during XML tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}



