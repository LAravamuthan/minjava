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
import java.util.concurrent.TimeUnit;


/*
This class keeps track of the node interval label, tag and the nodes parent
*/

class Node
{
	private int parent;  //store the start interval of the parent
	private int[] nodeIntLbl; //store the interval values in an array
	private String nodeTag;  // store the tag values

	public Node(int pt, int start, String Tag)
	{
		this.nodeIntLbl = new int[2];
		this.parent = pt;
		this.nodeIntLbl[0] = start;
		this.nodeTag = Tag;
	}

	public void SetEnd(int end)
	{
		this.nodeIntLbl[1] = end;
	}

	public int GetParent()
	{
		return this.parent;
	}

	public int GetStart()
	{
		return this.nodeIntLbl[0];
	}

	public int GetEnd()
	{
		return this.nodeIntLbl[1];
	}

	public int[] GetIntLabel()
	{
		return nodeIntLbl;
	}

	public String GetTag()
	{
		return this.nodeTag;
	}
}

/*
This class keeps track of a particular heap file and all its related data fields required to access the heap file
*/

class TagParams
{
	private iterator.Iterator Itrtr = null;	
	private List<Integer> FieldNos = null;

	private Heapfile tagheapfile = null; // The heap file pointer
	private RID tagrid = null; //RID for the heap file (record id) to access each record in the heap file
	private String hpfilename = null; // Name of the heap file

	private AttrType[] AtrTypes = null; // Attrtypes of the heap file
	private short[] AtrSizes = null; //Attrsize of the heap file
	private int SizeofTuple; // Size of the tuple stored in the heap file
 
	public TagParams(iterator.Iterator itr, List<Integer> fldnos, Heapfile hfile, RID rid, String hpfname, AttrType[] atrtypes, short[] atrsizes, int sizetup)
	{
		this.Itrtr = itr;
		this.FieldNos = fldnos;

		this.tagheapfile = hfile;
		this.tagrid = rid;
		this.hpfilename = hpfname;

		this.AtrTypes = atrtypes;
		this.AtrSizes = atrsizes;
		this.SizeofTuple = sizetup;
 	}

 	public iterator.Iterator GetIterator()
 	{
 		return this.Itrtr;
 	}

 	public List<Integer> GetFieldNos()
 	{
 		return this.FieldNos;
 	}

	public Heapfile GetHeapFile()
	{
		return this.tagheapfile;
	}

	public RID GetRID()
	{
		return this.tagrid;
	}

	public String GetHPFileName()
	{
		return this.hpfilename;
	}

	public void SetHPParams(Heapfile hpfile, RID rid, String fname)
	{
		this.tagheapfile = hpfile;
		this.tagrid = rid;
		this.hpfilename = fname;
	}

	public void SetIterator(iterator.Iterator itr)
	{
		this.Itrtr = itr;
	}

	public AttrType[] GetAtrTypes()
	{
		return this.AtrTypes;
	}

	public short[] GetAtrSizes()
	{
		return this.AtrSizes;
	}

	public int GetSizeofTuple()
	{
		return this.SizeofTuple;
	}

	public void ClearTagparam()
	{
		this.Itrtr = null;
		this.FieldNos = null;

		this.tagheapfile = null;
		this.tagrid = null;
		this.hpfilename = null;

		this.AtrTypes = null;
		this.AtrSizes = null;
		this.SizeofTuple = -1;	
	}
}

class BTreePars
{
	public BTreeFile btreefile = null;
	public String btreefname = null;

	public BTreePars(BTreeFile btf, String btfn)
	{
		this.btreefile = btf;
		this.btreefname = btfn;
	}

	public BTreeFile GetBTreeFile()
	{
		return this.btreefile;
	}

	public String GetBTreeFileName()
	{
		return this.btreefname;
	}
}

/*
This class contains parsing function which are responsible for parsing the XML into tags
*/


class XMLLineParser
{
	public XMLLineParser(){}

	// This will extract all the tags from a XML line contained in < and > characters
    public List<String> ExtraxtContent(String line)
    {   
        List<String> XMLTags = new ArrayList<String>();
       // String[] tag_values = line.trim();
        line = line.trim();
        int firstspace = line.indexOf(" ");
        if(firstspace < 0)
        {
            String justelem = line.length() > 5 ? line.substring(0, 5) : line; //trim if lenght is greater than 5 characters
            XMLTags.add("<"+justelem+">");
            return XMLTags;
        }

        String justelem = line.substring(0,firstspace);
        justelem = justelem.length() > 5 ? justelem.substring(0, 5) : justelem;
        XMLTags.add("<"+justelem+">");

        line = line.substring(firstspace+1)+" ";
        String[] tag_values = line.split("\" ");

        for(int i=0;i<tag_values.length;i++)
        {
            tag_values[i]=tag_values[i].trim();
            String[] tv = tag_values[i].split("=\"");
            String tg = tv[0].length() > 5 ? tv[0].substring(0, 5) : tv[0];
            String vl = tv[1].length() > 5 ? tv[1].substring(0, 5) : tv[1];
            XMLTags.add("<"+tg+">");
            XMLTags.add("<"+vl+">");
            XMLTags.add("</"+vl+">");
            XMLTags.add("</"+tg+">");
        }
       return XMLTags;
    }


    //This will parser an entire XML line and return a list of string which will contain the tags
    public List<String> ParseXMLLine(String line){

        List<String> XMLTags;
        line=line.trim();
        int lnlt = line.length();
        int counter = lnlt - line.replace("<", "").length();

        if(counter == 1)
        {
            if(line.charAt(lnlt-2) == '/')
            {
                XMLTags = ExtraxtContent(line.substring(1, lnlt-2));
                int lenftag = XMLTags.get(0).length();
                XMLTags.add("</"+XMLTags.get(0).substring(1, lenftag-1)+">");
            }
            else
            {
                XMLTags = ExtraxtContent(line.replaceAll("[<>]", ""));
            }
            return XMLTags;
        }
        else
        {
            int brk_indx = line.indexOf(">");
            XMLTags = ExtraxtContent(line.substring(1, brk_indx));
            line=line.substring(brk_indx+1);
            String vlu = line.substring(0, line.indexOf("<")).replaceAll("[/]", "");
            if(!vlu.equals("")){
                vlu=vlu.length() > 5 ? vlu.substring(0, 5) : vlu;
                XMLTags.add("<"+vlu+">");
                XMLTags.add("</"+vlu+">");       
            }
            int lenftag = XMLTags.get(0).length();
            XMLTags.add("</"+XMLTags.get(0).substring(1, lenftag-1)+">");
            return XMLTags;
        }
    }
}


class XMLDriver implements GlobalConst
{
	private BufferedReader reader;
	private XMLLineParser xmlobj;
	private Stack<Node> stack = null;
	private int IntervalNo;
	private Tuple tplwtr = null;

	private Heapfile hpfile = null;
	private RID rid = null;
	private String hpfilename;

	private AttrType[] Stypes = null;
	private short[] Ssizes = null;
	private int SizeofTuple;
	//private TagParams maintgpair = null;

	public XMLDriver(String fileName)
	{
		try
		{
			this.reader = new BufferedReader(new FileReader(fileName)); //initalize the file reader
			this.xmlobj = new XMLLineParser();	//initialize the class constructor
			this.stack = new Stack<Node>(); //create a stack for pushing and popping the nodes of XML this will help in assigning proper interval values
			this.IntervalNo = 1;
			this.rid = null;
			this.hpfilename = "XMLtags.in";

			//Attrtypes , these are used for the sethdr function which helps set the offset to where the data is stored
			this.Stypes = new AttrType[3];
			this.Stypes[0] = new AttrType (AttrType.attrInteger);
			this.Stypes[1] = new AttrType (AttrType.attrInterval);
			this.Stypes[2] = new AttrType (AttrType.attrString);

			this.Ssizes = new short [1];
			this.Ssizes[0] = 10; //first elt. is 10

			String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.xmldb"; 
			String logpath = "/tmp/"+System.getProperty("user.name")+".xmllog";
			SystemDefs sysdef = new SystemDefs( dbpath, 50000, MINIBASE_BUFFER_POOL_SIZE, "Clock" );

			this.tplwtr = new Tuple();
			try
			{
				this.tplwtr.setHdr((short) 3, Stypes, Ssizes);
			}
			catch (Exception e)
			{
				System.err.println("*** error in Tuple.setHdr() ***");
				e.printStackTrace();
			}

	    	this.SizeofTuple = this.tplwtr.size();
	    	this.tplwtr = new Tuple(this.SizeofTuple);
			try
			{
				this.tplwtr.setHdr((short) 3, Stypes, Ssizes); //set the header for the tuple to be stored in heap file
			}
			catch (Exception e)
			{
				System.err.println("*** error in Tuple.setHdr() ***");
				e.printStackTrace();
			}

			try
			{
				this.hpfile = new Heapfile(this.hpfilename);
			}
			catch (Exception e)
			{
				System.err.println("*** error in Heapfile constructor ***");
				e.printStackTrace();
			}

		}
		catch(IOException e)
		{
			System.out.println("Cannot load file");
		}
	}

	public AttrType[] GetAttrType()
	{
		return this.Stypes;
	}

	public short[] GetStrSizes()
	{
		return this.Ssizes;
	}

	public int GetTupleSize()
	{
		return this.SizeofTuple;
	}

	//push node on the stack , push when we get an opening node for a XML
	private void PushOnStack(String nodename)
	{
		Node nd;
		if(this.stack.empty())
		{
			nd = new Node(-1, this.IntervalNo, nodename);		//if it is the root then parent value should be -1
		}
		else
		{
			nd = new Node(this.stack.peek().GetStart(), this.IntervalNo, nodename); //otherwise get everyone parent and store it
		}
		this.stack.push(nd);
		this.IntervalNo+=1;
	}

	//pop a node from the stack, when you pop a node you get the end interval of the tag and you can now write it to the heap file
	private void PopFromStackWriteFile() throws IOException, EmptyStackException
	{
		Node nd = stack.pop();
		nd.SetEnd(this.IntervalNo);
		this.IntervalNo+=1;
		PushNodeToHeapFile(nd);
	}

	//Write all the tags of an XML line into the heap file (either push it or pop it from the stack)
	private void WriteFileLbyL(List<String> nodes)
	{
		for(int i=0;i<nodes.size();i++)
		{
			String nname = nodes.get(i);
			if(nname.charAt(1) == '/')  //if node is an end node then pop it from stack
			{
				try
				{
					PopFromStackWriteFile();
				}
				catch(IOException e)
				{
					System.out.println("Write File Error");
				}
			}
			else
			{
				PushOnStack(nname.replaceAll("[/<>]", ""));
			}
		}
	}


	//When a node is popped from the stack it has to be pushed in the heap file (now we can actually store it in a heap file becuase we have all the 
	//information of the tag, start , end parent and tag
	private void PushNodeToHeapFile(Node nd)
	{
		try
		{
			int parent = nd.GetParent();
			intervaltype val = new intervaltype();
			String tag = nd.GetTag();

			val.assign(nd.GetStart(), nd.GetEnd());

			tplwtr.setIntFld(1, parent);
			tplwtr.setIntervalFld(2, val);
			tplwtr.setStrFld(3, tag);	
		}
		catch(Exception e)
		{
			System.err.println("*** error in Heapfile.insertRecord() ***");
			e.printStackTrace();
		}

		try
		{
			this.rid = this.hpfile.insertRecord(this.tplwtr.returnTupleByteArray()); //store it in the heap file
		}
		catch (Exception e) 
		{
			System.err.println("*** error in Heapfile.insertRecord() ***");
			e.printStackTrace();
		}      		
	}


	//This function reads each line of an XML and passes the data to the parsing function and at the end it stores it in the heap file
	public TagParams ReadFileLbyLStoreInHeapFile()
	{
		String line;
		List<String> parsedxml = null;
		TagParams tgpr = null;

		try
		{
			while ((line = this.reader.readLine()) != null)
			{
				parsedxml = xmlobj.ParseXMLLine(line);
				WriteFileLbyL(parsedxml);
			}
			this.reader.close();
			tgpr = new TagParams(null, null, this.hpfile, this.rid, this.hpfilename, GetAttrType(), GetStrSizes(), GetTupleSize());

		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			parsedxml.clear();
			this.stack.clear();
			return tgpr;
		}
	}

 
	//This function generates the FLdSpec array which give the projections. i.e after joining two table which colums to keep from the outer and inner table.
	//we keep all the column from the outer table and only omit those columns which are passed in the ltfieldtoomit list.
	public FldSpec[] GetProjections(int outer, int inner, List<Integer> fieldtokeep)
	{
		FldSpec[] projections = new FldSpec[outer+fieldtokeep.size()];
		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		
		for(int i=0;i<outer;i++)
		{
			projections[i] = new FldSpec(rel_out, i+1);
		}
		for(int i=0;i<fieldtokeep.size();i++)
		{
			projections[outer+i]=new FldSpec(rel_in, fieldtokeep.get(i));		
		}
		return projections;
	}

	//This generate the condexpr required to see the containment or equality criteria
  	//ContainOrEquality == true check containment or else check equality
	public CondExpr[] GenerateCondExpr(int opersymbfld1, int opersymbfld2, boolean ContainOrEquality, boolean pcflag)
	{
		CondExpr[] outFilter  = new CondExpr[1];

		outFilter[0] = new CondExpr();
		outFilter[0].next = null;

		if(ContainOrEquality)
		{
			outFilter[0].op = new AttrOperator(AttrOperator.aopLT);			//if you need to check containment
			outFilter[0].pcflag = pcflag;

		}
		else
		{
			outFilter[0].pcflag = false;
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);		//if you need to check equality
		}

		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), opersymbfld1);
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), opersymbfld2);

		return outFilter;
	}

	//This funtion joins the attr type of two table depending upon the projection specified
	public AttrType[] JoinAttrtype(AttrType[] tagattrtype1, List<Integer> fieldtokeep)
	{
		AttrType[] JoinedTagAttrtype = new AttrType[tagattrtype1.length+fieldtokeep.size()];
		for(int i=0, n=JoinedTagAttrtype.length;i<n;i++)
		{
			JoinedTagAttrtype[i] = new AttrType (AttrType.attrInterval);
		}
		return JoinedTagAttrtype;
	}
 
 
	//This function calculates the tuple size after it has been joined
	public int JoinedTupSize(AttrType[] JoinedTagAttrtype, short[] JoinedStrtype)
	{
		Tuple temptup = new Tuple();
		try
		{
			temptup.setHdr((short) JoinedTagAttrtype.length, JoinedTagAttrtype, JoinedStrtype);
		}
		catch (Exception e)
		{
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		return temptup.size();	
	}


	public List<Integer> GetFieldtoKeep(List<Integer> fieldno1, List<Integer> fieldno2)
	{
		List<Integer> fieldtokeep = new ArrayList<Integer>();
		for(int i=0;i<fieldno2.size();i++)
		{
			if(fieldno1.indexOf(fieldno2.get(i)) == -1)
			{
				fieldtokeep.add(i+1);
			}
		}
		return fieldtokeep;
	}

	public List<Integer> GetCombinedFlds(List<Integer> fieldno1, List<Integer> fieldno2)
	{
		List<Integer> fields = new ArrayList<Integer>();
		for(int i=0;i<fieldno1.size();i++)
		{
			fields.add(fieldno1.get(i));
		}
		for(int i=0;i<fieldno2.size();i++)
		{
			if(fieldno1.indexOf(fieldno2.get(i)) < 0)
			{
				fields.add(fieldno2.get(i));
			}
		}
		return fields;
	}

	//This gives which fields can be joined for resultant join and a new joined table
	public int[] GetFieldsToJoin(List<Integer> fieldno1, List<Integer> fieldno2)
	{
		int[] fields = {-1, -1};
		int pos = 0;
		for(int i=0, n=fieldno2.size(); i<n; i++)
		{
			if(pos < 1)
			{
				int index = fieldno1.indexOf(fieldno2.get(i)); 
				if(index > -1)
				{
					fields[0] = index+1;
					fields[1] = i+1;
					pos++;
				}
			}
			else
			{
				break;
			}
		}
		return fields;
	}

	public String GetRandomName()
	{
		byte[] array = new byte[10]; 
		new Random().nextBytes(array); 
		return new String(array, Charset.forName("UTF-8")); 		
	}

  	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality
	public TagParams JoinTwoFields_sm(TagParams tag1, int joinfieldno1, TagParams tag2, int joinfieldno2, boolean ContainOrEquality, boolean parentchildflag)
	{

		iterator.Iterator indxscan1 = tag1.GetIterator();
    	AttrType [] tagattrtype1  = tag1.GetAtrTypes();
		short    [] tagattrsize1  = tag1.GetAtrSizes();
		int      tagtupsize1      = tag1.GetSizeofTuple();
		//String   taghpfilename1   = tag1.GetHPFileName();
		int      outer            = tagattrtype1.length;
		List<Integer> Fieldnos1   = tag1.GetFieldNos();
		int jtfld1                = joinfieldno1;

		iterator.Iterator indxscan2 = tag2.GetIterator();
    	AttrType [] tagattrtype2  = tag2.GetAtrTypes();
		short    [] tagattrsize2  = tag2.GetAtrSizes();
		int      tagtupsize2      = tag2.GetSizeofTuple();
		String   taghpfilename2   = null;
		int      inner            = tagattrtype2.length;
		List<Integer> Fieldnos2   = tag2.GetFieldNos();
		int jtfld2                = joinfieldno2;

		iterator.Iterator finalitr = null;
		AttrType[] JoinedTagAttrtype = null;
		short [] JoinedTagsize = null;
		int JoinedTagTupSize = 0;
		//List<Integer> Fieldscomb   = GetCombinedFlds(Fieldnos1, Fieldnos2);

		//String JoinedTaghpfilename = null; 
		//Heapfile JoinedTaghpfile = null;
		//RID JoinedTagRID = null;
		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		//List<Integer> ftoO1 = new ArrayList<Integer>();
		//List<Integer> ftoO2 = GetFieldtoKeep(Fieldnos1, Fieldnos2);
		int no_out_fields = 6;
		projlist_tag1 = new FldSpec[] {new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3)};			//generate projections for the left table
		projlist_tag2 = new FldSpec[] {new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3), new FldSpec(rel_in, 1), new FldSpec(rel_in, 2), new FldSpec(rel_in, 3)};		//generate projections for the right table

		/*		
		if(jtfld1==0 && jtfld2==0)
		{
			int[] jfld = GetFieldsToJoin(Fieldnos1, Fieldnos2);
			jtfld1 = jfld[0];
			jtfld2 = jfld[1];
		}*/
		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(jtfld1, jtfld2, ContainOrEquality, parentchildflag);	//generate condexpr for the two joining fields
		JoinedTagAttrtype = new AttrType[] { new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrString), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrString) };
		JoinedTagsize     = new short[] {10, 10};
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		//GenerateHeapfile(tag2);
		//taghpfilename2 = tag2.GetHPFileName();

    	SortMerge sm = null;
	    try 
	    {
			sm = new SortMerge(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, jtfld1, 10, jtfld2, 10, 2000, indxscan1, indxscan2, false, false, ascending, outFilter, projlist_tag2, no_out_fields);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for sort merge join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
		finalitr = (iterator.Iterator)sm;
		return new TagParams(finalitr, null, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
	}





	public void DeleteHeapFile(TagParams tagpar)
	{
		try
		{
			tagpar.GetHeapFile().deleteFile();
			tagpar.ClearTagparam();			
		}
		catch (Exception e) 
		{
			System.out.println("Error while deleting heap file\n");
			e.printStackTrace();
		}
	}


	public CondExpr[] GetCondExpr(String tagname)
	{
	    CondExpr[] expr = new CondExpr[2];
	    expr[0] = new CondExpr();
	    expr[0].pcflag = false;
	    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrString);
	    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    expr[0].operand2.string = tagname;
	    expr[0].next = null;
	    expr[1] = null;
	    return expr;	
	}

/*	public TagParams[] JoinAllPairTags(TagParams[] tag_param_arr, String[] Joins)
	{
		FileScan fscan = null;
		NestedLoopsJoins nlj = null;
		int TotalJoins = Joins.length;

		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		FldSpec[] proj_arr = {new FldSpec(rel_out, 1), new FldSpec(rel_out, 2)};
		FldSpec[] proj_arr_nlj = {new FldSpec(rel_out, 2), new FldSpec(rel_in, 2)};
		
		Heapfile    JoinedTaghpfile = null;
		String[]    JoinedTaghpname = new String[TotalJoins];
		RID         JoinedTagRID    = null;
		AttrType[]  JoinAttrtype    = null;
		short[]     Joinstrtype     = null;
		TagParams[] JoinedTagParam  = new TagParams[TotalJoins];
		int         JoinedTupSize   = 24;
		FileScan 	Joinedfscan     = null;

        List<Integer> fldnos = null;
		Tuple nljtuple;

        for(int i=0;i<TotalJoins;i++)
        {
        	JoinedTaghpname[i] =  GetRandomName();
        }

		for(int i=0;i<TotalJoins;i++)
		{
			String[] Jnfields = Joins[i].split(" ");
			int ftfld = Integer.parseInt(Jnfields[0])-1;
			int scfld = Integer.parseInt(Jnfields[1])-1;
			String typerel = Jnfields[2];
			boolean pcflag = typerel.equals("PC");

			String     tag1hpflname =  tag_param_arr[ftfld].GetHPFileName();
			AttrType[] tag1attrtype = tag_param_arr[ftfld].GetAtrTypes();
			short []   tag1strsz    = tag_param_arr[ftfld].GetAtrSizes();	

			String     tag2hpflname =  tag_param_arr[scfld].GetHPFileName();
			AttrType[] tag2attrtype = tag_param_arr[scfld].GetAtrTypes();
			short []   tag2strsz    = tag_param_arr[scfld].GetAtrSizes();	

			CondExpr[] outFilter = GenerateCondExpr(2, 2, true, pcflag);

			try 
			{
				fscan = new FileScan(tag1hpflname, tag1attrtype, tag1strsz, (short) 2, 2, proj_arr, null);  //file scan pointer 
				nlj = new NestedLoopsJoins(tag1attrtype, 2, tag1strsz, tag2attrtype, 2, tag2strsz, 50, fscan, tag2hpflname, outFilter, null, proj_arr_nlj, 2);
				JoinedTaghpfile = new Heapfile(JoinedTaghpname[i]);
			}
			catch (Exception e)
			{
				System.err.println ("*** Error setting join constructor / heapfile");
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}


			JoinedTagRID    = new RID();
			JoinAttrtype    = new AttrType[] { new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrInterval) };
			Joinstrtype     = new short[0];
			
			fldnos = new ArrayList<Integer>();
			fldnos.add(ftfld);
			fldnos.add(scfld);

			try 
			{
				while ((nljtuple=nlj.get_next()) !=null) 
				{
					JoinedTagRID = JoinedTaghpfile.insertRecord(nljtuple.returnTupleByteArray());
				}
				Joinedfscan = new FileScan(JoinedTaghpname[i], JoinAttrtype, Joinstrtype, (short) 2, 2, proj_arr, null);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			JoinedTagParam[i] = new TagParams(Joinedfscan, fldnos, JoinedTaghpfile, JoinedTagRID, JoinedTaghpname[i], JoinAttrtype, Joinstrtype, JoinedTupSize);
		}	
		return JoinedTagParam;
	}*/


	public void disp(String[] arr)
	{
		for(int i=0;i<arr.length;i++)
		{
			System.out.println(arr[i]);
		}
	}

	public TagParams[] ExtractBtreeTagToHeap(TagParams tag_param, BTreePars btreepar, String[] tagnames)
	{
		Heapfile heapfiletosearch   = tag_param.GetHeapFile();
		RID      ridtosearch        = tag_param.GetRID();
		String   hpnametosearch     = tag_param.GetHPFileName();

		int         tot_len         = tagnames.length;

		Heapfile[]  heapfiletostore = new Heapfile[tot_len];
		RID[]       ridtostore      = new RID[tot_len];
		String[]    hpnamestostore  = new String[tot_len];
		TagParams[] tag_pars_arr    = new TagParams[tot_len];

		AttrType [] attrtypestore = tag_param.GetAtrTypes();
		short []    strszsstore   = tag_param.GetAtrSizes();
		int         tupsizestore  = tag_param.GetSizeofTuple();
		Tuple       tupstore      = new Tuple(tupsizestore);

		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		FldSpec[] proj_arr = { new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3) };

		List<Integer> fldlist = null; 
		boolean done = false;
		String  tag  = null;
		Scan scan = null;
		int tagindex = -1;

		//disp(tagnames);

		FileScan fscan = null;
		try
		{
			for(int i=0;i<tot_len;i++)
			{
				if(!(tagnames[i].equals("*")))
				{
					hpnamestostore[i] = GetRandomName();
					heapfiletostore[i] =  new Heapfile(hpnamestostore[i]);						
				}
			}
			scan = heapfiletosearch.openScan();
		}
		catch (Exception e)
		{
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}
		while (!done)
		{ 
			try 
			{
				tupstore = scan.getNext(ridtosearch);
				if (tupstore == null) 
				{
					done = true;
					break;
				}
				tupstore.setHdr((short) 3, attrtypestore, strszsstore);
				tag = tupstore.getStrFld(3);
				for(int i=0;i<tot_len;i++)
				{
					if(tag.equals(tagnames[i]))
					{
						try
						{
							ridtostore[i] = heapfiletostore[i].insertRecord(tupstore.returnTupleByteArray()); //insert the data into a separate heap file
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
		try
		{
			for(int i=0;i<tot_len;i++)
			{
				if(tagnames[i].equals("*")){
					fscan = new FileScan(hpnametosearch, attrtypestore, strszsstore, (short) 3, 3, proj_arr, null);
					tag_pars_arr[i] =  new TagParams(fscan, null, heapfiletosearch, ridtosearch, hpnametosearch, attrtypestore, strszsstore, tupsizestore);//generate tag pars data type for each of the heap file
				}else{
					fscan = new FileScan(hpnamestostore[i], attrtypestore, strszsstore, (short) 3, 3, proj_arr, null);
					tag_pars_arr[i] =  new TagParams(fscan, null, heapfiletostore[i], ridtostore[i], hpnamestostore[i], attrtypestore, strszsstore, tupsizestore); 
				}
			}		
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		return tag_pars_arr;
		//return null;
	}


	public BTreePars CreateBTreeIndex(TagParams tagobject)
	{
		IndexType b_index = new IndexType (IndexType.B_Index);

		Heapfile hpfile = tagobject.GetHeapFile();
		String tagheapfilename = tagobject.GetHPFileName();
		AttrType[] tagattr = tagobject.GetAtrTypes();
		short [] tagstrszs = tagobject.GetAtrSizes();
		int tagTupSize = tagobject.GetSizeofTuple();	
		
		Tuple tup_dummy = new Tuple(tagTupSize);
		//Create a scan object or iterator on this heap file
		Scan scan = null;
		// create the index file
		BTreeFile btreefile = null;
		String btreefilename = "BTreeIndex";
		RID rid = new RID();
		String key = "";
		Tuple temp_tup = null;
		BTreePars btpar = null;

		try
		{
			tup_dummy.setHdr((short) 3, tagattr, tagstrszs);
			scan = new Scan(hpfile);
			btreefile = new BTreeFile(btreefilename, AttrType.attrString, 12, 1); 
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			temp_tup = scan.getNext(rid);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		while(temp_tup != null) 
		{
			tup_dummy.tupleCopy(temp_tup);
			try 
			{
				key = tup_dummy.getStrFld(3);
				btreefile.insert(new StringKey(key), rid); 
				temp_tup = scan.getNext(rid);
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}

		try
		{
			scan.closescan();
			btreefile.close();			
		}
		catch (Exception e) 
		{
			System.out.println("Closing Btree/scan issue");
			e.printStackTrace();
		}

		btpar = new BTreePars(btreefile, btreefilename);
		return btpar;					
	}


	public TagParams SortHeapFile(TagParams sorttag)
	{
		FldSpec[] projlist = new FldSpec[3];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);

		TagParams sorttag_pars = null;
		Heapfile sortheapfile = null;
		RID sortrid = new RID();
		String sorthpflname = "sort_"+sorttag.GetHPFileName();
		AttrType[] sortatrtypes = sorttag.GetAtrTypes();
		short [] sortstrszs = sorttag.GetAtrSizes();
		int sortTupSize = sorttag.GetSizeofTuple();

	    String heapflname = sorttag.GetHPFileName();
    	AttrType [] atrtypes = sorttag.GetAtrTypes();
		short [] strszs = sorttag.GetAtrSizes();
		int TupSize = sorttag.GetSizeofTuple();

    	FileScan fscan = null;
		Tuple tup = new Tuple(TupSize);
		
		try
		{
			tup.setHdr((short) 3, atrtypes, strszs);
			fscan = new FileScan(heapflname, atrtypes, strszs, (short) 3, 3, projlist, null);
		}
		catch (Exception e)
		{
		  	e.printStackTrace();
		}

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sort_names = null;
		try 
		{
			sort_names = new Sort (atrtypes, (short)3, strszs, fscan, 3, ascending, strszs[0], 4000);
		}
		catch (Exception e) 
		{
			System.err.println ("*** Error preparing for sorting");
			System.err.println (""+e);
			Runtime.getRuntime().exit(1);
		}

		try
		{
			sortheapfile =  new Heapfile(sorthpflname);
		}
		catch (Exception e)
		{
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		try 
		{
			while ((tup=sort_names.get_next()) !=null) 
			{
				sortrid = sortheapfile.insertRecord(tup.returnTupleByteArray());
			}
			sort_names.close();

		}
		catch (Exception e) 
		{
			System.err.println ("*** Error preparing for get_next tuple");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}


		sorttag_pars = new TagParams(null, null, sortheapfile, sortrid, sorthpflname, sortatrtypes, sortstrszs, sortTupSize);
		//delete the old heap file
		//DeleteHeapFile(sorttag);
		return sorttag_pars;
	}


	public TagParams QueryExecute(TagParams[] ExtractTags, String[] Joins)
	{
		//TagParams ResultTagPar = null;
		TagParams TempTagPar = null;
		FileScan fscan = null;
		//List<Integer> FieldTracker = new ArrayList<Integer>();
		//List<Integer> ltfieldtoomit = new ArrayList<Integer>();
		AttrType[] atrtypes = ExtractTags[0].GetAtrTypes();
		short [] strszs = ExtractTags[0].GetAtrSizes();
		int TupSize = ExtractTags[0].GetSizeofTuple();
		RelSpec rel_out = new RelSpec(RelSpec.outer);
		FldSpec[] proj_arr = { new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3) };	

		for(int i=0;i<Joins.length;i++)
		{
			String[] Jnfields = Joins[i].split(" ");
			int ftfld = Integer.parseInt(Jnfields[0])-1;
			int scfld = Integer.parseInt(Jnfields[1])-1;
			String typerel = Jnfields[2];
			boolean relflag = false;
			//int[] fields = null;

			if(typerel.equals("PC"))
			{
				relflag = true;
			}
			try
			{
				fscan = new FileScan(ExtractTags[ftfld].GetHPFileName(), atrtypes, strszs, (short) 3, 3, proj_arr, null);
				ExtractTags[ftfld].SetIterator(fscan);
				fscan = new FileScan(ExtractTags[scfld].GetHPFileName(), atrtypes, strszs, (short) 3, 3, proj_arr, null);
				ExtractTags[scfld].SetIterator(fscan);				
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}


			TempTagPar = JoinTwoFields_sm(ExtractTags[ftfld], 2, ExtractTags[scfld], 2, true, relflag); //create a join of two tags
			ScanTagParams(TempTagPar);
/*			if(ResultTagPar != null)
			{
				fields = GetFieldsToJoin(FieldTracker, ftfld, scfld); //get which fields need to be joined
				ltfieldtoomit = GetMyOmitList(FieldTracker, ltfieldtoomit, ftfld, scfld); //get which fields need to be omitted
				ResultTagPar = JoinTwoFields(ResultTagPar, fields[0], TempTagPar, fields[1], ltfieldtoomit, false, false); //join the tags to a resultant table
				AddField(FieldTracker, ftfld);
				AddField(FieldTracker, scfld);
				ltfieldtoomit.clear();
			}
			else
			{
				ResultTagPar = TempTagPar;
				AddField(FieldTracker, ftfld);
				AddField(FieldTracker, scfld);
			}*/
		}
		//System.out.println("done");
		//return new TagparamField(ResultTagPar, FieldTracker);
		return null;
	}
/*	//parentchildflag == true check parent child or else check ancester descendant
	//ContainOrEquality == true check containment or else check equality
	public TagParams QueryExecute(TagParams[] JoinedTags, String[] Joins)
	{
		TagParams ResultTagPar = null;
		int numberofjoins = Joins.length;
		System.out.println(numberofjoins);
		if(numberofjoins < 2)
		{
			return JoinedTags[0];
		}
		else
		{
			ResultTagPar = JoinedTags[0];
			for(int i=1;i<numberofjoins;i++)
			{
				//ResultTagPar = JoinTwoFields_SM(ResultTagPar, JoinedTags[i], false);	
			}
			return ResultTagPar;
		}
	}*/

/*	public String[] GetJoins(String[] NumberofJoins, int st, int end)
	{
		String[] subjoins = new String[end-st];
		int k = 0;
		for(int i=st;i<end;i++)
		{
			subjoins[k] = NumberofJoins[i];
			k+=1;
		}
		return subjoins;
	} */


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
			splitter+=1;
			firlot.clear();
			seclot.clear();	
			possiblesec = true;
			if(count == 2) //if we get two query plans then break (more query plans are also possible)
			{
				break;
			}
		}
		return splitlist;
	}

	//this just executes the three queries
	public TagParams[] MakeQueryPlanner(TagParams[] Tags, String[] NumberofJoins)
	{
		TagParams[] tgprarr = new TagParams[3];

		//PCounter.initialize();

		tgprarr[0] = QueryExecute(Tags, NumberofJoins); //query 1
		//System.out.println("Query 1 executed");
		//System.out.printf("reads = %s writes = %s\n", PCounter.getreads(), PCounter.getwrites());
		tgprarr[1] = null;
		tgprarr[2] = null;
		//ScanTagParams(tgprarr[0]);
		
/*		int[] spliter = querypossible(NumberofJoins);

		//loop for second and the third query
		for(int i=0;i<spliter.length;i++)
		{
			if(spliter[i] != -1)
			{
				tf1 = Query(AllTags, GetJoins(NumberofJoins, 0, spliter[i]));
				tf2 = Query(AllTags, GetJoins(NumberofJoins, spliter[i], NumberofJoins.length));
				tgprarr[i+1] = JoinQuery(tf1, tf2);		

				System.out.printf("reads = %s writes = %s\n", PCounter.getreads(), PCounter.getwrites());
				System.out.printf("Query %s executed\n", i+2);

			}
			else
			{
				System.out.printf("Query %s not possible\n", i+2);
			}

		}*/
		return tgprarr;
	}


/*	public String[] ReadMainQuery(String Querypath)
	{
		List<String> querylinelist = null;
		BufferedReader mainqueryreader = null;
		try
		{
			String line;
 			querylinelist = new ArrayList<String>();
			mainqueryreader = new BufferedReader(new FileReader(Queryfilename));
			while ((line = mainqueryreader.readLine()) != null)
			{
				querylinelist.add(line);
			}
			mainqueryreader.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();	
		}
		return querylinelist;
	}*/


	//Get the main heap file and the query file and fire queries
	public void ReadQueryAndExecute(TagParams Tagpar, BTreePars btreepr, String Queryfilename) 
	{

/*		String[] my = ReadMainQuery(Queryfilename);
		for(int i=0,n=my.length;i<n;i++)
		{
			System.out.println(my[i]);
		}*/
		TagParams QueryResult = null;
		List<String> querylinelist = null;
		BufferedReader queryreader = null;
		try
		{
			String line;
 			querylinelist = new ArrayList<String>();
			queryreader = new BufferedReader(new FileReader(Queryfilename));
			while ((line = queryreader.readLine()) != null)
			{
				querylinelist.add(line);
			}
			queryreader.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();	
		}
		int numberofnodes = Integer.parseInt(querylinelist.get(0));
		String[] searchtags = new String[numberofnodes];
		for(int i=1;i<=numberofnodes;i++)
		{
			searchtags[i-1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
		}

		String[] Joins = new String[querylinelist.size()-numberofnodes-1]; 
		for(int i=0;i<Joins.length;i++)
		{
			Joins[i] = querylinelist.get(numberofnodes+1+i);
		}

		//long startTime = System.nanoTime();		
/*		for(int i=0;i<searchtags.length;i++)
		{
 			System.out.println(searchtags[i]);
		}*/

		TagParams[] AllTags = ExtractBtreeTagToHeap(Tagpar, btreepr, searchtags);
		System.out.println("Tags Extracted from Heap File..");

		//for(int i=0;i<AllTags.length;i++)
		//{
			//ScanTagParams(AllTags[i]);
		//}
		long startTime = System.nanoTime();

		TagParams[] resultTags = MakeQueryPlanner(AllTags, Joins);
	 	long endTime = System.nanoTime();
	  	long durationInNano = (endTime - startTime);
	 	long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);  //Total execution time in nano seconds
	 	System.out.println("query time = "+durationInMillis);
		

		for(int i=0;i<AllTags.length;i++)
		{
			if(!(searchtags[i].equals("*")))
			{
				DeleteHeapFile(AllTags[i]);		
			}
		}	
		//TagParams tp =  JoinTwoFields_sm(AllTags[0], 2, AllTags[3], 2, true, false);

	 	//for(int i=0;i<AllTags.length;i++)
		//{
		//	ScanTagParams(tp);
		//}	
/*		TagParams[] JoinedTags = JoinAllPairTags(AllTags, Joins);


		System.out.println("Tags Joined..");
 		for(int i=0;i<JoinedTags.length;i++)
		{
			ScanTagParams(JoinedTags[i]);
		}

		long endTime = System.nanoTime();
		long durationInNano = (endTime - startTime);
		long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);  //Total execution time in nano seconds
     
    	System.out.println(durationInMillis);
    	for(int i=0;i<JoinedTags.length;i++)
    	{
    		ScanHeapFile(JoinedTags[i]);		
    	}

		
		//PrintQueryResults(resultTags, searchtags);
		ScanTagParams(resultTags[0]);
		for(int i=0;i<JoinedTags.length;i++)
		{
			DeleteHeapFile(JoinedTags[i]);
		}
		//return resultTags;*/
		//return null;
	}


	public void ScanTagParams(TagParams tgprms)
	{
		iterator.Iterator Itrtr = tgprms.GetIterator();	
		AttrType[] tagatrtypes = tgprms.GetAtrTypes();	
			
		Tuple temptup = null;
		int count_records = 0;
		try
		{
			while ((temptup=Itrtr.get_next())!=null)
			{ 
				//temptup.print(tagatrtypes);	
				count_records++;
			}
			Itrtr.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}

		System.out.printf("Total records fetched = %s\n", count_records);
	}

//scan a heap file and print all the values of each
	public void ScanHeapFile(TagParams tgprms)
	{
		boolean done = false;
		int count_records = 0;
		Heapfile hpfl       = tgprms.GetHeapFile();
		RID filerid         = tgprms.GetRID();
		AttrType [] Atrtyps = tgprms.GetAtrTypes();
		short [] Strsizes   = tgprms.GetAtrSizes();
		int TupSize         = tgprms.GetSizeofTuple();
		int numoffields     = Atrtyps.length;

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
				//temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
				//temptup.print(Atrtyps);
				//System.out.println();
				count_records+=1;
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}

		System.out.printf("Total records fetched = %s\n", count_records);
	}

}

//parentchildflag == true check parent child or else check ancester descendant
//ContainOrEquality == true check containment or else check equality

public class XMLTest3// implements  GlobalConst
{
	public static void main(String [] argvs) 
	{
		//String DataFileName = "./xml_sample_data_part.xml";
		String DataFileName = "./xml_sample_data.xml";
		//String QueryFileName = "./queryfile.txt";
		//String DataFileName = "./plane.xml";
		//String QFilePath = "./input_files/Query.txt";
		//String FolderPath = "./input_files/";
		try
		{
			System.out.println("this file");
			long startTime = System.nanoTime();		

			System.out.println("Initializing XML Test object"); 
			XMLDriver xmldvr = new XMLDriver(DataFileName);
			//System.out.println("Reading XML file lines");
			System.out.println("Reading the XML data file..");
			//TagParams MainTagPair = null;
			TagParams MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();
			System.out.println("File Parsing Completed..");

			TagParams SortedTagPair = xmldvr.SortHeapFile(MainTagPair);
			System.out.println("File Sorting Completed..");
			
			//xmldvr.ScanHeapFile(SortedTagPair);
			//MainTagPair.GetHeapFile().deleteFile();
			xmldvr.DeleteHeapFile(MainTagPair);
			MainTagPair=null;
			System.out.println("Original Heap File Deletion Completed..");

			//BTreePars btreepr = xmldvr.CreateBTreeIndex(SortedTagPair);
			//System.out.println("BTree Index Creation Completed");
			BTreePars btreepr = null;

			long endTime = System.nanoTime();
			long durationInNano = (endTime - startTime);
			long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);  //Total execution time in nano seconds
	     
	     	System.out.print("Preprocessing time = ");
	    	System.out.println(durationInMillis);
			//xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QueryFileName);
			//xmldvr.ScanTagParams(qresult[0]);
			//BTreePars btpar = xmldvr.CreateBTreeIndex(SortedTagPair);
			//xmldvr.ExtractTagBTree(SortedTagPair, btf, "crane");
			//xmldvr.ScanHeapFile(SortedTagPair);
			/*
			String[] tagarr = new String[2];
			tagarr[0]="year";
			tagarr[1]="model";*/
			//xmldvr.ExtractBtreeTagToIndex(SortedTagPair, btpar, tagarr);
			
			int choice;
			boolean breakflag = true;
			Console console = System.console();

			while(breakflag)
			{
				System.out.println("Enter you choice\n1. Execute Query\n2. Exit");
				try
				{
					choice = Integer.parseInt(console.readLine());
				}
				catch (NumberFormatException e)
				{
					choice = -1;
				}

				switch (choice)
				{
					case 1: 
						try
						{
							//System.out.println("Enter the two patter tree number with space in between\n");
							String QueryFileName = "queryfile.txt";//console.readLine();
							xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QueryFileName);		
						}						
						catch(Exception e)
						{
							e.printStackTrace();
						}
						finally
						{
							break;
						}
					case 2: 
						breakflag = false;
						System.out.println("Deleting heaps files and indexes");
						xmldvr.DeleteHeapFile(SortedTagPair);
						//btreepr.GetBTreeFile().destroyFile();
						SortedTagPair=null;
						btreepr=null;
						System.out.println("Exiting..");
						break;

					default:
						System.out.println("Invalid Choice or data entered is invalid..");
						break;
				} 			
			}
			

		//String[] schtag = new String[]{"root", "seqle", "102", "Speci", "Lycop"};
			//TagParams[] tgprs = xmldvr.ExtractTagToHeap(MainTagPair, schtag);
		//	List<Integer> ltfieldtoomit =  new ArrayList<Integer>() ;
			//ltfieldtoomit.add(1);
				//TagParams k =	xmldvr.JoinTwoFields(tgprs[0], 2, tgprs[1], 2, ltfieldtoomit, false, true);

			 //xmldvr.ScanHeapFile(k);
/*
			TagParams a1_2 = xmldvr.JoinTwoFields(tgprs[0], 2, tgprs[1], 2, false, true);
			TagParams a2_3 = xmldvr.JoinTwoFields(tgprs[1], 2, tgprs[2], 2, true, true);

			TagParams a1_2_3 = xmldvr.JoinTwoFields(a1_2, 5, a2_3, 2, false ,false);*/
			//TagParams newj = xmldvr.JoinTwoFields(firjoin, 2, tgprs[2], 2);
/*			for(int i=0;i<3;i++)
			{
				xmldvr.ScanHeapFile(tgprs[i]);
			}*/
			//xmldvr.ScanHeapFile(MainTagPair);

/*			for(int i=0;i<qresult.length;i++)
			{
				if(qresult[i] != null)
				{
					xmldvr.ScanHeapFile(qresult[i]);  //print all the heap file query results
				}
		
			}*/
			//xmldvr.ScanHeapFile(qresult[0]);
			//xmldvr.Sort_My_Field();
/*			System.out.println(PCounter.getreads());
			System.out.println(PCounter.getwrites());*/
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println ("Error encountered during XML tests:\n");
			Runtime.getRuntime().exit(1);
		}
	} 
}



