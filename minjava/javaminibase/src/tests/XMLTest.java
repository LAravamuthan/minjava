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
	public FldSpec[] GetProjections(int outer, int inner, List<Integer> ltfieldtoomit)
	{
		FldSpec[] projections = new FldSpec[outer+inner-(3*ltfieldtoomit.size())];
		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		
		int[] fieldstokeep = new int[(inner/3)-ltfieldtoomit.size()];
		int k = 0;
		for(int i=1;i<=(inner/3);i++)
		{
			if(ltfieldtoomit.indexOf(i) == -1)
			{
				fieldstokeep[k] = i; 
				k+=1;
			}
		}
		for(int i=0;i<outer;i++)
		{
			projections[i] = new FldSpec(rel_out, i+1);
		}
		k=outer;
		for(int i=0;i<fieldstokeep.length;i++)
		{
			int pos = 3*(fieldstokeep[i]-1) + 1;
			for(int p=0;p<3;p++)
			{
				projections[k]=new FldSpec(rel_in, pos);
				k+=1;
				pos+=1;			
			}
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
	public AttrType[] JoinAttrtype(AttrType[] tagattrtype1, AttrType[] tagattrtype2, int outer, FldSpec[] projections)
	{
		AttrType[] JoinedTagAttrtype = new AttrType[projections.length];
		System.arraycopy(tagattrtype1, 0, JoinedTagAttrtype, 0, outer); 

		for(int i=outer;i<projections.length;i++)
		{
			JoinedTagAttrtype[i] = tagattrtype2[projections[i].offset-1];
		}
		return JoinedTagAttrtype;
	}
 
 	//This function joins the attrstrsizes of the two tables depending upon the field to omit list
	public short[] JoinAttrsize(short[] tagattrsize1, short[] tagattrsize2, List<Integer> fieldtoomit)
	{
		short[] JoinedTagsize = new short[tagattrsize1.length + tagattrsize2.length - fieldtoomit.size()];
		System.arraycopy(tagattrsize1, 0, JoinedTagsize, 0, tagattrsize1.length); 
		int pos = 0;

		for(int i=tagattrsize1.length; i<JoinedTagsize.length; i++)
		{
			JoinedTagsize[i] = tagattrsize2[pos];
			pos+=1;
		}
		return JoinedTagsize;
	}

	//This function calculates the tuple size after it has been joined
	public int JoinedTupSize(AttrType[] JoinedTagAttrtype, short[] JoinedTagsize)
	{
		Tuple temptup = new Tuple();
		try
		{
			temptup.setHdr((short) JoinedTagAttrtype.length, JoinedTagAttrtype, JoinedTagsize);
		}
		catch (Exception e)
		{
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		return temptup.size();		
	}

	public List<Integer> GetFieldtoOmit(List<Integer> fieldno1, List<Integer> fieldno2)
	{
		List<Integer> fieldtoomit = new ArrayList<Integer>();
		for(int i=0;i<fieldno2.size();i++)
		{
			if(fieldno1.indexOf(fieldno2.get(i)) > -1)
			{
				fieldtoomit.add(i+1);
			}
		}
		return fieldtoomit;
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
					fields[0] = (3*index)+2;
					fields[1] = (3*i)+2;
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

/*	public String GetRandomName()
	{
		int n=10;
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789" + "abcdefghijklmnopqrstuvxyz";
        int alphalen =  AlphaNumericString.length();
        StringBuilder sb = new StringBuilder(n); 
        for (int i = 0; i < n; i++) { 
            int index = (int)(alphalen*Math.random()); 
            sb.append(AlphaNumericString.charAt(index)); 
        } 
        return sb.toString(); 			
	}*/

/*	public void GenerateHeapfile(TagParams tagpar)
	{
		if(tagpar.GetHeapFile()==null)
		{
			iterator.Iterator itr = tagpar.GetIterator();
			Tuple temptup = null;

			String hpfilename = GetRandomName();
			Heapfile hpfile = null;
			RID hprid = new RID();

			try
			{
				hpfile = new Heapfile(hpfilename);
			}
			catch (Exception e)
			{
				System.err.println("*** error in Heapfile constructor ***");
				e.printStackTrace();
			}

			try 
			{
				while ((temptup=itr.get_next())!=null) 
				{
					hprid = hpfile.insertRecord(temptup.returnTupleByteArray());	//insert the joined record into a new heap file	
				}
			}
			catch (Exception e)
			{
				System.err.println ("*** Error preparing for get_next tuple");
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			} 
			tagpar.SetHPParams(hpfile, hprid, hpfilename);			
		}
	}*/

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
		List<Integer> Fieldscomb   = GetCombinedFlds(Fieldnos1, Fieldnos2);

		//String JoinedTaghpfilename = null; 
		//Heapfile JoinedTaghpfile = null;
		//RID JoinedTagRID = null;

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> ftoO2 = GetFieldtoOmit(Fieldnos1, Fieldnos2);
		int no_out_fields = outer+inner-(3*ftoO2.size());
		projlist_tag1 = GetProjections(outer, 0, ftoO1);			//generate projections for the left table
		projlist_tag2 = GetProjections(outer, inner, ftoO2);		//generate projections for the right table

		if(jtfld1==0 && jtfld2==0)
		{
			int[] jfld = GetFieldsToJoin(Fieldnos1, Fieldnos2);
			jtfld1 = jfld[0];
			jtfld2 = jfld[1];
		}
		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(jtfld1, jtfld2, ContainOrEquality, parentchildflag);	//generate condexpr for the two joining fields

		JoinedTagAttrtype = JoinAttrtype(tagattrtype1, tagattrtype2, outer, projlist_tag2);
		JoinedTagsize     = JoinAttrsize(tagattrsize1, tagattrsize2, ftoO2);
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		//GenerateHeapfile(tag2);
		//taghpfilename2 = tag2.GetHPFileName();

    	SortMerge sm = null;
	    try 
	    {
			sm = new SortMerge(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, jtfld1, 10, jtfld2, 10, 800, indxscan1, indxscan2, false, false, ascending, outFilter, projlist_tag2, no_out_fields);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for sort merge join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
		finalitr = (iterator.Iterator)sm;
		return new TagParams(finalitr, Fieldscomb, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
	}


  	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality
	public TagParams JoinTwoFields_nlj(TagParams tag1, int joinfieldno1, TagParams tag2, int joinfieldno2, boolean ContainOrEquality, boolean parentchildflag)
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
		List<Integer> Fieldscomb   = GetCombinedFlds(Fieldnos1, Fieldnos2);

		//String JoinedTaghpfilename = null; 
		//Heapfile JoinedTaghpfile = null;
		//RID JoinedTagRID = null;

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> ftoO2 = GetFieldtoOmit(Fieldnos1, Fieldnos2);
		int no_out_fields = outer+inner-(3*ftoO2.size());
		projlist_tag1 = GetProjections(outer, 0, ftoO1);			//generate projections for the left table
		projlist_tag2 = GetProjections(outer, inner, ftoO2);		//generate projections for the right table

		if(jtfld1==0 && jtfld2==0)
		{
			int[] jfld = GetFieldsToJoin(Fieldnos1, Fieldnos2);
			jtfld1 = jfld[0];
			jtfld2 = jfld[1];
		}
		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(jtfld1, jtfld2, ContainOrEquality, parentchildflag);	//generate condexpr for the two joining fields

		JoinedTagAttrtype = JoinAttrtype(tagattrtype1, tagattrtype2, outer, projlist_tag2);
		JoinedTagsize     = JoinAttrsize(tagattrsize1, tagattrsize2, ftoO2);
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);

		//GenerateHeapfile(tag2);
		//taghpfilename2 = tag2.GetHPFileName();
	    NestedLoopsJoins nlj = null;
	    try 
	    {
	    	//initalizing nested loop join consructor
	    	nlj = new NestedLoopsJoins(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, 10, indxscan1, indxscan2, outFilter, null, projlist_tag2, no_out_fields);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for nested_loop_join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
		finalitr = (iterator.Iterator)nlj;
		return new TagParams(finalitr, Fieldscomb, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
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


	public TagParams[] ExtractBtreeTagToIndex(TagParams tag_param, BTreePars btreepar, String[] tagnames, String[] Joins)
	{
		CondExpr[] expr1 = null, expr2 = null;
		FldSpec[] projlist = new FldSpec[3];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);

	    int tot_len = tagnames.length;
	    int TotalJoins = Joins.length;
		TagParams[] TgPr = new TagParams[tot_len];
		List<Integer> fldlist = null; 

		Heapfile heaptosearch = tag_param.GetHeapFile();
		String heapfilename = tag_param.GetHPFileName();
		AttrType[] tagattr = tag_param.GetAtrTypes();
		short [] tagstrszs = tag_param.GetAtrSizes();
		int tagTupSize = tag_param.GetSizeofTuple();

		String btreefilename = btreepar.GetBTreeFileName();
	    TagParams[] JoinedTagPar = new TagParams[TotalJoins];
	    iterator.Iterator itr1 = null, itr2 = null;

		for(int i=0;i<tot_len;i++)
		{
			fldlist = new ArrayList<Integer>();
			fldlist.add(i);
			TgPr[i] =  new TagParams(null, fldlist, null, null, null, tagattr, tagstrszs, tagTupSize);
		}

		List<Integer> ftoO1 = new ArrayList<Integer>();
		FldSpec[] projlist_tag2 = GetProjections(3, 3, ftoO1);

		for(int i=0;i<TotalJoins;i++)
		{
			String[] Jnfields = Joins[i].split(" ");
			int ftfld = Integer.parseInt(Jnfields[0])-1;
			int scfld = Integer.parseInt(Jnfields[1])-1;
			String typerel = Jnfields[2];
			boolean relflag = false;

			if(typerel.equals("PC"))
			{
				relflag = true;
			}
			try
			{
				expr1 = GetCondExpr(tagnames[ftfld]);
				expr2 = GetCondExpr(tagnames[scfld]);
				itr1 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr, tagstrszs, 3, 3, projlist, expr1, 3, false);
				itr2 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr, tagstrszs, 3, 3, projlist, expr2, 3, false);
				TgPr[ftfld].SetIterator(itr1);
				TgPr[scfld].SetIterator(itr2);
				ScanTagParams(TgPr[ftfld]);				
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			//JoinedTagPar[i] = JoinTwoFields_nlj(TgPr[ftfld], 2, TgPr[scfld], 2, true, relflag); //create a join of two tags
			//JoinedTagPar[i] = JoinTwoFields_sm(TgPr[ftfld], 2, TgPr[scfld], 2, true, relflag); //create a join of two tags
			ScanTagParams(JoinedTagPar[i]);
			//System.out.println("------------------------------------------------");
		}
		return JoinedTagPar;
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

	//parentchildflag == true check parent child or else check ancester descendant
	//ContainOrEquality == true check containment or else check equality
	public TagParams Query(TagParams[] JoinedTags, String[] Joins)
	{
		TagParams ResultTagPar = null;
		int numberofjoins = Joins.length;

		if(numberofjoins < 2)
		{
			return JoinedTags[0];
		}
		else
		{
			ResultTagPar = JoinedTags[0];
			for(int i=1;i<numberofjoins;i++)
			{
				//ResultTagPar = JoinTwoFields_nlj(ResultTagPar, 0, JoinedTags[i], 0, false, false);
				ResultTagPar = JoinTwoFields_sm(ResultTagPar, 0, JoinedTags[i], 0, false, false);	
			}
			return ResultTagPar;
		}
	}

	public String[] GetJoins(String[] NumberofJoins, int st, int end)
	{
		String[] subjoins = new String[end-st];
		int k = 0;
		for(int i=st;i<end;i++)
		{
			subjoins[k] = NumberofJoins[i];
			k+=1;
		}
		return subjoins;
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
	public TagParams[] MakeQueryPlanner(TagParams[] PairTags, String[] NumberofJoins)
	{
		TagParams[] tgprarr = new TagParams[3];

		//PCounter.initialize();

		tgprarr[0] = Query(PairTags, NumberofJoins); //query 1
		System.out.println("Query 1 executed");
		System.out.printf("reads = %s writes = %s\n", PCounter.getreads(), PCounter.getwrites());

		
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

	//Get the main heap file and the query file and fire queries
	public TagParams[] ReadQueryAndExecute(TagParams Tagpar, BTreePars btreepr, String Queryfilename) 
	{
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

		String[] NumberofJoins = new String[querylinelist.size()-numberofnodes-1]; 
		for(int i=0;i<NumberofJoins.length;i++)
		{
			NumberofJoins[i] = querylinelist.get(numberofnodes+1+i);
		}

		//BTreePars btreepr = CreateBTreeIndex(Tagpar);
		TagParams[] JoinedTags = ExtractBtreeTagToIndex(Tagpar, btreepr, searchtags, NumberofJoins); //get all the heap file for each tag
		System.out.println("Tags Extracted From HeapFile");

		//TagParams[] resultTags = MakeQueryPlanner(JoinedTags, NumberofJoins);
		//return resultTags;
		return null;
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
				temptup.print(tagatrtypes);	
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

}

//parentchildflag == true check parent child or else check ancester descendant
//ContainOrEquality == true check containment or else check equality

public class XMLTest// implements  GlobalConst
{
	public static void main(String [] argvs) 
	{
		String DataFileName = "./xml_sample_data_part.xml";
		//String DataFileName = "./xml_sample_data.xml";
		String QueryFileName = "./queryfile.txt";
		//String DataFileName = "./plane.xml";
		try
		{
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

			BTreePars btreepr = xmldvr.CreateBTreeIndex(SortedTagPair);
			System.out.println("BTree Index Creation Completed");
	

			TagParams[] qresult = xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QueryFileName);
			//xmldvr.ScanTagParams(qresult[0]);
			//BTreePars btpar = xmldvr.CreateBTreeIndex(SortedTagPair);
			//xmldvr.ExtractTagBTree(SortedTagPair, btf, "crane");
			//xmldvr.ScanHeapFile(SortedTagPair);
			/*
			String[] tagarr = new String[2];
			tagarr[0]="year";
			tagarr[1]="model";*/
			//xmldvr.ExtractBtreeTagToIndex(SortedTagPair, btpar, tagarr);
			
/*			int choice;
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
							System.out.println("Enter you query file name with extension\n");
							String QueryFileName = console.readLine();
							TagParams[] qresult = xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QueryFileName);
							for(int i=0;i<qresult.length;i++)
							{
								if(qresult[i] != null)
								{
									xmldvr.ScanTagParams(qresult[i]);  //print all the heap file query results
								}
							}
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
						btreepr.GetBTreeFile().destroyFile();
						SortedTagPair=null;
						btreepr=null;
						System.out.println("Exiting..");
						break;

					default:
						System.out.println("Invalid Choice or data entered is invalid..");
						break;
				} 			
			}*/
			

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



