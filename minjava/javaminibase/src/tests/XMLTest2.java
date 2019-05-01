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


class PrintClass
{

        private  HashMap<Integer, HashSet<Integer>> pc = null;
        private  HashMap<Integer, Integer> nodeIndendation = null;
        private  HashSet<Integer> parents = null;
        private  Integer[] fldarr = null; 
        private  HashMap<Integer, String> space = null;
        private BufferedWriter writer =null;

        public PrintClass(String[] PT1Tag, String[] PT1Join, List<Integer> fld1, String[] PT2Tag, String[] PT2Join, List<Integer> fld2)
        throws IOException
        {
            this.pc              = new HashMap<Integer, HashSet<Integer>>();
            this.nodeIndendation = new HashMap<Integer, Integer>();
            this.parents         = new HashSet<Integer>();
			this.writer          = new BufferedWriter(new FileWriter("./Output/output.txt", false));
            List<Integer> spacearr = new ArrayList<Integer>();

            this.space = new HashMap<Integer, String>();
            this.space.put(0,"");
            this.space.put(1,"  ");
            this.space.put(2,"    ");
            this.space.put(3,"      ");
            this.space.put(4,"        ");
            this.space.put(5,"          ");
            this.space.put(6,"            ");
            this.space.put(7,"              ");
            this.space.put(8,"                ");
            this.space.put(9,"                  ");
            this.space.put(10,"                    ");
            this.space.put(11,"                      ");
            this.space.put(12,"                        ");
            this.space.put(13,"                          ");
            this.space.put(14,"                            ");
            this.space.put(15,"                              ");
            this.space.put(16,"                                ");

            GetSpaces(PT1Tag, PT1Join);   
            for(int i=0,n=fld1.size();i<n;i++)
            {
                spacearr.add(nodeIndendation.get(fld1.get(i)));
            }

            if(PT2Tag != null)
            {
                this.pc              = new HashMap<Integer, HashSet<Integer>>();
                this.nodeIndendation = new HashMap<Integer, Integer>();
                this.parents          = new HashSet<Integer>();
                

                GetSpaces(PT2Tag, PT2Join);
                for(int i=0,n=fld2.size();i<n;i++)
                {
                    spacearr.add(nodeIndendation.get(fld2.get(i)));
                }
                fldarr = spacearr.toArray(new Integer[spacearr.size()]);             
            }
            else
            {
                 for(int i=0,n=spacearr.size();i<n;i++)
                {
                    spacearr.set(i, spacearr.get(i)-1);
                }       
                fldarr = spacearr.toArray(new Integer[spacearr.size()]);          
            }
        }

        public void PrintStringArr(String[] mydata)
        throws IOException
        {
            StringBuffer s = new StringBuffer("");
            for(int i=0,n=fldarr.length;i<n;i++)
            {
                s.append(this.space.get(fldarr[i])+mydata[i]+"\n");
            }
            s.append("\n");
            this.writer.append(s);
        }

        public void CloseFile()
        throws IOException
        {
            this.writer.close();
        }


         private void incrementCount(Integer parent)
         {
             if(pc.containsKey(parent))
             {
                 HashSet<Integer> children = pc.get(parent);
                 for(Integer i: children)
                 {
                     
                     nodeIndendation.put(i, 1+nodeIndendation.get(i));
                     incrementCount(i);
                 }
             }
         }  

        private  void printTree(String[] nodes,String[] relations)
        {

          for (int i=0;i<nodes.length;i++)
          {
              nodeIndendation.put(i, 1);
          }
          for(int i=0;i<relations.length;i++)
          {
              String onerelation = relations[i];
              int j=Integer.parseInt(onerelation.split(" ")[0]);
              int k=Integer.parseInt(onerelation.split(" ")[1]);
              
              HashSet<Integer> temp;
              if(pc.containsKey(j-1))
              {

                  temp = pc.get(j-1);  
              }
              else
              {
                  parents.add(j-1);
                  temp = new HashSet<Integer>();
              }
              temp.add(k-1);
              pc.put(j-1,temp);
          }
          java.util.Iterator<Integer> it = parents.iterator();
             while(it.hasNext()){
                incrementCount(it.next());
             }
        }
         
        
        private void GetSpaces(String[] nodes, String[] relations) {
            ArrayList<String> sequence = new ArrayList<String>(); 
            // TODO Auto-generated method stub
            int k=1;
            for(int i=0;i<nodes.length;i++)
            {
                if(nodes[i].equals("*"))
                {
                    nodes[i]+=k++;
                }
            }
            printTree(nodes,relations);   
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

	private int countrecord;
 
	public TagParams(iterator.Iterator itr, List<Integer> fldnos, Heapfile hfile, RID rid, String hpfname, AttrType[] atrtypes, short[] atrsizes, int sizetup, int rc)
	{
		this.Itrtr = itr;
		this.FieldNos = fldnos;

		this.tagheapfile = hfile;
		this.tagrid = rid;
		this.hpfilename = hpfname;

		this.AtrTypes = atrtypes;
		this.AtrSizes = atrsizes;
		this.SizeofTuple = sizetup;

		this.countrecord = rc;
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

	public int GetRcCount()
	{
		return this.countrecord;
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
		this.countrecord = -1;
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
			tgpr = new TagParams(null, null, this.hpfile, this.rid, this.hpfilename, GetAttrType(), GetStrSizes(), GetTupleSize(), -1);

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
	public FldSpec[] GetProjections(int outer, List<Integer> fieldtokeep)
	{
		FldSpec[] projections = new FldSpec[outer+(fieldtokeep.size()*2)];
		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		
		for(int i=0;i<outer;i++)
		{
			projections[i] = new FldSpec(rel_out, i+1);
		}
		for(int i=0;i<fieldtokeep.size();i++)
		{
			projections[outer+(2*i)]   = new FldSpec(rel_in, (2*fieldtokeep.get(i))-1);
			projections[outer+(2*i)+1] = new FldSpec(rel_in, 2*fieldtokeep.get(i));		
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
		AttrType[] JoinedTagAttrtype = new AttrType[tagattrtype1.length+(fieldtokeep.size()*2)];
		for(int i=0, n=(JoinedTagAttrtype.length)/2;i<n;i++)
		{
			JoinedTagAttrtype[2*i] = new AttrType (AttrType.attrInterval);
			JoinedTagAttrtype[2*i+1] = new AttrType (AttrType.attrString);
		}
		return JoinedTagAttrtype;
	}
 
 	public short[] JoinStrtype(AttrType[] JoinedTagAttrtype)
 	{
 		short[] JoinedStrtype = new short[(JoinedTagAttrtype.length)/2];
 		for(int i=0;i<JoinedStrtype.length;i++)
 		{
 			JoinedStrtype[i]=10;
 		}
 		return JoinedStrtype;
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
					fields[0] = 2*index+1;
					fields[1] = 2*i+1;
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

	private boolean DecideForNLJ(String[] badtags, String lttag, String rttag)
	{
		String tempstr = lttag+" "+rttag;
		return Arrays.asList(badtags).contains(tempstr);
	}

	private String[] GetBadTags()
	{
		String[] badtags = new String[2];
		badtags[0]="Ref Autho";
		badtags[1]="Featu Descr";
		return badtags;
	}

	//parentchildflag == true check parent child or else check ancester descendant
	//ContainOrEquality == true check containment or else check equality
	public TagParams JoinTwoFields_SM(TagParams tag1, TagParams tag2, boolean ContainOrEquality)
	{

		iterator.Iterator indxscan1 = tag1.GetIterator();
    	AttrType [] tagattrtype1    = tag1.GetAtrTypes();
		short    [] tagattrsize1    = tag1.GetAtrSizes();
		int      tagtupsize1        = tag1.GetSizeofTuple();
		//String   taghpfilename1   = tag1.GetHPFileName();
		int      outer              = tagattrtype1.length;
		List<Integer> Fieldnos1     = tag1.GetFieldNos();
		int jtfld1                  = -1;

		iterator.Iterator indxscan2 = tag2.GetIterator();
    	AttrType [] tagattrtype2   = tag2.GetAtrTypes();
		short    [] tagattrsize2   = tag2.GetAtrSizes();
		int      tagtupsize2       = tag2.GetSizeofTuple();
		String   taghpfilename2    = null;
		int      inner             = tagattrtype2.length;
		List<Integer> Fieldnos2    = tag2.GetFieldNos();
		int jtfld2                 = -1;

		iterator.Iterator finalitr = null;
		AttrType[] JoinedTagAttrtype = null;
		short [] JoinedTagsize = null;
		int JoinedTagTupSize = 0;
		List<Integer> Fieldscomb = GetCombinedFlds(Fieldnos1, Fieldnos2);

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> ftoO2 = GetFieldtoKeep(Fieldnos1, Fieldnos2);
		int no_out_fields = outer+(ftoO2.size()*2);
		projlist_tag1 = GetProjections(outer, ftoO1);			//generate projections for the left table
		projlist_tag2 = GetProjections(outer, ftoO2);		    //generate projections for the right table


		int[] jfld = GetFieldsToJoin(Fieldnos1, Fieldnos2);
		jtfld1 = jfld[0];
		jtfld2 = jfld[1];

		//System.out.printf("joint fields %s %s\n", jtfld1, jtfld2);

		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(jtfld1, jtfld2, ContainOrEquality, false);	//generate condexpr for the two joining fields

		JoinedTagAttrtype = JoinAttrtype(tagattrtype1, ftoO2);
		JoinedTagsize     = JoinStrtype(JoinedTagAttrtype);
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype, JoinedTagsize);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
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
		return new TagParams(finalitr, Fieldscomb, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize, -1);
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

	public TagParams[] JoinAllPairTags(TagParams[] tag_param_arr, String[] tag_param_tags, String[] PTreetags, String[] PTreeJoins, List<Integer> tagtodelete)
	{
		FileScan fscan1 = null;
		FileScan fscan2 = null;
		NestedLoopsJoins nlj = null;
		SortMerge sm = null;
		iterator.Iterator finalitr = null;
		int TotalJoins = PTreeJoins.length;

		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel); 
		FldSpec[] proj_arr = {new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3)};
		FldSpec[] proj_arr_join = {new FldSpec(rel_out, 2), new FldSpec(rel_out, 3), new FldSpec(rel_in, 2), new FldSpec(rel_in, 3)};
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

		Heapfile    JoinedTaghpfile = null;
		String[]    JoinedTaghpname = new String[TotalJoins];
		RID         JoinedTagRID    = null;
		AttrType[]  JoinAttrtype    = null;
		short[]     Joinstrtype     = null;
		TagParams[] JoinedTagParam  = new TagParams[TotalJoins];
		int         JoinedTupSize   = 52;
		FileScan 	Joinedfscan     = null;
		FldSpec[]   JoinedProjarr   = null;

		boolean nljflag = false;

        List<Integer> fldnos = null;
		Tuple nljtuple;

		HashMap<String, Integer> TagtoPos = new HashMap<>();
		for(int i=0,n=tag_param_tags.length;i<n;i++)
		{
			TagtoPos.put(tag_param_tags[i], i);
		}

		String[] badtags = GetBadTags();
/*		System.out.println(Arrays.asList(TagtoPos));
		displist(PTreeJoins);
		displist(PTreetags);*/
        for(int i=0;i<TotalJoins;i++)
        {
        	JoinedTaghpname[i] =  GetRandomName();
        }
//        System.out.println(TotalJoins);
		for(int i=0;i<TotalJoins;i++)
		{
			
			String[] Jnfields = PTreeJoins[i].split(" ");
			//System.out.printf("the values %s %s\n", PTreetags[Integer.parseInt(Jnfields[0])-1], PTreetags[Integer.parseInt(Jnfields[1])-1]);
			int rlftfld = Integer.parseInt(Jnfields[0])-1;
			int rlscfld = Integer.parseInt(Jnfields[1])-1;

			int proftfld = TagtoPos.get(PTreetags[rlftfld]);
			int proscfld = TagtoPos.get(PTreetags[rlscfld]);

			String typerel = Jnfields[2];
			boolean pcflag = typerel.equals("PC");

			String     tag1hpflname = tag_param_arr[proftfld].GetHPFileName();
			AttrType[] tag1attrtype = tag_param_arr[proftfld].GetAtrTypes();
			short []   tag1strsz    = tag_param_arr[proftfld].GetAtrSizes();	

			String     tag2hpflname =  tag_param_arr[proscfld].GetHPFileName();
			AttrType[] tag2attrtype = tag_param_arr[proscfld].GetAtrTypes();
			short []   tag2strsz    = tag_param_arr[proscfld].GetAtrSizes();	

			CondExpr[] outFilter = GenerateCondExpr(2, 2, true, pcflag);


			nljflag = DecideForNLJ(badtags, tag_param_tags[proftfld], tag_param_tags[proscfld]);
			//System.out.printf("%s %s ", tag_param_tags[proftfld], tag_param_tags[proscfld]);
			//System.out.println(nljflag);

			try 
			{	 
				fscan1 = new FileScan(tag1hpflname, tag1attrtype, tag1strsz, (short) 3, 3, proj_arr, null);  //file scan pointer
				if(nljflag)
				{
					tagtodelete.add(i);
					nlj = new NestedLoopsJoins(tag1attrtype, 3, tag1strsz, tag2attrtype, 3, tag2strsz, 50, fscan1, tag2hpflname, outFilter, null, proj_arr_join, 4);
					finalitr = nlj;
				}
				else
				{
					fscan2 = new FileScan(tag2hpflname, tag2attrtype, tag2strsz, (short) 3, 3, proj_arr, null);  //file scan pointer
					sm = new SortMerge(tag1attrtype, 3, tag1strsz, tag2attrtype, 3, tag2strsz, 2, 10, 2, 10, 2000, fscan1, fscan2, true, true, ascending, outFilter, proj_arr_join, 4);					
					finalitr = sm;
				}			
				JoinedTaghpfile = new Heapfile(JoinedTaghpname[i]);

			}
			catch (Exception e)
			{
				System.err.println ("*** Error setting join constructor / heapfile");
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
		
			JoinAttrtype    = new AttrType[] { new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrString), new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrString) };
			Joinstrtype     = new short[] {10, 10};
			JoinedProjarr   = new FldSpec[] {new FldSpec(rel_out, 1), new FldSpec(rel_out, 2), new FldSpec(rel_out, 3), new FldSpec(rel_out, 4)};
			fldnos = new ArrayList<Integer>();
			fldnos.add(rlftfld);
			fldnos.add(rlscfld);

			if(nljflag)
			{
				try 
				{
					JoinedTagRID  = new RID();
					while ((nljtuple=finalitr.get_next()) !=null) 
					{
						JoinedTagRID = JoinedTaghpfile.insertRecord(nljtuple.returnTupleByteArray());
					}
					Joinedfscan = new FileScan(JoinedTaghpname[i], JoinAttrtype, Joinstrtype, (short) 4, 4, JoinedProjarr, null);
					finalitr = Joinedfscan;
				}
				catch (Exception e)
				{
					e.printStackTrace();
					Runtime.getRuntime().exit(1);
				}	
				System.out.println("heapfile created");			
			}


			JoinedTagParam[i] = new TagParams(finalitr, fldnos, JoinedTaghpfile, JoinedTagRID, JoinedTaghpname[i], JoinAttrtype, Joinstrtype, JoinedTupSize, -1);
		}	
		return JoinedTagParam;
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
		int[]       countrctostore  = new int[tot_len];
 
		AttrType [] attrtypestore = tag_param.GetAtrTypes();
		short []    strszsstore   = tag_param.GetAtrSizes();
		int         tupsizestore  = tag_param.GetSizeofTuple();
		Tuple       tupstore      = new Tuple(tupsizestore);

		HashMap<String, Integer> TagtoPos = new HashMap<>();
		List<Integer> fldlist = null; 
		boolean done = false;
		String  tag  = null;
		Scan scan = null;
		int tagindex = -1;
		int tot_tup_count = 0;

		try
		{
			for(int i=0;i<tot_len;i++)
			{
				if(!tagnames[i].equals("*"))
				{
					TagtoPos.put(tagnames[i], i);
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
				tot_tup_count+=1;
				if(TagtoPos.get(tag) != null)
				{
					tagindex = TagtoPos.get(tag); 
					try
					{
						countrctostore[tagindex]+=1;
						ridtostore[tagindex] = heapfiletostore[tagindex].insertRecord(tupstore.returnTupleByteArray()); //insert the data into a separate heap file
					}
					catch (Exception e) 
					{
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					} 				
				}
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}

		for(int i=0;i<tot_len;i++)
		{
			if(tagnames[i].equals("*")){
				tag_pars_arr[i] =  new TagParams(null, null, heapfiletosearch, ridtosearch, hpnametosearch, attrtypestore, strszsstore, tupsizestore, tot_tup_count);//generate tag pars data type for each of the heap file
			}else{
				tag_pars_arr[i] =  new TagParams(null, null, heapfiletostore[i], ridtostore[i], hpnamestostore[i], attrtypestore, strszsstore, tupsizestore, countrctostore[i]); 
			}
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
			sort_names = new Sort (atrtypes, (short)3, strszs, fscan, 2, ascending, strszs[0], 4000);
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

		sorttag_pars = new TagParams(null, null, sortheapfile, sortrid, sorthpflname, sortatrtypes, sortstrszs, sortTupSize, -1);
		//delete the old heap file
		//DeleteHeapFile(sorttag);
		return sorttag_pars;
	}

	//parentchildflag == true check parent child or else check ancester descendant
	//ContainOrEquality == true check containment or else check equality
	public TagParams QueryExecute(TagParams[] JoinedTags, String[] Joins)
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
				ResultTagPar = JoinTwoFields_SM(ResultTagPar, JoinedTags[i], false);	
			}
			return ResultTagPar;
		}
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
	public TagParams[] MakeQueryPlanner(TagParams[] PairTags, String[] Joins)
	{
		TagParams[] tgprarr = new TagParams[3];

		//PCounter.initialize();

		tgprarr[0] = QueryExecute(PairTags, Joins); //query 1
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


	private String[] GetQueryTags(List<String> PTreeLns)
	{
		int No_tags = Integer.parseInt(PTreeLns.get(0));
		String[] QTags = new String[No_tags];
		String tagstore = null;
		for(int i=0;i<No_tags;i++)
		{
			tagstore = PTreeLns.get(i+1);
			QTags[i] = tagstore.length() > 5 ? tagstore.substring(0, 5) : tagstore;
		} 
		return QTags;
	}

	private String[] GetQueryJoins(List<String> PTreeLns)
	{
		int offset = Integer.parseInt(PTreeLns.get(0))+1;
		int No_joins = PTreeLns.size()-offset;
		String[] QJoins = new String[No_joins];
		for(int i=0;i<No_joins;i++)
		{
			QJoins[i] = PTreeLns.get(offset+i);
		}
		return QJoins;
	}

	private List<String> ReadQuery(String Querypath)
	{
		List<String> querylinelist = null;
		BufferedReader mainqueryreader = null;
		try
		{
			String line;
 			querylinelist = new ArrayList<String>();
			mainqueryreader = new BufferedReader(new FileReader(Querypath));
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
	}

	private String[] GetCombinedTags(String[] PT1Tags, String[] PT2Tags)
	{
		Set<String> Tag_hash_Set = new HashSet<String>();
		String[] unique_ele = null;
		String storestr = null;
		int itr = 0;

		Tag_hash_Set.addAll(Arrays.asList(PT1Tags));
		if(PT2Tags != null)
		{
			Tag_hash_Set.addAll(Arrays.asList(PT2Tags));			
		}

		//Tag_hash_Set.remove("*");

		unique_ele = new String[Tag_hash_Set.size()];
		java.util.Iterator<String> it = Tag_hash_Set.iterator();

		while(it.hasNext()){
			storestr = it.next();
			storestr = storestr.length() > 5 ? storestr.substring(0, 5) : storestr;
			unique_ele[itr++] = storestr;			
		}		
     	return unique_ele;
	}

	private void displist(String[] d)
	{
		for(int i=0,n=d.length;i<n;i++)
		{
			System.out.println(d[i]);
		}
		System.out.println("----------");
	}

    private int[] GetProjListForTagJoin(List<Integer> Fieldnos, String[] PTJoins, int Tagindex)
    {
        List<Integer> mychildrens = new ArrayList<Integer>();
        mychildrens.add(Tagindex-1);
        for(int i=0,n=PTJoins.length;i<n;i++)
        {
            String[] splitstr = PTJoins[i].split(" ");
            if(Tagindex == Integer.parseInt(splitstr[0]))
            {
                mychildrens.add(Integer.parseInt(splitstr[1])-1);
            }
        }
        int[] arr = new int[mychildrens.size()*2];
        int pos=0;
        for(int i=0,n=mychildrens.size();i<n;i++)
        {
            int ind = (Fieldnos.indexOf(mychildrens.get(i))+1)*2;
            arr[pos++]=ind-1;
            arr[pos++]=ind;
        }
        Arrays.sort(arr);
        return arr;
    }  

    private int FindParentGBy(String[] PTJoins, int index1)
    {
    	List<Integer> ancesters = new ArrayList<Integer>();
    	List<String> relation = new ArrayList<String>();

    	for(int i=0,n=PTJoins.length;i<n;i++)
    	{
    		String[] splitstr = PTJoins[i].split(" ");
    		int ft = Integer.parseInt(splitstr[0]);
    		int sc = Integer.parseInt(splitstr[1]);
    		String rel = splitstr[2];

    		if(sc == index1)
    		{
    			ancesters.add(ft);
    			relation.add(rel);
    		}
    	}
    	if(ancesters.size() == 0)
    	{
    		return -1;
    	}
    	else if(relation.indexOf("PC")>-1)
    	{
    		return ancesters.get(relation.indexOf("PC"));
    	}
    	else
    	{
    		return ancesters.get(ancesters.size()-1);
    	}
    }

    private void PrintStringTuple(Tuple tup, AttrType[] attrtype, PrintClass pobj)
    {
    	String[] tempstrarr = new String[attrtype.length/2];
    	intervaltype itval;
    	String str;
    	try
    	{
	     	for(int i=0,n=tempstrarr.length;i<n;i++)
	    	{
	    		itval = tup.getIntervalFld(2*i+1);
	    		str   = tup.getStrFld(2*i+2);
	    		tempstrarr[i] = str+" ["+itval.get_s()+","+itval.get_e()+"]";
	    		//System.out.print(tempstrarr[i]+"  ");
	    	}
	    	pobj.PrintStringArr(tempstrarr);   	
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }

    private boolean CheckForNullJoin(TagParams[] tag_param_arr, String[] tag_param_tags, String[] PTTags, String[] PTJoins)
    {

    	boolean nullflag = false;
    	HashMap<String, Integer> TagtoPos = new HashMap<>();
		for(int i=0,n=tag_param_tags.length;i<n;i++)
		{
			TagtoPos.put(tag_param_tags[i], i);
		}

		for(int i=0,n=PTJoins.length;i<n;i++)
		{
			String[] Jnfields = PTJoins[i].split(" ");
			int rlftfld = Integer.parseInt(Jnfields[0])-1;
			int rlscfld = Integer.parseInt(Jnfields[1])-1;

			int proftfld = TagtoPos.get(PTTags[rlftfld]);
			int proscfld = TagtoPos.get(PTTags[rlscfld]);

			nullflag = ((tag_param_arr[proftfld].GetRcCount() == 0) || (tag_param_arr[proscfld].GetRcCount() == 0));
			if(nullflag)
				break;
		}
		return nullflag;
    }

    private void QueryPlanDisp(String[] PTTags, String[] PTJoins)
    {
    	String merge = "";

    	for(int i=0,n=PTJoins.length;i<n;i++)
    	{
    		String temp = null;
			String[] Jnfields = PTJoins[i].split(" ");
			int rlftfld = Integer.parseInt(Jnfields[0])-1;
			int rlscfld = Integer.parseInt(Jnfields[1])-1;
			String relation = Jnfields[2];

			temp = "("+PTTags[rlftfld]+" "+relation+" "+PTTags[rlscfld]+")";
			if(i!=0)
				merge = "("+merge+" EQ "+ temp+")";				
			else
				merge = new String(temp);
    	}
    	System.out.println("Query Plan...");
    	System.out.println(merge);
    }

	//Get the main heap file and the query file and fire queries
	public TagParams[] ReadQueryAndExecute(TagParams Tagpar, BTreePars btreepr, String Queryfilename, String folderpath)
	throws IOException
	{
		long startTime, endTime, durationInNano, durationInMillis;

		String PatterTreeName1    = null, PatterTreeName2 = null;
		String[] PTree1Tags       = null, PTree2Tags      = null;
		String[] PTree1Joins      = null, PTree2Joins     = null;
		List<String> PTree1Lns    = null, PTree2Lns       = null; 
		TagParams[] JoinedTagsPT1 = null, JoinedTagsPT2   = null;
		TagParams[] ResultPT1     = null, ResultPT2       = null;
		List<Integer> NLJTags1    = null, NLJTags2        = null;

		List<String> MainQLines     = ReadQuery(Queryfilename);
		int          No_lines       = MainQLines.size();
		String[]     split_string   = null;
 		boolean      PT2TreePresent = false;
		String       Operation      = null;
		int          index1         = -1, index2 = -1;
		boolean      nulljoin1      = false, nulljoin2 = false;
		int          bufferframes   = Integer.parseInt((MainQLines.get(No_lines-1).split(" "))[1]);
		PrintClass pobj = null;


		switch(No_lines)
		{
			case 4:
				Operation = "Display";
				PT2TreePresent = false;
				break;

			case 5:
				PT2TreePresent = false;
				split_string = MainQLines.get(3).split(" ");
				Operation = split_string[1];
				index1 = Integer.parseInt(split_string[2]);
				break;

			case 6:
				PT2TreePresent = true;
				split_string = MainQLines.get(4).split(" ");
				Operation = split_string[1];
				if(split_string.length>2)
				{
					index1 = Integer.parseInt(split_string[2]);
					index2 = Integer.parseInt(split_string[3]); 				
				}
				break;

			default:
				throw new IOException("Error in query file\n");
				//break;
		}

/*		System.out.print(PT2TreePresent);
		System.out.print("  "+Operation);
		System.out.printf(" %s %s ", index1, index2);*/

		PatterTreeName1 = folderpath+((MainQLines.get(2).split(" "))[1])+".txt";
		PTree1Lns       = ReadQuery(PatterTreeName1);
		PTree1Tags      = GetQueryTags(PTree1Lns);
		PTree1Joins     = GetQueryJoins(PTree1Lns);

		if(PT2TreePresent)
		{
			PatterTreeName2 = folderpath+((MainQLines.get(3).split(" "))[1])+".txt";	
			PTree2Lns       = ReadQuery(PatterTreeName2);		
			PTree2Tags      = GetQueryTags(PTree2Lns);
			PTree2Joins     = GetQueryJoins(PTree2Lns);			
		}

		String[] UniqueTags = GetCombinedTags(PTree1Tags, PTree2Tags);

		displist(UniqueTags);
		startTime = System.nanoTime();		

		TagParams[] AllTags = ExtractBtreeTagToHeap(Tagpar, btreepr, UniqueTags);	
		System.out.println("Tags Extracted from Heap File..");

		endTime = System.nanoTime();
		durationInNano = (endTime - startTime);
		durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);  //Total execution time in nano seconds
    	System.out.println("Tag extraction time = "+durationInMillis);

    	nulljoin1 = CheckForNullJoin(AllTags, UniqueTags, PTree1Tags, PTree1Joins);
    	if(PT2TreePresent)
    	{
    		nulljoin2 = CheckForNullJoin(AllTags, UniqueTags, PTree2Tags, PTree2Joins);
    	}

	    NLJTags1 = new ArrayList<Integer>();
	    NLJTags2 = new ArrayList<Integer>();


    	if(nulljoin1 || nulljoin2)
    	{
    		Operation = "Record Zero";			
    	}
    	else
    	{

    		JoinedTagsPT1 = JoinAllPairTags(AllTags, UniqueTags, PTree1Tags, PTree1Joins, NLJTags1);
			if(PT2TreePresent)
			{
				JoinedTagsPT2 = JoinAllPairTags(AllTags, UniqueTags, PTree2Tags, PTree2Joins, NLJTags2);
			}
			System.out.println("Tags Joined..");
		
			ResultPT1 = MakeQueryPlanner(JoinedTagsPT1, PTree1Joins);
			if(PT2TreePresent)
			{
				ResultPT2 = MakeQueryPlanner(JoinedTagsPT2, PTree2Joins);
				
			}
			if(PT2TreePresent)
			{
				pobj = new PrintClass(PTree1Tags, PTree1Joins, ResultPT1[0].GetFieldNos(), PTree2Tags, PTree2Joins, ResultPT2[0].GetFieldNos());
			}
			else
			{
				pobj = new PrintClass(PTree1Tags, PTree1Joins, ResultPT1[0].GetFieldNos(), null, null, null);
			}	
    	}
		try
		{
			switch(Operation)
			{
				case "TJ":
					System.out.println("Tag Join\n");
					TagJoin(ResultPT1[0], ResultPT2[0], index1, index2, bufferframes, pobj);
					break;

				case "SRT":
					System.out.println("Sort\n");
					SortRes(ResultPT1[0], index1, bufferframes, pobj);
					break; 

				case "GRP":
					System.out.println("Group By\n");
					GroupBy(ResultPT1[0], PTree1Tags, index1, bufferframes, pobj);
					break;

				case "CP":
					System.out.println("Cartesian Product\n");
					CartProd(ResultPT1[0], ResultPT2[0], bufferframes, pobj);
					break;

				case "NJ":
					System.out.println("Node Join on ID\n");
					NodeJoinID(ResultPT1[0], ResultPT2[0], index1, index2, bufferframes, pobj);
					break;

				case "Display":
					System.out.println("Display\n");
					Display(ResultPT1[0], bufferframes, pobj);
					break;

				default:
					System.out.println("Invalid Case/Zero Result Case");
					System.out.println("Total Records Fetched = 0");
					break;
			}			
		}

		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			System.out.println("Clearing Memory");
			for(int i=0;i<AllTags.length;i++)
			{
				if(!UniqueTags[i].equals("*"))
				{
					DeleteHeapFile(AllTags[i]);			
				}
			}
			for(int i=0,n=NLJTags1.size();i<n;i++)
			{
				DeleteHeapFile(JoinedTagsPT1[NLJTags1.get(i)]);
			}

			for(int i=0,n=NLJTags2.size();i<n;i++)
			{
				DeleteHeapFile(JoinedTagsPT2[NLJTags2.get(i)]);
			}
			QueryPlanDisp(PTree1Tags, PTree1Joins);	
			System.out.printf("Query executed\nreads = %s writes = %s\nNumber of Buffer Frames = %s\n", PCounter.getreads(), PCounter.getwrites(), bufferframes);
			//PCounter.initialize();
		}
		return null;
	}

	private void Display(TagParams leftPT, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator ltItrtr  = leftPT.GetIterator();	
		AttrType [] ltattrtype     = leftPT.GetAtrTypes();
		short []    ltstrszs       = leftPT.GetAtrSizes();
		int         ltrttupsize    = leftPT.GetSizeofTuple();
		List<Integer> ltField      = leftPT.GetFieldNos();
		int outer = ltattrtype.length;	
		Tuple temptup = null;
		int count_records = 0;
		try
		{
			while ((temptup=ltItrtr.get_next())!=null)
			{ 
				PrintStringTuple(temptup, ltattrtype, pobj);
				temptup.print(ltattrtype);	 
				count_records++;
			}
			System.out.printf("Total records fetched = %s\n", count_records);
			ltItrtr.close();
			pobj.CloseFile();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}		
	}

	private void GroupBy(TagParams leftPT, String[] PT1Tags, int index1, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator ltItrtr  = leftPT.GetIterator();	
		AttrType [] ltattrtype     = leftPT.GetAtrTypes();
		short []    ltstrszs       = leftPT.GetAtrSizes();
		int         ltrttupsize    = leftPT.GetSizeofTuple();
		List<Integer> ltField      = leftPT.GetFieldNos();
		int outer = ltattrtype.length;	

		Tuple temptup = null;
		int count_records = 0;
		int fldtosrch = index1-1;
		int index = (ltField.indexOf(fldtosrch)+1)*2;
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sortobj = null;
		if(!PT1Tags[index1-1].equals("*"))
		{
			index-=1;
		}
		try
		{
			sortobj = new Sort (ltattrtype, (short)outer, ltstrszs, ltItrtr, index, ascending, 10, bufferframes);
			while ((temptup=sortobj.get_next())!=null)
			{ 
				PrintStringTuple(temptup, ltattrtype, pobj);
				temptup.print(ltattrtype);	
				count_records++;
			}
			sortobj.close();
			System.out.printf("Total records fetched = %s\n", count_records);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
	}

	private void CartProd(TagParams leftPT, TagParams righPT, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator ltItrtr  = leftPT.GetIterator();	
		AttrType [] ltattrtype   = leftPT.GetAtrTypes();
		short []    ltstrszs     = leftPT.GetAtrSizes();
		int         ltrttupsize  = leftPT.GetSizeofTuple();
		List<Integer> ltField    = leftPT.GetFieldNos();
		int outer = ltattrtype.length;
  	
  		iterator.Iterator rtItrtr  = righPT.GetIterator();	
		AttrType [] rtattrtype = righPT.GetAtrTypes();
		short []    rtstrszs   = righPT.GetAtrSizes();
		int         rttupsize  = righPT.GetSizeofTuple();
		List<Integer> rtField  = righPT.GetFieldNos();
		int inner = rtattrtype.length;

		String hpflname =  GetRandomName();
		Tuple temptup   = null;
		RID rtrid       = new RID(); 
		
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> fldtokeep = new ArrayList<Integer>();
		for(int i=0;i<rtField.size();i++)
		{
			fldtokeep.add(i+1);
		}
		FldSpec[] proj_arr = GetProjections(inner, ftoO1);
		//List<Integer> ftoO2 = GetFieldtoKeep(Fieldnos1, Fieldnos2);

		AttrType [] joinattrtype = JoinAttrtype(ltattrtype, fldtokeep);
		short [] joinstrtype = JoinStrtype(joinattrtype);
		int jointupsz = JoinedTupSize(joinattrtype, joinstrtype);

		FldSpec[] proj_arr_nlj = GetProjections(outer, fldtokeep);
		int count_records =0;
		//FileScan fscan = null; 
		NestedLoopsJoins nlj = null;

		try
		{
			Heapfile hpfile = new Heapfile(hpflname);
			while ((temptup=rtItrtr.get_next())!=null)
			{ 
				rtrid = hpfile.insertRecord(temptup.returnTupleByteArray());
			}
			rtItrtr.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		try
		{
			//fscan = new FileScan(hpflname, rtattrtype, rtstrszs, (short) inner, inner, proj_arr, null);  //file scan pointer 
			nlj = new NestedLoopsJoins(ltattrtype, outer, ltstrszs, rtattrtype, inner, rtstrszs, bufferframes, ltItrtr, hpflname, null, null, proj_arr_nlj, outer+inner);
			while ((temptup=nlj.get_next())!=null)
			{ 
				PrintStringTuple(temptup, joinattrtype, pobj);
				temptup.print(joinattrtype);	 
				count_records++;
			}
			System.out.printf("Total records fetched = %s\n", count_records);
			nlj.close();
			pobj.CloseFile();

		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	private void SortRes(TagParams leftPT, int fldtosort, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator itr = leftPT.GetIterator();
		AttrType [] attrtype  = leftPT.GetAtrTypes();
		short []    strszs    = leftPT.GetAtrSizes();
		int         tupsize   = leftPT.GetSizeofTuple();	
		List<Integer> Fields    = leftPT.GetFieldNos();
		int outer = attrtype.length;

		Tuple temptup = null;
		int count_records = 0;
		int fldtosrch = fldtosort-1;
		int index = (Fields.indexOf(fldtosrch))*2+1;
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sortobj = null;

		try
		{
			sortobj = new Sort (attrtype, (short)outer, strszs, itr, index, ascending, strszs[0], bufferframes);
			while ((temptup=sortobj.get_next())!=null)
			{ 
				PrintStringTuple(temptup, attrtype, pobj);
				temptup.print(attrtype);	 
				count_records++;
			}
			System.out.printf("Total records fetched = %s\n", count_records);
			sortobj.close();
			pobj.CloseFile();	
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
	}

	private void NodeJoinID(TagParams leftPT, TagParams righPT, int indexlt, int indexrt, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator ltindxscan = leftPT.GetIterator();
    	AttrType [] lttagattrtype    = leftPT.GetAtrTypes();
		short    [] lttagattrsize    = leftPT.GetAtrSizes();
		int      lttagtupsize        = leftPT.GetSizeofTuple();
		//String   taghpfilename1    = tag1.GetHPFileName();
		int      outer               = lttagattrtype.length;
		List<Integer> ltFieldnos     = leftPT.GetFieldNos();

		iterator.Iterator rtindxscan = righPT.GetIterator();
    	AttrType [] rttagattrtype    = righPT.GetAtrTypes();
		short    [] rttagattrsize    = righPT.GetAtrSizes();
		int      rttagtupsize        = righPT.GetSizeofTuple();
		String   rttaghpfilename     = null;
		int      inner               = rttagattrtype.length;
		List<Integer> rtFieldnos     = righPT.GetFieldNos();

		int indexltjn = (ltFieldnos.indexOf(indexlt-1))*2+1;
		int indexrtjn = (rtFieldnos.indexOf(indexrt-1))*2+1;

		CondExpr[] outFilter = GenerateCondExpr(indexltjn, indexrtjn, false, false);
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		List<Integer> fldtokeep = new ArrayList<Integer>();
		for(int i=0;i<rtFieldnos.size();i++)
		{
			fldtokeep.add(i+1);
		}

		FldSpec[] proj_arr_sm = GetProjections(outer, fldtokeep);
		AttrType [] joinattrtype = JoinAttrtype(lttagattrtype, fldtokeep);
		Tuple temptup = null;
		int count_records = 0;
    	SortMerge sm = null;
		try 
	    {
			sm = new SortMerge(lttagattrtype, outer, lttagattrsize, rttagattrtype, inner, rttagattrsize, indexltjn, 10, indexrtjn, 10, bufferframes, ltindxscan, rtindxscan, false, false, ascending, outFilter, proj_arr_sm, outer+inner);
			while ((temptup=sm.get_next())!=null)
			{ 
				PrintStringTuple(temptup, joinattrtype, pobj);
				temptup.print(joinattrtype);	
				count_records++;
			}
			sm.close();
			System.out.printf("Total records fetched = %s\n", count_records);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for sort merge join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
	}

	private void TagJoin(TagParams leftPT, TagParams rightPT, int index1, int index2, int bufferframes, PrintClass pobj)
	{
		iterator.Iterator ltindxscan = leftPT.GetIterator();
    	AttrType [] lttagattrtype    = leftPT.GetAtrTypes();
		short    [] lttagattrsize    = leftPT.GetAtrSizes();
		int      lttagtupsize        = leftPT.GetSizeofTuple();
		//String   taghpfilename1    = tag1.GetHPFileName();
		int      outer               = lttagattrtype.length;
		List<Integer> ltFieldnos     = leftPT.GetFieldNos();

		iterator.Iterator rtindxscan = rightPT.GetIterator();
    	AttrType [] rttagattrtype    = rightPT.GetAtrTypes();
		short    [] rttagattrsize    = rightPT.GetAtrSizes();
		int      rttagtupsize        = rightPT.GetSizeofTuple();
		String   rttaghpfilename     = null;
		int      inner               = rttagattrtype.length;
		List<Integer> rtFieldnos     = rightPT.GetFieldNos();

		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel);

		int indexltjn = ((ltFieldnos.indexOf(index1-1))+1)*2;
		int indexrtjn = ((rtFieldnos.indexOf(index2-1))+1)*2;

		List<Integer> fldtokeep = new ArrayList<Integer>();
		for(int i=0;i<rtFieldnos.size();i++)
		{
			fldtokeep.add(i+1);
		}

		CondExpr[] outFilter = GenerateCondExpr(indexltjn, indexrtjn, false, false);
		FldSpec[] projlist_sm = GetProjections(outer, fldtokeep);
		AttrType [] joinattrtype = JoinAttrtype(lttagattrtype, fldtokeep);
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

		int no_out_fields = outer + inner;
		Tuple temptup_sm = null;
		int count_records = 0;
		SortMerge sm = null;

		try
		{
			sm = new SortMerge(lttagattrtype, outer, lttagattrsize, rttagattrtype, inner, rttagattrsize, indexltjn, 10, indexrtjn, 10, bufferframes, ltindxscan, rtindxscan, false, false, ascending, outFilter, projlist_sm, no_out_fields);
			while ((temptup_sm=sm.get_next())!=null)
			{ 
				PrintStringTuple(temptup_sm, joinattrtype, pobj);
				temptup_sm.print(joinattrtype);	
				count_records++;
			}
			sm.close();
			System.out.printf("Total records fetched = %s\n", count_records);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
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

public class XMLTest2// implements  GlobalConst
{
	public static void main(String [] argvs) 
	{
		//String DataFileName = "./xml_sample_data_part.xml";
		String DataFileName = "./xml_sample_data.xml";
		//String QueryFileName = "./queryfile.txt";
		//String DataFileName = "./plane.xml";
		String QFilePath = "./input_files/Query.txt";
		String FolderPath = "./input_files/";
		try
		{
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
/*	    	try
	    	{
				BufferedWriter obj = new BufferedWriter(new FileWriter("output.txt", true));    		
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}*/


			//TagParams[] qresult = xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QueryFileName);
			//xmldvr.ScanTagParams(qresult[0]);
			//BTreePars btpar = xmldvr.CreateBTreeIndex(SortedTagPair);
			//xmldvr.ExtractTagBTree(SortedTagPair, btf, "crane");
			//xmldvr.ScanHeapFile(SortedTagPair);
			/*
			String[] tagarr = new String[2];
			tagarr[0]="year";
			tagarr[1]="model";*/
			//xmldvr.ExtractBtreeTagToIndex(SortedTagPair, btpar, tagarr);
		
/*			Set<String> hash_Set = new HashSet<String>(); 
			String[] arr = {"aaa", "bbb", "ccc", "ddd","bbb", "ccc", "ddd" };
			hash_Set.addAll(Arrays.asList(arr));;
			hash_Set.add("Geeks"); 
			hash_Set.add("For"); 
			hash_Set.add("Geeks"); 
			hash_Set.add("Example"); 
			hash_Set.add("Set"); 	
			System.out.println(hash_Set.contains("Geeks"));	
			System.out.println(hash_Set.remove("Geeddks"));*/
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
							//System.out.println("Enter you query file name with extension\n");
							//String QueryFileName = "input_files/queryfile.txt";//console.readLine();
							xmldvr.ReadQueryAndExecute(SortedTagPair, btreepr, QFilePath, FolderPath);
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



