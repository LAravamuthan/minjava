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

class Node
{
	private int parent;
	private int[] nodeIntLbl;
	private String nodeTag;

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


class TagParams
{
	private Heapfile tagheapfile = null;
	private RID tagrid = null;
	private String hpfilename = null;

	private AttrType[] AtrTypes = null;
	private short[] AtrSizes = null;
	private int SizeofTuple;
 
	public TagParams(Heapfile hfile, RID rid, String hpfname, AttrType[] atrtypes, short[] atrsizes, int sizetup)
	{
		this.tagheapfile = hfile;
		this.tagrid = rid;
		this.hpfilename = hpfname;
		this.AtrTypes = atrtypes;
		this.AtrSizes = atrsizes;
		this.SizeofTuple = sizetup;
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
}

class TagparamField
{
	private TagParams tgpr = null;
	private List<Integer> Fldtrk = null;

	public TagparamField(TagParams tagpar, List<Integer> fldtrk)
	{
		this.tgpr = tagpar;
		this.Fldtrk = fldtrk;
	}

	public TagParams GetTagParams()
	{
		return this.tgpr;
	}

	public List<Integer> GetFldtrk()
	{
		return this.Fldtrk;
	}
}


class XMLLineParser
{
	public XMLLineParser(){}

    public List<String> ExtraxtContent(String line)
    {   
        List<String> XMLTags = new ArrayList<String>();
       // String[] tag_values = line.trim();
        line = line.trim();
        int firstspace = line.indexOf(" ");
        if(firstspace < 0)
        {
            String justelem = line.length() > 5 ? line.substring(0, 5) : line;
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
			this.reader = new BufferedReader(new FileReader(fileName));
			this.xmlobj = new XMLLineParser();
			this.stack = new Stack<Node>();
			this.IntervalNo = 1;
			this.rid = null;
			this.hpfilename = "XMLtags.in";

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
				this.tplwtr.setHdr((short) 3, Stypes, Ssizes);
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

	private void PushOnStack(String nodename)
	{
		Node nd;
		if(this.stack.empty())
		{
			nd = new Node(-1, this.IntervalNo, nodename);		
		}
		else
		{
			nd = new Node(this.stack.peek().GetStart(), this.IntervalNo, nodename);
		}
		this.stack.push(nd);
		this.IntervalNo+=1;
	}

	private void PopFromStackWriteFile() throws IOException, EmptyStackException
	{
		Node nd = stack.pop();
		nd.SetEnd(this.IntervalNo);
		this.IntervalNo+=1;
		PushNodeToHeapFile(nd);
	}

	private void WriteFileLbyL(List<String> nodes)
	{
		for(int i=0;i<nodes.size();i++)
		{
			String nname = nodes.get(i);
			if(nname.charAt(1) == '/')
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
			this.rid = this.hpfile.insertRecord(this.tplwtr.returnTupleByteArray());
		}
		catch (Exception e) 
		{
			System.err.println("*** error in Heapfile.insertRecord() ***");
			e.printStackTrace();
		}      		
	}


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
			tgpr = new TagParams(this.hpfile, this.rid, this.hpfilename, GetAttrType(), GetStrSizes(), GetTupleSize());

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

  	//ContainOrEquality == true check containment or else check equality
	public CondExpr[] GenerateCondExpr(int opersymbfld1, int opersymbfld2, boolean ContainOrEquality)
	{
		CondExpr[] outFilter  = new CondExpr[1];

		outFilter[0] = new CondExpr();
		outFilter[0].next = null;

		if(ContainOrEquality)
		{
			outFilter[0].op = new AttrOperator(AttrOperator.aopLT);			
		}
		else
		{
			outFilter[0].flag = 0;
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);	
		}

		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), opersymbfld1);
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), opersymbfld2);

		return outFilter;
	}


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

  	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality

	public TagParams JoinTwoFields(TagParams tag1, int joinfieldno1, TagParams tag2, int joinfieldno2, List<Integer> ltfieldtoomit, boolean parentchildflag, boolean ContainOrEquality)
	{

    	AttrType [] tagattrtype1  = tag1.GetAtrTypes();
		short    [] tagattrsize1  = tag1.GetAtrSizes();
		int      tagtupsize1      = tag1.GetSizeofTuple();
		String   taghpfilename1   = tag1.GetHPFileName();
		int      outer            = tagattrtype1.length;

    	AttrType [] tagattrtype2  = tag2.GetAtrTypes();
		short    [] tagattrsize2  = tag2.GetAtrSizes();
		int      tagtupsize2      = tag2.GetSizeofTuple();
		String   taghpfilename2   = tag2.GetHPFileName();
		int      inner            = tagattrtype2.length;


		AttrType[] JoinedTagAttrtype = null;
		short [] JoinedTagsize = null;
		int JoinedTagTupSize = 0;

		byte[] array = new byte[10]; 
		new Random().nextBytes(array); 

		String JoinedTaghpfilename = new String(array, Charset.forName("UTF-8")); 
		Heapfile JoinedTaghpfile = null;

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
		projlist_tag1 = GetProjections(outer, 0, ftoO1);
		projlist_tag2 = GetProjections(outer, inner, ftoO2);

		/*
		for(int i=0;i<projlist_tag2.length;i++)
		{
			System.out.println(projlist_tag2[i].offset);
		}*/

		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(joinfieldno1, joinfieldno2, ContainOrEquality);

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

		FileScan fscan = null;
		try 
		{
			fscan = new FileScan(taghpfilename1, tagattrtype1, tagattrsize1, (short) outer, outer, projlist_tag1, null);
		}
		catch (Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
		}

	    NestedLoopsJoins nlj = null;
	    try 
	    {
	    	nlj = new NestedLoopsJoins(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, 10, fscan, taghpfilename2, outFilter, null, projlist_tag2, outer+inner-(3*ltfieldtoomit.size()));
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
		intervaltype intval;
		try 
		{
			while ((temptup=nlj.get_next()) !=null) 
			{
				if(parentchildflag)
				{
					if(temptup.getIntervalFld(2).get_s() == temptup.getIntFld(4))
					{
						JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());		
					}
				}
				else
				{
					JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());
				}
			}
		}
		catch (Exception e)
		{
			System.err.println ("*** Error preparing for get_next tuple");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return new TagParams(JoinedTaghpfile, JoinedTagRID, JoinedTaghpfilename, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
	}



	public TagParams[] ExtractTagToHeap(TagParams tag_params, String[] tagnames)
	{

		Heapfile heaptosearch = tag_params.GetHeapFile();
		RID ridtosearch = tag_params.GetRID();

		int tot_len = tagnames.length;

		Heapfile[] heaptostore = new Heapfile[tot_len];
		RID[] ridtostore = new RID[tot_len];
		String[] hpfilenm = new String[tot_len];

		TagParams[] tag_pars = new TagParams[tot_len];

		for(int i=0;i<tot_len;i++)
		{
			hpfilenm[i] = tagnames[i]+".in";
		}
	
		boolean done = false;
		String tag;
		int sizetup;
		AttrType [] Stps = GetAttrType();
		short [] Sszs = GetStrSizes();
		int TupSize  = GetTupleSize();

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
				heaptostore[i] =  new Heapfile(hpfilenm[i]);
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
					if(tag.equals(tagnames[i]))
					{
						try
						{
							ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray());
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

		for(int i=0;i<tot_len;i++)
		{
			tag_pars[i] =  new TagParams(heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize);
		}
		return tag_pars;
	}


	public void ReverseArray(String[] arr)
	{
		String temp;
		int len = arr.length;

		for(int i=0;i<(len/2);i++)
		{
			temp = arr[i];
			arr[i] = arr[len-1-i];
			arr[len-1-i] = temp;
		}
	}

	public void AddField(List<Integer> FieldTracker, int num)
	{
		if(FieldTracker.indexOf(num) == -1)
		{
			FieldTracker.add(num);
		}
	}

	public int[] GetFieldsToJoin(List<Integer> FieldTracker, int ftfld, int scfld)
	{
		int[] fields = new int[2];
		if(FieldTracker.indexOf(ftfld) > -1)
		{
			fields[0] = (3*FieldTracker.indexOf(ftfld))+2;
			fields[1] = 2;
		}
		else if(FieldTracker.indexOf(scfld) > -1)
		{
			fields[0] = (3*FieldTracker.indexOf(scfld))+2;
			fields[1] = 5;		
		}
		else
		{
			fields[0] = -1;
			fields[1] = -1;
		}
		return fields;
	}

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
  	//parentchildflag == true check parent child or else check ancester descendant
  	//ContainOrEquality == true check containment or else check equality

	public TagparamField Query(TagParams[] ExtractTags, String[] Joins)
	{
		TagParams ResultTagPar = null;
		TagParams TempTagPar = null;
		List<Integer> FieldTracker = new ArrayList<Integer>();
		List<Integer> ltfieldtoomit = new ArrayList<Integer>();

		for(int i=0;i<Joins.length;i++)
		{
			String[] Jnfields = Joins[i].split(" ");
			int ftfld = Integer.parseInt(Jnfields[0])-1;
			int scfld = Integer.parseInt(Jnfields[1])-1;
			String typerel = Jnfields[2];
			boolean relflag = false;
			int[] fields = null;

			if(typerel.equals("PC"))
			{
				relflag = true;
			}

			TempTagPar = JoinTwoFields(ExtractTags[ftfld], 2, ExtractTags[scfld], 2, ltfieldtoomit, relflag, true);

			if(ResultTagPar != null)
			{
				fields = GetFieldsToJoin(FieldTracker, ftfld, scfld);
				ltfieldtoomit = GetMyOmitList(FieldTracker, ltfieldtoomit, ftfld, scfld);
				ResultTagPar = JoinTwoFields(ResultTagPar, fields[0], TempTagPar, fields[1], ltfieldtoomit, false, false);
				AddField(FieldTracker, ftfld);
				AddField(FieldTracker, scfld);
				ltfieldtoomit.clear();
			}
			else
			{
				ResultTagPar = TempTagPar;
				AddField(FieldTracker, ftfld);
				AddField(FieldTracker, scfld);
			}
		}
		return new TagparamField(ResultTagPar, FieldTracker);
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

	public TagParams JoinQuery(TagparamField query1,TagparamField query2)
	{
		TagParams tagpar1 = query1.GetTagParams();
		TagParams tagpar2 = query2.GetTagParams();
		List<Integer> fldtrk1 = query1.GetFldtrk();
		List<Integer> fldtrk2 =query2.GetFldtrk();

		List<Integer> ltfieldtoomit = new ArrayList<Integer>();

		int[] fields = new int[2];

		for(int i=0;i<fldtrk2.size();i++)
		{
			if(fldtrk1.indexOf(fldtrk2.get(i)) > -1)
			{
				ltfieldtoomit.add(i+1);
			}
		}

		for(int i=0;i<fldtrk2.size();i++)
		{
			if(fldtrk1.indexOf(fldtrk2.get(i)) > -1)
			{
				fields[0] = 3*fldtrk1.indexOf(fldtrk2.get(i))+2;
				fields[1] = 3*i+2;
				break;
			}
		}


		TagParams ResQueryJoin = JoinTwoFields(tagpar1, fields[0], tagpar2, fields[1], ltfieldtoomit, false, false);
		return ResQueryJoin;

	}


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
			if(count == 2)
			{
				break;
			}
		}
		return splitlist;
	}

	public TagParams[] MakeQueryPlanner(TagParams[] AllTags, String[] NumberofJoins)
	{
		TagparamField tf1 = null;
		TagparamField tf2 = null;

		TagParams[] tgprarr = new TagParams[3];

		//PCounter.initialize();

		tgprarr[0] = Query(AllTags, NumberofJoins).GetTagParams();
		System.out.println("Query 1 executed");
		System.out.printf("reads = %s writes = %s\n", PCounter.getreads(), PCounter.getwrites());

		int[] spliter = querypossible(NumberofJoins);

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
				System.out.printf("Query %s not possible\n", i+1);
			}

		}
		
		return tgprarr;
	}

	public TagParams[] ReadQueryAndExecute(TagParams MainTagpair, String Queryfilename) 
	{
		TagParams QueryResult = null;
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
			searchtags[i-1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
		}

		String[] NumberofJoins = new String[querylinelist.size()-numberofnodes-1]; 
		for(int i=0;i<NumberofJoins.length;i++)
		{
			NumberofJoins[i] = querylinelist.get(numberofnodes+1+i);
		}
		TagParams[] AllTags = ExtractTagToHeap(MainTagpair, searchtags);

		System.out.println("File Parsing Completed");
/*		try
		{
			MainTagpair.GetHeapFile().deleteFile();			
		}
		catch(Exception e)
		{
			e.printStackTrace();		
		}*/


		//ReverseArray(NumberofJoins);
		TagParams[] finalresult = MakeQueryPlanner(AllTags, NumberofJoins);
		//QueryResult = Query(AllTags, NumberofJoins);
		return finalresult;
	}

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
				temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
				temptup.print(Atrtyps);
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

public class XMLTest// implements  GlobalConst
{
	public static void main(String [] argvs) 
	{
		String DataFileName = "./xml_sample_data.xml";
		String QueryFileName = "./queryfile.txt";
		//String DataFileName = "./plane.xml";
		try
		{
			//System.out.println("Initializing XML Test object"); 
			XMLDriver xmldvr = new XMLDriver(DataFileName);
			//System.out.println("Reading XML file lines");
			TagParams MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();



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
			TagParams[] qresult = xmldvr.ReadQueryAndExecute(MainTagPair, QueryFileName);
			for(int i=0;i<qresult.length;i++)
			{
				if(qresult[i] != null)
				{
					xmldvr.ScanHeapFile(qresult[i]);
				}
		
			}
			//xmldvr.ScanHeapFile(qresult[0]);
			//xmldvr.Sort_My_Field();
			System.out.println(PCounter.getreads());
			System.out.println(PCounter.getwrites());
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println ("Error encountered during XML tests:\n");
			Runtime.getRuntime().exit(1);
		}
	} 
}





/*	public void Sort_My_Field()
	{

	    FldSpec[] projlist = new FldSpec[3];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    projlist[2] = new FldSpec(rel, 3);
	    int sizetup;

    	AttrType [] Stps = GetAttrType();
		short [] Sszs = GetStrSizes();
		int TupSize = GetTupleSize();

    	FileScan fscan = null;

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
		  fscan = new FileScan("Entry.in", Stps, Sszs, (short) 3, 3, projlist, null);
		}
		catch (Exception e)
		{
		  	e.printStackTrace();
		}

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sort_names = null;
		try 
		{
			sort_names = new Sort (Stps, (short)3, Sszs, fscan, 2, ascending, Sszs[0], 10);
		}
		catch (Exception e) 
		{
			System.err.println ("*** Error preparing for sorting");
			System.err.println (""+e);
			Runtime.getRuntime().exit(1);
		}
		try 
		{
			while ((tup=sort_names.get_next()) !=null) 
			{
				tup.print(Stps);
			}
		}
		catch (Exception e) 
		{
			System.err.println ("*** Error preparing for get_next tuple");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}
*/
