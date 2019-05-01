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

/*	public void PrintQueryResults(TagParams[] result_tgpr_arr, String[] searchtags)
	{
		for(int i=0,n=result_tgpr_arr.length;i<n;i++)
		{
			if(result_tgpr_arr[i]!=null)
			{
				ScanTagParamsWTags(result_tgpr_arr[i], searchtags);			
			}

		}
	}

	public void ScanTagParamsWTags(TagParams tgprms, String[] searchtags)
	{
		iterator.Iterator Itrtr = tgprms.GetIterator();	
		AttrType[] tagatrtypes  = tgprms.GetAtrTypes();	
		List<Integer> fields    = tgprms.GetFieldNos();
		
		String[] tagnames = new String[fields.size()];
		Tuple temptup = null;
		int count_records = 0;
		for(int i=0, n=tagnames.length;i<n;i++)
		{
			tagnames[i] = searchtags[fields.get(i)];
		}

		try
		{
			while ((temptup=Itrtr.get_next())!=null)
			{ 
				temptup.print_interval(tagatrtypes, tagnames);
				count_records++;
			}
			Itrtr.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}

		System.out.printf("Total records fetched = %s\n", count_records);
	}*/


/*	//parentchildflag == true check parent child or else check ancester descendant
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
		List<Integer> Fieldscomb   = GetCombinedFlds(Fieldnos1, Fieldnos2);

		//String JoinedTaghpfilename = null; 
		//Heapfile JoinedTaghpfile = null;
		//RID JoinedTagRID = null;

		FldSpec[] projlist_tag1 = null, projlist_tag2  = null;
		List<Integer> ftoO1 = new ArrayList<Integer>();
		List<Integer> ftoO2 = GetFieldtoKeep(Fieldnos1, Fieldnos2);
		int no_out_fields = outer+(ftoO2.size());
		projlist_tag1 = GetProjections(outer, 0, ftoO1);			//generate projections for the left table
		projlist_tag2 = GetProjections(outer, inner, ftoO2);		//generate projections for the right table


		int[] jfld = GetFieldsToJoin(Fieldnos1, Fieldnos2);
		jtfld1 = jfld[0];
		jtfld2 = jfld[1];

		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(jtfld1, jtfld2, ContainOrEquality, false);	//generate condexpr for the two joining fields

		JoinedTagAttrtype = JoinAttrtype(tagattrtype1, ftoO2);
		JoinedTagsize     = new short[0];
		JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

    	SortMerge sm = null;
	    try 
	    {
			sm = new SortMerge(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, jtfld1, 10, jtfld2, 10, 500, indxscan1, indxscan2, false, false, ascending, outFilter, projlist_tag2, no_out_fields);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for sort merge join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
		finalitr = (iterator.Iterator)sm;
		return new TagParams(finalitr, Fieldscomb, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
	}*/
/*	public void GenerateHeapfile(TagParams tagpar)
	{
		iterator.Iterator itr = tagpar.GetIterator();
		if(itr!=null)
		{
			Tuple temptup = null;
			AttrType [] tagattrtype1  = tagpar.GetAtrTypes();
			short    [] tagattrsize1  = tagpar.GetAtrSizes();
			int      outer            = tagattrtype1.length;
			
			List<Integer> ftoO1 = new ArrayList<Integer>();
			FldSpec[] projlist = GetProjections(outer, 0, ftoO1);
			
			iterator.Iterator newhpfileitr = null;
			String hpfilename = GetRandomName();
			Heapfile hpfile = null;
			RID hprid = new RID();

			try
			{
				hpfile = new Heapfile(hpfilename);
				while ((temptup=itr.get_next())!=null) 
				{
					hprid = hpfile.insertRecord(temptup.returnTupleByteArray());	//insert the joined record into a new heap file	
				}
				itr.close();
			}
			catch (Exception e)
			{
				System.err.println("*** error in Heapfile constructor ***");
				e.printStackTrace();
			}
    		try
    		{
      			newhpfileitr  = new FileScan(hpfilename, tagattrtype1, tagattrsize1, (short) outer, (short) outer, projlist, null);
    		}
    		catch (Exception e)
    		{
      			System.err.println (""+e);
    		}

			tagpar.SetHPParams(hpfile, hprid, hpfilename);	
			tagpar.SetIterator(newhpfileitr);		
		}
	}*/
/*	public void GenerateHeapfile(TagParams tagpar)
	{
		iterator.Iterator itr = tagpar.GetIterator();
		if(itr!=null)
		{
			Tuple temptup = null;
			AttrType [] tagattrtype1  = tagpar.GetAtrTypes();
			short    [] tagattrsize1  = tagpar.GetAtrSizes();
			int      outer            = tagattrtype1.length;
			
			List<Integer> ftoO1 = new ArrayList<Integer>();
			FldSpec[] projlist = GetProjections(outer, 0, ftoO1);
			
			iterator.Iterator newhpfileitr = null;
			String hpfilename = GetRandomName();
			Heapfile hpfile = null;
			RID hprid = new RID();

			try
			{
				hpfile = new Heapfile(hpfilename);
				while ((temptup=itr.get_next())!=null) 
				{
					hprid = hpfile.insertRecord(temptup.returnTupleByteArray());	//insert the joined record into a new heap file	
				}
				itr.close();
			}
			catch (Exception e)
			{
				System.err.println("*** error in Heapfile constructor ***");
				e.printStackTrace();
			}
    		try
    		{
      			newhpfileitr  = new FileScan(hpfilename, tagattrtype1, tagattrsize1, (short) outer, (short) outer, projlist, null);
    		}
    		catch (Exception e)
    		{
      			System.err.println (""+e);
    		}

			tagpar.SetHPParams(hpfile, hprid, hpfilename);	
			tagpar.SetIterator(newhpfileitr);		
		}
	}*/
	public void PrintQueryResults(TagParams[] result_tgpr_arr, String[] searchtags)
	{
		for(int i=0,n=result_tgpr_arr.length;i<n;i++)
		{
			if(result_tgpr_arr[i]!=null)
			{
				ScanTagParamsWTags(result_tgpr_arr[i], searchtags);			
			}

		}
	}

	public void ScanTagParamsWTags(TagParams tgprms, String[] searchtags)
	{
		iterator.Iterator Itrtr = tgprms.GetIterator();	
		AttrType[] tagatrtypes  = tgprms.GetAtrTypes();	
		List<Integer> fields    = tgprms.GetFieldNos();
		
		String[] tagnames = new String[fields.size()];
		Tuple temptup = null;
		int count_records = 0;
		for(int i=0, n=tagnames.length;i<n;i++)
		{
			tagnames[i] = searchtags[fields.get(i)];
		}

		try
		{
			while ((temptup=Itrtr.get_next())!=null)
			{ 
				temptup.print_interval(tagatrtypes, tagnames);
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

/*	public TagParams[] ExtractBtreeTagToIndex(TagParams tag_param, BTreePars btreepar, String[] tagnames, String[] Joins)
	{
		CondExpr[] expr1 = null, expr2 = null;
		RelSpec rel = new RelSpec(RelSpec.outer); 
		FldSpec[] projlist = { new FldSpec(rel, 1), new FldSpec(rel, 2) };

	    int tot_len = tagnames.length;
	    int TotalJoins = Joins.length;
		TagParams[] TgPr = new TagParams[tot_len];
		List<Integer> fldlist = null; 

		String heapfilename = tag_param.GetHPFileName();
		AttrType[] tagattr_main = tag_param.GetAtrTypes();
		short [] tagstrszs_main = tag_param.GetAtrSizes();

		AttrType[] tagattrtype = { new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInterval) };
		short []   tagstrsize  = new short[0];
		int        tagTupSize  = 20;  //hard coded tuples size for 1 integer and 1 interval

		String btreefilename = btreepar.GetBTreeFileName();
	    TagParams[] JoinedTagPar = new TagParams[TotalJoins];
	    iterator.Iterator itr1 = null, itr2 = null;

	    try
	    {
			for(int i=0;i<tot_len;i++)
			{
				fldlist = new ArrayList<Integer>();
				fldlist.add(i);
				TgPr[i] =  new TagParams(null, fldlist, null, null, null, tagattr, tagstrszs, tagTupSize);
				expr1 = GetCondExpr(tagnames[i]);
				itr1 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr_main, tagstrszs_main, 3, 2, projlist, expr1, 3, false);
				TgPr[i].SetIterator(itr1);
				GenerateHeapfile(TgPr[i]);
				//tagpar.SetHPParams(hpfile, hprid, hpfilename);	
			}	    	
	    }
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    }




		for(int i=0;i<TotalJoins;i++)
		{
			String[] Jnfields = Joins[i].split(" ");
			int ftfld = Integer.parseInt(Jnfields[0])-1;
			int scfld = Integer.parseInt(Jnfields[1])-1;
			String typerel = Jnfields[2];
			boolean pcflag = false;

			if(typerel.equals("PC"))
			{
				pcflag = true;
			}
			try
			{
				expr1 = GetCondExpr(tagnames[ftfld]);
				expr2 = GetCondExpr(tagnames[scfld]);
				itr1 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr_main, tagstrszs_main, 3, 2, projlist, expr1, 3, false);
				itr2 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr_main, tagstrszs_main, 3, 2, projlist, expr2, 3, false);
				TgPr[ftfld].SetIterator(itr1);
				TgPr[scfld].SetIterator(itr2);	

			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			//JoinedTagPar[i] = JoinTwoFields_nlj_strip(TgPr[ftfld], TgPr[scfld], pcflag);
			//JoinedTagPar[i] = JoinTwoFields_nlj(TgPr[ftfld], 2, TgPr[scfld], 2, true, relflag); //create a join of two tags
			//JoinedTagPar[i] = JoinTwoFields_sm(TgPr[ftfld], 2, TgPr[scfld], 2, true, pcflag); //create a join of two tags
			//ScanTagParams(JoinedTagPar[i]);
			//System.out.println("------------------------------------------------");
		}
		//return JoinedTagPar;
		return null;
	}*/
	//Extract all the data from the heap file which match to the given tags in tagnames array and create new heap file for each of them
//basically this function creates new heap files where each individual heap file contains only a particular tag value
		//AttrType[] projattrtype = { new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInterval) };
/*		short[]    projstrtype  = new short[0];
		int        projtupsize  = 20;
		Tuple      projtuple    = new Tuple(projtupsize);*/

/*	    if(mergeflag)
	    {
	    	JoinedTaghpfilename = GetRandomName();	
			JoinedTagRID = new RID();
		    try
			{
				JoinedTaghpfile = new Heapfile(JoinedTaghpfilename);
			}
			catch (Exception e)
			{
				System.err.println("*** error in Heapfile constructor ***");
				e.printStackTrace();
			}

			Tuple tptup;
			int parent;
			intervaltype intval;
			try 
			{
				if(parentchildflag)
				{
					while((tptup=nlj.get_next())!=null) 
					{
						if(tptup.getIntervalFld(2).get_s() == tptup.getIntFld(4))
						{
							JoinedTagRID = JoinedTaghpfile.insertRecord(tptup.returnTupleByteArray());	//insert the joined record into a new heap file	
						}		
					}
				}
				else
				{
					while((tptup=nlj.get_next())!=null) 
					{
						JoinedTagRID = JoinedTaghpfile.insertRecord(tptup.returnTupleByteArray());  //get the tag RID of the resulting tuple
					}
				}
			}
			catch (Exception e)
			{
				System.err.println ("*** Error preparing for get_next tuple");
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			int outernew = JoinedTagAttrtype.length;
			FldSpec[] projlist_tag_join = GetProjections(outernew, 0, ftoO1);
			try
			{
				finalitr = (iterator.Iterator) new FileScan(JoinedTaghpfilename, JoinedTagAttrtype, JoinedTagsize, (short) outernew, outernew, projlist_tag_join, null);
			}
			catch(Exception e)
			{
				System.err.println ("*** Error preparing for get_next tuple");
				e.printStackTrace();
			}
			//DeleteHeapFile(tag2);	
	    }*/	



	    /*	public TagParams JoinTwoFields_NLJ(TagParams tag1, TagParams tag2, boolean parentchildflag)
	{
		iterator.Iterator indxscan1 = tag1.GetIterator();
    	AttrType [] tagattrtype1    = tag1.GetAtrTypes();
		short    [] tagattrsize1    = tag1.GetAtrSizes();
		int      tagtupsize1        = tag1.GetSizeofTuple();
		int      outer              = tagattrtype1.length;
		List<Integer> Fieldnos1     = tag1.GetFieldNos();

    	AttrType [] tagattrtype2    = tag2.GetAtrTypes();
		short    [] tagattrsize2    = tag2.GetAtrSizes();
		int      tagtupsize2        = tag2.GetSizeofTuple();
		String   taghpfilename2     = null;
		int      inner              = tagattrtype2.length;
		List<Integer> Fieldnos2     = tag2.GetFieldNos();

		CondExpr[] outFilter = GenerateCondExpr(2, 2, true, parentchildflag);
		NestedLoopsJoins nlj = null;

		GenerateHeapfile(tag2);
		taghpfilename2 = tag2.GetHPFileName();

		RelSpec rel_out = new RelSpec(RelSpec.outer); 
		RelSpec rel_in = new RelSpec(RelSpec.innerRel);
		FldSpec[] projection_arr = {new FldSpec(rel_out, 2), new FldSpec(rel_out, 2)};

		try
		{
			nlj = new NestedLoopsJoins (tagattrtype1, 2, tagattrsize1, tagattrtype2, 2, tagattrsize2, 50, indxscan1, taghpfilename2, outFilter, null, projection_arr, 2);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		List<Integer> Fieldscomb = GetCombinedFlds(Fieldnos1, Fieldnos2);
		AttrType [] JoinedTagAttrtype = { new AttrType (AttrType.attrInterval), new AttrType (AttrType.attrInterval) };
		short    [] JoinedTagsize     = new short[0];
		int JoinedTagTupSize  = JoinedTupSize(JoinedTagAttrtype);
		TagParams JoinedTagParam = null; 

		JoinedTagParam = new TagParams(nlj, Fieldscomb, null, null, null, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);

		GenerateHeapfile(JoinedTagParam);
		return JoinedTagParam;
		//return null;
	}	*/	




/*  	//parentchildflag == true check parent child or else check ancester descendant
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
*/
class IndexTagParams
{
 	private IndexScan IndexScanItr = null;
	private AttrType[] AtrTypes = null; // Attrtypes of the heap file
	private short[] AtrSizes = null; //Attrsize of the heap file
	private int SizeofTuple; // Size of the tuple stored in the heap file
 
	public TagParams(IndexScan inxscn, AttrType[] atrtypes, short[] atrsizes, int sizetup)
	{
		this.IndexScanItr = inxscn;
		this.AtrTypes = atrtypes;
		this.AtrSizes = atrsizes;
		this.SizeofTuple = sizetup;
 	}

 	public IndexScan GetIndexScanItr()
 	{
 		return this.IndexScanItr;
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




	public void ExtractTagBTree(TagParams tag_param, BTreeFile btreefile, String tagname)
	{
		int keyType=AttrType.attrString;;

		KeyClass lowkey, hikey;
		KeyDataEntry entry;
		lowkey=new StringKey(tagname);
		hikey=new StringKey(tagname);
		//int k=0;

		try
		{
			BTFileScan btscan=btreefile.new_scan(lowkey, hikey);
	        while(true)
	        {
	        	entry=btscan.get_next();
		  		//entry=scan.get_next();
				if(entry!=null) 
					System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
				else
					break;
		  		//++k;
		  	}		
		}
		catch (Exception e) 
		{
			System.out.println("Error while deleting heap file\n");
			e.printStackTrace();
		}
		//System.out.println(k);
	  	System.out.println("done with scan");

	}





			/*		
		try
		{
			BT.printBTree(btreefile.getHeaderPage()); 
			BT.printAllLeafPages(btreefile.getHeaderPage());		
		}
		catch (Exception e)
		{
		  	e.printStackTrace();
		}*/


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
							ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray()); //inset the data into a separate heap file
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
			tag_pars[i] =  new TagParams(heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize); //generate tag pars data type for each of the heap file
		}
		return tag_pars;
	}



		public IndexTagParams[] ExtractBtreeTagToIndex(TagParams tag_param, BTreePars btreepar, String[] tagnames)
	{

		CondExpr[] expr = null;
		FldSpec[] projlist = new FldSpec[3];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);

	    int tot_len = tagnames.length;
		IndexTagParams[] IndxTgPr = new IndexTagParams[tot_len];

		Heapfile heaptosearch = tag_param.GetHeapFile();
		String heapfilename = tag_param.GetHPFileName();
		AttrType[] tagattr = tag_param.GetAtrTypes();
		short [] tagstrszs = tag_param.GetAtrSizes();
		int tagTupSize = tag_param.GetSizeofTuple();

		String btreefilename = btreepar.GetBTreeFileName();
	    // start index scan

	    IndexScan[] iscan_array = new IndexScan[tot_len];
	    Tuple t = null;

		try
		{
			for(int i=0;i<tot_len;i++)
			{
				expr = GetCondExpr(tagnames[i]);
				iscan_array[i] = new IndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr, tagstrszs, 3, 3, projlist, expr, 3, false);
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}	

		for(int i=0;i<tot_len;i++)
		{
			IndxTgPr[i] =  new IndexTagParams(iscan_array[i], tagattr, tagstrszs, tagTupSize);
		}


		try
		{
			for(int i=0;i<tot_len;i++)
			{
				while((t = iscan_array[i].get_next()) != null)
				{
					t.print(tagattr);
				}			
			}

	    }
	    catch (Exception e) {
	      e.printStackTrace(); 
	    }
		return IndxTgPr;
	}


	//Extract all the data from the heap file which match to the given tags in tagnames array and create new heap file for each of them
	//basically this function creates new heap files where each individual heap file contains only a particular tag value
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
							ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray()); //inset the data into a separate heap file
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
			tag_pars[i] =  new TagParams(null, heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize); //generate tag pars data type for each of the heap file
		}
		return tag_pars;
	}
	//This gives which fields can be joined for resultant join and a new joined table
	public int[] GetFieldsToJoissn(List<Integer> FieldTracker, int ftfld, int scfld)
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




	public void AddField(List<Integer> FieldTracker, int num)
	{
		if(FieldTracker.indexOf(num) == -1)
		{
			FieldTracker.add(num);
		}
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
	// This function joins the two tables obtained from two queires and finally gives you result which we print as query result obtained
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
		//TagParams ResQueryJoin = null;//JoinTwoFields(tagpar1, fields[0], tagpar2, fields[1], ltfieldtoomit, false, false);
		//TagParams ResQueryJoin = JoinTwoFields(tagpar1, fields[0], tagpar2, fields[1], ltfieldtoomit, false, false);
		//return ResQueryJoin;
		return null;
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

/*
This class keeps track of a heap file and its field after a join has been made between two tables
when we join two table we keep track of the fields which are present in the resultant table so when a new table is added we can eleminate the duplicate columns
that is why we keep track of the fields or maintain a list<Integer> 
*/

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



/*class IndexTagParams
{
 	private IndexScan IndexScanItr = null;
	private AttrType[] AtrTypes = null; // Attrtypes of the heap file
	private short[] AtrSizes = null; //Attrsize of the heap file
	private int SizeofTuple; // Size of the tuple stored in the heap file
 
	public IndexTagParams(IndexScan inxscn, AttrType[] atrtypes, short[] atrsizes, int sizetup)
	{
		this.IndexScanItr = inxscn;
		this.AtrTypes = atrtypes;
		this.AtrSizes = atrsizes;
		this.SizeofTuple = sizetup;
 	}

 	public IndexScan GetIndexScanItr()
 	{
 		return this.IndexScanItr;
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
}*/

/*	public void clonelist(iterator.Iterator itr, AttrType[] tagatrtypes)
	{
		iterator.Iterator itr2 = ((IndexScan)itr).IndexScan_Copy();

		Tuple temptup = null;
		int count_records = 0;
		try
		{
			while ((temptup=itr.get_next())!=null)
			{ 
				temptup.print(tagatrtypes);	
				count_records++;
			}
					itr.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
		count_records=0;
		System.out.println("----------------------------");

		try
		{
			while ((temptup=itr2.get_next())!=null)
			{ 
				temptup.print(tagatrtypes);	
				count_records++;
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}		
	}*/

/*		expr1 = GetCondExpr("root");
		expr2 = GetCondExpr("Autho");
		CondExpr[] outFilter = null;
		outFilter = GenerateCondExpr(2, 2, true, false);	//generate condexpr for the two joining fields
		try
		{
			itr1 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr, tagstrszs, 3, 3, projlist, expr1, 3, false);
			itr2 = new BTIndexScan(new IndexType(IndexType.B_Index), heapfilename, btreefilename, tagattr, tagstrszs, 3, 3, projlist, expr2, 3, false);
		}
		catch(Exception e) 
		{
			e.printStackTrace();
		}		

	    NestedLoopsJoins nlj = null;
	    try 
	    {
	    	//initalizing nested loop join consructor
	    	nlj = new NestedLoopsJoins(tagattr, 3, tagstrszs, tagattr, 3, tagstrszs, 10, itr1, itr2, outFilter, null, projlist_tag2, 6);
	    }
	    catch (Exception e) 
	    {
			System.err.println ("*** Error preparing for nested_loop_join");
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
	    }
		AttrType[] JoinedTagAttrtype = JoinAttrtype(tagattr, tagattr, 3, projlist_tag2);
		Tuple temptup = null;
		int count_records = 0;
		try
		{
			while ((temptup=nlj.get_next())!=null)
			{ 
				temptup.print(JoinedTagAttrtype);	
				count_records++;
			}
					nlj.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		System.out.println(count_records);*/