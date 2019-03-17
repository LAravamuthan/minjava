//originally from : joins.C
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;


class QueryPlan
{

private static Vector intervals;

public QueryPlan()
{
    intervals = new Vector();
    intervalType A = new intervalType();  
    A.assign(1,10,1);
    intervalType B = new intervalType();  
    B.assign(2,7,2);
    intervalType C = new intervalType();  
    C.assign(3,4,3);
    intervalType D = new intervalType();  
    D.assign(5,6,3);
    intervalType E = new intervalType();  
    E.assign(8,9,2);
    intervals.addElement(A);
    intervals.addElement(B);
    intervals.addElement(C);
    intervals.addElement(D);
    intervals.addElement(E);
    for(int i = 0 ; i < intervals.size() ; i++)
	System.out.println(A.getStart() + " " + A.getEnd() + " " + A.getLevel());
}

//Implements select on the given table. That is, the table will have a certain column, pos = key.
public static FileScan readtable(String key, int pos)	//read in the initial heapfile and return pointer. 
{
    String nodes[] = {"A","B","C","D","E"};

    System.out.println("key = " + key);

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
      e.printStackTrace();
    }

    int size = t.size();
    
    RID rid = null;
    Heapfile f = null;
    try{
	f = new Heapfile("intervalslatest.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor for intervals relation ***");
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 2, Itypes, Isizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() for intervals relation***");
      e.printStackTrace();
    }

    AttrType[] attr = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
    
    intervalType iobj = (intervalType)intervals.elementAt(0);
    System.out.println("start = " + iobj.getStart());
    
    int numintervals = intervals.size();

    for(int i = 0 ; i < numintervals ; i++){
      try{
//	  System.out.println(iobj.getStart() + " : " + iobj.getEnd() + " : " + iobj.getLevel());
	  t.setIntervalFld(1, (intervalType)intervals.elementAt(i));   //Interval String <>
	  t.setStrFld(2, nodes[i]);
//          System.out.println("Inserting...");
//	  t.print(attr);          
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() for intervals relation ***");
	e.printStackTrace();
      }      
      
      try {
	rid = f.insertRecord(t.getTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() for the intervals relation***");
	e.printStackTrace();
      }   
   
/*
   if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for intervals");
      Runtime.getRuntime().exit(1);
    }
*/
  }

   FileScan am = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns. 
      try {
	am  = new FileScan("intervalslatest.in", Itypes, Isizes, 
			   (short)2, (short)2,
			   Sprojection, null /*selectFilter*/);	//first just check if one table is properly written to or not. 
      }
       catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
      }

/*
After we reach here, we will have a single table with < <interval>,node >
*/

  System.out.println("Records have been inserted into interval table successfully!!");
  return am;
}





//Create the conditional expression for the join query. 
void createjoinquery_condexpr(CondExpr[] expr, String rel, int intervalpos1, int intervalpos2)
{
    System.out.println("In create join query conditional expression");
    expr[0].next = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);		//S.interval > R.interval
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),intervalpos1);  // R.id = S.id
    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),intervalpos2); 
    System.out.println("rel = "  + rel);
    if(rel.compareTo("AD") == 0)
    {
       expr[0].ad = 1;
       System.out.println("setting ad to 1");
    }
    else
    {
       System.out.println("Setting pc to 1"); 
       expr[0].pc = 1; 
    }
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
/*
@Parameters
FileScan am : the left table file scan object
intervalpos1 : the index of the interval in first table
intervalpos2 : the index of interval in second table 
attr : attribute types of outer table
rel: relationship between the nodes (PC/AD) ? 
num_attrs: number of attributes of outer table. 
pos: The position of the column in outer table, on which select must be applied.
*/
/* First read parent table. Then perform one level of join. 


res, childvalue, intervalpos[parent],1,Dtypes,num_attrs,rel
*/
public iterator.Iterator createjoinquery(FileScan am, String child , int intervalpos1, int intervalpos2, AttrType[] attr, String rel, short num_attrs) throws JoinsException
{

CondExpr[] outFilter = new CondExpr[2];		//will give 3 
outFilter[0] = new CondExpr();
outFilter[1] = new CondExpr();

//define conditions for join query
createjoinquery_condexpr(outFilter, rel, intervalpos1, intervalpos2);
System.out.println("pc = " + outFilter[0].pc + " ad = " + outFilter[0].ad);
    
System.out.println("conditional expression created");

AttrType[] Stypes = new AttrType[num_attrs];				
for(int i = 0 ; i < num_attrs ; i++)
 Stypes[i] = new AttrType(attr[i].attrType);				

short [] Ssizes = new short[1];
Ssizes[0] = 1; //first elt. is 30
    
/*Code for filescan using select*/

/* Attribute types for the big intervals.in relation */
AttrType [] Rtypes = new AttrType[2];				
Rtypes[0] = new AttrType(AttrType.attrInterval);
Rtypes[1] = new AttrType(AttrType.attrString);         //am and the data table where column value  equals child. [interval,string] 

//SOS
short [] Rsizes = new short[1];
Rsizes[0] = 1; 

FldSpec[] RProjection = new FldSpec[2];
RProjection[0] =  new FldSpec(new RelSpec(RelSpec.outer), 1); 
RProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

FileScan table = null;	

//System.out.println("pos = " + pos);

CondExpr[] selectFilter = new CondExpr[2];
selectFilter[0] = new CondExpr();
selectFilter[0].next = null;
selectFilter[0].op  = new AttrOperator(AttrOperator.aopEQ);
selectFilter[0].type1 = new AttrType(AttrType.attrSymbol);		//The equality condition on main table. 
selectFilter[0].type2 = new AttrType(AttrType.attrString);
selectFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);   //pos = 2 for String. 
selectFilter[0].operand2.string = child; //child value for which we have to filter from the right table; 
selectFilter[1] = null;
		
try {
      table  = new FileScan("intervalstest.in", Rtypes, Rsizes, (short)2, (short)2, RProjection, selectFilter);          //read the child node values from right data table. 
}
catch (Exception e) {
      System.err.println (""+e);
}

System.out.println("table pointer created!");

/*
try{
Tuple t = new Tuple();
AttrType[] Dtypes = {new AttrType(AttrType.attrInterval),new AttrType(AttrType.attrString)};
System.out.println("Scanned tuples = " );
while((t = table.get_next()) != null){
  System.out.println("\n\nValid tuple!");
  t.print(Dtypes);
}
}
catch(Exception e){

}
*/

/*Code for filescan based on select condition ends*/


/*Code to get the nested loop join iterator */

//Deciding the schema for the final output table. 

FldSpec [] Projection = new FldSpec[num_attrs+2];				

for(int i = 0 ; i < num_attrs ; i++)
  Projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
Projection[num_attrs] =  new FldSpec(new RelSpec(RelSpec.innerRel), 1); //table passed : <interval,string> data : <interval,string> <interval,string,interval,string>
Projection[num_attrs+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

if(outFilter == null)
	System.out.println("outfilter is null");


NestedLoopsJoins nlj = null;
try {
      nlj = new NestedLoopsJoins (Stypes, num_attrs, Ssizes,
				  Rtypes, 2, Rsizes,
				  50,
				  /*table*/ am, "intervalstest.in",                      //table contains all As, the other table contains all decendants of A.
				  outFilter, null, Projection, num_attrs+2);
}
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

/*
Tuple t = new Tuple();
t.setHdr(short(num_attrs+2), )
*/

return (iterator.Iterator)nlj;			//returns the nested loop join object constructed. 
}


/*Reads data from an iterator, stores it in heap file. */
public FileScan readdata (iterator.Iterator am,short num_attrs,AttrType[] attr_types)
{
	Heapfile f = null;
	try{
	  f = new Heapfile("intmdt.in");
	}
	catch(Exception e){
	  e.printStackTrace();
	}
	
	Tuple t = new Tuple();			//list of attribute types, no of string sizes. 
	short n = num_attrs;	
	short s_sizes[] = new short[1];
	s_sizes[0] = 1;
	
        try {
      		t.setHdr((short) n,attr_types, s_sizes);
    	}
    	catch (Exception e) {
      		System.err.println("*** error in Tuple.setHdr() ***");
      		e.printStackTrace();
    	}

	int size = t.size();
	RID rid;

	try{
	   while((t = am.get_next()) != null){
		  try {
			rid = f.insertRecord(t.getTupleByteArray());
      		  }
      		catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			e.printStackTrace();
      		}      
    	    }
	}
	catch(Exception e){
	}

	FldSpec[] SProjection = new FldSpec[num_attrs];
	for(int i = 0 ; i < num_attrs ; i++)
		SProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

	FileScan scan = null;					//Finally am will hold all the data from intervals.in file, containing all 3 columns. 
      	try {
		scan  = new FileScan("intmdt.in", attr_types, s_sizes, (short)n, (short)n, SProjection, null /*selectFilter*/);	//first just check if one table is properly written to or not. 
      	}
       catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
      }

      System.out.println("Records have been inserted into interval table successfully!!");
      return scan;
}

  public static void generate_query(ArrayList<Integer>[] graph,String[] keys, int[][] PC, int[][] AD, int n)         //0 1 2 3 4 5 (in case of nodes uptil 5)
  {

	System.out.println("Graph size = " + n);
	System.out.println("Graph is : ");

	for(int i = 1 ; i <= 5 ; i++)
        {
           for(int j = 0 ; j < graph[i].size() ; j++)
             System.out.print(graph[i].get(j) + " " );
           System.out.println();
        }

        Queue<Integer> q = new LinkedList<>();
	q.add(1);						//1 is given as root node. 
 
	System.out.println("Queue is : ");
        while(q.isEmpty() == false)
        {
	    int element = q.remove();
	    System.out.println(element + " " );			//print elmeents of queue one by one. 
	    for(int i = 0 ; i < graph[element].size() ; i++)
		q.add(graph[element].get(i));
        }

        int parent = 1;
	String rootkey = keys[1];						//get string label of root node. 
	FileScan root = readtable(rootkey,2);				//read the root node value and store it in a table. 
	
	System.out.println("In generate query : Filescan done successfully!");

	String rel;
	short num_attrs = 2;			           			//store column count in final result table.. 
	AttrType[] Dtypes = new AttrType[50];
	Dtypes[0] = new AttrType(AttrType.attrInterval);
	Dtypes[1] = new AttrType(AttrType.attrString);
	int end = 2;

	int[] intervalpos = new int[n+1];					//store the position of interval field for key k.(from 1 to n).
	intervalpos[1] = 1;							//the interval for first node is at position 1. 
	iterator.Iterator res;							//store the file pointer to resulting table. 

/*
	boolean first = true;
	for(int child = 0 ; child < graph[parent].size() ; child++)		//go through all child nodes in the graph. 	
        {
		if(PC[parent][child] == 1)
			rel = "PC";
		else if(AD[parent][child] == 1)
			rel = "AD";		
		else
			continue;

		String childvalue = keys[child];					//get the string key of child node.
		if(first == true) 
		{
			res = createjoinquery(root, childvalue, intervalpos[parent],1,Dtypes,rel,num_attrs);
			first = false;
		}
		else
		{
			res = createjoinquery(res, childvalue, intervalpos[parent],1,Dtypes,rel,num_attrs);
		}
		Dtypes[end] = new AttrType(AttrType.attrInterval);
		intervalpos[child] = end+1;
		end++;						//store the position of interval column for the child node. 
		Dtypes[end] = new AttrType(AttrType.attrString);
		end++;	
		num_attrs += 2; 	
	}

Examine the results obtained 
	Tuple t = new Tuple();
	AttrType[] types = new AttrType[num_attrs];
	for(int i = 0 ; i < num_attrs ; i++)
		types[i] = Dtypes[i];

	while( (t = res.get_next()) != null)
	{
		System.out.println("This tuple is part of result!--");
		t.print(types);
	}
*/
  }
/*
@Parameters
FileScan am : the left table file scan object
child : the key of the child. 
intervalpos1 : the index of the interval in first table
intervalpos2 : the index of interval in second table 
attr : attribute types of outer table
rel: relationship between the nodes (PC/AD) ? 
num_attrs: number of attributes of outer table. 
pos: The position of the column in outer table, on which select must be applied.
*/

public static ArrayList<Integer>[] graph;
public static String[] keys;
public static int[][] PC;
public static int[][] AD;
public static int n;

public static ArrayList<Integer>[] getgraph() throws IOException {
		// TODO Auto-generated method stub

		String path = System.getProperty("user.dir") + "/patterntree";			//a A -> C (AD)
		System.out.println(path);
		File file = new File(path);

                BufferedReader br = null;
		int i,count,tagcount;
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

public static void main(String[] args)
{

//CODE BELOW IS FOR CREATING PATTERN TREE. REFER THESE FUNCTIONS :
    try {
       graph = getgraph();
       generate_query(graph,keys,PC,AD,n); 
    }
    catch(IOException ioe){
	ioe.printStackTrace();
    }
}

}
