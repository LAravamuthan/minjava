package intervaltree;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;


public class IntervalT implements GlobalConst
{

	public IntervalT()
	{

	}

	static int keyCompare(KeyClass key1, KeyClass key2)
	{
		intervaltype k1 = ((IntervalKey)key1).getKey();
		intervaltype k2 = ((IntervalKey)key2).getKey();

		int s1 = k1.get_s(), e1 = k1.get_e();
		int s2 = k2.get_s(), e2 = k2.get_e();

		if(s1 < s2)
		{
			if(e1 < e2)
			{
				if(e1 < s2) return -2; //left non overlap
				else return -3; //other overlap
			}
			else return -1; //containment
		}
		else if(s2 < s1)
		{
			if(e2 < e1)
			{
				if(e2 < s1) return 2; //right non overlap
				else return 3;  //other overlap
			}
			else return 1; //enclosure or contained by
		}
		else
		{
			return 0; //equality
		}
	}

  /** It gets the length of the key
   *@param key  specify the key whose length will be calculated.
   * Input parameter.
   *@return return the length of the key
   *@exception  KeyNotMatchException  key is neither StringKey nor  IntegerKey 
   *@exception IOException   error  from the lower layer  
   */  
  protected final static int getKeyLength(KeyClass key) 
    throws  KeyNotMatchException, 
	    IOException
    {
      if ( key instanceof IntervalKey)
	return 8;
      else throw new KeyNotMatchException(null, "key types do not match"); 
    }

  /** It gets the length of the data 
   *@param  pageType  NodeType.LEAF or  NodeType.INDEX. Input parameter.
   *@return return 8 if it is of NodeType.LEA; 
   *  return 4 if it is of NodeType.INDEX.
   *@exception  NodeNotMatchException pageType is neither NodeType.LEAF 
   *  nor NodeType.INDEX.
   */  
  protected final static int getDataLength(short pageType) 
    throws  NodeNotMatchException
    {
      if ( pageType==NodeType.LEAF)
	return 8;
      else if ( pageType==NodeType.INDEX)
	return 4;
      else throw new  NodeNotMatchException(null, "key types do not match"); 
    }


  /** It gets the length of the (key,data) pair in leaf or index page. 
   *@param  key    an object of KeyClass.  Input parameter.
   *@param  pageType  NodeType.LEAF or  NodeType.INDEX. Input parameter.
   *@return return the lenrth of the (key,data) pair.
   *@exception  KeyNotMatchException key is neither StringKey nor  IntegerKey 
   *@exception NodeNotMatchException pageType is neither NodeType.LEAF 
   *  nor NodeType.INDEX.
   *@exception IOException  error from the lower layer 
   */ 
  protected final static int getKeyDataLength(KeyClass key, short pageType ) 
    throws KeyNotMatchException, 
	   NodeNotMatchException, 
	   IOException
    {
      return getKeyLength(key) + getDataLength(pageType);
    } 



  /** It gets an keyDataEntry from bytes array and position
   *@param from  It's a bytes array where KeyDataEntry will come from. 
   * Input parameter.
   *@param offset the offset in the bytes. Input parameter.
   *@param keyType It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger.
   *               Input parameter. 
   *@param nodeType It specifes NodeType.LEAF or NodeType.INDEX. 
   *                Input parameter.
   *@param length  The length of (key, data) in byte array "from".
   *               Input parameter.
   *@return return a KeyDataEntry object
   *@exception KeyNotMatchException  key is neither StringKey nor  IntegerKey
   *@exception NodeNotMatchException  nodeType is neither NodeType.LEAF 
   *  nor NodeType.INDEX.
   *@exception ConvertException  error from the lower layer 
   */
  protected final static 
      KeyDataEntry getEntryFromBytes( byte[] from, int offset,  
				      int length, int keyType, short nodeType )
    throws KeyNotMatchException, 
	   NodeNotMatchException, 
	   ConvertException
    {
      KeyClass key;
      DataClass data;
      int n;
      try {
	
	if ( nodeType==NodeType.INDEX ) {
	  n=4;
	  data= new IndexData( Convert.getIntValue(offset+length-4, from));
	}
	else if ( nodeType==NodeType.LEAF) {
	  n=8;
	  RID rid=new RID();
	  rid.slotNo =   Convert.getIntValue(offset+length-8, from);
	  rid.pageNo =new PageId();
	  rid.pageNo.pid= Convert.getIntValue(offset+length-4, from); 
	  data = new LeafData(rid);
	}
	else throw new NodeNotMatchException(null, "node types do not match"); 
	
	if ( keyType== AttrType.attrInterval) {
	  key= new IntervalKey( (Convert.getIntervalValue(offset, from)));
	}
	else 
          throw new KeyNotMatchException(null, "key types do not match");
	
	return new KeyDataEntry(key, data);
	
      } 
      catch ( IOException e) {
	throw new ConvertException(e, "convert faile");
      }
    } 


  /** It convert a keyDataEntry to byte[].
   *@param  entry specify  the data entry. Input parameter.
   *@return return a byte array with size equal to the size of (key,data). 
   *@exception   KeyNotMatchException  entry.key is neither StringKey nor  IntegerKey
   *@exception NodeNotMatchException entry.data is neither LeafData nor IndexData
   *@exception ConvertException error from the lower layer
   */
  protected final static byte[] getBytesFromEntry( KeyDataEntry entry ) 
    throws KeyNotMatchException, 
	   NodeNotMatchException, 
	   ConvertException
    {
      byte[] data;
      int n, m;
      try{
        n=getKeyLength(entry.key);
        m=n;
        if( entry.data instanceof IndexData )
	  n+=4;
        else if (entry.data instanceof LeafData )      
	  n+=8;
	
        data=new byte[n];
	
        if ( entry.key instanceof IntervalKey ) {
	  Convert.setIntervalValue( ((IntervalKey)entry.key).getKey(),
			       0, data);
        }
        else throw new KeyNotMatchException(null, "key types do not match");
        
        if ( entry.data instanceof IndexData ) {
	  Convert.setIntValue( ((IndexData)entry.data).getData().pid,
			       m, data);
        }
        else if ( entry.data instanceof LeafData ) {
	  Convert.setIntValue( ((LeafData)entry.data).getData().slotNo,
			       m, data);
	  Convert.setIntValue( ((LeafData)entry.data).getData().pageNo.pid,
			       m+4, data);
	  
        }
        else throw new NodeNotMatchException(null, "node types do not match");
        return data;
      } 
      catch (IOException e) {
        throw new  ConvertException(e, "convert failed");
      }
    } 

	public static void printPage(PageId pageno, int keyType)
	    throws  IOException, 
	    IteratorException, 
	    ConstructPageException,
            HashEntryNotFoundException, 
	    ReplacerException, 
	    PageUnpinnedException, 
            InvalidFrameNumberException
	{
		IntervalTSortedPage sortedPage=new IntervalTSortedPage(pageno, keyType);
      int i;
      i=0;
            if ( sortedPage.getType()==NodeType.INDEX ) {
        IntervalTIndexPage indexPage=new IntervalTIndexPage((Page)sortedPage, keyType);
        System.out.println("");
        System.out.println("**************To Print an Index Page ********");
        System.out.println("Current Page ID: "+ indexPage.getCurPage().pid);
        System.out.println("Left Link      : "+ indexPage.getLeftLink().pid);

         RID rid=new RID();
        for(KeyDataEntry entry=indexPage.getFirst(rid); entry!=null; 
	    entry=indexPage.getNext(rid)){
	  if( keyType==AttrType.attrInterval) 
	    System.out.println(i+" (key, pageId):   ("+ 
			       (IntervalKey)entry.key + ",  "+(IndexData)entry.data+ " )");  
	  i++;    
        }

        System.out.println("************** END ********");
        System.out.println("");

    	}
      else if ( sortedPage.getType()==NodeType.LEAF ) {
        IntervalTLeafPage leafPage=new IntervalTLeafPage((Page)sortedPage, keyType);
        System.out.println("");
        System.out.println("**************To Print an Leaf Page ********");
        System.out.println("Current Page ID: "+ leafPage.getCurPage().pid);
        System.out.println("Left Link      : "+ leafPage.getPrevPage().pid);
        System.out.println("Right Link     : "+ leafPage.getNextPage().pid);
	
        RID rid=new RID();
	
        for(KeyDataEntry entry=leafPage.getFirst(rid); entry!=null; 
	    entry=leafPage.getNext(rid)){
	  if( keyType==AttrType.attrInterval) 
	    System.out.println(i+" (key, [pageNo, slotNo]):   ("+ 
			       (IntervalKey)entry.key+ ",  "+(LeafData)entry.data+ " )");
  
	  i++;
        }
	
	System.out.println("************** END ********");
	System.out.println("");
      }
      else {
	System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
      }      
      
      SystemDefs.JavabaseBM.unpinPage(pageno, true/*dirty*/);
	}


	public static void printintervalTree(IntervalTreeHeaderPage header)
    throws IOException, 
   ConstructPageException, 
   IteratorException,
   HashEntryNotFoundException,
   InvalidFrameNumberException,
   PageUnpinnedException,
   ReplacerException 
	{
      if(header.get_rootId().pid == INVALID_PAGE) {
	System.out.println("The Tree is Empty!!!");
	return;
      }
      
      System.out.println("");
      System.out.println("");
      System.out.println("");
      System.out.println("---------------The B+ Tree Structure---------------");
      
      
      System.out.println(1+ "     "+header.get_rootId());
      
      _printTree(header.get_rootId(), "     ", 1, header.get_keyType());
      
      System.out.println("--------------- End ---------------");
      System.out.println("");
      System.out.println("");
	}

  private static void _printTree(PageId currentPageId, String prefix, int i, 
				 int keyType) 
    throws IOException, 
	   ConstructPageException, 
	   IteratorException,
	   HashEntryNotFoundException,
	   InvalidFrameNumberException,
	   PageUnpinnedException,
	   ReplacerException 
    {
      
      IntervalTSortedPage sortedPage=new IntervalTSortedPage(currentPageId, keyType);
      prefix=prefix+"       ";
      i++;
      if( sortedPage.getType()==NodeType.INDEX) {  
	IntervalTIndexPage indexPage=new IntervalTIndexPage((Page)sortedPage, keyType);
	
	System.out.println(i+prefix+ indexPage.getPrevPage());
	_printTree( indexPage.getPrevPage(), prefix, i, keyType);
	
	RID rid=new RID();
	for( KeyDataEntry entry=indexPage.getFirst(rid); entry!=null; 
	     entry=indexPage.getNext(rid)) {
	  System.out.println(i+prefix+(IndexData)entry.data);
	  _printTree( ((IndexData)entry.data).getData(), prefix, i, keyType);
	}
      }
      SystemDefs.JavabaseBM.unpinPage(currentPageId , true/*dirty*/);
    }


	public static void printTreeUtilization(IntervalTreeHeaderPage header)
	{

	}

	public static void printNonLeafTreeUtilization(IntervalTreeHeaderPage header)
	{

	}
}

