import java.io.File; 
import java.io.FileNotFoundException; 
import java.io.FileReader; 
import java.util.Iterator; 
import javax.xml.namespace.QName; 
import javax.xml.stream.XMLEventReader; 
import javax.xml.stream.XMLInputFactory; 
import javax.xml.stream.XMLStreamException; 
import javax.xml.stream.events.*; 
import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;

public class xmltree2
{

	public static String[] str = new String[400000];
	public static int i = 0;
	public static int l = 0;
	public static int s = 0;
	public static int e = 0;
	public static int[] level = new int[400000];
	public static int[] start = new int[400000];
	public static int[] end = new int[400000];
	
	public static void main(String[] args) throws FileNotFoundException, 
                                                  XMLStreamException 
    { 
        // Create a File object with appropriate xml file name 
        File file = new File("xml_sample_data.xml"); 
  
        // Function for accessing the data 
        parser(file, i, l);
        /*
        for(int y=0; y<20; y++)
        {
        	System.out.println(str[y] + "-------> level = " + level[y] + " (s,e) = (" + start[y] + "," + end[y] + ")");
        }
        */
        
    } 
    
    public static void parser(File file, int x, int y) throws FileNotFoundException, 
                                                   XMLStreamException 
    { 
        
        int maxLen = 5; // Max length of the tags 
  		//int a= 1, b=1, c=1;
        // Instance of the class which helps on reading tags 
        XMLInputFactory factory = XMLInputFactory.newInstance(); 
  
        // Initializing the handler to access the tags in the XML file 
        XMLEventReader eventReader = 
                 factory.createXMLEventReader(new FileReader(file)); 
  
        // Checks the availabilty of the next tag 
        while (eventReader.hasNext()) 
        { 

            XMLEvent event = eventReader.nextEvent(); 
  
            // This will trigger when the tag is of type <...> 
            if (event.isStartElement()) 
            { 
                StartElement element = (StartElement)event; 
  				str[x++] = element.getName().toString();
  				if(str[x-1].length() > maxLen)
  				{
					str[x-1] = str[x-1].substring(0, maxLen);
				}
  				y++;
  				s++;
				level[x-1] = y;
				start[x-1] = s;
  				//System.out.println(element.getName());
  				//System.out.println("a = " + a++);
                // Iterator for accessing the metadeta related 
                // the tag started. 
                Iterator<Attribute> iterator = element.getAttributes(); 
                while (iterator.hasNext()) 
                { 
                    Attribute attribute = iterator.next(); 
                    QName name = attribute.getName(); 
                    String value = attribute.getValue();
                    
                    str[x++] = name.toString();
                    if(str[x-1].length() > maxLen)
	  				{
						str[x-1] = str[x-1].substring(0, maxLen);
					}
                    y++;
                    s++;
                    level[x-1] = y;
                    start[x-1] = s;
                    str[x++] = value;
                    if(str[x-1].length() > maxLen)
	  				{
						str[x-1] = str[x-1].substring(0, maxLen);
					}
                    y++;
                    s++;
                    level[x-1] = y;
                    start[x-1] = s;
                    y--;
                    for(int k = x-1; k>=0 ; k--)
                    {
						if(end[k] == 0)
						{
							end[k] = ++s;
							break;
						}
					}	
                    y--;
                    for(int k = x-1; k>=0 ; k--)
                    {
						if(end[k] == 0)
						{
							end[k] = ++s;
							break;
						}
					}	 
                    /*
                    System.out.println(name);
                    System.out.println(value); 
                    System.out.println("b = " + b++);
                    */
                } 
  
                
            } 
  			
            // This will be triggered when the tag is of type </...> 
            if (event.isEndElement()) 
            { 
                EndElement element = (EndElement) event; 
  				y--;
  				for(int k = x-1; k>=0 ; k--)
		        {
					if(end[k] == 0)
					{
						end[k] = ++s;
						break;
					}
				}	
                 
            } 
  			
            // Triggered when there is data after the tag which is 
            // currently opened. 
            if (event.isCharacters() && !event.asCharacters().isWhiteSpace()) 
            { 
                // Depending upon the tag opened the data is retrieved . 
                Characters element = (Characters) event;
                //if(!element.getData().isWhiteSpace())
	            str[x++] = element.getData();
	            if(str[x-1].length() > maxLen)
	  			{
					str[x-1] = str[x-1].substring(0, maxLen);
				}
	            y++;
	            s++;
                level[x-1] = y;
                start[x-1] = s;
	            y--;
	            for(int k = x-1; k>0 ; k--)
		        {
					if(end[k] == 0)
					{
						end[k] = ++s;
						break;
					}
				} 
                //System.out.println(element.getData());
                //System.out.println("Characters length = "+ element.getData().toString().length());
                //System.out.println("c = " + c++);
                
            } 
        } 
        String nameRoot = "xmltree2";
        String dbpath = "/tmp/"+nameRoot+System.getProperty("user.name")+".minibase-db"; 
	    String logpath = "/tmp/"+nameRoot +System.getProperty("user.name")+".minibase-log";
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
	   
		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";
		
		newdbpath = dbpath;
		newlogpath = logpath;
		
		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;
		
		
		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
		  Runtime.getRuntime().exec(remove_logcmd);
		  Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
		  System.err.println ("IO error: "+e);
		}
		
		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;
		
		try {
		  Runtime.getRuntime().exec(remove_logcmd);
		  Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
		  System.err.println ("IO error: "+e);
		}

		boolean OK = true;
		boolean FAIL = false;
  		
  		
  		
		int choice;
		for(choice = 0; str[choice] != null; choice++) {}
		//public final static int reclen = 16;


		boolean status = OK;
		RID rid = new RID();
		Heapfile f = null;
		
		/*
		String str[] = {"A", "B", "D", "F", "G", "E", "H", "C", "I", "J"};
		int start[] = {1,2,3,4,6,9,10,14,15,17};
		int end[] = {20,13,8,5,7,12,11,19,16,18};
		int level[] = {1,2,3,4,4,3,4,2,3,3};
		*/
		intervalType ivl = new intervalType();

		System.out.println ("  - Create a heap file\n");
		try {
		  f = new Heapfile("file_1");
		}
		catch (Exception e) {
		  status = FAIL;
		  System.err.println ("*** Could not create heap file\n");
		  e.printStackTrace();
		}

		if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
		 != SystemDefs.JavabaseBM.getNumBuffers() ) {
		  System.err.println ("*** The heap file has left pages pinned\n");
		  status = FAIL;
		}

		if ( status == OK ) {
		  System.out.println ("  - Add " + choice + " records to the file\n");
		  for (int i =0; (i < choice) && (status == OK); i++) {
		
				//fixed length record
				xmlRecord rec = new xmlRecord();
				ivl.assign (start[i], end[i], level[i]);
				System.out.println(start[i] + "	" + end[i] + "	" + level[i] + "	" + str[i]);
				rec.interval = ivl;
				rec.tag = str[i];

				try {
				  rid = f.insertRecord(rec.toByteArray());
				}
				catch (Exception e) {
				  status = FAIL;
				  System.err.println ("*** Error inserting record " + i + "\n");
				  e.printStackTrace();
				}

				if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
					 != SystemDefs.JavabaseBM.getNumBuffers() ) {
				  
				  System.err.println ("*** Insertion left a page pinned\n");
				  status = FAIL;
				}
		  }
		  
		  try {
				if ( f.getRecCnt() != choice ) {
				  status = FAIL;
				  System.err.println ("*** File reports " + f.getRecCnt() + 
							  " records, not " + choice + "\n");
				}
		  }
		  catch (Exception e) {
		status = FAIL;
		System.out.println (""+e);
		e.printStackTrace();
		  }
		}
		
	  } 
}
