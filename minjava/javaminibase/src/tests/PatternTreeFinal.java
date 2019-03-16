package tests;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

/**
 * 
 */

/**
 * @author user
 *

     1
 2      3
      4
 */


public class PatternTreeFinal {

	/**
	 * @param args
	 * @throws IOException 
	 */
	static int depth = 0;
	static List<Integer> prefix = new ArrayList<Integer>();
	static boolean addprefix = true;
	String[] keys;				//give mapping from node number to string key. 
        int PC[][];					//create 2d arrays for PC/AD matrix.
	int AD[][];	
	
	static void dfs(ArrayList<Integer>[] graph,int s,List<Integer> pathtillnow, int level)
	{
//		System.out.println("Current : " + s);
		pathtillnow.add(s);
		
		if(graph[s].size() == 0)		//leaf node?
			System.out.println(pathtillnow);
		
		if(addprefix)
		{
			prefix.add(s);
			if(graph[s].size() > 1)
				addprefix = false;
		}
		
		if(depth < level)
			depth = level;
		
		for(int i = 0 ; i < graph[s].size() ; i++)
		{
			int dest = graph[s].get(i);
			dfs(graph, dest, pathtillnow, level+1);
		}
		pathtillnow.remove(pathtillnow.size()-1);
	}

        public int[][] getADMatrix(){
		return AD;
        }

        public int[][] getPCMatrix(){
		return PC;
        }

	public String[] getKeys(){
		return keys;
	}

	public ArrayList<Integer>[] getgraph() throws IOException {
		// TODO Auto-generated method stub

		String path = System.getProperty("user.dir") + "\\data.txt";
		System.out.println(path);
		File file = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		int n = Integer.parseInt(line);
	
		keys = new String[n+1];
		int i = 0;
		int count = n;
		
 		String[] tagmap = new String[count];		//store the mapping from node number to tag. 

		//get the tags from the next few lines. 
		while(count >= 1)
		{
			String tag = br.readLine();
			keys[i] = tag;
			count--;
			System.out.println(tag);
		}
		
		PC = new int[n+1][n+1];			//parent child matrix. 
		AD = new int[n+1][n+1];
		
		for(i = 0 ; i <= n ; i++)
			for(int j = 0 ; j <= n ; j++)
				AD[i][j] = PC[i][j] = 0;
		
		System.out.println("Starting next loop now : \n");
	
		ArrayList<Integer>[] graph = new ArrayList[n+1];
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
			System.out.println(tokens[0] + " " +  tokens[1] +  " " + rel);
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
	
/
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
};
  
/*
 *    1
 *   2
 *  3  4
 *  1  1
 *  2  
 *  3
 * 
 * 
 * */
