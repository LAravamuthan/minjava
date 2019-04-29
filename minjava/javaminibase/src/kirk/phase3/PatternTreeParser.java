package phase3;

import java.util.Scanner;

import java.io.File;
import java.io.FileNotFoundException;

public class PatternTreeParser {
  /**
   * Read Input from file specified
   * @param	filename
   * @return	The <code>PatternTree</code> representation of the file
   */
  public static PatternTree ParseInput(String filename){

    PatternTree tree = new PatternTree();

    File file = new File(filename);
    try{
      Scanner scan = new Scanner(file);

      int numNodes = Integer.parseInt(scan.next());

      for(int i = 0; i < numNodes; i++){
        tree.addTag(scan.next());
      }
      tree.setNumNodes(numNodes);
      String n1;
      String n2;
      int node1 = 0;
      int node2 = 0;
      String relationship = "";

      while(scan.hasNext()){
        //array positions need to be decreased by one because
        //node numbers start at 1

        n1 = scan.next();	// first argument (Parent or Ancestor)
        n2 = scan.next();	// second Argument (Child or Descendant)

        //have to check for star
        if(n1.equals("*")){
          node1 = -1;
        }
        else{
          node1 = Integer.parseInt(n1) - 1;
        }
        if(n2.equals("*")){
          node2 = -1;
        }
        else{
          node2 = Integer.parseInt(n2) - 1;
        }

        relationship = scan.next();
        tree.addRelation(n1 + " " + n2 + " " + relationship);

        if(relationship.equals("AD")){
          tree.addAncestorDescendant(node1, node2);
        }
        else{
          tree.addParentChild(node1, node2);
        }
      }

      scan.close();
    }
    catch(FileNotFoundException e){
      System.out.println("File not found.");
    }


    return tree;
  }
}