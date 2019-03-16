package tests;
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


class QueryGen
{
/*
  private void Query7_CondExpr(CondExpr[] expr)
  {
    expr[0].next = null;
    expr[0].flag = 0;
    expr[0].op = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    expr[1] = null;
  }

*/

public boolean isinteger(String s)
{
 try { 
            // checking valid integer using parseInt() method 
            int input = Integer.parseInt(s); 
            System.out.println(input + " is a valid integer number");
	    return true;
  }  
  catch (NumberFormatException e)  { 
     System.out.println(input + " is not a valid integer number"); 
  }
  return false;
}

public boolean isFloat(String s)
{
 try { 
            // checking valid float using parseFloat() method 
            float input = Float.parseFloat(s); 
            System.out.println(input + " is a valid float number");
	    return true;
  }  
  catch (NumberFormatException e)  { 
     System.out.println(input + " is not a valid float number"); 
  } 
  return false;
}

CondExpr create_cond_expr(String s)
{

  CondExpr expr = new CondExpr();
  expr.next = null;
  expr.flag = 0;
  String[] fullexpr = s.split(" ");

  String op1[] = fullexpr[0].split(".");		//splitting the first operand by space. 

/* SETTING THE ATTRIBUTE TYPE FOR FIRST OPERAND */
  if(op1[0] == 'A' || op1[0] == 'B')
  {
     expr.type1 = new AttrType(AttrType.attrSymbol);
     RelSpec spec;
     if(op1[0] == "A")
       spec = new RelSpec(RelSpec.outer);
     else
       spec = new RelSpec(RelSpec.inner);

     if(op1[1] == "interval")
          expr.operand1.symbol = new FldSpec(spec,1);
     if(op1[1] == "name")
          expr.operand1.symbol = new FldSpec(spec,2);
     if(op1[1] == "level")
          expr.operand1.symbol = new FldSpec(spec,3);
  } 
  if(isInteger(op1[0]))
  {
     int a = Integer.parseInt(op1[0]); 
     expr.type1 = new AttrType(AttrType.attrInteger);
     expr.operand1.integer = a;  
  }
  else if(isFloat(op1[0])) 
  {
     float a = Float.parseFloat(op1[0]);          
     expr.type1 = new AttrType(AttrType.attrReal);  
     expr.operand1.real = a;
  }
  else
  {
       expr.type1 = new AttrType(attrType.attrString);
       expr.operand1.string = op1[0];
  }

/* SET THE OPERATOR */
  String oper = fullexpr[1];
  if(oper == "=")
     expr.op = new AttrOperator(AttrOperator.aopEQ);
  else if(oper == ">")
     expr.op = new AttrOperator(AttrOperator.aopGT);
  else if(oper == "<")
     expr.op = new AttrOperator(AttrOperator.aopLT);
  else if(oper == "!=")
     expr.op = new AttrOperator(AttrOPerator.aopNE);

/* SET THE TYPE FOR SECOND OPERAND */
  String op2 = fullexpr[2].split(".");			//A.interval > 4 => fullexpr[0] = A.interval , fullexpr[1] = > and fullexpr[2] = 4
  if(op2[0] == 'A' || op2[0] == 'B')
  {
     expr.type2 = new AttrType(AttrType.attrSymbol);
     RelSpec spec;
     if(op2[0] == "A")
       spec = new RelSpec(RelSpec.outer);
     else
       spec = new RelSpec(RelSpec.inner);

     if(op2[1] == "interval")
          expr.operand2.symbol = new FldSpec(spec,1);
     if(op2[1] == "name")
          expr.operand2.symbol = new FldSpec(spec,2);
     if(op2[1] == "level")
          expr.operand2.symbol = new FldSpec(spec,3);
  } 
  if(isInteger(op2[0]))
  {
     int a = Integer.parseInt(op2[0]); 
     expr.type2 = new AttrType(AttrType.attrInteger);
     expr.operand2.integer = a;  
  }
  else if(isFloat(op2[0]))
  {
     float a = Float.parseFloat(op2[0]);          
     expr.type2 = new AttrType(AttrType.attrReal);  
     expr.operand2.real = a;
  }
  else
  {
       expr.type2 = new AttrType(attrType.attrString);
       expr.operand2.string = op1[0];
  }

  return expr;
}


CondExpr[] CondExprGen(String where,int queryno)			//query no indicates which phase of the join we are in.
{

int num_expr = 0;
for(int i = 0 ; i < where.length() ; i++)
  if(where[i].equals("and"))
    num_expr++;

num_expr += 1;
int i = 0 ;
CondExpr[] expr = new CondExpr[num_expr+1];
int count = 0;

while(i < where.length())
{
String s = "";
s = s + where[i++];
s = s + " ";
s = s + where[i++];
s = s + " ";
s = s + where[i++];
expr[count] = create_cond_expr(s);
count++;
}

for(int i = 0 ; i < num_expr ; i++)
{
    System.out.println("Operator " + expression[i].op); 
    System.out.println("Type 1 = " + expr[i].type1); 
    System.out.println("Type 2 = " + expr[i].type2); 
    System.out.println("Operand1 symbol = " + expr[i].operand1.symbol); 
    System.out.println("OPerand2 symbol = " + expr[i].operand2.symbol); 
}

expr[num_expr] = null;	//last one is null to indicate end.
return expr;
}

public static void main(String[] args){

 	String query = "SELECT A.interval, A.name, A.lvl, B.interval, B.name, B.lvl\n"
				+ "FROM A join B\n"
				+ "WHERE A.interval > B.interval";
		
	Map<String,String> attrtypemap = new HashMap<String,String>();
	attrtypemap.put("interval","AttrType.attrInterval");
	attrtypemap.put("name","AttrType.attrString");
	attrtypemap.put("lvl","AttrType.attrInteger");
		
	Map<String,String> relmap = new HashMap<String,String>();
	relmap.put("A","new RelSpec(RelSpec.outer)");
	relmap.put("B","new RelSpec(RelSpec.inner)");
		
	String[] queryarr = new String[3];
	String[] select;
	String[] from;
	String[] where;
	queryarr = query.split("\n");
		
	select = queryarr[0].split(" ");
	from = queryarr[1].split(" ");
	where = queryarr[2].split(" ");
       
	List<String> atypes = new ArrayList<String>();    //array containing attribute types for relation A to construct ATypes[]
	List<String> btypes = new ArrayList<String>();    //array containing attribute types for relation B to construct BTypes[]
	CondExpr[] expr;

	for(int i = 0 ; i < select.length() ; i++)
 	{
	   String flds = select[i].split(".");
	   if(flds[0] == 'A')
 	   {
		if(flds[1] == "interval")
		   atypes.add("interval");
		else if(flds[1] == "name")
		   atypes.add("string");
		else
		   atypes.add("integer");		
           }
           else 
	   {
		if(flds[1] == "interval")
		   btypes.add("interval");
		else if(flds[1] == "name")
		   btypes.add("string");
		else
		   btypes.add("integer");
           }                
        }

        expr = CondExprGen(where);
}

}
