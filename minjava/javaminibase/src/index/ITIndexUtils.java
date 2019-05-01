package index;
import global.*;
import intervaltree.*;
import iterator.*;
import java.io.*;


/**
 * IndexUtils class opens an index scan based on selection conditions.
 * Currently only BTree_scan is supported
 */
public class ITIndexUtils {

 public static IndexFileScan IntervalTree_scan(CondExpr[] selects, IndexFile indFile) 
    throws IOException, 
	   UnknownKeyTypeException, 
	   InvalidSelectionException,
	   KeyNotMatchException,
	   UnpinPageException,
	   PinPageException,
	   IteratorException,
	   ConstructPageException
    {
      IndexFileScan indScan;
      
      if (selects == null || selects[0] == null) {
	indScan = ((IntervalTreeFile)indFile).new_scan(null, null);
	return indScan;
      }
      
      if (selects[1] == null) {
	if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
	  throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition"); 
	}
	
	KeyClass key;
	
	// symbol = value
	if (selects[0].op.attrOperator == AttrOperator.aopEQ) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((IntervalTreeFile)indFile).new_scan(key, key);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((IntervalTreeFile)indFile).new_scan(key, key);
	  }
	  return indScan;
	}
	
	// symbol < value or symbol <= value
	if (selects[0].op.attrOperator == AttrOperator.aopLT || selects[0].op.attrOperator == AttrOperator.aopLE) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((IntervalTreeFile)indFile).new_scan(null, key);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((IntervalTreeFile)indFile).new_scan(null, key);
	  }
	  return indScan;
	}
	
	// symbol > value or symbol >= value
	if (selects[0].op.attrOperator == AttrOperator.aopGT || selects[0].op.attrOperator == AttrOperator.aopGE) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((IntervalTreeFile)indFile).new_scan(key, null);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((IntervalTreeFile)indFile).new_scan(key, null);
	  }
	  return indScan;
	}
	
	// error if reached here
	System.err.println("Error -- in IndexUtils.ITree_scan()");
	return null;
      }
      else {
	// selects[1] != null, must be a range query
	if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
	  throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition"); 
	}
	if (selects[1].type1.attrType != AttrType.attrSymbol && selects[1].type2.attrType != AttrType.attrSymbol) {
	  throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition"); 
	}
	
	// which symbol is higher??
	KeyClass key1, key2;
	AttrType type;
	
	if (selects[0].type1.attrType != AttrType.attrSymbol) {
	  key1 = getValue(selects[0], selects[0].type1, 1);
	  type = selects[0].type1;
	}
	else {
	  key1 = getValue(selects[0], selects[0].type2, 2);
	  type = selects[0].type2;
	}
	if (selects[1].type1.attrType != AttrType.attrSymbol) {
	  key2 = getValue(selects[1], selects[1].type1, 1);
	}
	else {
	  key2 = getValue(selects[1], selects[1].type2, 2);
	}
	
	switch (type.attrType) {

	case AttrType.attrInterval:
	  if (((IntervalKey)key1).getKey().get_s() - ((IntervalKey)key2).getKey().get_s() < 0) {
	    indScan = ((IntervalTreeFile)indFile).new_scan(key1, key2);
	  }
	  else {
	    indScan = ((IntervalTreeFile)indFile).new_scan(key2, key1);
	  }
	  return indScan;

	  
	case AttrType.attrReal:
	  /*
	    if ((FloatKey)key1.getKey().floatValue() < (FloatKey)key2.getKey().floatValue()) {
	    indScan = ((BTreeFile)indFile).new_scan(key1, key2);
	    }
	    else {
	    indScan = ((BTreeFile)indFile).new_scan(key2, key1);
	    }
	    return indScan;
	  */
	default:
	  // error condition
	  throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");	
	}
      } // end of else 
      
    } 





  /**
   * getValue returns the key value extracted from the selection condition.
   * @param cd the selection condition
   * @param type attribute type of the selection field
   * @param choice first (1) or second (2) operand is the value
   * @return an instance of the KeyClass (IntegerKey or StringKey)
   * @exception UnknownKeyTypeException only int and string keys are supported 
   */
  private static KeyClass getValue(CondExpr cd, AttrType type, int choice)
       throws UnknownKeyTypeException
  {
    // error checking
    if (cd == null) {
      return null;
    }
    if (choice < 1 || choice > 2) {
      return null;
    }
    
    switch (type.attrType) {
    case AttrType.attrInterval:
      if (choice == 1) return new IntervalKey(new intervaltype(cd.operand1.interval));
      else return new IntervalKey(new intervaltype(cd.operand2.interval));
    case AttrType.attrReal:
      /*
      // need FloatKey class in bt.java
      if (choice == 1) return new FloatKey(new Float(cd.operand.real));
      else return new FloatKey(new Float(cd.operand.real));
      */
    default:
	throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");
    }
    
  }
  
}
