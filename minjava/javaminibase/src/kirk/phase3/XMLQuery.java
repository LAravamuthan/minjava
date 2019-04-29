package phase3;

import java.io.*;
import java.lang.*;

import global.*;
import bufmgr.*;
import iterator.*;
import heap.*;


/**
 * Pattern file data class
 * opCode list:
 * 0 : Select
 * 1 : CP - cartesian product
 * 2 : TJ - TAG tree join
 * 3 : NJ - ID tree join
 * 4 : SRT - tree results sort
 * 5 : GRP - tree results group
 */
class QueryData {
  String ptree1 = null;
  String ptree2 = null;
  PatternTree pt1 = null;
  PatternTree pt2 = null;
  String op = null;
  int opCode = -1;
  int opArg1 = -1;
  int opArg2 = -1;
  int[] buf_size = null;
}

//Set set the structures needed
//enum Order{UNSORT,SORT};
class Order {
  static final int UNSORT = 0;
  static final int SORT = 1;
  int order;

  public Order(int _order) {
    order = _order;
  }
}

/*
 * struct Group{ int len; // The num of tuple in the group. int count; // The
 * count for correct answers in the group. Order order; // order of tuples in
 * the group int mark[Max_answer]; // 1:tuple correct. 0:not yet checked Tuple *
 * mytuple[Max_answer]; // The answer in the group. };
 */

class Group {
  static int Max_answer = 15;
  int len;
  int count;
  Order order;
  int[] mark;
  Tuple[] mytuple;

  public Group() {
    mark = new int[Max_answer];
    mytuple = new Tuple[Max_answer];
  }
}

class TupleList {
  Tuple tuple;
  TupleList next;

  public TupleList() {
  }
}

public class XMLQuery implements GlobalConst {

  public static final int Max_group_num = 5;
  public static final int Max_answer = 15;

  private AttrType[] types;
  private short[] sizes;// sizes of attributes in answer tuple
  private short columnum; // number of attributes in answer tuple
  private int curGroup; // current group number
  private int tuplenum; // total number of answer tuples
  private int groupnum; // number of groups in answer tuples
  private Order grouporder; // order of groups

  private Group[] mygroup;

  // group mark, 1: checked already, 0: not checked
  private int gmark[];

  private int total; // total number of correct answers
  private int G_O_flag; // error flag for group order wrong
  // error flag for tuple order wrong
  private int[] T_O_flag;

  private TupleList missing;
  private TupleList extra;

  /**
   * Constructor
   */
  public XMLQuery(int q_index) {

    types = new AttrType[10];
    sizes = new short[10];// sizes of attributes in answer tuple
    mygroup = new Group[Max_group_num];
    gmark = new int[Max_group_num];
    T_O_flag = new int[Max_group_num];
    missing = new TupleList();
    extra = new TupleList();

    // more initializing
    for (int i = 0; i < Max_group_num; i++) {
      mygroup[i] = new Group();
    }
  }

  void AddtoList(TupleList list, Tuple t) {

    TupleList cur = new TupleList();
    cur.tuple = new Tuple();
    try {
      cur.tuple.setHdr(columnum, types, sizes);
    } catch (Exception e) {
      System.err.println("**** Error setting up the tuples");
    }
    TupleCopy(cur.tuple, t, (int) columnum, types);
    cur.next = list;
    list = cur;
  }


  /* Copies  one tuple to another location
   * @param Tuple to
   * @param Tuple from
   * @param int fldnum
   * @param AttrType type
   * @return pattern tree in patternData object
   */
  void TupleCopy(Tuple to, Tuple from, int fldnum, AttrType[] type) {

    int temp_i;
    float temp_f;
    String temp_s;

    for (int i = 1; i <= fldnum; i++) {
      switch ((type[i - 1]).attrType) {
        case AttrType.attrInteger:
          try {
            temp_i = from.getIntFld(i);
            to = to.setIntFld(i, temp_i);
            // to.tupleCopy(from);
          } catch (Exception e) {
            System.err.println("**** Error setting up the tuples");
          }
          break;
        case AttrType.attrReal:
          try {
            temp_f = from.getFloFld(i);
            to = to.setFloFld(i, temp_f);
            // to.tupleCopy(from);
          } catch (Exception e) {
            System.err.println("**** Error setting up the tuples");
          }
          break;
        case AttrType.attrString:
          try {
            temp_s = from.getStrFld(i);
            to = to.setStrFld(i, temp_s);
            // to.tupleCopy(from);
          } catch (Exception e) {
            System.err.println("**** Error setting up the tuples");
          }
          break;
        case AttrType.attrInterval:
          try {
            to = to.setIntervalFld(i, from.getIntervalFld(i));
            // to.tupleCopy(from);
          } catch (Exception e) {
            System.err.println("**** Error copying Interval field of Tuple");
          }
          break;
        default:
          // error(Don't know what to do with attrSymbol, attrNull--TupleCopy(..);
          break;
      }
    }
  }

  public void Check(Tuple t) {

    // first find curGroup
    if (curGroup == -1) {
      if (grouporder.order == Order.SORT) {
        if (gmark[0] == 0) {
          curGroup = 0;
        } else {
          AddtoList(extra, t);
          return;
        }
      } else { // grouporder == UNSORT
        int temp[] = new int[1];
        temp[0] = -1;
        curGroup = Search(t, temp);
        if (curGroup == -1) {
          // t not in answer
          AddtoList(extra, t);
          return;
        }
      }
    }

    int count = mygroup[curGroup].count; // shorthand

    // in curGroup
    if (mygroup[curGroup].order.order == Order.SORT) {

      // mygroup[curGroup].mytuple[count].print(types);

      try {
        TupleUtils tUtil = new TupleUtils();
        if (tUtil.Equal(mygroup[curGroup].mytuple[count], t, types, (int) columnum)) {
          MarkTuple(curGroup, count);
        } else {
          MisMatch(t);
        }
      } catch (Exception e) {
        System.err.println("" + e);
        System.err.println("***** Error comparing the value of tuples");
      }
      return;
    } else { // no order inside curGroup

      // look for tuple t inside curGroup
      for (int i = 0; i < mygroup[curGroup].len; i++) {
        try {
          TupleUtils tUtil = new TupleUtils();
          if ((mygroup[curGroup].mark[i] == 0)
                  && (tUtil.Equal(mygroup[curGroup].mytuple[i], t, types, (int) columnum))) {// found
            MarkTuple(curGroup, i);
            return;
          }
        } catch (Exception e) {
          System.err.println("" + e);
          System.err.println("***** Error comparing the value of tuples");
        }
      }

      // not found
      MisMatch(t);

      return;
    }
  }

  void MisMatch(Tuple t) {
    int t_num[] = new int[1];

    t_num[0] = -1;
    // first look for it in other groups
    int tempGroup = Search(t, t_num);

    if (tempGroup == -1) { // t not in answer tuples
      AddtoList(extra, t);
      return;
    } else if (tempGroup == curGroup) {
      if (mygroup[curGroup].order.order == Order.UNSORT) {
        // this should not happen
        System.out.print("*****Tuple in current group, but " + "checking failed to find it.\n\n");
        return;
      } else { // tuple sorted order in curGroup is wrong
        System.out.print("\n*****Tuples in group " + curGroup + " should be sorted.\n\n");

        // change order to UNSORT to facilitate further checking.
        mygroup[curGroup].order.order = Order.UNSORT;

        // set tuple order error flag
        T_O_flag[curGroup] = 1;

        MarkTuple(curGroup, t_num[0]);

        return;
      }
    } else { // found in another group
      // if mygroup[curGroup].count == 0, it's probably due to groups are not
      // sorted when they are suppose to
      // Leave curGroup open for further checking
      if (mygroup[curGroup].count != 0) {
        // add remaining tuple in curGroup to missing list
        if (mygroup[curGroup].count < mygroup[curGroup].len) {
          System.out.print("\n*****Group " + curGroup + " has missing tuples.\n\n");
          for (int i = 0; i < mygroup[curGroup].len; i++) {
            if (mygroup[curGroup].mark[i] == 0) {
              AddtoList(missing, mygroup[curGroup].mytuple[i]);
            }
          }
        }

        // mark the current group
        gmark[curGroup] = 1;
      }

      // now ready to reset curGroup
      if (grouporder.order == Order.SORT) {
        curGroup++;
        if (tempGroup != curGroup) {
          System.out.print("\n*****Group order is wrong.\n\n");

          // set group order error flag
          G_O_flag = 1;

          // change the grouporder to UNSORT to facilitate further checking
          grouporder.order = Order.UNSORT;
          curGroup = tempGroup;
        }
      } else { // group not sorted
        curGroup = tempGroup;
      }

      // now check the sorting status of the new curGroup
      if (mygroup[curGroup].order.order == Order.UNSORT) {
        MarkTuple(curGroup, t_num[0]);
      }
      // tuple should be sorted, check whether it's the first tuple of group
      else if (t_num[0] == 0) {
        MarkTuple(curGroup, t_num[0]);
      }
      // tuple in new curGroup not in correct sorted order
      else {
        System.out.print("\n*****Tuples in group " + curGroup + " should be sorted.\n\n");

        // set tuple sort order error flag
        T_O_flag[curGroup] = 1;

        // reset tuple order to UNSORT to facilitate further checking
        mygroup[curGroup].order.order = Order.UNSORT;

        MarkTuple(curGroup, t_num[0]);
      }
      return;
    }
  }

  // MarkTuple will mark the tuple in current group
  void MarkTuple(int groupNum, int tupleNum) {
    mygroup[groupNum].mark[tupleNum] = 1;
    mygroup[groupNum].count++;
    total++;

    // check to see whether current group is done
    if (mygroup[groupNum].count == mygroup[groupNum].len) {
      // mark the group
      gmark[groupNum] = 1;

      if (grouporder.order == Order.SORT) {
        curGroup++;
        if (curGroup >= groupnum) {
          curGroup = -1;
        }
      } else {
        curGroup = -1;
      }
    }
    return;
  }

  // Search() will look for a tuple and return the group number
  // and tuple number if found
  int Search(Tuple t, int[] t_num) {
    for (int i = 0; i < groupnum; i++) {
      if (gmark[i] == 0) {
        for (int j = 0; j < mygroup[i].len; j++) {
          try {
            TupleUtils tUtil = new TupleUtils();
            if ((mygroup[i].mark[j] == 0) && (tUtil.Equal(mygroup[i].mytuple[j], t, types, (int) columnum))) {
              t_num[0] = j;
              return i;
            }
          } catch (Exception e) {
            System.err.println("" + e);
            System.err.println("***** Error comparing the value of tuples");
          }
        }
      }
    }

    // not found
    t_num[0] = -1;
    return -1;
  }

  // report the status of the query
  public void report(int querynum) {
    if (total < tuplenum)
      System.out.print("\n*****Error occured in QueryCheck.\n\n");

    TupleList temp;

    try {
      if (missing != null) {
        System.out.print("\n***The following tuples are missing " + "from your answer:\n");
        temp = missing;
        while (temp != null) {
          temp.tuple.print(types);
          temp = temp.next;
        }
      }

      if (extra != null) {
        System.out.print("\n***The following tuples from your answer " + "are incorrect:\n");
        temp = extra;
        while (temp != null) {
          temp.tuple.print(types);
          temp = temp.next;
        }
      }
    } catch (Exception e) {
      System.err.println("" + e);
      System.err.println("**** Error printing the tuples out");
    }

    if (missing != null || extra != null) {
      System.out.print("\nIf you see the same tuples in the " + "missing list and the extra\n");
      System.out.print("  list, your tuples are probably not " + "grouped correctly.\n");
    }

    // check group order error flag
    if (G_O_flag != 0)
      System.out.print("\n*****Your group ordering is wrong.\n\n");

    // check tuple order error flag
    int t_order_error = 0;
    for (int j = 0; j < groupnum; j++) {
      if (T_O_flag[j] == 1) {
        t_order_error = 1;
        System.out.print("\n*****Your tuple order in group " + j + " is wrong.\n\n");
      }
    }

    if (total == tuplenum && missing == null && extra == null && G_O_flag == 0 && t_order_error == 0) {
      System.out.print("\nQuery" + querynum + " completed successfully!\n");
      System.out.print("*******************Query" + querynum + " finished!!!*****************\n\n");
    }
    return;
  }

  //  ----------------------  CopyPasta temoporary code above -----------------------
  //  +++++++++++++++++++++++++++  New code below  ++++++++++++++++++++++++++++++++++

  public static void SelectProcessing(XMLDriver xmldrvr, NodeContext mainTagPair, QueryData data) {
    NodeContext[] qresult = xmldrvr.SelectQuery(mainTagPair, data.pt1);
    System.out.println("KIRK: SelectProcessing:");
    System.out.println("qresult: " + qresult);

    for (int i = 0; i < qresult.length; i++) {

      if (qresult[i] != null) {
        xmldrvr.ScanHeapFile(qresult[i]); // print all the heap file query results
      }
    }
  }

  /**
   * @param data - Derived data from input files
   * @param type - Decides type of query
   *             0 - Join based on TAG
   *             1 - Join based on ID
   */
  public static void JoinProcessing(XMLDriver xmldrvr, NodeContext mainTagPair, QueryData data, int type) {

  }

  public static void SortProcessing(XMLDriver xmldrvr, NodeContext mainTagPair, QueryData data) {

  }

  public static void GroupProcessing(XMLDriver xmldrvr, NodeContext mainTagPair, QueryData data) {

  }

  public static QueryData readQuery(String fileName) {
    QueryData query = new QueryData();
    BufferedReader fileReader;
    String line;
    String[] tmp, tmp2;

    try {
      fileReader = new BufferedReader(new FileReader(fileName));
      while ((line = fileReader.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("ptree_1")) {
          tmp = line.split(" ");
          query.ptree1 = tmp[1];
        } else if (line.startsWith("ptree_2")) {
          tmp = line.split(" ");
          query.ptree2 = tmp[1];
        } else if (line.startsWith("operation")) {
          tmp = line.split(" ");
          query.op = tmp[1];
          if (query.op.contains("CP")) {
            query.opCode = 1;
          } else {
            if (query.op.contains("TJ")) {
              query.opCode = 2;
              if (!tmp[3].isEmpty()) {
                query.opArg2 = Integer.parseInt(tmp[3]);
              }
            } else if (query.op.contains("NJ")) {
              query.opCode = 3;
              if (!tmp[3].isEmpty()) {
                query.opArg2 = Integer.parseInt(tmp[3]);
              }
            } else if (query.op.contains("SRT")) {
              query.opCode = 4;
            } else if (query.op.contains("GRP")) {
              query.opCode = 5;
            } else {
              query.opCode = 0; // Default is a select with no arguments
            }
            if (!tmp[2].isEmpty()) {
              query.opArg1 = Integer.parseInt(tmp[2]);
            }
          }
        } else if (line.startsWith("buf_size")) {
          if (line.contains(" ")) {
            tmp = line.split(" ");
          } else {
            tmp = line.split(":");
          }
          tmp2 = tmp[1].split(",");
          query.buf_size = new int[tmp2.length];
          for (int i = 0; i < tmp2.length; i++) {
            query.buf_size[i] = Integer.parseInt(tmp2[i]);
          }
        }
      }
      fileReader.close();
      System.out.println("Query data:");
      System.out.println("- ptree_1: " + query.ptree1);
      if (query.ptree2 != null) {
        System.out.println("- ptree_2: " + query.ptree2);
      }
      if (query.op != null) {
        System.out.print("- op: " + query.op + " ");
      }
      if (query.opArg1 > 0) {
        System.out.print(query.opArg1 + " ");
      }
      if (query.opArg2 > 0) {
        System.out.print(query.opArg2);
      }
      if (query.op != null) {
        System.out.println();
      }
      System.out.print("- buf_size: ");
      for (int i = 0; i < query.buf_size.length; i++) {
        System.out.print(query.buf_size[i]);
        if (i < query.buf_size.length - 1) {
          System.out.print(", ");
        }
      }
      System.out.println("\n");
    } catch (Exception e) {
      System.err.println("*** error reading complex pattern file ***");
      e.printStackTrace();
    }
    return query;
  }


  public static void main(String[] args) {

    String dataFileName;
    String queryFileName;
    QueryData qData;
    NodeContext[] qresult;


    if (args.length == 0) {
      System.out.println("Usage:\n\n  java -classpath .:..:../* phase3.XMLQuery " +
              "[full path to database file] [path to complex pattern file]\n\n");
      System.out.println("  $ java -classpath .:..:../* phase3.XMLLoad test." +
              "db xml_sample_data.xml");
    } else {
      System.out.println("################### XMLQuery: Beginning ###################");
      dataFileName = args[0];
      queryFileName = args[1];

      try {
        XMLDriver xmldrvr = new XMLDriver(dataFileName);
        NodeContext MainTagpair = xmldrvr.OpenExistingDBinHeapFile();
        System.out.println("Database File Name: " + dataFileName);
        System.out.println("Pattern File Name: " + queryFileName);

        System.out.println("==> Reading complex pattern file");
        // Read the complex pattern file
        qData = readQuery(queryFileName);

        System.out.println("==> Routing Query ...");
        // chose next path based on operation
        switch ((qData.opCode)) {
          case 1:   // CP - cartesian product
            System.out.println("==> Query Operation: Cartesian Product");
            System.out.println("====> Not implemented due to group size. Exiting");
            Runtime.getRuntime().exit(0);
            break;
          case 2:   // TJ - TAG tree join
            System.out.println("==> Query Operation: Join based on TAG");
            qData.pt1 = PatternTreeParser.ParseInput(qData.ptree1 + ".txt");
            qData.pt2 = PatternTreeParser.ParseInput(qData.ptree2 + ".txt");
            System.out.println("===> INPUT: Pattern Tree 1");
            System.out.println(qData.pt1 + "---------------------------------------\n");
            System.out.println("===> INPUT: Pattern Tree 2");
            System.out.println(qData.pt2 + "---------------------------------------\n");
            System.out.println("--------------- Output ----------------\n");
            JoinProcessing(xmldrvr, MainTagpair, qData, 0);
            break;
          case 3:   // NJ - ID tree join
            System.out.println("==> Query Operation: Join based on ID");
            qData.pt1 = PatternTreeParser.ParseInput(qData.ptree1 + ".txt");
            qData.pt2 = PatternTreeParser.ParseInput(qData.ptree2 + ".txt");
            System.out.println("===> INPUT: Pattern Tree 1");
            System.out.println(qData.pt1 + "---------------------------------------\n");
            System.out.println("===> INPUT: Pattern Tree 2");
            System.out.println(qData.pt2 + "---------------------------------------\n");
            System.out.println("--------------- Output ----------------\n");
            JoinProcessing(xmldrvr, MainTagpair, qData, 1);
            break;
          case 4:   // SRT - tree results sort
            System.out.println("==> Query Operation: Sort");
            qData.pt1 = PatternTreeParser.ParseInput(qData.ptree1 + ".txt");
            System.out.println("===> INPUT: Pattern Tree 1");
            System.out.println(qData.pt1 + "---------------------------------------\n");
            System.out.println("--------------- Output ----------------\n");
            SortProcessing(xmldrvr, MainTagpair, qData);
            break;
          case 5:   // GRP - tree results group
            System.out.println("==> Query Operation: Group By");
            qData.pt1 = PatternTreeParser.ParseInput(qData.ptree1 + ".txt");
            System.out.println("===> INPUT: Pattern Tree 1");
            System.out.println(qData.pt1 + "---------------------------------------\n");
            System.out.println("--------------- Output ----------------\n");
            GroupProcessing(xmldrvr, MainTagpair, qData);
            break;
          default:   // Select - Default option
            System.out.println("==> No Query Operation provided: Defaulting to Select");
            System.out.println("--------------- Output ----------------\n");
            qData.pt1 = PatternTreeParser.ParseInput(qData.ptree1 + ".txt");
            SelectProcessing(xmldrvr, MainTagpair, qData);
        }

        /*
        NodeContext MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();
        NodeContext[] qresult = xmldvr.ReadQueryAndExecute(MainTagPair, QueryFileName);
        for (int i = 0; i < qresult.length; i++) {
          if (qresult[i] != null) {
            xmldvr.ScanHeapFile(qresult[i]); // print all the heap file query results
          }
        }*/
        System.out.println("Reads: " + PageCounter.getreads());
        System.out.println("Writes: " + PageCounter.getwrites());
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Error encountered during XML Query:\n");
        Runtime.getRuntime().exit(1);
      }
      System.out.println("################### XMLQuery: End ###################\n\n\n");
      Runtime.getRuntime().exit(0);
    }
  }
}