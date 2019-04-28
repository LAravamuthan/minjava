package global;

import heap.Heapfile;
import iterator.Iterator;

import java.io.Serializable;
import java.util.Arrays;

public class NodeContext implements Serializable {


    private AttrType[] tupleAtrTypes = null; // Column types of the Tuples in the heap File
    private short[] tupleStringSizes = null; // Sizes list of string field present in the Tuples of the heap file
    private int nodeSizeofTuple; // Total size of the tuple in the heap file

    //private Heapfile nodeHeapFile = null;
    private RID nodeRid = null; //record id for the heap file (record id) to access each record in the heap file
    private String nodeHeapFileName = null; // Name of the heap file pointed by the instance of this class
    private Iterator itr = null;

    public NodeContext(){

    }

    public NodeContext(RID nodeRid, String nodeHeapFileName, AttrType[] tupleAtrTypes, short[] tupleStringSizes, int nodeSizeofTuple, Iterator itr) {
        this.tupleAtrTypes = tupleAtrTypes;
        this.tupleStringSizes = tupleStringSizes;
        this.nodeSizeofTuple = nodeSizeofTuple;
        //this.nodeHeapFile = nodeHeapFile;
        this.nodeRid = nodeRid;
        this.nodeHeapFileName = nodeHeapFileName;
        this.itr = itr;
    }

    public NodeContext(Heapfile nodeHeapFile, RID nodeRid, String nodeHeapFileName, AttrType[] tupleAtrTypes, short[] tupleStringSizes, int nodeSizeofTuple) {
        this.tupleAtrTypes = tupleAtrTypes;
        this.tupleStringSizes = tupleStringSizes;
        this.nodeSizeofTuple = nodeSizeofTuple;
        //this.nodeHeapFile = nodeHeapFile;
        this.nodeRid = nodeRid;
        this.nodeHeapFileName = nodeHeapFileName;
    }

    /*public Heapfile getNodeHeapFile() {
        return this.nodeHeapFile;
    }*/

    public RID getNodeRID() {
        return this.nodeRid;
    }

    public String getNodeHeapFileName() {
        return this.nodeHeapFileName;
    }

    public AttrType[] getTupleAtrTypes() {
        return this.tupleAtrTypes;
    }

    public short[] getTupleStringSizes() {
        return this.tupleStringSizes ;
    }

    public int getNodeSizeofTuple() {
        return this.nodeSizeofTuple;
    }

    public Iterator getItr(){
        return this.itr;
    }

    public void setItr(Iterator itr){
        this.itr = itr;
    }

    @Override
    public String toString() {
        return "NodeContext{" +
                "tupleAtrTypes=" + Arrays.toString(tupleAtrTypes) +
                ", tupleStringSizes=" + Arrays.toString(tupleStringSizes) +
                ", nodeSizeofTuple=" + nodeSizeofTuple +
                ", nodeRid=" + nodeRid +
                ", nodeHeapFileName='" + nodeHeapFileName + '\'' +
                ", itr=" + itr +
                '}';
    }
}
