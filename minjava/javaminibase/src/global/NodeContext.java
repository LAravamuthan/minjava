package global;

import heap.Heapfile;

public class NodeContext {


    private AttrType[] tupleAtrTypes = null; // Column types of the Tuples in the heap File
    private short[] tupleStringSizes = null; // Sizes list of string field present in the Tuples of the heap file
    private int nodeSizeofTuple; // Total size of the tuple in the heap file

    private Heapfile nodeHeapFile = null;
    private RID nodeRid = null; //record id for the heap file (record id) to access each record in the heap file
    private String nodeHeapFileName = null; // Name of the heap file pointed by the instance of this class

    public NodeContext(Heapfile nodeHeapFile, RID nodeRid, String nodeHeapFileName, AttrType[] tupleAtrTypes, short[] tupleStringSizes, int nodeSizeofTuple) {
        this.tupleAtrTypes = tupleAtrTypes;
        this.tupleStringSizes = tupleStringSizes;
        this.nodeSizeofTuple = nodeSizeofTuple;
        this.nodeHeapFile = nodeHeapFile;
        this.nodeRid = nodeRid;
        this.nodeHeapFileName = nodeHeapFileName;
    }

    public Heapfile getNodeHeapFile() {
        return this.nodeHeapFile;
    }

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

}
