package tests;

import global.AttrType;
import global.RID;
import heap.Heapfile;

public class TagParameter {

    private Heapfile tag_heap_file = null;
    private RID tag_rid = null;
    private String hp_filename = null;

    private AttrType[] AtrTypes = null;
    private short[] AtrSizes = null;
    private int SizeofTuple;

    public TagParameter(Heapfile hpfile, RID rid, String hpfname, AttrType[] atrtypes, short[] atrsizes, int sizetuple) {
        this.tag_heap_file = hpfile;
        this.tag_rid = rid;
        this.hp_filename = hpfname;
        this.AtrTypes = atrtypes;
        this.AtrSizes = atrsizes;
        this.SizeofTuple = sizetuple;
    }

    public Heapfile getTagHeapFile() {
        return this.tag_heap_file;
    }

    public RID getRID() {
        return this.tag_rid;
    }

    public String getHPFileNameForATagRelation() {
        return this.hp_filename;
    }

    public AttrType[] getAtrTypesForATagRelation() {
        return this.AtrTypes;
    }

    public short[] getAtrSizesForATagRelation() {
        return this.AtrSizes;
    }

    public int getSizeofTupleForATagRelation() {
        return this.SizeofTuple;
    }
}
