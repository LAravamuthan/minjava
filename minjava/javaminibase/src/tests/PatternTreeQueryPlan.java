package tests;

import iterator.*;
import heap.*;
import global.*;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.charset.*;


@SuppressWarnings("ALL")
class QueryPlanDriver implements GlobalConst {
    private BufferedReader reader;
    private int IntervalNo;
    private Tuple tplwtr = null;

    private Heapfile hpfile = null;
    private RID rid = null;
    private String hpfilename;

    private AttrType[] Stypes = null;
    private short[] Ssizes = null;
    private int SizeofTuple;
    private List<XmlData> xmlDataList = new ArrayList<XmlData>();
    //private TagParams maintgpair = null;

    public QueryPlanDriver(String fileName) {
        this.IntervalNo = 1;
        this.rid = null;
        this.hpfilename = "XMLtags.in";

        this.Stypes = new AttrType[2];
        this.Stypes[0] = new AttrType(AttrType.attrInterval);
        this.Stypes[1] = new AttrType(AttrType.attrString);

        this.Ssizes = new short[1];
        this.Ssizes[0] = 10; //first elt. is 10


        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.xmldb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".xmllog";
        SystemDefs sysdef = new SystemDefs(dbpath, 50000, MINIBASE_BUFFER_POOL_SIZE, "Clock");

        this.tplwtr = new Tuple();
        try {
            this.tplwtr.setHdr((short) 2, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        this.SizeofTuple = this.tplwtr.size();
        this.tplwtr = new Tuple(this.SizeofTuple);
        try {
            this.tplwtr.setHdr((short) 2, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            this.hpfile = new Heapfile(this.hpfilename);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        for(XmlData  xd1 : xmlDataList){
            try{
                this.tplwtr.setIntervalFld(1, xd1.getIt());
                this.tplwtr.setStrFld(2, xd1.getTag());

                rid = this.hpfile.insertRecord(this.tplwtr.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() for the intervals relation***");
                e.printStackTrace();
            }
        }
    }

    public AttrType[] getAttrType() {
        return this.Stypes;
    }

    public TagParameter rootFileTagParameter(){
        TagParameter rootTagParameter = new TagParameter(this.hpfile, this.rid, this.hpfilename, getAttrType(), getStrSizes(), getTupleSize());
        return rootTagParameter;
    }

    public short[] getStrSizes() {
        return this.Ssizes;
    }

    public int getTupleSize() {
        return this.SizeofTuple;
    }


    public FldSpec[] getProjectionsGeneralised(int outerField, int innerField) {

        FldSpec[] projections = new FldSpec[outerField+innerField];
        RelSpec rel_out = new RelSpec(RelSpec.outer);
        RelSpec rel_in = new RelSpec(RelSpec.innerRel);

        for(int i=0;i<outerField;i++)
        {
            projections[i] = new FldSpec(rel_out, i+1);
        }
        for(int i=outerField;i<outerField+innerField;i++)
        {
            projections[i] = new FldSpec(rel_in, i-outerField+1);
        }
        return projections;
    }

    //ContainOrEquality == true check containment or else check equality
    public CondExpr[] generateCondExpr(int opersymbfld1, int opersymbfld2, boolean ContainOrEquality) {
        CondExpr[] outFilter = new CondExpr[2];

        outFilter[0] = new CondExpr();
        outFilter[0].next = null;

        if (ContainOrEquality) {
            outFilter[0].op = new AttrOperator(AttrOperator.aopLT);
        } else {
            outFilter[0].flag = 0;
            outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        }

        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), opersymbfld1);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), opersymbfld2);
        outFilter[1] = null;
        return outFilter;
    }


    //parentchildflag == true check parent child or else check ancester descendant
    //ContainOrEquality == true check containment or else check equality

    public TagParameter joinTwoFields(TagParameter tagParam1, int joinfieldno1, TagParameter tagParam2, int joinfieldno2, boolean parentchildflag, boolean ContainOrEquality) {

        AttrType[] tagattrtype1 = tagParam1.getAtrTypesForATagRelation();
        short[] tagattrsize1 = tagParam1.getAtrSizesForATagRelation();
        int tagtupsize1 = tagParam1.getSizeofTupleForATagRelation();
        String taghpfilename1 = tagParam1.getHPFileNameForATagRelation();
        int outer = tagattrtype1.length;

        AttrType[] tagattrtype2 = tagParam2.getAtrTypesForATagRelation();
        short[] tagattrsize2 = tagParam2.getAtrSizesForATagRelation();
        int tagtupsize2 = tagParam2.getSizeofTupleForATagRelation();
        String taghpfilename2 = tagParam2.getHPFileNameForATagRelation();
        int inner = tagattrtype2.length;

        AttrType[] JoinedTagAttrtype = new AttrType[outer + inner];
        System.arraycopy(tagattrtype1, 0, JoinedTagAttrtype, 0, outer);
        System.arraycopy(tagattrtype2, 0, JoinedTagAttrtype, outer, inner);

        short[] JoinedTagsize = new short[tagattrsize1.length + tagattrsize2.length];
        System.arraycopy(tagattrsize1, 0, JoinedTagsize, 0, tagattrsize1.length);
        System.arraycopy(tagattrsize2, 0, JoinedTagsize, tagattrsize1.length, tagattrsize2.length);

        byte[] array = new byte[10];
        new Random().nextBytes(array);

        int JoinedTagTupSize = tagtupsize1 + tagtupsize2;
        //String JoinedTaghpfilename = taghpfilename1+"_"+taghpfilename2;
        String JoinedTaghpfilename = new String(array, Charset.forName("UTF-8"));
        Heapfile JoinedTaghpfile = null;

        try {
            JoinedTaghpfile = new Heapfile(JoinedTaghpfilename);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        RID JoinedTagRID = new RID();

        FldSpec[] projlist_tag1 = null, projlist_tag2 = null;
        projlist_tag1 = getProjectionsGeneralised(outer, 0);
        projlist_tag2 = getProjectionsGeneralised(outer, inner);

        CondExpr[] outFilter = null;
        outFilter = generateCondExpr(joinfieldno1, joinfieldno2, ContainOrEquality);

        FileScan fscan = null;
        try {
            fscan = new FileScan(taghpfilename1, tagattrtype1, tagattrsize1, (short) outer, outer, projlist_tag1, null);
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
        }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(tagattrtype1, outer, tagattrsize1, tagattrtype2, inner, tagattrsize2, 10, fscan, taghpfilename2, outFilter, null, projlist_tag2, outer + inner);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple temptup;
        int parent;
        IntervalType intval;
        try {
            while ((temptup = nlj.get_next()) != null) {
                if (parentchildflag) {
                    if (temptup.getIntervalFld(2).getS() == temptup.getIntFld(4)) {
                        JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());
                    }
                } else {
                    JoinedTagRID = JoinedTaghpfile.insertRecord(temptup.returnTupleByteArray());
                }
            }
        } catch (Exception e) {
            System.err.println("*** Error preparing for get_next tuple");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return new TagParameter(JoinedTaghpfile, JoinedTagRID, JoinedTaghpfilename, JoinedTagAttrtype, JoinedTagsize, JoinedTagTupSize);
    }


    public TagParameter[] ExtractTagToHeap(TagParameter tag_params, String[] tagnames) {
        Heapfile heaptosearch = tag_params.getTagHeapFile();
        RID ridtosearch = tag_params.getRID();
        int all_tags_len = tagnames.length;
        Heapfile[] heaptostore = new Heapfile[all_tags_len];
        RID[] ridtostore = new RID[all_tags_len];
        String[] hpfilenm = new String[all_tags_len];

        TagParameter[] tag_pars = new TagParameter[all_tags_len];

        for (int i = 0; i < all_tags_len; i++) {
            hpfilenm[i] = tagnames[i] + ".in";
        }

        boolean done = false;
        String tag;
        int sizetup;
        AttrType[] Stps = getAttrType();
        short[] Sszs = getStrSizes();
        int TupSize = getTupleSize();

        Tuple tup = new Tuple(TupSize);
        try {
            tup.setHdr((short) 2, Stps, Sszs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < all_tags_len; i++) {
                heaptostore[i] = new Heapfile(hpfilenm[i]);
            }
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        Scan scan = null;
        try {
            scan = heaptosearch.openScan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        while (!done) {
            try {
                tup = scan.getNext(ridtosearch);
                if (tup == null) {
                    done = true;
                    break;
                }
                tup.setHdr((short) 2, Stps, Sszs);
                tag = tup.getStrFld(2);

                for (int i = 0; i < all_tags_len; i++) {
                    if (tag.equals(tagnames[i])) {
                        try {
                            ridtostore[i] = heaptostore[i].insertRecord(tup.returnTupleByteArray());
                            break;
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < all_tags_len; i++) {
            tag_pars[i] = new TagParameter(heaptostore[i], ridtostore[i], hpfilenm[i], Stps, Sszs, TupSize);
        }
        return tag_pars;
    }


    public void ReverseArray(String[] arr) {
        String temp;
        int len = arr.length;

        for (int i = 0; i < (len / 2); i++) {
            temp = arr[i];
            arr[i] = arr[len - 1 - i];
            arr[len - 1 - i] = temp;
        }
    }

    public int[] GetFieldsToJoin(List<Integer> FieldTracker, int ftfld, int scfld) {
        int[] fields = new int[2];
        if (FieldTracker.indexOf(ftfld) > -1) {
            fields[0] = (3 * FieldTracker.indexOf(ftfld)) + 2;
            fields[1] = 2;
        } else if (FieldTracker.indexOf(scfld) > -1) {
            fields[0] = (3 * FieldTracker.indexOf(scfld)) + 2;
            fields[1] = 5;
        } else {
            fields[0] = -1;
            fields[1] = -1;
        }
        return fields;
    }

    //parentchildflag == true check parent child or else check ancester descendant
    //ContainOrEquality == true check containment or else check equality

    public TagParameter Query(TagParameter[] ExtractTags, String[] Joins) {
        TagParameter ResultTagPar = null;
        TagParameter TempTagPar = null;
        List<Integer> FieldTracker = new ArrayList<Integer>();

        for (int i = 0; i < Joins.length; i++) {
            String[] Jnfields = Joins[i].split(" ");
            int ftfld = Integer.parseInt(Jnfields[0]) - 1;
            int scfld = Integer.parseInt(Jnfields[1]) - 1;
            String typerel = Jnfields[2];
            boolean relflag = false;
            int[] fields = null;


            if (typerel.equals("PC")) {
                relflag = true;
            }

            TempTagPar = joinTwoFields(ExtractTags[ftfld], 2, ExtractTags[scfld], 2, relflag, true);

            if (ResultTagPar != null) {
                fields = GetFieldsToJoin(FieldTracker, ftfld, scfld);
                ResultTagPar = joinTwoFields(ResultTagPar, fields[0], TempTagPar, fields[1], false, false);
                FieldTracker.add(ftfld);
                FieldTracker.add(scfld);
            } else {
                ResultTagPar = TempTagPar;
                FieldTracker.add(ftfld);
                FieldTracker.add(scfld);
            }
        }
        return ResultTagPar;
    }


    public TagParameter ReadQueryAndExecute(TagParameter MainTagpair, String Queryfilename) {
        TagParameter QueryResult;
        List<String> querylinelist = null;
        try {
            String line;
            querylinelist = new ArrayList<String>();
            BufferedReader queryreader = new BufferedReader(new FileReader(Queryfilename));
            while ((line = queryreader.readLine()) != null) {
                querylinelist.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int numberofnodes = Integer.parseInt(querylinelist.get(0));
        String[] searchtags = new String[numberofnodes];
        for (int i = 1; i <= numberofnodes; i++) {
            searchtags[i - 1] = querylinelist.get(i).length() > 5 ? querylinelist.get(i).substring(0, 5) : querylinelist.get(i);
        }

        String[] NumberofJoins = new String[querylinelist.size() - numberofnodes - 1];
        for (int i = 0; i < NumberofJoins.length; i++) {
            NumberofJoins[i] = querylinelist.get(numberofnodes + 1 + i);
        }

        TagParameter[] AllTags = ExtractTagToHeap(MainTagpair, searchtags);


        ReverseArray(NumberofJoins);

        QueryResult = Query(AllTags, NumberofJoins);
        return QueryResult;

    }

    public void ScanHeapFile(TagParameter tgprms) {
        boolean done = false;
        int count_records = 0;
        Heapfile hpfl = tgprms.getTagHeapFile();
        RID filerid = tgprms.getRID();
        AttrType[] Atrtyps = tgprms.getAtrTypesForATagRelation();
        short[] Strsizes = tgprms.getAtrSizesForATagRelation();
        int TupSize = tgprms.getSizeofTupleForATagRelation();
        int numoffields = Atrtyps.length;

        Tuple temptup = null;

        Scan scan = null;
        try {
            scan = hpfl.openScan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        while (!done) {
            try {
                temptup = scan.getNext(filerid);
                if (temptup == null) {
                    done = true;
                    break;
                }
                temptup.setHdr((short) numoffields, Atrtyps, Strsizes);
                temptup.print(Atrtyps);
                System.out.println();
                count_records += 1;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.printf("Total records fetched = %s\n", count_records);
    }

}
//parentchildflag == true check parent child or else check ancester descendant
//ContainOrEquality == true check containment or else check equality

public class PatternTreeQueryPlan// implements  GlobalConst
{
    public static void main(String[] argvs) {
        String QueryFileName = System.getProperty("user.dir") + "/queryfile.txt";
        //String fname = "./plane.xml";
        try {
            QueryPlanDriver xmldvr = new QueryPlanDriver("");
            TagParameter rootTag = xmldvr.rootFileTagParameter();

            TagParameter qresult = xmldvr.ReadQueryAndExecute(rootTag, QueryFileName);
            xmldvr.ScanHeapFile(qresult);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during XML tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}
