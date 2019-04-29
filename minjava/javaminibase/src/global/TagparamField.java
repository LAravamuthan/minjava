package global;


import java.util.List;

public class TagparamField {
    private NodeContext tgpr = null;
    private List<Integer> Fldtrk = null;

    public TagparamField(NodeContext tagpar, List<Integer> fldtrk) {
        this.tgpr = tagpar;
        this.Fldtrk = fldtrk;
    }

    public NodeContext GetTagParams() {
        return this.tgpr;
    }

    public List<Integer> GetFldtrk() {
        return this.Fldtrk;
    }
}