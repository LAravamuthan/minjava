package global;

public class XMLNode {


    private int parentStartId;  //keeps the start index of the parent
    private int[] nodeInnterval; //keeps the end index of the parent
    private String nodeString;  // keeps the xml Tag

    public XMLNode(int parentStartID, int startIndex, String nodeString) {
        this.nodeInnterval = new int[2];
        this.parentStartId = parentStartID;
        this.nodeInnterval[0] = startIndex;
        this.nodeString = nodeString;
    }

    public void setEndOfInterval(int end) {
        this.nodeInnterval[1] = end;
    }

    public int getParentStartId() {
        return this.parentStartId;
    }

    public int getStartOfInterval() {
        return this.nodeInnterval[0];
    }

    public int getEndOfInterval() {
        return this.nodeInnterval[1];
    }

    public int[] getNodeInterval() {
        return nodeInnterval;
    }

    public String getNodeString() {
        return this.nodeString;
    }
}
