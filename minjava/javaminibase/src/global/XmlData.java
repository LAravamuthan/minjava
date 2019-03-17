package global;

import java.util.Objects;

public class XmlData {

    private String tag;
    private IntervalType it;

    public XmlData(String tag, IntervalType it){
        this.tag = tag;
        this.it = it;
    }

    @Override
    public String toString() {
        return "XmlData{" +
                "tag='" + tag + '\'' +
                ", it=" + it +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XmlData xmlData = (XmlData) o;
        return Objects.equals(tag, xmlData.tag) &&
                Objects.equals(it, xmlData.it);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, it);
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public IntervalType getIt() {
        return it;
    }

    public void setIt(IntervalType it) {
        this.it = it;
    }
}
