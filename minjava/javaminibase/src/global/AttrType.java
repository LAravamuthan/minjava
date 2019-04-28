package global;

import java.io.Serializable;

/**
 * Enumeration class for AttrType
 */

public class AttrType implements Serializable {

    public static final int attrString = 0;
    public static final int attrInteger = 1;
    public static final int attrReal = 2;
    public static final int attrSymbol = 3;
    public static final int attrNull = 4;
    public static final int attrInterval = 5;

    public int attrType;

    public String toString() {

        switch (attrType) {
            case attrString:
                return "attrString";
            case attrInteger:
                return "attrInteger";
            case attrReal:
                return "attrReal";
            case attrSymbol:
                return "attrSymbol";
            case attrNull:
                return "attrNull";
            case attrInterval:
                return "attrInterval";
        }
        return ("Unexpected AttrType " + attrType);
    }

    /**
     * AttrType Constructor
     * <br>
     * An attribute type of String can be defined as
     * <ul>
     * <li>   AttrType attrType = new AttrType(AttrType.attrString);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (attrType.attrType == AttrType.attrString) ....
     * </ul>
     *
     * @param _attrType The types of attributes available in this class
     */

    public AttrType(int _attrType) {
        attrType = _attrType;
    }
}