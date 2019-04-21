package btree;
import global.*;
public class IntervalKey extends KeyClass {

    public IntervalType key;

    public String toString(){
        return key.toString();
    }

    /** Class constructor
     *  @param     value   the value of the integer key to be set
     */
    public IntervalKey()
    {
        key.assign(0,0);

//        key.setS(0);
//        key.setE(0);
    }
    public IntervalKey(IntervalType value)
    {
        key = new IntervalType();
        key.assign(value.getS(),value.getE());

    }

    /** Class constructor
     *  @param     value   the value of the integer key to be set
     */
    public IntervalType getKey() {return  key;}



    /** get a copy of the integer key
     *  @return the reference of the copy
     */

    /** set the integer key value
     */
    public void setKey(IntervalType value)
    {
        key.assign(value.getS(),value.getE());
    }
}
