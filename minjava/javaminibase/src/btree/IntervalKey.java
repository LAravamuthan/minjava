package btree;

import global.intervaltype;
public class IntervalKey extends KeyClass {

    public intervaltype key;

    public String toString(){
        return key.toString();
    }

    /** Class constructor
     *
     */
    public IntervalKey()
    {
        key.assign(0,0);

//        key.setS(0);
//        key.setE(0);
    }
    public IntervalKey(intervaltype value)
    {
        key = new intervaltype();
        key.assign(value.get_s(),value.get_e());

    }

    /** Class constructor
     *
     */
    public intervaltype getKey() {return  key;}



    /** get a copy of the integer key
     *  @return the reference of the copy
     */

    /** set the integer key value
     */
    public void setKey(intervaltype value)
    {
        key.assign(value.get_s(),value.get_e());
    }
}
