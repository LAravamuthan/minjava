package btree;
import global.*;
/**  IntegerKey: It extends the KeyClass.
 *   It defines the integer Key.
 */ 
public class IntervalKey extends KeyClass {

  private intervaltype key;

  public String toString(){
     return Integer.toString(key.get_s())+" "+Integer.toString(key.get_e());
  }

  /** Class constructor
   *  @param     value   the value of the integer key to be set 
   */
  public IntervalKey(intervaltype value) 
  { 
    key=new intervaltype(value.get_s(), value.get_e());
  }

  /** get a copy of the integer key
   *  @return the reference of the copy 
   */
  public intervaltype getKey() 
  {
    return new intervaltype(key.get_s(), key.get_e());
  }

  /** set the integer key value
   */  
  public int compareTo(IntervalKey keytocompare)
  {
    if(this.getKey().get_s() < keytocompare.getKey().get_s())
      return -1;
    else
      return 1;
  }


  public void setKey(intervaltype value) 
  { 
    key=new intervaltype(value.get_s(), value.get_e());
  }
}
