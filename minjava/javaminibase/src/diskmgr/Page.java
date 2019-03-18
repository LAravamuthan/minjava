/* File Page.java */

package diskmgr;

import global.*;

 /**
  * class Page
  */

public class Page implements GlobalConst{
  
  
  /**
   * default constructor
   */
  
  public Page()  
    {
      data = new byte[MAX_SPACE];
      
    }
  
  /**
   * Constructor of class Page
   */
  public Page(byte [] apage)
    {
      data = apage;
    }
  
  /**
   * return the data byte array
   * @return 	the byte array of the page
   */
  public byte [] getpage()
    {
      return data;
      
    }

    /**
     * return the object of IntervalType we need to access.
     * @data 	the byte array of the page
     * @position the position in the data array we are converting.
     * @return the object of IntervalType we need to access.
     */
    public IntervalType getIntervalValue(int position, byte[] data)
      {
        //Use convert class to get the interval field for the data array
        IntervalType temp = Convert.getIntervalFld(position, data);

        //return the IntervalType object
        return temp;

      }

    /**
     * return nothing
     * @value the IntervalType value we are updating with in the byte array
     * @data 	the byte array of the page
     * @position the position in the data array we are converting.
     */
    public void setIntervalValue(IntervalType value, int position, byte[] data) throws java.io.IOException
      {
        //use the convert class method to set the value of the intervaltype object in the byte array

          Convert.setIntervalFld(value, position, data);




      }
  /**
   * set the page with the given byte array
   * @param 	array   a byte array of page size
   */
  public void setpage(byte [] array)
    {
      data = array;
    }
  
  /**
   * protected field: An array of bytes (for the page). 
   * 
   */
  protected byte [] data;
  
}
