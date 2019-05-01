/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package intervaltree;
import global.*;

/** KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
   /** key in the (key, data)
    */  
   public KeyClass key;
   /** data in the (key, data)
    */
   public DataClass data;
   
  /** Class constructor
   */
  public KeyDataEntry( intervaltype key, PageId pageNo) {
     this.key = new IntervalKey(key); 
     this.data = new IndexData(pageNo);
  }; 


  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, PageId pageNo) {
    data = new IndexData(pageNo); 
    this.key= new IntervalKey(((IntervalKey)key).getKey());
  };



  /** Class constructor.
   */
  public KeyDataEntry( intervaltype key, RID rid) {
     this.key = new IntervalKey(key); 
     this.data = new LeafData(rid);
  };

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, RID rid){
    data = new LeafData(rid); 
    this.key= new IntervalKey(((IntervalKey)key).getKey());
  
  };


  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key,  DataClass data) {
    
    this.key= new IntervalKey(((IntervalKey)key).getKey());

     if ( data instanceof IndexData ) 
        this.data= new IndexData(((IndexData)data).getData());
     else if ( data instanceof LeafData ) 
        this.data= new LeafData(((LeafData)data).getData()); 
  }

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(KeyDataEntry entry) {
      boolean st1,st2;

         st1= ((IntervalKey)key).getKey().equals
                  (((IntervalKey)entry.key).getKey());

      if( data instanceof IndexData )
         st2= ( (IndexData)data).getData().pid==
              ((IndexData)entry.data).getData().pid ;
      else
         st2= ((RID)((LeafData)data).getData()).equals
                (((RID)((LeafData)entry.data).getData()));

      return (st1&&st2);
  }     
}
