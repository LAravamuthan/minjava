/* file Convert.java */

package global;

import java.io.*;
import java.lang.*;


public class Convert{
 
 /**
 * read 4 bytes from given byte array at the specified position
 * convert it to an integer
 * @param  	data 		a byte array 
 * @param       position  	in data[]
 * @exception   java.io.IOException I/O errors
 * @return      the integer 
 */
  public static int getIntValue (int position, byte []data)
   throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      int value;
      byte tmp[] = new byte[4];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 4);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readInt();  
      
      return value;
    }
  
 /**
 * read 4 bytes from given byte array at the specified position
 * convert it to an integer
 * @param   data    a byte array 
 * @param       position    in data[]
 * @exception   java.io.IOException I/O errors
 * @return      the integer 
 */
  public static intervaltype getIntervalValue (int position, byte []data)
   throws java.io.IOException
    {
      DataInputStream intstream1, intstream2;
      intervaltype value = new intervaltype();
      byte int1[] = new byte[4];
      byte int2[] = new byte[4];
      // Copy the values from the data byte[] to two byte array using the position location
      System.arraycopy (data, position, int1, 0, 4);
      System.arraycopy (data, position+4, int2, 0, 4);
      // Create a new data input Stream from the ByteArrrayInputStream
      intstream1 = new DataInputStream(new ByteArrayInputStream(int1));
      intstream2 = new DataInputStream(new ByteArrayInputStream(int2));
      value.assign(intstream1.readInt(), intstream2.readInt());
      return value;
    }


  /**
   * read 4 bytes from given byte array at the specified position
   * convert it to a float value
   * @param  	data 		a byte array 
   * @param       position  	in data[]
   * @exception   java.io.IOException I/O errorsintervaltype
   * @return      the float value
   */
  public static float getFloValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      float value;
      byte tmp[] = new byte[4];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 4);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readFloat();  
      
      return value;
    }
  
  
  /**
   * read 2 bytes from given byte array at the specified position
   * convert it to a short integer
   * @param  	data 		a byte array
   * @param       position  	the position in data[]
   * @exception   java.io.IOException I/O errors
   * @return      the short integer
   */
  public static short getShortValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      short value;
      byte tmp[] = new byte[2];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 2);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readShort();
      
      return value;
    }
  
  /**
   * reads a string that has been encoded using a modified UTF-8 format from
   * the given byte array at the specified position
   * @param       data            a byte array
   * @param       position        the position in data[]
   * @param 	length 		the length of the string in bytes
   *			         (=strlength +2)
   * @exception   java.io.IOException I/O errors
   * @return      the string
   */
  public static String getStrValue (int position, byte []data, int length)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      String value;
      byte tmp[] = new byte[length];  
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, length);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readUTF();
      return value;
    }
  
  /**
   * reads 2 bytes from the given byte array at the specified position
   * convert it to a character
   * @param       data            a byte array
   * @param       position        the position in data[]
   * @exception   java.io.IOException I/O errors
   * @return      the character
   */
  public static char getCharValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      char value;
      byte tmp[] = new byte[2];
      // copy the value from data array out to a tmp byte array  
      System.arraycopy (data, position, tmp, 0, 2);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readChar();
      return value;
    }
  
  
  /**
   * update an integer value in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into the data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setIntValue (int value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeInt(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 4 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 4);
      
    }
 
  /**
   * update an integer value in the given byte array at the specified position
   * @param   data    a byte array
   * @param value     the value to be copied into the data[]
   * @param position    the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setIntervalValue (intervaltype value, int position, byte []data) 
    throws java.io.IOException
    {

      int starting_index,ending_index;
      starting_index = value.get_s();
      ending_index = value.get_e();
      DataOutputStream outstream1, outstream2;
      OutputStream out1, out2 ;


      out1 = new ByteArrayOutputStream();
      out2 = new ByteArrayOutputStream();
      outstream1 = new DataOutputStream (out1);
      outstream2 = new DataOutputStream (out2);
      // write the value to the output stream
      outstream1.writeInt(starting_index);
      outstream2.writeInt(ending_index);
      // create valid byte array and push the the byte array to valid positions
      byte []B1 = ((ByteArrayOutputStream) out1).toByteArray();
      byte []B2 = ((ByteArrayOutputStream) out2).toByteArray();
      // copies the first 4 bytes of this byte array into data[]
      System.arraycopy (B1, 0, data, position, 4);
      System.arraycopy (B2, 0, data, position+4, 4);
    }


  /**
   * update a float value in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into the data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setFloValue (float value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeFloat(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 4 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 4);
      
    }
  
  /**
   * update a short integer in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setShortValue (short value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeShort(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 2 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 2);
      
    }
  
  /**
   * Insert or update a string in the given byte array at the specified 
   * position.
   * @param       data            a byte array
   * @param       value           the value to be copied into data[]
   * @param       position        the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
 public static void setStrValue (String value, int position, byte []data)
        throws java.io.IOException
 {
  /* creates a new data output stream to write data to
   * underlying output stream
   */
 
   OutputStream out = new ByteArrayOutputStream();
   DataOutputStream outstr = new DataOutputStream (out);
   
   // write the value to the output stream
   
   outstr.writeUTF(value);
   // creates a byte array with this output stream size and the 
   // valid contents of the buffer have been copied into it
   byte []B = ((ByteArrayOutputStream) out).toByteArray();
   
   int sz =outstr.size();  
   // copies the contents of this byte array into data[]
   System.arraycopy (B, 0, data, position, sz);
   
 }
  
  /**
   * Update a character in the given byte array at the specified position.
   * @param       data            a byte array
   * @param       value           the value to be copied into data[]
   * @param       position        the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setCharValue (char value, int position, byte []data)
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      outstr.writeChar(value);  
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies contents of this byte array into data[]
      System.arraycopy (B, 0, data, position, 2);
      
    }
}
