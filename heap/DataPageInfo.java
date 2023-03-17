package heap;


/** File DataPageInfo.java */


import BigT.Map;
import global.*;
import java.io.*;

/** DataPageInfo class : the type of records stored on a directory page.
*
* April 9, 1998
*/

public class DataPageInfo implements GlobalConst{


  /** HFPage returns int for avail space, so we use int here */
  int    availspace; 
  
  /** for efficient implementation of getRecCnt() */
  int    recct;    
  
  /** obvious: id of this particular data page (a HFPage) */
  public PageId pageId = new PageId();
    
  /** auxiliary fields of DataPageInfo */

  public static final int size = 12;// size of DataPageInfo object in bytes

  private byte [] data;  // a data buffer
  
  private int offset;


/**
 *  We can store roughly pagesize/sizeof(DataPageInfo) records per
 *  directory page; for any given HeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */


  /** Default constructor
   */
  public DataPageInfo()
  {  
    data = new byte[12]; // size of datapageinfo
    int availspace = 0;
    recct =0;
    pageId.pid = INVALID_PAGE;
    offset = 0;
  }
  
  /** Constructor 
   * @param array  a byte array
   */
  public DataPageInfo(byte[] array)
  {
    data = array;
    offset = 0;
  }

      
   public byte [] returnByteArray()
   {
     return data;
   }
      
      
  /** constructor: translate a tuple to a DataPageInfo object
   *  it will make a copy of the data in the tuple
   * @param _amap
   */
  public DataPageInfo(Map _amap) throws IOException {
    data = _amap.getMapByteArray();
    offset = _amap.getOffset();
      
    availspace = Convert.getIntValue(offset, data);
    recct = Convert.getIntValue(offset+4, data);
    pageId = new PageId();
    pageId.pid = Convert.getIntValue(offset+8, data);
  }
  
  
  /** convert this class objcet to a tuple(like cast a DataPageInfo to Tuple)
   *  
   *
   */
  public Map convertToMap() throws IOException {
    // 1) write availspace, recct, pageId into data []
    Convert.setIntValue(availspace, offset, data);
    Convert.setIntValue(recct, offset+4, data);
    Convert.setIntValue(pageId.pid, offset+8, data);


    // 2) creat a map object using this array
    Map amap = new Map(data, offset);
 
    // 3) return map object
    return amap;
  }
  
    
  /** write this object's useful fields(availspace, recct, pageId) 
   *  to the data[](may be in buffer pool)
   *  
   */
  public void flushToByteArray() throws IOException {
     // write availspace, recct, pageId into "data[]"
    Convert.setIntValue(availspace, offset, data);
    Convert.setIntValue(recct, offset+4, data);
    Convert.setIntValue(pageId.pid, offset+8, data);

    // here we assume data[] already points to buffer pool
  }
  
}






