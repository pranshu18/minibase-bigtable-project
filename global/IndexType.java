package global;

/** 
 * Enumeration class for IndexType
 * 
 */

public class IndexType {

  public static final int None = 1;
  public static final int ROW = 2;
  public static final int COL = 3;
  public static final int COLROW = 4;
  public static final int ROWVAL = 5;
  public static final int Time_Index  = 6;



  public int indexType;

  /** 
   * IndexType Constructor
   * <br>
   * An index type can be defined as 
   * <ul>
   * <li>   IndexType indexType = new IndexType(IndexType.Hash);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (indexType.indexType == IndexType.Hash) ....
   * </ul>
   *
   * @param _indexType The possible types of index
   */

  public IndexType (int _indexType) {
    indexType = _indexType;
  }

    public String toString() {

    switch (indexType) {
    case None:
      return "No indexing";
    case ROW:
      return "row indexing";
    case COL:
      return "Column indexing";
    case COLROW:
      return "One indexing on Column and row and timestamp";
    case ROWVAL:
      return "Row,value and timestamp";

    }
    return ("Unexpected IndexType " + indexType);
  }
}
