package global;

/** 
 * Enumeration class for IndexType
 * 
 */

public class IndexType {

  public static final int None = 0;
  public static final int ROW = 1;
  public static final int COL  = 2;
  public static final int ROW_COL = 3;
  public static final int ROW_VAL  = 4;

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
          return "no index";
        case ROW:
          return "row index";
        case COL:
          return "col index";
        case ROW_COL:
          return "row_col index";
        case ROW_VAL:
          return "row_val index";
      }
      return ("Unexpected IndexType " + indexType);
  }
}
