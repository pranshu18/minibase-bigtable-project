package iterator;

import BigT.Map;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;

import java.io.IOException;

public class MapUtils {

    /**
     * This function compares a map with another map in respective field, and
     *  returns:
     *
     *    0        if the two are equal,
     *    1        if the map is greater,
     *   -1        if the map is smaller,
     *
     *@param    m1        one map.
     *@param    m2        another map.
     *@param    map_fld_no the field numbers in the maps to be compared.
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     *@return   0        if the two are equal,
     *          1        if the map is greater,
     *         -1        if the map is smaller,
     */
    public static int CompareMapWithMap(Map m1, Map m2, int map_fld_no)
            throws IOException, MapUtilsException {
        int t1_i, t2_i;
        String t1_s=null, t2_s=null;

        try {
            switch(map_fld_no){
                case 1:
                    t1_s = m1.getRowLabel();
                    t2_s = m2.getRowLabel();
                    break;
                case 2:
                    t1_s = m1.getColumnLabel();
                    t2_s = m2.getColumnLabel();
                    break;
                case 3:
                    t1_i = m1.getTimeStamp();
                    t2_i = m2.getTimeStamp();
                    if (t1_i == t2_i) return  0;
                    if (t1_i <  t2_i) return -1;
                    if (t1_i >  t2_i) return  1;
                    break;
                case 4:
                    t1_s = m1.getValue();
                    t2_s = m2.getValue();
                    break;
                default:
                    throw new FieldNumberOutOfBoundException();
            }
        }catch (FieldNumberOutOfBoundException e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
        }

        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;

    }

    public static int CompareForSorting(Map m1, Map m2, int sortType)
            throws UnknowAttrType {
        switch (sortType) {
            case 1:
                try {
                    int row_cmp = m1.getRowLabel().compareTo(m2.getRowLabel());
                    if (row_cmp != 0) {
                        return row_cmp;
                    }

                    int col_cmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
                    if (col_cmp != 0) {
                        return col_cmp;
                    }

                    if (m1.getTimeStamp() == m2.getTimeStamp())
                        return 0;
                    if (m1.getTimeStamp() < m2.getTimeStamp())
                        return -1;
                    if (m1.getTimeStamp() > m2.getTimeStamp())
                        return 1;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            case 2:
                try {
                    int col_cmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
                    if (col_cmp != 0) {
                        return col_cmp;
                    }

                    int row_cmp = m1.getRowLabel().compareTo(m2.getRowLabel());
                    if (row_cmp != 0) {
                        return row_cmp;
                    }

                    if (m1.getTimeStamp() == m2.getTimeStamp())
                        return 0;
                    if (m1.getTimeStamp() < m2.getTimeStamp())
                        return -1;
                    if (m1.getTimeStamp() > m2.getTimeStamp())
                        return 1;

                } catch (Exception e) {
                    e.printStackTrace();
                }

            case 3:
                try {

                    int row_cmp = m1.getRowLabel().compareTo(m2.getRowLabel());
                    if (row_cmp != 0) {
                        return row_cmp;
                    }

                    if (m1.getTimeStamp() == m2.getTimeStamp())
                        return 0;
                    if (m1.getTimeStamp() < m2.getTimeStamp())
                        return -1;
                    if (m1.getTimeStamp() > m2.getTimeStamp())
                        return 1;

                } catch (Exception e) {
                    e.printStackTrace();
                }

            case 4:
                try {

                    int col_cmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
                    if (col_cmp != 0) {
                        return col_cmp;
                    }

                    if (m1.getTimeStamp() == m2.getTimeStamp())
                        return 0;
                    if (m1.getTimeStamp() < m2.getTimeStamp())
                        return -1;
                    if (m1.getTimeStamp() > m2.getTimeStamp())
                        return 1;

                } catch (Exception e) {
                    e.printStackTrace();
                }

            case 5:
                try {
                    if (m1.getTimeStamp() == m2.getTimeStamp())
                        return 0;
                    if (m1.getTimeStamp() < m2.getTimeStamp())
                        return -1;
                    if (m1.getTimeStamp() > m2.getTimeStamp())
                        return 1;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
        }
    }

    /**
     *This function compares two maps in all fields
     * @param m1 the first map
     * @param m2 the secocnd map
     * @return  0        if the two are not equal,
     *          1        if the two are equal,
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */

    public static boolean Equal(Map m1, Map m2)
            throws IOException,MapUtilsException
    {
        int i;

        for (i = 1; i <= 4; i++)
            if (CompareMapWithMap(m1, m2, i) != 0)
                return false;
        return true;
    }

    /**
     *get the string value specified by the field number
     *@param map the map
     *@param fldno the field number
     *@return the content of the field number
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static String Value(Map  map, int fldno)
            throws IOException, MapUtilsException {
        String temp;
        try{
            switch(fldno){
                case 1:
                    temp = map.getRowLabel();
                    break;
                case 2:
                    temp = map.getColumnLabel();
                    break;
                case 4:
                    temp = map.getValue();
                    break;
                default:
                    throw new FieldNumberOutOfBoundException();
            }
        }catch (FieldNumberOutOfBoundException e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
        }
        return temp;
    }


    /**
     *set up a map in specified field from a map
     *@param value the tuple to be set
     *@param map the given map
     *@param fld_no the field number
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static void SetValue(Map value, Map  map)
            throws IOException {
        value.setRowLabel(map.getRowLabel());
        value.setColumnLabel(map.getColumnLabel());
        value.setTimeStamp(map.getTimeStamp());
        value.setValue(map.getValue());

    }

    /**
     *set up the Jmap's string size for using project
     *@param Jmap  reference to an actual tuple  - no memory has been malloced
     *@param t1_str_sizes shows the length of the string fields in S
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */

    public static short[] setup_op_map(Map Jmap, short t1_str_sizes[])
            throws MapUtilsException {
        try {
            Jmap.setHdr(t1_str_sizes);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return t1_str_sizes;
    }

}
