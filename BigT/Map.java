package BigT;

import global.AttrType;
import global.Convert;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

import static global.GlobalConst.MINIBASE_PAGESIZE;

public class Map {

    private byte [] data;

    private short fldCnt = 4;

    private short [] fldOffset;

    private int data_offset;

    public static final int max_size = MINIBASE_PAGESIZE;

    public Map(){
        data = new byte[max_size];
        data_offset = 0;
    }

    public Map(byte [] atuple, int offset)
    {
        data = atuple;
        data_offset = offset;
    }

    public Map(Map fromMap)
    {
        data = fromMap.getMapByteArray();
        data_offset = 0;
        fldOffset = fromMap.copyFldOffset();
    }

    public String getRowLabel() throws IOException {
        int fldNo = 1;
        String val = Convert.getStrValue(fldOffset[fldNo -1], data,
                fldOffset[fldNo] - fldOffset[fldNo -1]);
        return val;
    }

    public String getColumnLabel() throws IOException {
        int fldNo = 2;
        String val = Convert.getStrValue(fldOffset[fldNo -1], data,
                fldOffset[fldNo] - fldOffset[fldNo -1]);
        return val;
    }

    public int getTimeStamp() throws IOException {
        int fldNo = 3;
        int val = Convert.getIntValue(fldOffset[fldNo -1], data);
        return val;
    }

    public String getValue() throws IOException {
        int fldNo = 4;
        String val = Convert.getStrValue(fldOffset[fldNo -1], data,
                fldOffset[fldNo] - fldOffset[fldNo -1]);
        return val;
    }

    public Map setRowLabel(String rowLabel) throws IOException {
        int fldNo = 1;
        Convert.setStrValue (rowLabel, fldOffset[fldNo -1], data);
        return this;
    }

    public Map setColumnLabel(String columnLabel) throws IOException {
        int fldNo = 2;
        Convert.setStrValue (columnLabel, fldOffset[fldNo -1], data);
        return this;
    }

    public Map setTimeStamp(int timeStamp) throws IOException {
        int fldNo = 3;
        Convert.setIntValue (timeStamp, fldOffset[fldNo -1], data);
        return this;
    }

    public Map setValue(String value) throws IOException {
        int fldNo = 4;
        Convert.setStrValue (value, fldOffset[fldNo -1], data);
        return this;
    }

    public byte[] getMapByteArray(){
        byte [] array_copy = new byte [data.length];
        System.arraycopy(data, 0, array_copy, 0, data.length);
        return array_copy;
    }

    public void print() throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        AttrType type[] = new AttrType[]{new AttrType(AttrType.attrString), new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};

        for (i=0; i< fldCnt-1; i++)
        {
            switch(type[i].attrType) {

                case AttrType.attrInteger:
                    val = Convert.getIntValue(fldOffset[i], data);
                    System.out.print(val);
                    break;

                case AttrType.attrString:
                    sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                    System.out.print(sval);
                    break;
            }
            System.out.print(", ");
        }

        switch(type[fldCnt-1].attrType) {

            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");
    }

    public short size(){
        return ((short) (fldOffset[fldCnt] - data_offset));
    }

    public void mapCopy(Map fromMap){
        byte [] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, data_offset, temparray.length);
    }

    public void mapInit(byte[] amap, int offset){
        data = amap;
        data_offset = offset;
    }

    public void mapSet(byte[] frommap, int offset){
        System.arraycopy(frommap, offset, data, 0, frommap.length - offset);
        data_offset = 0;
    }

    public void setHdr (short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        short numFlds = 4;

        AttrType types[] = new AttrType[]{new AttrType(AttrType.attrString), new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};

        if((numFlds +2)*2 > max_size)
            throw new InvalidTupleSizeException (null, "TUPLE: TUPLE_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, data_offset, data);
        fldOffset = new short[numFlds+1];
        int pos = data_offset+2;

        fldOffset[0] = (short) ((numFlds +2) * 2 + data_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos +=2;
        short strCount =0;
        short incr;
        int i;

        for (i=1; i<numFlds; i++)
        {
            switch(types[i-1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
            }
            fldOffset[i]  = (short) (fldOffset[i-1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos +=2;

        }
        switch(types[numFlds -1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrString:
                incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        int map_length = fldOffset[numFlds] - data_offset;

        if(map_length > max_size)
            throw new InvalidTupleSizeException (null, "TUPLE: TUPLE_TOOBIG_ERROR");
    }

    public short[] copyFldOffset()
    {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i=0; i<=fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

}

