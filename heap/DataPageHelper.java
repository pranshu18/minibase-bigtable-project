//To store information in directory page

package heap;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.GlobalConst;

public class DataPageHelper implements GlobalConst {

	
	private short[] fldOffset;

	/**
	 * 
	 * Number of fields
	 * 
	 * This has 3 fields, they are page id of
	 * data page, availSpace, record count 
	 * 
	 */

	private final short fldCnt = 3;


	/**
	 * 
	 * Maximum size
	 * 
	 */

	public static final int max_size = MINIBASE_PAGESIZE;

	private int length_helper;

	/**
	 * 
	 * a byte array to hold data
	 * 
	 */

	private byte[] data;

	/**
	 * 
	 * start position in data[]
	 * 
	 */

	private int offset_helper;



	/**
	 * 
	 * Class constructor Create a new info with length = max_size,info offset = 0.
	 * 
	 */

	public DataPageHelper() {

		data = new byte[max_size];

		offset_helper = 0;

		length_helper = max_size;

	}

	/**
	 * 
	 * Constructor
	 *
	 * 
	 * 
	 * @param ahelper  a byte array which contains the info
	 * 
	 * @param offset the offset of the info in the byte array
	 * 
	 * @param length the length of the info
	 * 
	 */

	public DataPageHelper(byte[] ahelper, int offset, int length) {

		data = ahelper;

		offset_helper = offset;

		length_helper = length;

	}

	/**
	 * 
	 * Constructor(used as info copy)
	 *
	 * 
	 * 
	 * @param fromInfo a byte array which contains the info
	 *
	 * 
	 * 
	 */

	public DataPageHelper(DataPageHelper fromInfo) {

		data = fromInfo.getByteArray();

		length_helper = fromInfo.getLength();

		offset_helper = 0;

		fldOffset = fromInfo.copyFldOffset();

	}

	/**
	 * 
	 * Class constructor Create a new info with length = size,info offset = 0.
	 * 
	 */

	public DataPageHelper(int size) {

		data = new byte[size];

		offset_helper = 0;

		length_helper = size;

	}

	/**
	 * 
	 * setHdr will set the header of this info.
	 *
	 * 
	 * 
	 * @param numFlds    number of fields
	 * 
	 * @param types[]    contains the types that will be in this info
	 * 
	 * @param strSizes[] contains the sizes of the string
	 *
	 * 
	 * 
	 * @exception IOException              I/O errors
	 * 
	 * @exception InvalidTypeException     Invalid tupe type
	 * 
	 * @exception InvalidInfoSizeException Info size too big
	 *
	 * 
	 * 
	 */

	public void setHdr(short numFlds, AttrType types[]) // removed short strSizes[] attribute

			throws IOException, InvalidTypeException {

//fldCnt = numFlds; --> fixed as 3

		Convert.setShortValue(numFlds, offset_helper, data);

		fldOffset = new short[numFlds + 1];

		int pos = offset_helper + 2; // start position for fldOffset[]

// sizeof short =2 +2: array siaze = numFlds +1 (0 - numFilds) and

// another 1 for fldCnt

		fldOffset[0] = (short) ((numFlds + 2) * 2 + offset_helper);

		Convert.setShortValue(fldOffset[0], pos, data);

		pos += 2;

		short incr;

		int i;

		for (i = 1; i < numFlds; i++) {

			switch (types[i - 1].attrType) {

			case AttrType.attrInteger:

				incr = 4;

				break;

			default:

				throw new InvalidTypeException(null, "INFO: INFO_TYPE_ERROR");

			}

			fldOffset[i] = (short) (fldOffset[i - 1] + incr);

			Convert.setShortValue(fldOffset[i], pos, data);

			pos += 2;

		}

		switch (types[numFlds - 1].attrType) {

		case AttrType.attrInteger:

			incr = 4;

			break;

		default:

			throw new InvalidTypeException(null, "INFO: INFO_TYPE_ERROR");

		}

		fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);

		Convert.setShortValue(fldOffset[numFlds], pos, data);

		length_helper = fldOffset[numFlds] - offset_helper;

	}

	/**
	 * 
	 * Returns number of fields in this info
	 * 
	 * @return the number of fields in this info
	 *
	 * 
	 * 
	 */

	public short noOfFlds() {

		return fldCnt;

	}

	/**
	 * 
	 * Makes a copy of the fldOffset array
	 * 
	 * @return a copy of the fldOffset arrray
	 *
	 */

	public short[] copyFldOffset() {

		short[] newFldOffset = new short[fldCnt + 1];

		for (int i = 0; i <= fldCnt; i++) {

			newFldOffset[i] = fldOffset[i];

		}

		return newFldOffset;

	}

	/**
	 * 
	 * Print out the info
	 *
	 * 
	 * 
	 * @param type the types in the info
	 * 
	 * @Exception IOException I/O exception
	 * 
	 */

	public void print(AttrType type[]) throws IOException {

		int i, val;

		System.out.print("[");

		for (i = 0; i < fldCnt - 1; i++) {

			switch (type[i].attrType) {

			case AttrType.attrInteger:

				val = Convert.getIntValue(fldOffset[i], data);

				System.out.print(val);

				break;

			}

			System.out.print(", ");

		}

		switch (type[fldCnt - 1].attrType) {

		case AttrType.attrInteger:

			val = Convert.getIntValue(fldOffset[i], data);

			System.out.print(val);

			break;

		}

		System.out.println("]");

	}

	/**
	 * 
	 * Copy a info to the current info position you must make sure the info
	 * 
	 * lengths must be equal
	 * 
	 * @param fromInfo the info being copied
	 * 
	 */

	public void infoCopy(DataPageHelper fromInfo) {

		byte[] temparray = fromInfo.getByteArray();

		System.arraycopy(temparray, 0, data, offset_helper, length_helper);

	}

	/**
	 * 
	 * This is used when you don't want to use the constructor
	 *
	 * 
	 * 
	 * @param ahelper  a byte array which contains the info
	 * 
	 * @param offset the offset of the info in the byte array
	 * 
	 * @param length the length of the info
	 * 
	 */

	public void infoInit(byte[] ahelper, int offset, int length) {

		data = ahelper;

		offset_helper = offset;

		length_helper = length;

	}

	/**
	 * 
	 * Set a info with the given info length and offset
	 *
	 * 
	 * 
	 * @param record a byte array contains the info
	 * 
	 * @param offset the offset of the info ( =0 by default)
	 * 
	 * @param length the length of the info
	 * 
	 */

	public void infoSet(byte[] record, int offset, int length) {

		System.arraycopy(record, offset, data, 0, length);

		offset_helper = 0;

		length_helper = length;

	}

	/**
	 * 
	 * get the length of a info, call this method if you did not call setHdr ()
	 * 
	 * before
	 *
	 * 
	 * 
	 * @return length of this info in bytes
	 * 
	 */

	public int getLength() {

		return length_helper;

	}

	/**
	 * 
	 * get the length of a info, call this method if you did call setHdr () before
	 * 
	 * @return size of this info in bytes
	 * 
	 */

	public short size() {

		return ((short) (fldOffset[fldCnt] - offset_helper));

	}

	/**
	 * 
	 * get the offset of a info
	 *
	 * 
	 * 
	 * @return offset of the info in byte array
	 * 
	 */

	public int getOffset() {

		return offset_helper;

	}

	/**
	 * 
	 * Copy the byte array out
	 * 
	 * @return byte[], a byte array contains the info
	 * 
	 */

	public byte[] getByteArray() {

		byte[] infocopy = new byte[length_helper];

		System.arraycopy(data, offset_helper, infocopy, 0, length_helper);

		return infocopy;

	}

	/**
	 * 
	 * return the data byte array
	 *
	 * 
	 * 
	 * @return data byte array
	 * 
	 */

	public byte[] returnByteArray() {

		return data;

	}

	/**
	 * 
	 * Convert this field into integer
	 *
	 * 
	 * 
	 * @param fldNo the field number
	 * 
	 * @return the converted integer if success
	 *
	 * 
	 * 
	 * @exception IOException                    I/O errors
	 * 
	 * @exception FieldNumberOutOfBoundException Info field number out of bound
	 * 
	 */

	public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {

		int val;

		if ((fldNo > 0) && (fldNo <= fldCnt)) {

			val = Convert.getIntValue(fldOffset[fldNo - 1], data);

			return val;

		} else

			throw new FieldNumberOutOfBoundException(null, "INFO:INFO_FLDNO_OUT_OF_BOUND");

	}

	/**
	 * 
	 * Set this field to integer value
	 *
	 * 
	 * 
	 * @param fldNo the field number
	 * 
	 * @param val   the integer value
	 * 
	 * @exception IOException                    I/O errors
	 * 
	 * @exception FieldNumberOutOfBoundException Info field number out of bound
	 * 
	 */

	public DataPageHelper setIntFld(int fldNo, int val) throws IOException, FieldNumberOutOfBoundException {

		if ((fldNo > 0) && (fldNo <= fldCnt)) {

			Convert.setIntValue(val, fldOffset[fldNo - 1], data);

			return this;

		} else

			throw new FieldNumberOutOfBoundException(null, "INFO:INFO_FLDNO_OUT_OF_BOUND");

	}

}