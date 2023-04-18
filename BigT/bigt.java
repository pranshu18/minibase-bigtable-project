package BigT;

import java.io.IOException;
import java.util.*;

import global.*;
import heap.*;
import index.IndexScan;
import iterator.*;
import btree.*;
import bufmgr.PageNotReadException;

/**
 * 
 * Creation of a bigTable
 *
 */
public class bigt {

	public String name;
	public int type;
	public Heapfile heapfile;
	public BTreeFile _bf0 = null;
	public BTreeFile _bf1 = null;
	public BTreeFile _bftemp = null;
	FileScan fscan;
	IndexScan iscan;
	CondExpr[] expr;
	AttrType[] attrType;
	short[] attrSize;
	String key;
	public HashMap<ArrayList<String>, ArrayList<MID>> rocolMID = new HashMap<ArrayList<String>, ArrayList<MID>>();
	
	/**
	 * 
	 * @param name - name of the bigT
	 * @param type - type of the Database
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws GetFileEntryException
	 * @throws ConstructPageException
	 * @throws AddFileEntryException
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 */
	public bigt(String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
	GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException,
	TupleUtilsException, InvalidRelation, InvalidTypeException {

		this.name = name;
		this.type = type;
		this.heapfile = new Heapfile(name);

		switch (type) {
		case IndexType.None: {
			break;
		}

		case IndexType.ROW: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
			break;
		}

		case IndexType.COL: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
			break;
		}

		case IndexType.COLROW: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
			break;
		}
		case IndexType.ROWVAL: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
			break;
		}
		}

		attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 22;
		attrSize[2] = 4;
		attrSize[3] = 22;

	}

	/**
	 * 
	 * Delete the bigt within the Database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws FileAlreadyDeletedException
	 * @throws InvalidInfoSizeException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws ConstructPageException
	 * @throws PinPageException
	 */
	void deleteBigT() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidInfoSizeException,
	HFBufMgrException, HFDiskMgrException, IOException, IteratorException, UnpinPageException,
	FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException {

		heapfile.deleteFile();

		switch (type) {
		case IndexType.None: {
			break;
		}

		case IndexType.ROW: {
			_bf0.destroyFile();
			break;

		}

		case IndexType.COL: {
			_bf0.destroyFile();
			break;

		}
		case IndexType.COLROW: {
			_bf0.destroyFile();
			_bf1.destroyFile();
			break;
		}
		case IndexType.ROWVAL: {
			_bf0.destroyFile();
			_bf1.destroyFile();
			break;
		}
		}
	}

	/**
	 * Get the count of total number of maps
	 * 
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidInfoSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws IOException
	 */
	public int getMapCnt() throws InvalidSlotNumberException, InvalidInfoSizeException, HFDiskMgrException,
	HFBufMgrException, IOException {
		return heapfile.getRecCnt();
	}

	/**
	 * Gets the total number of unique rows in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               row count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getRowCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		Stream stream = this.openStream(1, "", "", "*",numbuf);

		Map map = stream.getNext();

		// stores distinct row labels

		HashSet noDupSet = new HashSet();

		while (map != null) {

		//System.out.println(map.getRowLabel());

		noDupSet.add(map.getRowLabel());

		map = stream.getNext();

		}

		stream.closestream();

		return noDupSet.size();	}

	/**
	 * 
	 * Gets the total number of unique columns in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               column count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getColumnCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		Stream stream = this.openStream(1, "", "", "*",numbuf);

		Map map = stream.getNext();

		// stores distinct column labels

		HashSet noDupSet = new HashSet();

		while (map != null) {

		noDupSet.add(map.getColumnLabel());

		map = stream.getNext();

		}

		stream.closestream();

		return noDupSet.size();
	}

	/**
	 * Generate the index files for the maps corresponding to the desired type
	 * 
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FieldNumberOutOfBoundException
	 * @throws InvalidInfoSizeException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 */
	public void insertIndex() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException, FieldNumberOutOfBoundException, InvalidInfoSizeException,
	FreePageException, DeleteFileEntryException, GetFileEntryException, AddFileEntryException {
		Scan scan = new Scan(heapfile);
		MID mid = new MID();
		String key = null;
		int key_timeStamp = 0;
		Map temp = null;

		if(type==IndexType.None) {
			return;
		}
		if (!(type == IndexType.COLROW || type == IndexType.ROWVAL)) {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);

		} else {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
		}

		temp = scan.getNext(mid);
		while (temp != null) {
			switch (type) {

			case IndexType.ROW: {
				key = temp.getRowLabel();
				_bf0.insert(new StringKey(key), mid);
				break;
			}

			case IndexType.COL: {
				key = temp.getColumnLabel();
				_bf0.insert(new StringKey(key), mid);
				break;
			}

			case IndexType.COLROW: {
				key = temp.getColumnLabel() + temp.getRowLabel();
				key_timeStamp = temp.getTimeStamp();
				_bf0.insert(new StringKey(key), mid);
				_bf1.insert(new IntegerKey(key_timeStamp), mid);
				break;
			}

			case IndexType.ROWVAL: {
				key = temp.getRowLabel() + temp.getValue();
				key_timeStamp = temp.getTimeStamp();
				_bf0.insert(new StringKey(key), mid);
				_bf1.insert(new IntegerKey(key_timeStamp), mid);
				break;
			}

			}
			temp = scan.getNext(mid);
		}
		scan.closescan();
	}
	
	public void populateBtree() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException, FileScanException, TupleUtilsException, InvalidRelation,
	InvalidTypeException, JoinsException, InvalidTupleSizeException, PageNotReadException, PredEvalException,
	UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, GetFileEntryException, AddFileEntryException {

_bftemp = new BTreeFile(name + "Temp", AttrType.attrString, 64, 1);
fscan = new FileScan(name, attrType, attrSize, (short) 4, 4, null, null);
MapMidPair mpair = fscan.get_nextMidPair();
while (mpair != null) {
	String  s = String.format("%06d", mpair.map.getTimeStamp());
	String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel()+"%"+s;
	_bftemp.insert(new StringKey(key), mpair.mid);
//	_bftemp.insert(new StringKey(mpair.map.getRowLabel() + mpair.map.getColumnLabel()), mpair.mid);
	mpair = fscan.get_nextMidPair();
}

fscan.close();

}
	
	
	
	public void purge()
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);
		iscan = new IndexScan(new IndexType(IndexType.Row_Index), name, name + "Temp", attrType, attrSize, 4, 4, null,
				expr, 1, true);

		MapMidPair mpair = iscan.get_nextMidPair();
		String key = "";
		String oldKey = "";
		if (mpair != null) {
			oldKey = mpair.indKey.split("%")[0];
		}
		List<MID> list = new ArrayList<MID>();
		while (mpair != null) {

			key = mpair.indKey.split("%")[0];

			if (key.equals(oldKey)) {
				list.add(mpair.mid);
			} else {
				list.clear();
				oldKey = key;
				list.add(mpair.mid);
			}

			if (list.size() == 4) {
				MID delmid = list.get(0);

				_hf.deleteRecord(delmid);
				list.remove(0);
			}

			mpair = iscan.get_nextMidPair();
		}
		iscan.close();
		_bftemp.destroyFile();

	}
	

	/**
	 * Initialize a map in the database
	 * 
	 * @param mapPtr
	 * @return
	 * @throws InvalidTypeException
	 * @throws IOException
	 * @throws InvalidTupleSizeException 
	 */
	public Map constructMap(byte[] mapPtr) throws InvalidTypeException, IOException, InvalidTupleSizeException {
		Map map = new Map(mapPtr, 0);
		map.setHdr(null);
		return map;
	}

	/**
	 * Insert data given into bytes in the map
	 * 
	 * @param mapPtr - data to be inserted in bytes
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public MID insertMap(byte[] mapPtr) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFBufMgrException, HFDiskMgrException, Exception {

		MID mid = heapfile.insertRecord(mapPtr);

		//      // Checks for more than three maps with the same row and column label, and

		//      // deletes the oldest map

//		Map map = heapfile.getRecord(mid);
//
//		map.setMid(mid);
//
//		String rowLabel = map.getRowLabel();
//
//		String colLabel = map.getColumnLabel();
//
//		ArrayList<String> key = new ArrayList<String>();
//
//		key.add(rowLabel);
//
//		key.add(colLabel);
//
//		if(rocolMID.containsKey(key)){
//
//			ArrayList<MID> mid_arr = rocolMID.get(key);
//
//			mid_arr.add(mid);
//
//			rocolMID.remove(key);
//
//			rocolMID.put(key,mid_arr);
//
//		}
//
//		else if(!rocolMID.containsKey(key)){
//
//			ArrayList<MID> mid_arr2 = new ArrayList<MID>();
//
//			mid_arr2.add(mid);
//
//			rocolMID.put(key,mid_arr2);
//
//		}
//
//		if(rocolMID.get(key).size()>3){
//
//			MID mid_to_be_removed = rocolMID.get(key).get(0);
//
//			heapfile.deleteRecord(mid_to_be_removed);
//
//			rocolMID.get(key).remove(0);
//
//		}

		return mid;
	}


	/**
	 * Fucntion to display all the maps in the database
	 * 
	 * @throws InvalidInfoSizeException
	 * @throws IOException
	 */
	public void scanAllMaps() throws InvalidInfoSizeException, IOException {
		Scan scan = heapfile.openScan();
		MID mid = new MID();
		Map map = scan.getNext(mid);
		System.out.println("Maps are ");
		while (map != null) {
			map.print();
			System.out.println();
			map = scan.getNext(mid);
		}
	}

	/**
	 * Utility for purpose of testing
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public void generateMapsAndIndex() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFBufMgrException, HFDiskMgrException, Exception {
		String row[] = new String[] { "aa", "kkdab", "kk", "zz", "ee", "cc", "kk", "ff", "tt", "uu", "kkdaa", "kkdba",
				"cc", "kkdaa", "aa", "kk", "kk" };
		String col[] = new String[] { "e", "daaab", "a", "b", "d", "p", "a", "a", "e", "u", "daaa", "p", "s", "f", "b",
				"a", "b" };
		String val[] = new String[] { "america", "zzzzz", "ksks", "india", "china", "japan", "pakistan", "korea", "ggs",
				"australia", "kl", "zimbambwe", "bfgjh", "antarctica", "LKL", "pou", "Italy" };

		for (int i = 0; i < row.length; i++) {
			Map newmap = new Map();
			newmap.setHdr(null);
			newmap.setRowLabel(row[i]);
			newmap.setColumnLabel(col[i]);
			newmap.setTimeStamp(i);
			newmap.setValue(val[i]);
			insertMap(newmap.getMapByteArray());

		}
	}

	/**
	 * Opens the stream of maps
	 * 
	 * @param orderType - Desired order of Results
	 * @param rowFilter - Filtering condition on row
	 * @param colFilter - Filtering condition on column
	 * @param valFilter - Filtering condition on value
	 * @param numbuf    - number of buffers allocated
	 * @return
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public Stream openStream(int orderType, String rowFilter, String colFilter, String valFilter, int numbuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Stream stream = new Stream(this, orderType, rowFilter, colFilter, valFilter, numbuf);
		return stream;

	}

	public String getName() {
		return this.name;
	}

}