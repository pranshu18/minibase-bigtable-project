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

	public Heapfile[] heapfile = new Heapfile[5];
	public ArrayList<BTreeFile>[] indexFiles = new ArrayList[5];
	public BTreeFile _bftemp = null;

	public String tableName;
	FileScan fscan;
	IndexScan iscan;
	CondExpr[] expr;
	AttrType[] attrType;
	short[] attrSize;
	String key;
	public int distinctRowCnt = 0;
	public int distinctColCnt = 0;
	public int mapCnt = 0;

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
	public bigt(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
	GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException,
	TupleUtilsException, InvalidRelation, InvalidTypeException {

		this.tableName = name;
		for(int i=0; i<IndexType.indexList.length; i++) {
			int type = IndexType.indexList[i];
			this.indexFiles[i] = new ArrayList<BTreeFile>();
			String filename="";
			switch (type) {
			case IndexType.None: {
				filename = name;
				break;
			}

			case IndexType.ROW: {
				filename = name + "Index2";
				this.indexFiles[i].add(new BTreeFile(filename+"_Row", AttrType.attrString, 22, 1));
				break;
			}

			case IndexType.COL: {
				filename = name + "Index3";
				this.indexFiles[i].add(new BTreeFile(filename+"_Col", AttrType.attrString, 22, 1));
				break;
			}

			case IndexType.COLROW: {
				filename = name + "Index4";
				this.indexFiles[i].add(new BTreeFile(filename+"_ColRow", AttrType.attrString, 44, 1));
				break;
			}
			case IndexType.ROWVAL: {
				filename = name + "Index5";
				this.indexFiles[i].add(new BTreeFile(filename+"_RowVal", AttrType.attrString, 44, 1));
				break;
			}
			}
			this.heapfile[i] = new Heapfile(filename);
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
	 * @throws InvalidTupleSizeException 
	 */
	void deleteBigT() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidInfoSizeException,
	HFBufMgrException, HFDiskMgrException, IOException, IteratorException, UnpinPageException,
	FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException, InvalidTupleSizeException {

		for(int i=0; i<IndexType.indexList.length; i++) {
			heapfile[i].deleteFile();

			for(int j=0;j<this.indexFiles[i].size();j++) {
				this.indexFiles[i].get(j).destroyFile();
			}

		}

	}

	public void cleanUpAllIndices() throws PinPageException, KeyNotMatchException, IteratorException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException, IOException {

		for(int i=0; i<IndexType.indexList.length; i++) {
			for(int j=0;j<this.indexFiles[i].size();j++) {				
				BTFileScan scan = this.indexFiles[i].get(j).new_scan(null, null);
				boolean isScanComplete = false;
				while(!isScanComplete) {
					KeyDataEntry entry = scan.get_next();
					if(entry == null) {
						isScanComplete = true;
						break;
					}
					scan.delete_current();
				}

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
	 * @throws InvalidTupleSizeException 
	 */
	public int getMapCnt() throws InvalidSlotNumberException, InvalidInfoSizeException, HFDiskMgrException,
	HFBufMgrException, IOException, InvalidTupleSizeException {
		int total = 0;

		for(int i=0; i<IndexType.indexList.length; i++) {
			total += heapfile[i].getRecCnt();
		}
		return total;
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
	 * @throws Exception 
	 * @throws HFDiskMgrException 
	 * @throws HFBufMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public void insertIndex() throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {

		for(int i=0; i<IndexType.indexList.length; i++) {
			int type = IndexType.indexList[i];
			String filename="";
			switch (type) {
			case IndexType.ROW: {
				filename = tableName + "Index2";
				this.indexFiles[i].set(0, new BTreeFile(filename+"_Row", AttrType.attrString, 22, 1));
				break;
			}

			case IndexType.COL: {
				filename = tableName + "Index3";
				this.indexFiles[i].add(0, new BTreeFile(filename+"_Col", AttrType.attrString, 22, 1));
				break;
			}

			case IndexType.COLROW: {
				filename = tableName + "Index4";
				this.indexFiles[i].add(0, new BTreeFile(filename+"_ColRow", AttrType.attrString, 44, 1));
				break;
			}
			case IndexType.ROWVAL: {
				filename = tableName + "Index5";
				this.indexFiles[i].add(0, new BTreeFile(filename+"_RowVal", AttrType.attrString, 44, 1));
				break;
			}
			}
		}


		for(int i=0; i<IndexType.indexList.length; i++) {

			int type = IndexType.indexList[i];

			fscan = new FileScan(heapfile[i].getFileName(), attrType, attrSize, (short) 4, 4, null, null);
			MapMidPair mpair = fscan.get_nextMidPair();
			String key = null;

			while (mpair != null) {
				switch (type) {

				case IndexType.ROW: {
					key = mpair.map.getRowLabel();
					this.indexFiles[i].get(0).insert(new StringKey(key), mpair.mid);
					break;
				}

				case IndexType.COL: {
					key = mpair.map.getColumnLabel();
					this.indexFiles[i].get(0).insert(new StringKey(key), mpair.mid);
					break;
				}

				case IndexType.COLROW: {
					key = mpair.map.getColumnLabel() + mpair.map.getRowLabel();
					this.indexFiles[i].get(0).insert(new StringKey(key), mpair.mid);
					break;
				}

				case IndexType.ROWVAL: {
					key = mpair.map.getRowLabel() + mpair.map.getValue();
					this.indexFiles[i].get(0).insert(new StringKey(key), mpair.mid);
					break;
				}

				}

				mpair = fscan.get_nextMidPair();
			}

			fscan.close();


		}

	}


	
	
	public void insertIndexSingular(String[] values, MID mid) throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {


		String rowLabel = values[1];

		String colLabel = values[2];

		String mapVal = values[3];

		int timestamp = Integer.parseInt(values[4]);

		int type = Integer.parseInt(values[5]);

		String bigTableName = values[6];

		int numbuf = Integer.parseInt(values[7]);

		
		for(int i=0; i<IndexType.indexList.length; i++) {

			
			String key = null;

				switch (type) {

				case IndexType.ROW: {
					key = rowLabel;
					this.indexFiles[i].get(0).insert(new StringKey(key), mid);
				}

				case IndexType.COL: {
					key = colLabel;
					this.indexFiles[i].get(0).insert(new StringKey(key), mid);
				}

				case IndexType.COLROW: {
					key = colLabel + rowLabel;
					this.indexFiles[i].get(0).insert(new StringKey(key), mid);
				}

				case IndexType.ROWVAL: {
					key = rowLabel + mapVal;
					this.indexFiles[i].get(0).insert(new StringKey(key), mid);
				}

				}

	

		}

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
		map.setHdr();
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

	public MID insertMap(byte[] mapPtr, int storageType) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFBufMgrException, HFDiskMgrException, Exception {

		MID mid = heapfile[storageType-1].insertRecord(mapPtr);

		//      // Checks for more than three maps with the same row and column label, and

		//      // deletes the oldest map

		//		Map map = heapfile[storageType-1].getRecord(mid);
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
		//			heapfile[storageType-1].deleteRecord(mid_to_be_removed);
		//
		//			rocolMID.get(key).remove(0);
		//
		//		}
		//
		return mid;
	}


	public void populateBtree() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException, FileScanException, TupleUtilsException, InvalidRelation,
	InvalidTypeException, JoinsException, InvalidTupleSizeException, PageNotReadException, PredEvalException,
	UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, GetFileEntryException, AddFileEntryException {

		_bftemp = new BTreeFile(tableName + "Temp", AttrType.attrString, 64, 1);

		for(int i=0; i<IndexType.indexList.length; i++) {
			fscan = new FileScan(heapfile[i].getFileName(), attrType, attrSize, (short) 4, 4, null, null);
			MapMidPair mpair = fscan.get_nextMidPair();
			while (mpair != null) {
				String  s = String.format("%06d", mpair.map.getTimeStamp());
				String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel()+"%"+s;
				_bftemp.insert(new StringKey(key), mpair.mid);
				mpair = fscan.get_nextMidPair();
			}

			fscan.close();
		}

	}


	/**
	 * Remove Duplicates to handle the versioning in the database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void removeDuplicates()
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);
		iscan = new IndexScan(new IndexType(IndexType.ROW), tableName, tableName + "Temp", attrType, attrSize, 4, 4, null,
				expr, null, 1, true);

		MapMidPair mpair = iscan.getNextMIDPair();
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

				for(int i=0; i<IndexType.indexList.length; i++) {
					if(heapfile[i].getRecord(delmid)!=null) {
						heapfile[i].deleteRecord(delmid);
						break;
					}
				}
				list.remove(0);
			}

			mpair = iscan.getNextMIDPair();
		}
		iscan.close();
		_bftemp.destroyFile();

	}

	/**
	 * Fucntion to display all the maps in the database
	 * 
	 * @throws InvalidInfoSizeException
	 * @throws IOException
	 * @throws InvalidTupleSizeException 
	 */
	public void scanAllMaps() throws InvalidInfoSizeException, IOException, InvalidTupleSizeException {
		for(int i=0; i<IndexType.indexList.length; i++) {
			Scan scan = heapfile[i].openScan();
			MID mid = new MID();
			Map map = scan.getNext(mid);
			System.out.println("Maps are ");
			while (map != null) {
				map.print();
				System.out.println();
				map = scan.getNext(mid);
			}

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

	
	public void setCountValues() throws FileScanException, TupleUtilsException, InvalidRelation, InvalidTypeException, InvalidTupleSizeException, IOException, JoinsException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {		
		if(mapCnt>0)
			return;
		
		HashSet<String> uniqueRowNames = new HashSet<>();
		HashSet<String> uniqueColNames = new HashSet<>();

		
		for(int i=0; i<IndexType.indexList.length; i++) {

			FileScan fs = new FileScan(heapfile[i].getFileName(), attrType, attrSize, (short) 4, 4, null, null);
			MapMidPair mpair = fs.get_nextMidPair();

			while (mpair != null) {
				mapCnt++;
				String row = mpair.map.getRowLabel();
				String col = mpair.map.getColumnLabel();
				
				if(!uniqueRowNames.contains(row)) {
					uniqueRowNames.add(row);
					distinctRowCnt++;
				}
				if(!uniqueColNames.contains(col)) {
					uniqueColNames.add(col);
					distinctColCnt++;
				}
				
				mpair = fs.get_nextMidPair();
			}
			
			fs.close();
		}
	}


}