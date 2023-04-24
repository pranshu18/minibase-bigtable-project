
package tests;

import BigT.Map;
import BigT.RowJoin;
import BigT.Stream;
import BigT.bigt;
import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.MID;
import global.MapMidPair;
import global.SystemDefs;
import heap.*;
import index.IndexScan;
import iterator.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class TestRun {

	static String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase-db";
	static String logpath;
	static String remove_logcmd;
	static String remove_dbcmd;

	static SystemDefs sysDef;

	public static void displayOptions(){
		System.out.println("OPTIONS-");
		System.out.println("1. Batch Insert");
		System.out.println("2. Run query");
		System.out.println("3. Insert Map");
		System.out.println("4. Get Counts");
		System.out.println("5. Create Index");
		System.out.println("6. Row join");
		System.out.println("7. Row sort");
		System.out.println("8. Exit");

	}

	public static int getChoice () {

		BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
		int choice = -1;

		try {
			choice = Integer.parseInt(in.readLine());
		}
		catch (NumberFormatException e) {
			return -1;
		}
		catch (IOException e) {
			return -1;
		}

		return choice;
	}

	public static String getLine(){
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		String name = null;
		try {
			name = reader.readLine();
		} catch (IOException e) {
			return "";
		}
		return name;
	}

	public static void insert() throws HFDiskMgrException, HFException, HFBufMgrException, IOException, InvalidTypeException, ConstructPageException, AddFileEntryException, GetFileEntryException, IteratorException, PinPageException, UnpinPageException, FreePageException, DeleteFileEntryException, PageNotFoundException, HashOperationException, BufMgrException, PagePinnedException, PageUnpinnedException, FileScanException, TupleUtilsException, InvalidRelation, KeyNotMatchException, ScanIteratorException, ScanDeleteException, InvalidInfoSizeException {

		System.out.println("Usage - batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
		PCounter.initialize();
		String[] values = getLine().split(" ");
		if(values.length !=5){
			System.out.println("Invalid format!");
			return;
		}
		String dataFileName = values[1];
		int type = Integer.parseInt(values[2]);
		String bigTableName = values[3];
		int numBuffs = Integer.parseInt(values[4]);

		boolean newFile=false;
		File f = new File(dbpath);
		if(f.exists()) { 
			newFile = true;
		}
		
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath) || SystemDefs.JavabaseDB.b==null ||  !SystemDefs.JavabaseDB.b.tableName.equals(bigTableName)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffs, "Clock", newFile);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffs, "Clock");
		}

		SystemDefs.JavabaseDB.b.cleanUpAllIndices();


		try {
			File file = new File(dataFileName);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			String[] tempArr;
			while((line = br.readLine()) != null) {
				line = line.replaceAll("[^\\x00-\\x7F]", "");
				tempArr = line.split(",");
				Map map = new Map();
				map.setHdr();
				map.setRowLabel(tempArr[0]);
				map.setColumnLabel(tempArr[1]);
				map.setTimeStamp(Integer.parseInt(tempArr[2]));

				map.setValue(tempArr[3]);

				MID mid = SystemDefs.JavabaseDB.b.insertMap(map.getMapByteArray(), type);
			}
			br.close();
			

			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();

			SystemDefs.JavabaseDB.b.populateBtree();
			SystemDefs.JavabaseDB.b.removeDuplicates();


			SystemDefs.JavabaseDB.b.insertIndex();

			

		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

	}

	public static void query() throws Exception {
		System.out.println("query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");

		String[] values = getLine().split(" ");
		if(values.length !=7){
			System.out.println("Invalid format!");
			return;
		}

		String bigTableName = values[1];
		int orderType = Integer.parseInt(values[2]);
		String rowFilter = values[3];
		String columnFilter = values[4];
		String valueFilter = values[5];

		int numBuffers = Integer.parseInt(values[6]);

		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath) || SystemDefs.JavabaseDB.b==null ||  !SystemDefs.JavabaseDB.b.tableName.equals(bigTableName)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffers + 100, "Clock", true);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers + 100, "Clock");
		}

		PCounter.initialize();

		Stream st = SystemDefs.JavabaseDB.b.openStream(orderType, rowFilter, columnFilter, valueFilter, numBuffers);
		Map mp  = st.getNext();
		while(mp != null){
			mp.print();
			System.out.println();
			mp = st.getNext();
		}
		st.closestream();

		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

	}
	
	public static void mapinsert() throws Exception{

		System.out.println("Usage - mapinsert RL CL VAL TS TYPE BIGTABLENAME NUMBUF");

		PCounter.initialize();

		String[] values = getLine().split(" ");

		if(values.length !=8){

		System.out.println("Invalid format!");

		return;

		}

		String rowLabel = values[1];

		String colLabel = values[2];

		String mapVal = values[3];

		int timestamp = Integer.parseInt(values[4]);

		int type = Integer.parseInt(values[5]);

		String bigTableName = values[6];

		int numbuf = Integer.parseInt(values[7]);

		
		boolean newFile=false;
		File f = new File(dbpath);
		if(f.exists()) { 
			newFile = true;
		}
		
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath) || SystemDefs.JavabaseDB.b==null ||  !SystemDefs.JavabaseDB.b.tableName.equals(bigTableName)) {
			sysDef = new SystemDefs(dbpath, 10000, numbuf, "Clock", newFile);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numbuf, "Clock");
		}


		Map map = new Map();
		map.setHdr();
		map.setRowLabel(rowLabel);
		map.setColumnLabel(colLabel);
		map.setTimeStamp(timestamp);

		map.setValue(mapVal);





		MID mid = SystemDefs.JavabaseDB.b.insertMap(map.getMapByteArray(),type);



		//Verify if these are actually required
		

		SystemDefs.JavabaseDB.b.populateBtree();

		SystemDefs.JavabaseDB.b.removeDuplicates();

		//SystemDefs.JavabaseDB.b.insertIndex();

		SystemDefs.JavabaseDB.b.insertIndexSingular(values,mid);
		

		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();
		
		System.out.println("Successfully inserted map \n");
		
		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

		}

	public static void getCounts() throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException, HFException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException, TupleUtilsException, InvalidRelation, InvalidTypeException, InvalidTupleSizeException, JoinsException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {
		System.out.println("getCounts BIGTABLENAME NUMBUF");
		PCounter.initialize();

		String[] values = getLine().split(" ");
		if(values.length !=3){
			System.out.println("Invalid format!");
			return;
		}

		String bigTableName = values[1];

		int numBuffers = Integer.parseInt(values[2]);

		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath) || SystemDefs.JavabaseDB.b==null ||  !SystemDefs.JavabaseDB.b.tableName.equals(bigTableName)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffers, "Clock", true);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers, "Clock");
		}

		SystemDefs.JavabaseDB.b.setCountValues();
		
		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();

		System.out.println("Count values are: ");
		System.out.println("Map Count = "+SystemDefs.JavabaseDB.b.mapCnt);
		System.out.println("Distinct Row Count = "+SystemDefs.JavabaseDB.b.distinctRowCnt);
		System.out.println("Distinct Col Count = "+SystemDefs.JavabaseDB.b.distinctColCnt);
		
		System.out.println("\n");

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

	}


	public static void createIndex() throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException, HFException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException, TupleUtilsException, InvalidRelation, InvalidTypeException, InvalidTupleSizeException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, JoinsException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {

		System.out.println("createindex BTNAME ORIGINAL_STORAGE_TYPE NEW_INDEX_TYPE");
		PCounter.initialize();

		String[] values = getLine().split(" ");
		if(values.length !=4){
			System.out.println("Invalid format!");
			return;
		}

		String bigTableName = values[1];
		int originalStorageType = Integer.parseInt(values[2]);
		int newIndexType = Integer.parseInt(values[3]);
		int numBuffers = 1000;

		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath) || SystemDefs.JavabaseDB.b==null ||  !SystemDefs.JavabaseDB.b.tableName.equals(bigTableName)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffers, "Clock", true);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers, "Clock");
		}

		String filename = "";

		switch (originalStorageType) {
		case IndexType.None: {
			filename = bigTableName;
			break;
		}

		case IndexType.ROW: {
			filename = bigTableName + "Index2";
			break;
		}

		case IndexType.COL: {
			filename = bigTableName + "Index3";
			break;
		}

		case IndexType.COLROW: {
			filename = bigTableName + "Index4";
			break;
		}
		case IndexType.ROWVAL: {
			filename = bigTableName + "Index5";
			break;
		}
		}

		boolean exists = true;
		BTreeFile indexFile = null;
		
		//check if index already exists, exit if yes.
		switch (newIndexType) {
		case IndexType.ROW: {
			indexFile = new BTreeFile(filename+"_Row", AttrType.attrString, 22, 1);
			if(indexFile.newFile) {
				exists = false;
				SystemDefs.JavabaseDB.b.indexFiles[originalStorageType-1].add(indexFile);
			}
			break;
		}

		case IndexType.COL: {
			indexFile = new BTreeFile(filename+"_Col", AttrType.attrString, 22, 1);
			if(indexFile.newFile) {
				exists = false;
				SystemDefs.JavabaseDB.b.indexFiles[originalStorageType-1].add(indexFile);
			}
			break;
		}

		case IndexType.COLROW: {
			indexFile = new BTreeFile(filename+"_ColRow", AttrType.attrString, 44, 1);
			if(indexFile.newFile) {
				exists = false;
				SystemDefs.JavabaseDB.b.indexFiles[originalStorageType-1].add(indexFile);
			}
			break;
		}
		case IndexType.ROWVAL: {
			indexFile = new BTreeFile(filename+"_RowVal", AttrType.attrString, 44, 1);
			if(indexFile.newFile) {
				exists = false;
				SystemDefs.JavabaseDB.b.indexFiles[originalStorageType-1].add(indexFile);
			}
			break;
		}
		}
		
		if(!exists) {
			//insert all records into the corresponding index file
			SystemDefs.JavabaseDB.b.insertIntoIndexFile(indexFile, originalStorageType, newIndexType);
			System.out.println("Successfully created new index file! \n");
		}else {
			System.out.println("Index file already exists \n");
		}
		
		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);
		
	}

	public static void rowJoin() throws Exception {
		System.out.println("rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER JOINTYPE NUMBUF");
		PCounter.initialize();

		String[] values = getLine().split(" ");
		if(values.length !=7){
			System.out.println("Invalid format!");
			return;
		}

		String bigTableName = values[1];
		String secondTableName = values[2];
		String outputTableName = values[3];
		String columnFilter = values[4];
		String joinType = values[5];
		int numBuffers = Integer.parseInt(values[6]);

		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffers * 10 + 100, "Clock", true);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers * 10 + 100 , "Clock");
		}
		bigt firstTable = new bigt(bigTableName);
		bigt secondTable = new bigt(secondTableName);
		
		Stream outputStream;
		RowJoin rowJoin;

		try {
			rowJoin = new RowJoin(numBuffers, firstTable, secondTable, columnFilter, outputTableName, joinType);

		} catch (Exception e){
			e.printStackTrace();
			return;

		}
		System.out.println();
		outputStream = rowJoin.getResult();
		Map temp;

		while ((temp = outputStream.getNext()) != null) {
			temp.print();
			System.out.println();
		}

		outputStream.closestream();

		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

	}

	public static void invokeRowSort() throws Exception {
		System.out.println("rowSort INBTNAME OUTBTNAME COLUMNNAME NUMBUF");
		PCounter.initialize();

		String[] values = getLine().split(" ");
		if(values.length !=5){
			System.out.println("Invalid format!");
			return;
		}
		String inputBT = values[1];
		String outputBT = values[2];
		String columnName = values[3];
		int numBuffers = Integer.parseInt(values[4]);

		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffers * 10 + 100, "Clock", true);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers * 10 + 100, "Clock");
		}

		bigt inputBT1 = new bigt(inputBT);
		Stream allSorted = inputBT1.openStream(6, "*", "*", "*", numBuffers/2);
		Stream filtered = inputBT1.openStream(3, "*", columnName, "*", numBuffers/2);

		Map mp1  = allSorted.getNext();
		Map mp2  = filtered.getNext();

		
		bigt resultTable = new bigt(outputBT);
		while(mp1 != null){
			if(mp2 != null && mp1.getRowLabel().compareTo(mp2.getRowLabel()) == 0 && mp1.getColumnLabel().compareTo(mp2.getColumnLabel()) == 0){
				while(mp2 != null && mp1.getRowLabel().compareTo(mp2.getRowLabel()) == 0 && mp1.getColumnLabel().compareTo(mp2.getColumnLabel()) == 0){
					resultTable.insertMap(mp2.getMapByteArray(), 1);
					mp2.print();
					System.out.println();
					mp1 = allSorted.getNext();
					mp2 = filtered.getNext();
				}
			} else {
				resultTable.insertMap(mp1.getMapByteArray(), 1);
				mp1.print();
				System.out.println();
				mp1 = allSorted.getNext();
			}
		}

		allSorted.closestream();
		filtered.closestream();

		resultTable.populateBtree();
		resultTable.removeDuplicates();

		SystemDefs.JavabaseBM.unpinAllPages();
		SystemDefs.JavabaseBM.flushAllPages();
		
		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);


	}

	public static void main(String[] args) {

		try {
			int choice = 1;
			while(choice!=8){

				displayOptions();
				choice = getChoice();

				switch (choice) {
				case 1:
					insert();
					break;
				case 2:
					query();
					break;
				case 3:
					mapinsert();
					break;
				case 4:
					getCounts();
					break;
				case 5:
					createIndex();
					break;
				case 6:
					rowJoin();
					break;
				case 7:
					invokeRowSort();
					break;
				case 8:
					break;
				default:
					System.out.println("Invalid choice!");
					break;
				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
