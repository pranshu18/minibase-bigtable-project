
package tests;

import BigT.Map;
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

import bufmgr.BufMgr;


public class TestRun {

	static String dbpath;
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
		System.out.println("5. Exit");

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

		dbpath = "/tmp/" + bigTableName+ System.getProperty("user.name")
		+ ".minibase-db";
		
		boolean newFile=false;
		File f = new File(dbpath);
		if(f.exists()) { 
			newFile = true;
		}
		
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
			sysDef = new SystemDefs(dbpath, 10000, numBuffs, "Clock", newFile);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(1000, "Clock");
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

		dbpath = "/tmp/" + bigTableName+ System.getProperty("user.name")
		+ ".minibase-db";
		
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
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


		dbpath = "/tmp/" + bigTableName+ System.getProperty("user.name")
		+ ".minibase-db";

		
		boolean newFile=false;
		File f = new File(dbpath);
		if(f.exists()) { 
			newFile = true;
		}
		
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
			sysDef = new SystemDefs(dbpath, 10000, numbuf, "Clock", newFile);
			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(1000, "Clock");
		}

	
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
			sysDef = new SystemDefs(dbpath, 10000, numbuf + 100, "Clock", newFile );

			SystemDefs.JavabaseDB.b = new bigt(bigTableName);
		} else {
			SystemDefs.JavabaseBM.unpinAllPages();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseBM = new BufMgr(numbuf + 100, "Clock");
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

		dbpath = "/tmp/" + bigTableName+ System.getProperty("user.name")
		+ ".minibase-db";
		if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
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


	public static void main(String[] args) {

		try {
			int choice = 1;
			while(choice!=5){

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
