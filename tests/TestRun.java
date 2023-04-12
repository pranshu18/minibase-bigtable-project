
package tests;

import BigT.Map;
import BigT.Stream;
import BigT.bigt;
import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;
import heap.*;
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

	//runs code from TestDriver as it is to setup db files
	public static void preprocess(){
		dbpath = "/tmp/batchInsert"+System.getProperty("user.name")+".minibase-db";
		logpath = "/tmp/batchInsert" +System.getProperty("user.name")+".minibase-log";

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		//This step seems redundant for me.  But it's in the original
		//C++ code.  So I am keeping it as of now, just in case I
		//I missed something
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}
	}

	//runs code from TestDriver for cleanup
	public static void cleanup(){
		//Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}

		System.out.println (".\n\n");

	}

	public static void displayOptions(){
		System.out.println("OPTIONS-");
		System.out.println("1. Batch Insert");
		System.out.println("2. Run query");
		System.out.println("3. Exit");
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
//			SystemDefs.JavabaseBM.flushAllPagesForcibly();
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

//		SystemDefs.JavabaseBM.flushAllPagesForcibly();
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
//			SystemDefs.JavabaseBM.flushAllPagesForcibly();
			SystemDefs.JavabaseBM = new BufMgr(numBuffers + 100, "Clock");
		}

		PCounter.initialize();

		System.out.println(SystemDefs.JavabaseDB.b.indexFiles[1].get(0).cnt);
		System.out.println((new BTreeFile(SystemDefs.JavabaseDB.b.indexFiles[1].get(0).getDbname())).cnt);

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

//		SystemDefs.JavabaseBM.flushAllPagesForcibly();

		System.out.println("Disk pages read = "+ PCounter.rcounter);
		System.out.println("Disk pages written = "+ PCounter.wcounter);

	}

	public static void main(String[] args) {

		try {
			int choice = 1;
			while(choice!=3){
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
