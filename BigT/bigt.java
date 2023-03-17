package BigT;

import global.MID;
import heap.*;

import java.io.IOException;
import java.util.HashSet;


public class bigt {


    private int type;

    private String name;

    private Heapfile heapfile;

    public bigt(java.lang.String name, int type) throws InvalidTypeException, HFDiskMgrException, HFException, HFBufMgrException, IOException {

    this.type = type;

    if(type < 1 || type > 5)
        throw new InvalidTypeException(null, "TUPLE: TUPLE_TYPE_ERROR");


    this.name = name;

    this.heapfile = new Heapfile(name);

    }

    public Heapfile heapfile(){
        return this.heapfile;
    }
    //Requires the modified Heapfile class
    public void deleteBigt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, FileAlreadyDeletedException, IOException {

        heapfile.deleteFile();

    }

    //Requires the modified Heapfile class
    public int getMapCnt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {

        return heapfile.getMapCnt();

    }

    //Requires the Stream class
    public int getRowCnt() throws IOException, InvalidTupleSizeException, HFBufMgrException {

        Stream stream = this.openStream(1,"","","");
        MID mid = null;
        Map map = stream.getNext(mid);

        //stores distinct row labels
        HashSet noDupSet = new HashSet();

        while(map!=null){


            noDupSet.add(map.getRowLabel());
            map = stream.getNext(mid);
        }

        return noDupSet.size();
    }


    //Requires the Stream class
    public int getColumnCnt() throws IOException, InvalidTupleSizeException, HFBufMgrException {

        Stream stream = this.openStream(1,"","","");
        MID mid = null;
        Map map = stream.getNext(mid);

        //stores distinct column labels
        HashSet noDupSet = new HashSet();

        while(map!=null){

            noDupSet.add(map.getColumnLabel());
            map = stream.getNext(mid);
        }

        return noDupSet.size();


    }

    public MID insertMap(byte[] mapPtr) throws Exception {


        //Inserts map
        MID mid = heapfile.insertMap(mapPtr);


        //Checks for more than three maps with the same row and column label, and deletes the map with the oldest timestamp
        Map map = heapfile.getMap(mid);
        map.setMid(mid);
        String rowLabel = map.getRowLabel();
        String colLabel = map.getColumnLabel();

        Stream stream = this.openStream(1,"","","");
        MID stream_mid = null;
        Map map_it = stream.getNext(stream_mid);
        int count = 0;
        int minTimestamp = Integer.MAX_VALUE;
        MID minTimestampMID = null;
        while(map_it!=null){

            if(mid!=map_it.getMid() && map_it.getRowLabel().equalsIgnoreCase(rowLabel) && map_it.getColumnLabel().equalsIgnoreCase(colLabel)){
                count++;
                if(minTimestamp>map_it.getTimeStamp()){
                    minTimestamp = map_it.getTimeStamp();
                    minTimestampMID = map_it.getMid();
                }
            }
            if(count==3){
                heapfile.deleteMap(minTimestampMID);
            }

            map_it = stream.getNext(stream_mid);
        }

        return mid;
    }

    //Requires the Stream class
     Stream openStream(int orderType, java.lang.String rowFilter,
java.lang.String columnFilter, java.lang.String valueFilter) throws IOException, InvalidTupleSizeException, HFBufMgrException {

        //placeholder constructor
        return new Stream(this,orderType,rowFilter,columnFilter,valueFilter);

     }


}
