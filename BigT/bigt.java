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
    public int getRowCnt() throws IOException {

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
    public int getColumnCnt() throws IOException {

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

    public MID insertMap(byte[] mapPtr){


        return heapfile.insertMap(mapPtr);

    }

    //Requires the Stream class
     Stream openStream(int orderType, java.lang.String rowFilter,
java.lang.String columnFilter, java.lang.String valueFilter) throws IOException, InvalidTupleSizeException, HFBufMgrException {

        //placeholder constructor
        return new Stream(this,orderType,rowFilter,columnFilter,valueFilter);

     }


}
