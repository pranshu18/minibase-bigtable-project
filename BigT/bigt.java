package BigT;

import global.AttrType;
import global.MID;
import global.MapMidPair;
import heap.*;
import index.IndexException;
import index.InvalidSelectionException;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.util.HashSet;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFileEntryException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.PageNotReadException;

public class bigt {

    public int type;

    public String name;

    private Heapfile heapfile;

    AttrType[] attrType;
    short[] attrSize;
    FileScan fscan;

    public BTreeFile _bf0 = null;
    public BTreeFile _bf1 = null;
    public BTreeFile _bftemp = null;

    public bigt(java.lang.String name, int type)
            throws InvalidTypeException, HFDiskMgrException, HFException, HFBufMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException {

        this.type = type;

        if (type < 1 || type > 5)
            throw new InvalidTypeException(null, "TUPLE: TUPLE_TYPE_ERROR");

        this.name = name;

        this.heapfile = new Heapfile(name);

        switch (type) {
            case 1: {
                break;
            }

            case 2: {
                this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
                break;
            }

            case 3: {
                this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
                break;
            }

            case 4: {
                this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
                this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
                break;
            }
            case 5: {
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

    public int getType() {
        return this.type;
    }

    public Heapfile heapfile(){
        return this.heapfile;
    }

    // Requires the modified Heapfile class
    public void deleteBigt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException,
            HFBufMgrException, FileAlreadyDeletedException, IOException, IteratorException, UnpinPageException,
            FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException {

        heapfile.deleteFile();

        switch (type) {
            case 1: {
                break;
            }

            case 2: {
                _bf0.destroyFile();
                break;

            }

            case 3: {
                _bf0.destroyFile();
                break;

            }
            case 4: {
                _bf0.destroyFile();
                _bf1.destroyFile();
                break;
            }
            case 5: {
                _bf0.destroyFile();
                _bf1.destroyFile();
                break;
            }
        }

    }

    public void insertIndex() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException, FieldNumberOutOfBoundException,
            FreePageException, DeleteFileEntryException, GetFileEntryException, AddFileEntryException,
            InvalidTupleSizeException {
        Scan scan = new Scan(heapfile);
        MID mid = new MID();
        String key = null;
        int key_timeStamp = 0;
        Map temp = null;

        if (type == 1) {
            return;
        }
        if (!(type == 4 || type == 5)) {
            this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);

        } else {
            this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
            this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
        }

        temp = scan.getNext(mid);
        while (temp != null) {
            switch (type) {

                case 2: {
                    key = temp.getRowLabel();
                    _bf0.insert(new StringKey(key), mid);
                    break;
                }

                case 3: {
                    key = temp.getColumnLabel();
                    _bf0.insert(new StringKey(key), mid);
                    break;
                }

                case 4: {
                    key = temp.getColumnLabel() + temp.getRowLabel();
                    key_timeStamp = temp.getTimeStamp();
                    _bf0.insert(new StringKey(key), mid);
                    _bf1.insert(new IntegerKey(key_timeStamp), mid);
                    break;
                }

                case 5: {
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

    // Requires the modified Heapfile class
    public int getMapCnt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException,
            HFBufMgrException, IOException {

        return heapfile.getMapCnt();

    }

    // Requires the Stream class
    public int getRowCnt() throws Exception {

        Stream stream = this.openStream(1, "", "", "");
        Map map = stream.getNext();

        // stores distinct row labels
        HashSet noDupSet = new HashSet();

        while (map != null) {

            noDupSet.add(map.getRowLabel());
            map = stream.getNext();
        }

        return noDupSet.size();
    }

    // Requires the Stream class
    public int getColumnCnt() throws Exception {

        Stream stream = this.openStream(1, "", "", "");
        Map map = stream.getNext();

        // stores distinct column labels
        HashSet noDupSet = new HashSet();

        while (map != null) {

            noDupSet.add(map.getColumnLabel());
            map = stream.getNext();
        }

        return noDupSet.size();

    }

    public MID insertMap(byte[] mapPtr) throws Exception {

        // Inserts map
        MID mid = heapfile.insertMap(mapPtr);

        // Checks for more than three maps with the same row and column label, and
        // deletes the map with the oldest timestamp
        Map map = heapfile.getMap(mid);
        map.setMid(mid);
        String rowLabel = map.getRowLabel();
        String colLabel = map.getColumnLabel();

        Stream stream = this.openStream(1, "", "", "");
        Map map_it = stream.getNext();
        int count = 0;
        int minTimestamp = Integer.MAX_VALUE;
        MID minTimestampMID = null;
        while (map_it != null) {

            if (mid != map_it.getMid() && map_it.getRowLabel().equalsIgnoreCase(rowLabel)
                    && map_it.getColumnLabel().equalsIgnoreCase(colLabel)) {
                count++;
                if (minTimestamp > map_it.getTimeStamp()) {
                    minTimestamp = map_it.getTimeStamp();
                    minTimestampMID = map_it.getMid();
                }
            }
            if (count == 3) {
                heapfile.deleteMap(minTimestampMID);
            }

            map_it = stream.getNext();
        }
        stream.closestream();

        return mid;
    }

    // Requires the Stream class
    public Stream openStream(int orderType, java.lang.String rowFilter,
            java.lang.String columnFilter, java.lang.String valueFilter)
            throws IOException, InvalidTupleSizeException, HFBufMgrException, IteratorException, ConstructPageException, InvalidRelation, IndexException, FileScanException, GetFileEntryException, PinPageException, MapUtilsException, UnknownIndexTypeException, InvalidSelectionException, UnpinPageException, SortException, UnknownKeyTypeException, KeyNotMatchException, TupleUtilsException, InvalidTypeException {

        // placeholder constructor
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);

    }

    public String getName() {
        return name;
    }
}
