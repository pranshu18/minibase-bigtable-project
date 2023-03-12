package BigT;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class Stream implements GlobalConst {

    private bigt bigtable;
    private int orderType;
    private String rowFilter, columnFilter, valueFilter;

    private PageId dirpageId = new PageId();
    private HFPage dirpage = new HFPage();
    private RID datapageRid = new RID();
    private PageId datapageId = new PageId();
    private HFPage datapage = new HFPage();
    private RID userrid = new RID();
    private boolean nextUserStatus;

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws IOException, InvalidTupleSizeException, HFBufMgrException {
        this.bigtable = bigtable;
        this.orderType = orderType;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        firstDataPage();

    }

    private boolean firstDataPage() throws InvalidTupleSizeException, IOException, HFBufMgrException {
        DataPageInfo dpinfo;
        Tuple recordTuple = null;
        Boolean bst;

        dirpageId.pid = bigtable.heapfile()._firstDirPageId.pid;
        nextUserStatus = true;

        try {
            dirpage = new HFPage();
            pinPage(dirpageId, dirpage, false);
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        }

        datapageRid = dirpage.firstRecord();

        if(datapageRid != null) {
            try {
                recordTuple = dirpage.getRecord(datapageRid);
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            }

            assert recordTuple != null;
            dpinfo = new DataPageInfo(recordTuple);
            datapageId.pid = dpinfo.pageId.pid;
        } else {
            PageId nextDirPageId = dirpage.getNextPage();

            if (nextDirPageId.pid != INVALID_PAGE) {
                unpinPage(dirpageId, false);
                dirpage = null;

                try {
                    dirpage = new HFPage();
                    pinPage(nextDirPageId, dirpage, false);

                } catch (HFBufMgrException e) {
                    e.printStackTrace();
                }

                try {
                    datapageRid = dirpage.firstRecord();
                } catch (Exception e) {
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if (datapageRid != null) {
                    try {
                        recordTuple = dirpage.getRecord(datapageRid);
                    } catch (InvalidSlotNumberException e) {
                        e.printStackTrace();
                    }

                    if (recordTuple.getLength() != DataPageInfo.size)
                        return false;

                    dpinfo = new DataPageInfo(recordTuple);
                    datapageId.pid = dpinfo.pageId.pid;
                } else {
                    datapageId.pid = INVALID_PAGE;
                }
            } else {
                datapageId.pid = INVALID_PAGE;
            }

        }
        datapage = null;
        try{
            nextDataPage();
        }

	    catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }
    private boolean nextDataPage() throws InvalidTupleSizeException, IOException {
        DataPageInfo dpinfo;

        PageId nextDirPageId;
        Tuple rectuple = null;

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            } else {

                try {
                    datapage  = new HFPage();
                    pinPage(datapageId, datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    userrid = datapage.firstRecord();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }


        if (dirpage == null) {
            return false;
        }

        datapageRid = dirpage.nextRecord(datapageRid);

        if (datapageRid == null) {

            nextDirPageId = dirpage.getNextPage();

            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {
                dirpageId = nextDirPageId;

                try {
                    dirpage  = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageRid = dirpage.firstRecord();
                }
                catch (Exception e){
                    return false;
                }
            }
        }
        try {
            rectuple = dirpage.getRecord(datapageRid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        assert rectuple != null;
        if (rectuple.getLength() != DataPageInfo.size)
            return false;

        dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        userrid = datapage.firstRecord();

        if(userrid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }

    private void pinPage(PageId pageId, Page page, boolean emptyPage) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
        }

    }


    public void closestream() {

    }
}
