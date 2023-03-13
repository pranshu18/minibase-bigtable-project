package BigT;

import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.Objects;

public class Stream implements GlobalConst {

    private bigt bigtable;
    private int orderType;
    private String rowFilter, columnFilter, valueFilter;

    private PageId dirpageId = new PageId();
    private HFPage dirpage = new HFPage();
    private MID datapageMid = new MID();
    private PageId datapageId = new PageId();
    private HFPage datapage = new HFPage();
    private MID userMid = new MID();
    private boolean nextUserStatus;

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws IOException, InvalidTupleSizeException, HFBufMgrException {
        this.bigtable = bigtable;
        this.orderType = orderType;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        firstDataPage();
    }

    public Map getNext(MID mid) throws InvalidTupleSizeException, IOException {
        Map recmap = null;

        if (!nextUserStatus) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        mid.pageNo.pid = userMid.pageNo.pid;
        mid.slotNo = userMid.slotNo;

        try {
            recmap = datapage.getRecord(mid);
        }

        catch (Exception e) {
            e.printStackTrace();
        }

        assert recmap != null;

        userMid = datapage.nextRecord(mid);
        nextUserStatus = userMid != null;

        if((this.rowFilter != null && !Objects.equals(recmap.getRowLabel(), this.rowFilter)) ||
                (this.columnFilter != null && !Objects.equals(recmap.getColumnLabel(), this.columnFilter)) ||
                (this.valueFilter != null && !Objects.equals(recmap.getValue(), this.valueFilter)))
            return getNext(mid);

        return recmap;
    }

    private boolean firstDataPage() throws InvalidTupleSizeException, IOException, HFBufMgrException {
        DataPageInfo dpinfo;
        Map recordMap = null;
        Boolean bst;

        dirpageId.pid = bigtable.heapfile()._firstDirPageId.pid;
        nextUserStatus = true;

        try {
            dirpage = new HFPage();
            pinPage(dirpageId, dirpage, false);
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        }

        datapageMid = dirpage.firstRecord();

        if(datapageMid != null) {
            try {
                recordMap = dirpage.getRecord(datapageMid);
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            }

            assert recordMap != null;
            dpinfo = new DataPageInfo(recordMap);
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
                    datapageMid = dirpage.firstRecord();
                } catch (Exception e) {
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if (datapageMid != null) {
                    try {
                        recordMap = dirpage.getRecord(datapageMid);
                    } catch (InvalidSlotNumberException e) {
                        e.printStackTrace();
                    }

                    assert recordMap != null;
                    dpinfo = new DataPageInfo(recordMap);
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
        Map recordmap = null;

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
                    userMid = datapage.firstRecord();
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

        datapageMid = dirpage.nextRecord(datapageMid);

        if (datapageMid == null) {

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
                    datapageMid = dirpage.firstRecord();
                }
                catch (Exception e){
                    return false;
                }
            }
        }
        try {
            recordmap = dirpage.getRecord(datapageMid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        assert recordmap != null;

        dpinfo = new DataPageInfo(recordmap);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        userMid = datapage.firstRecord();

        if(userMid == null)
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
        if (datapage != null) {
            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {

            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        dirpage = null;
        nextUserStatus = true;
    }
}
