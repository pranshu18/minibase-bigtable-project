package iterator;


import BigT.Map;
import BigT.bigt;
import heap.*;
import index.IndexScan;
import global.*;
import bufmgr.*;


import java.io.*;

public class CustomScan extends Iterator {
    private Map map1;
    private Iterator[] iterators;


    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public CustomScan(bigt bigtable, AttrType[] attributes, short[] attributeSizes,
                           CondExpr[] outFilter,
                           CondExpr[] index_2_filter,
                           CondExpr[] index_3_filter,
                           CondExpr[] index_4_filter,
                           CondExpr[] index_5_filter, boolean dontUseIndex2, boolean dontUseIndex3, boolean dontUseIndex4, boolean dontUseIndex5)
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation {
        map1 = new Map();
        try{
            iterators = new Iterator[5];
            iterators[0] = new FileScan(bigtable.heapfile[0].getFileName(), attributes, attributeSizes, (short) 4, 4, null, outFilter);
            
            if(dontUseIndex2) {
                iterators[1] = new FileScan(bigtable.heapfile[1].getFileName(), attributes, attributeSizes, (short) 4, 4, null, outFilter);
            } else {
            	iterators[1] = new IndexScan(
        				new IndexType(IndexType.ROW), bigtable.heapfile[1].getFileName(),
        				bigtable.indexFiles[1].get(0).getName(),
        				attributes, attributeSizes, 4, 4, null,
        				index_2_filter, outFilter, 1, false);
            }
            
            if(dontUseIndex3) {
                iterators[2] = new FileScan(bigtable.heapfile[2].getFileName(), attributes, attributeSizes, (short) 4, 4, null, outFilter);
            } else {
            	iterators[2] = new IndexScan(
        				new IndexType(IndexType.COL), bigtable.heapfile[2].getFileName(),
        				bigtable.indexFiles[2].get(0).getName(),
        				attributes, attributeSizes, 4, 4, null,
        				index_3_filter, outFilter, 1, false);
            }
            
            if(dontUseIndex4) {
                iterators[3] = new FileScan(bigtable.heapfile[3].getFileName(), attributes, attributeSizes, (short) 4, 4, null, outFilter);
            } else {
            	iterators[3] = new IndexScan(
        				new IndexType(IndexType.COLROW), bigtable.heapfile[3].getFileName(),
        				bigtable.indexFiles[3].get(0).getName(),
        				attributes, attributeSizes, 4, 4, null,
        				index_4_filter, outFilter, 1, false);
            }

            if(dontUseIndex5) {
                iterators[4] = new FileScan(bigtable.heapfile[4].getFileName(), attributes, attributeSizes, (short) 4, 4, null, outFilter);
            } else {
            	iterators[4] = new IndexScan(
        				new IndexType(IndexType.ROWVAL), bigtable.heapfile[4].getFileName(),
        				bigtable.indexFiles[4].get(0).getName(),
        				attributes, attributeSizes, 4, 4, null,
        				index_5_filter, outFilter, 1, false);
            }
        } catch(Exception e) {
            throw new FileScanException(e, "CustomScanMap creation failed");
        }
    }

    //TODO change logic
    /**
     * @return the result map
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Map get_next()
            throws Exception {
        while (true) {
            if ((map1 = iterators[0].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[1].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[2].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[3].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[4].get_next()) == null) {
                return null;
            }
            return map1;
        }
    }

    public void close() {
        try {
            for(int i=0; i<5; i++) {
                iterators[i].close();
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while closing custom scan!");
        }
    }
    

}