package BigT;

import global.AttrType;
import global.SystemDefs;
import global.TupleOrder;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class RowJoin {
    private Stream outputStream;
    private String columnFilter;
    private bigt bigt1;
    private bigt bigt2;
    private bigt outputTable;

    private int numBuf;

    private ArrayList<Map> duplicates;


    public RowJoin(int mem, Stream leftStream, bigt rightTable, String col, String outputTableName) throws Exception{
        this.columnFilter = col;
        this.bigt2 = rightTable;
        this.numBuf = mem;
        this.outputTable = new bigt(outputTableName);

        Stream rightStream = rightTable.openStream(1, "*", "*", "*", this.numBuf);

        bigt _bigt1 = removeDuplicates(leftStream, "temp1");
        bigt _bigt2 = removeDuplicates(rightStream, "temp2");

        Map left, right;
        leftStream = _bigt1.openStream(1, "*", "*", "*", this.numBuf);
        rightStream = _bigt2.openStream(1, "*", "*", "*", this.numBuf);

        while((left = leftStream.getNext()) != null) {
            while((right = rightStream.getNext()) != null) {
                if(left.getValue().equals(right.getValue())) {
                    Map temp1 = new Map(left);
                    Map temp2 = new Map(right);
                    
                    temp1.setRowLabel(left.getRowLabel() + ":" + right.getRowLabel());
                    temp2.setRowLabel(left.getRowLabel() + ":" + right.getRowLabel());

                    if(left.getColumnLabel().equals(right.getColumnLabel())) {
                        temp1.setColumnLabel(left.getColumnLabel() + "_left");
                        temp2.setColumnLabel(right.getColumnLabel() + "_right");
                    }
                    outputTable.insertMap(temp1.getMapByteArray(), 1);
                    outputTable.insertMap(temp2.getMapByteArray(), 1);
                }
            }
            rightStream = _bigt2.openStream(1, "*", "*", "*", this.numBuf);
        }

        leftStream.closestream();
        rightStream.closestream();
        this.outputStream = outputTable.openStream(1, "*", columnFilter, "*",this.numBuf/2);
    }

    private bigt removeDuplicates(Stream stream, String name) throws Exception {
        bigt bigt = new bigt(name);
        Map prev = new Map(stream.getNext());
        Map temp = null;

        while((temp = stream.getNext()) != null) {
            if(temp.getRowLabel().equals(prev.getRowLabel()) && temp.getColumnLabel().equals(prev.getColumnLabel())) {
                prev = new Map(temp);
                continue;
            }
            bigt.insertMap(prev.getMapByteArray(), 1);
            prev = new Map(temp);
        }

        bigt.insertMap(prev.getMapByteArray(), 1);
        stream.closestream();
        return bigt;
    }

    public Stream getResult() {
        return this.outputStream;
    }
}