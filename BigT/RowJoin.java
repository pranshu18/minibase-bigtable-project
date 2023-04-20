package BigT;

import global.AttrType;

public class RowJoin {
    private Stream outputStream;
    private String columnFilter;
    private bigt outputTable;

    private int numBuf;

    public RowJoin(int mem, Stream leftStream, bigt rightTable, String col, String outputTableName) throws Exception{
        this.columnFilter = col;
        this.numBuf = mem;
        this.outputTable = new bigt(outputTableName);

        Stream rightStream = rightTable.openStream(1, "*", "*", "*", this.numBuf);
        Map left, right;
        Map prev_left = new Map(leftStream.getNext()), prev_right = new Map(rightStream.getNext());

        while((left = leftStream.getNext()) != null) {
            if(left.getRowLabel().equals(prev_left.getRowLabel()) && left.getColumnLabel().equals(prev_left.getColumnLabel())) {
                prev_left = new Map(left);
                continue;
            }

            while((right = rightStream.getNext()) != null) {
                if(right.getRowLabel().equals(prev_right.getRowLabel()) && right.getColumnLabel().equals(prev_right.getColumnLabel())) {
                    prev_right = new Map(right);
                    continue;
                }

                if(prev_left.getValue().equals(prev_right.getValue())) {
                    Map temp1 = new Map(prev_left);
                    Map temp2 = new Map(prev_right);

                    temp1.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());
                    temp2.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());

                    temp1.setColumnLabel(prev_left.getColumnLabel());

                    if(prev_left.getColumnLabel().equals(prev_right.getColumnLabel())) {
                        temp1.setColumnLabel(prev_left.getColumnLabel() + "_left");
                        temp2.setColumnLabel(prev_right.getColumnLabel() + "_right");
                    }
                    outputTable.insertMap(temp1.getMapByteArray(), 1);
                    outputTable.insertMap(temp2.getMapByteArray(), 1);
                }

                prev_right = new Map(right);
            }
            prev_left = new Map(left);
        }

        if(prev_left != null) {
            while((right = rightStream.getNext()) != null) {
                if(right.getRowLabel().equals(prev_right.getRowLabel()) && right.getColumnLabel().equals(prev_right.getColumnLabel())) {
                    prev_right = new Map(right);
                    continue;
                }

                if(prev_left.getValue().equals(prev_right.getValue())) {
                    Map temp1 = new Map(prev_left);
                    Map temp2 = new Map(prev_right);

                    temp1.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());
                    temp2.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());

                    temp1.setColumnLabel(prev_left.getColumnLabel());

                    if(prev_left.getColumnLabel().equals(prev_right.getColumnLabel())) {
                        temp1.setColumnLabel(prev_left.getColumnLabel() + "_left");
                        temp2.setColumnLabel(prev_right.getColumnLabel() + "_right");
                    }
                    outputTable.insertMap(temp1.getMapByteArray(), 1);
                    outputTable.insertMap(temp2.getMapByteArray(), 1);
                }

                prev_right = new Map(right);
            }
        }
        
        if(prev_left != null && prev_right != null) {
            if(prev_left.getValue().equals(prev_right.getValue())) {
                Map temp1 = new Map(prev_left);
                Map temp2 = new Map(prev_right);

                temp1.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());
                temp2.setRowLabel(prev_left.getRowLabel() + ":" + prev_right.getRowLabel());

                temp1.setColumnLabel(prev_left.getColumnLabel());

                if(prev_left.getColumnLabel().equals(prev_right.getColumnLabel())) {
                    temp1.setColumnLabel(prev_left.getColumnLabel() + "_left");
                    temp2.setColumnLabel(prev_right.getColumnLabel() + "_right");
                }

                outputTable.insertMap(temp1.getMapByteArray(), 1);
                outputTable.insertMap(temp2.getMapByteArray(), 1);
            }
        }
        this.outputStream = outputTable.openStream(1, "*", columnFilter, "*",this.numBuf/2);
    }

    public Stream getResult() {
        return this.outputStream;
    }
}