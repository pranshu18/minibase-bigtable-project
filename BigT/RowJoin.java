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


    public RowJoin(int mem, bigt leftTable, bigt rightTable, String col, String outputTableName, String joinType) throws Exception{
        this.columnFilter = col;
        this.bigt2 = rightTable;
        this.numBuf = mem;
        this.outputTable = new bigt(outputTableName);

		Stream leftStream = leftTable.openStream(1, "*", "*", "*", this.numBuf);
        Stream rightStream = rightTable.openStream(1, "*", "*", "*", this.numBuf);
        

        Map left, right;
        
        if(joinType.equals("1")) {
        	
            while((left = leftStream.getNext()) != null) {
                while((right = rightStream.getNext()) != null) {
                    if(left.getValue().equals(right.getValue())) {
//                    	System.out.println("Left =");
//                    	left.print();
//                    	System.out.println("Right =");
//                    	right.print();
                    	
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
                rightStream.closestream();
                rightStream = rightTable.openStream(1, "*", "*", "*", this.numBuf);
            }

        }else if(joinType.equals("2")) {
        	
            leftStream = leftTable.openStream(7, "*", "*", "*", this.numBuf);
            rightStream = rightTable.openStream(7, "*", "*", "*", this.numBuf);
            
            Map m1 = leftStream.getNext();
            Map m2 = rightStream.getNext();
            
        	long l1 = Long.parseLong(m1.getValue());
        	long l2 = Long.parseLong(m2.getValue());
        	
        	int leftMark = 0;
        	
            while (true) {
            	boolean breakOuter = false;
            	while(l1 != l2) {
            		if(l1 < l2) {
            			m1 = leftStream.getNext();
            			leftMark++;
            			if(m1 == null) {
            				breakOuter = true;
            				break;
            			}
            			l1 = Long.parseLong(m1.getValue());
            		}else {
            			m2 = rightStream.getNext();
            			if(m2 == null) {
            				breakOuter = true;
            				break;
            			}
            			l2 = Long.parseLong(m2.getValue());
            		}
            	}
            	if(breakOuter)
            		break;
            	
            	while(true) {
            		int leftAdd = 0;
            		
            		while(l1 == l2) {
            			
                        Map temp1 = new Map(m1);
                        Map temp2 = new Map(m2);
                        
//                    	System.out.println("Left =");
//                    	m1.print();
//                    	System.out.println("Right =");
//                    	m2.print();

                        temp1.setRowLabel(m1.getRowLabel() + ":" + m2.getRowLabel());
                        temp2.setRowLabel(m1.getRowLabel() + ":" + m2.getRowLabel());

                        if(m1.getColumnLabel().equals(m2.getColumnLabel())) {
                            temp1.setColumnLabel(m1.getColumnLabel() + "_left");
                            temp2.setColumnLabel(m2.getColumnLabel() + "_right");
                        }
                        outputTable.insertMap(temp1.getMapByteArray(), 1);
                        outputTable.insertMap(temp2.getMapByteArray(), 1);
                        
            			m1 = leftStream.getNext();
            			leftAdd++;
            			if(m1 == null) {
            				break;
            			}
            			l1 = Long.parseLong(m1.getValue());
            		}
            		
            		m2 = rightStream.getNext();
            		if(m2 == null) {
        				breakOuter = true;
        				break;
            		}
        			l2 = Long.parseLong(m2.getValue());
        			
        			Stream markLeftStream = leftTable.openStream(7, "*", "*", "*", this.numBuf);
        			Map m3 = markLeftStream.getNext();
        			int leftVal = leftMark;
        			while(leftVal>0) {
        				m3 = markLeftStream.getNext();
        				leftVal--;
        			}
        			
        			long l3 = Long.parseLong(m3.getValue());
        			
            		if(l3 == l2) {
            			leftStream.closestream();
            			leftStream = markLeftStream;
            			m1 = m3;
            		}else {
            			markLeftStream.closestream();
            			if(m1 == null) {
            				breakOuter = true;
            				break;
            			}
            			leftMark = leftMark + leftAdd;
            		}
        			l1 = Long.parseLong(m1.getValue());

            	}
            	if(breakOuter)
            		break;
            }
        }

        leftStream.closestream();
        rightStream.closestream();
        
        outputTable.populateBtree();
        outputTable.removeDuplicates();

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