package BigT;

/** JAVA */
/**
 * Scan.java-  class Scan
 *
 */

import java.io.*;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.UnknowAttrType;

/**
 * 
 * Stream class retrieves maps in a specified order
 *
 */
public class Stream implements GlobalConst {

	Sort sorter;
	private bigt _bigT;
	static int indexEpr_index = 0;
	static int condExpr_index = 0;
	private FileScan fileScan;
	private IndexScan indexScan;
	private static short RecLen = 22;


	/**
	 * Constructor for stream class
	 * 
	 * @param bigtable      - BigTable used in the database
	 * @param orderType - order type of results
	 * @param rowFilter - filter for the row
	 * @param colFilter - filter for the column
	 * @param valFilter - filter for the value
	 * @param numbuf    - number of buffers allocated
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public Stream(bigt bigtable, int orderType, String rowFilter, String colFilter, String valFilter, int numbuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

		this._bigT = bigtable;
		AttrType[] attribute_types = new AttrType[]{
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString)
		};

		short[] attribute_sizes = new short[]{
				22,
				22,
				4,
				22
		};

		TupleOrder[] order = new TupleOrder[]{
				new TupleOrder(TupleOrder.Ascending),
				new TupleOrder(TupleOrder.Descending)
		};

		int len_so_far = 3;
		if (rowFilter.contains(",")) {
			len_so_far++;

		}
		if (colFilter.contains(",")) {
			len_so_far++;

		}
		if (valFilter.contains(",")) {
			len_so_far++;

		}
		CondExpr[] evalExpr = new CondExpr[len_so_far + 1];
		CondExpr[] indExpr;
		evalExpr[len_so_far] = null;

		int type = _bigT.type;

		switch (type) {
		case 1: {
			parseNormalFilter(rowFilter, 1, evalExpr);
			parseNormalFilter(colFilter, 2, evalExpr);
			parseNormalFilter(valFilter, 4, evalExpr);
			fileScan = new FileScan(_bigT.name, attribute_types, attribute_sizes, (short) 4, 4, null, evalExpr);
			sorter = new Sort(attribute_types, (short) 4, attribute_sizes, fileScan, orderType, order[0], RecLen, numbuf);
			break;
		}

		case 2: {
			if (rowFilter.contains("*")) {
				parseNormalFilter(rowFilter, 1, evalExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);

				fileScan = new FileScan(_bigT.name, attribute_types, attribute_sizes, (short) 4, 4, null, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, fileScan, orderType, order[0], RecLen, numbuf);
			} else {
				if (rowFilter.contains(","))
					indExpr = new CondExpr[3];
				else
					indExpr = new CondExpr[2];
				parseIndexFilter(rowFilter, 1, indExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);
				indexScan = new IndexScan(new IndexType(IndexType.ROW), _bigT.name, _bigT.name + "Index0", attribute_types,
						attribute_sizes, 4, 4, null, indExpr, 1, false, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, indexScan, orderType, order[0], RecLen, numbuf);
			}

			break;
		}

		case 3: {

			if (colFilter.contains("*")) {
				parseNormalFilter(rowFilter, 1, evalExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);

				fileScan = new FileScan(_bigT.name, attribute_types, attribute_sizes, (short) 4, 4, null, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, fileScan, orderType, order[0], RecLen, numbuf);
			} else {
				if (colFilter.contains(","))
					indExpr = new CondExpr[3];
				else
					indExpr = new CondExpr[2];
				parseIndexFilter(colFilter, 2, indExpr);
				
				parseNormalFilter(rowFilter, 1, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);
				indexScan = new IndexScan(new IndexType(IndexType.COL), _bigT.name, _bigT.name + "Index0", attribute_types,
						attribute_sizes, 4, 4, null, indExpr, 1, false, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, indexScan, orderType, order[0], RecLen, numbuf);
			}
			break;

		}

		case 4: {
			if (colFilter.contains("*") || rowFilter.contains("*") || colFilter.contains(",")
					|| rowFilter.contains(",")) {

				parseNormalFilter(rowFilter, 1, evalExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);


				fileScan = new FileScan(_bigT.name, attribute_types, attribute_sizes, (short) 4, 4, null, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, fileScan, orderType, order[0], RecLen, numbuf);
			} else {
				indExpr = new CondExpr[2];
				parseIndexFilter(colFilter + rowFilter, 5, indExpr);
				parseNormalFilter(valFilter, 4, evalExpr);
				indexScan = new IndexScan(new IndexType(IndexType.COLROW), _bigT.name, _bigT.name + "Index0", attribute_types,
						attribute_sizes, 4, 4, null, indExpr, 1, false, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, indexScan, orderType, order[0], RecLen, numbuf);
			}

			break;
		}

		case 5: {
			if (valFilter.contains("*") || rowFilter.contains("*") || valFilter.contains(",")
					|| rowFilter.contains(",")) {

				parseNormalFilter(rowFilter, 1, evalExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				parseNormalFilter(valFilter, 4, evalExpr);

				fileScan = new FileScan(_bigT.name, attribute_types, attribute_sizes, (short) 4, 4, null, evalExpr);
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, fileScan, orderType, order[0], RecLen, numbuf);
			} else {
				indExpr = new CondExpr[2];
				parseIndexFilter(rowFilter + valFilter, 6, indExpr);
				parseNormalFilter(colFilter, 2, evalExpr);
				indexScan = new IndexScan(new IndexType(IndexType.ROWVAL), _bigT.name, _bigT.name + "Index0",
						attribute_types, attribute_sizes, 4, 4, null, indExpr, 1, false, evalExpr);
			
				sorter = new Sort(attribute_types, (short) 4, attribute_sizes, indexScan, orderType, order[0], RecLen, numbuf);
			}

			break;
		}

		}

		indexEpr_index = 0;
		condExpr_index = 0;
	}

	/**
	 * Filter function to generate an expression on index based fields
	 * 
	 * @param filter          - filter condition
	 * @param fld           - field on which the filter is to be performed
	 * @param indExpression - filtering expression
	 */
	private void parseIndexFilter(String filter, int fld, CondExpr[] indExpression) {
		CondExpr indexExpression = new CondExpr();
		if (filter.startsWith("[") && filter.endsWith("]")) {
			filter = filter.substring(1, filter.length()-1);

			String operand1 = filter.split(",")[0];
			String operand2 = filter.split(",")[1];

			indexExpression = new CondExpr();
			indexExpression.op = new AttrOperator(AttrOperator.aopGE);
			indexExpression.type1 = new AttrType(AttrType.attrSymbol);
			indexExpression.type2 = new AttrType(AttrType.attrString);
			indexExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indexExpression.operand2.string = operand1;
			indexExpression.next = null;
			indExpression[condExpr_index] = indexExpression;
			condExpr_index++;
			
			indexExpression = new CondExpr();
			indexExpression.op = new AttrOperator(AttrOperator.aopLE);
			indexExpression.type1 = new AttrType(AttrType.attrSymbol);
			indexExpression.type2 = new AttrType(AttrType.attrString);
			indexExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indexExpression.operand2.string = operand2;
			indexExpression.next = null;
			indExpression[condExpr_index] = indexExpression;
			condExpr_index++;

		} else {
			indexExpression = new CondExpr();
			indexExpression.op = new AttrOperator(AttrOperator.aopEQ);
			indexExpression.type1 = new AttrType(AttrType.attrSymbol);
			indexExpression.type2 = new AttrType(AttrType.attrString);
			indexExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indexExpression.operand2.string = filter;
			indexExpression.next = null;
			indExpression[condExpr_index] = indexExpression;
			condExpr_index++;
			
		}
	}

	/**
	 * Filter function to generate an expression on non index fields
	 * 
	 * @param filt           - filter condition
	 * @param fld            - field on which the filter is to be performed
	 * @param evalExpression - filtering expression
	 */
	private void parseNormalFilter(String filt, int fld, CondExpr[] evalExpression) {
		CondExpr evaluationExpression = new CondExpr();
		if (filt.equals("*") || filt.equals(null)) {
			evalExpression[indexEpr_index] = null;
			indexEpr_index++;

		} else if (filt.startsWith("[") && filt.endsWith("]")) {
			filt = filt.substring(1, filt.length() - 1);

			String op1 = filt.split(",")[0];
			String op2 = filt.split(",")[1];

			evaluationExpression = new CondExpr();
			evaluationExpression.op = new AttrOperator(AttrOperator.aopGE);
			evaluationExpression.type1 = new AttrType(AttrType.attrSymbol);
			evaluationExpression.type2 = new AttrType(AttrType.attrString);
			evaluationExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evaluationExpression.operand2.string = op1;
			evaluationExpression.next = null;
			evalExpression[indexEpr_index] = evaluationExpression;
			indexEpr_index++;
			evaluationExpression = new CondExpr();
			evaluationExpression.op = new AttrOperator(AttrOperator.aopLE);
			evaluationExpression.type1 = new AttrType(AttrType.attrSymbol);
			evaluationExpression.type2 = new AttrType(AttrType.attrString);
			evaluationExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evaluationExpression.operand2.string = op2;
			evaluationExpression.next = null;
			evalExpression[indexEpr_index] = evaluationExpression;
			indexEpr_index++;

		} else {
			evaluationExpression = new CondExpr();
			evaluationExpression.op = new AttrOperator(AttrOperator.aopEQ);
			evaluationExpression.type1 = new AttrType(AttrType.attrSymbol);
			evaluationExpression.type2 = new AttrType(AttrType.attrString);
			evaluationExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evaluationExpression.operand2.string = filt;
			evaluationExpression.next = null;
			evalExpression[indexEpr_index] = evaluationExpression;
			indexEpr_index++;

		}
	}

	/**
	 * Retrieve the next record in a sequential scan
	*/
	public Map getNext() throws SortException, UnknowAttrType, LowMemException, JoinsException, Exception {
		return sorter.get_next();
	}

	/**
	 * Closes the Scan object
	 * 
	 * @throws IOException
	 * @throws SortException
	 * @throws IndexException
	 */
	public void closestream() throws SortException, IOException, IndexException {
		sorter.close();
	}
}
