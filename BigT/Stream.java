package BigT;

import btree.*;
import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import index.IndexScan;
import index.InvalidSelectionException;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static global.GlobalConst.MINIBASE_BUFFER_POOL_SIZE;

public class Stream implements GlobalConst {
	private bigt _bigt;
	private FileScan filescanObject;
	private IndexScan indexscanObject;
	private Sort sorter;

	public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int numbuf) throws InvalidRelation, FileScanException, IOException, TupleUtilsException, MapUtilsException, SortException, IndexException, InvalidTupleSizeException, UnknownIndexTypeException, InvalidTypeException, IteratorException, ConstructPageException, UnknownKeyTypeException, KeyNotMatchException, GetFileEntryException, PinPageException, InvalidSelectionException, UnpinPageException {
		this._bigt = bigtable;

		AttrType[] attributes = {
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString)
		};

		short[] attributeSizes = {Map.STRING_ATTR_SIZE, Map.STRING_ATTR_SIZE, Map.INT_ATTR_SIZE, Map.STRING_ATTR_SIZE}; // attribute byte sizes

		List<CondExpr> condExprs = new ArrayList<>(); // store all the condition expressions from the filters
		List<CondExpr> indexExprs = new ArrayList<>(); // store all the index expressions from the filters

		parseFilter(condExprs, rowFilter, 1);
		parseFilter(condExprs, columnFilter, 2);
		parseFilter(condExprs, valueFilter, 4);

		if(_bigt.type == IndexType.None ||
				(_bigt.type == IndexType.ROWVAL && emptyFilter(rowFilter)) ||
				(_bigt.type == IndexType.COL && emptyFilter(columnFilter)) ||
				(_bigt.type == IndexType.COLROW && (emptyFilter(rowFilter) || emptyFilter(columnFilter) || rangeFilter(rowFilter) || rangeFilter(columnFilter))) ||
				(_bigt.type == IndexType.ROWVAL && (rangeFilter(rowFilter) || emptyFilter(rowFilter) || emptyFilter(valueFilter) || rangeFilter(columnFilter)))
		) {
			filescanObject = new FileScan(_bigt.name, attributes, attributeSizes, (short) 4, 4, null, getCondExprArray(condExprs));
			sorter = new Sort(attributes, (short) 4,
					attributeSizes, filescanObject,
					orderType, new TupleOrder(TupleOrder.Ascending),
					Map.STRING_ATTR_SIZE, numbuf);

			return;
		}

		if(_bigt.type == IndexType.ROW)
			parseFilter(indexExprs, rowFilter, 1);
		else if(_bigt.type == IndexType.COL)
			parseFilter(indexExprs, columnFilter, 2);
		else if(_bigt.type == IndexType.COLROW)
			parseFilter(indexExprs, columnFilter + rowFilter, 5);
		else if(_bigt.type == IndexType.ROWVAL)
			parseFilter(indexExprs, rowFilter + valueFilter, 6);
		
		if(indexExprs.size() == 1)
			indexExprs.add(null);

		indexscanObject = new IndexScan(
				new IndexType(IndexType.ROW), _bigt.name,
				_bigt.name + "Index0",
				attributes, attributeSizes, 4, 4, null,
				getCondExprArray(indexExprs), getCondExprArray(condExprs), 1, false);

		sorter = new Sort(attributes, (short) 4,
				attributeSizes, indexscanObject,
				orderType, new TupleOrder(TupleOrder.Ascending),
				Map.STRING_ATTR_SIZE, numbuf);
	}

	private void parseFilter(List<CondExpr> condExprs , String filter, int fldno) {
		if(emptyFilter(filter))
			condExprs.add(null);

		else if(rangeFilter(filter)) {
			filter = filter.substring(1, filter.length() - 1);
			List<String> operands = new ArrayList<>();

			for(String operand: filter.split(","))
				operands.add(operand.trim());

			// [x, y] is basically two operands value >= x and value <= y

			CondExpr greaterOperand = new CondExpr(
					new AttrOperator(AttrOperator.aopGE),
					new AttrType(AttrType.attrSymbol),
					new AttrType(AttrType.attrString),
					new FldSpec(new RelSpec(RelSpec.outer), fldno),
					operands.get(0));

			CondExpr lesserOperand = new CondExpr(
					new AttrOperator(AttrOperator.aopLE),
					new AttrType(AttrType.attrSymbol),
					new AttrType(AttrType.attrString),
					new FldSpec(new RelSpec(RelSpec.outer), fldno),
					operands.get(1));

			condExprs.add(greaterOperand);
			condExprs.add(lesserOperand);

		} else {
			// an equality expression with no range
			CondExpr expr = new CondExpr(
					new AttrOperator(AttrOperator.aopEQ),
					new AttrType(AttrType.attrSymbol),
					new AttrType(AttrType.attrString),
					new FldSpec(new RelSpec(RelSpec.outer), fldno),
					filter);
			condExprs.add(expr);
		}
	}

	private boolean emptyFilter(String filter) {
		return filter == null || filter.isEmpty() || filter.equals("*");
	}

	private boolean rangeFilter(String filter) {
		return filter.charAt(0) == '[' && filter.charAt(filter.length() - 1) == ']';
	}

	private CondExpr[] getCondExprArray(List<CondExpr> condExprs) {
		CondExpr[] condExprsArr = new CondExpr[condExprs.size()];
		int i = 0;

		for(i = 0; i < condExprs.size(); i++)
			condExprsArr[i] = condExprs.get(i);

		return condExprsArr;
	}

	public Map getNext() throws Exception {
		return sorter.get_next();
	}

	public void closestream() throws Exception {
		sorter.close();
	}
}