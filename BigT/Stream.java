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

		short[] attributeSizes = {22, 22, 4, 22}; // attribute byte sizes

		List<CondExpr> condExprs = new ArrayList<>(); // store all the condition expressions from the filters
		parseFilter(condExprs, rowFilter, 1);
		parseFilter(condExprs, columnFilter, 2);
		parseFilter(condExprs, valueFilter, 4);

		List<CondExpr> indexExprs2 = new ArrayList<>(); // store all the index expressions from the filters
		List<CondExpr> indexExprs3 = new ArrayList<>(); // store all the index expressions from the filters
		List<CondExpr> indexExprs4 = new ArrayList<>(); // store all the index expressions from the filters
		List<CondExpr> indexExprs5 = new ArrayList<>(); // store all the index expressions from the filters


		parseFilter(indexExprs2, rowFilter, 1);
		parseFilter(indexExprs3, columnFilter, 2);
		parseFilter(indexExprs4, columnFilter + rowFilter, 5);
		parseFilter(indexExprs5, rowFilter + valueFilter, 6);

		if(indexExprs2.size() == 1)
			indexExprs2.add(null);
		if(indexExprs3.size() == 1)
			indexExprs3.add(null);
		if(indexExprs4.size() == 1)
			indexExprs4.add(null);
		if(indexExprs5.size() == 1)
			indexExprs5.add(null);

		CondExpr[] condExpArray = getCondExprArray(condExprs);
		CondExpr[] indExp2Array = getCondExprArray(indexExprs2);
		CondExpr[] indExp3Array = getCondExprArray(indexExprs3);
		CondExpr[] indExp4Array = getCondExprArray(indexExprs4);
		CondExpr[] indExp5Array = getCondExprArray(indexExprs5);

		CustomScan customScanObject = new CustomScan(bigtable, attributes, attributeSizes, condExpArray, 
				indExp2Array, indExp3Array, indExp4Array, indExp5Array, emptyFilter(rowFilter), emptyFilter(columnFilter), 
				(emptyFilter(rowFilter) || emptyFilter(columnFilter) || rangeFilter(rowFilter) || rangeFilter(columnFilter)),
				(rangeFilter(rowFilter) || emptyFilter(rowFilter) || emptyFilter(valueFilter) || rangeFilter(columnFilter)));

		sorter = new Sort(attributes, (short) 4,
				attributeSizes, customScanObject,
				orderType, new TupleOrder(TupleOrder.Ascending),
				22, numbuf);
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