package BigT;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import index.IndexScan;
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

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws InvalidRelation, FileScanException, IOException, TupleUtilsException, MapUtilsException, SortException {
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

        if(_bigt.getType() == 1 ||
                (_bigt.getType() == 2 && emptyFilter(rowFilter)) ||
                (_bigt.getType() == 3 && emptyFilter(columnFilter)) ||
                (_bigt.getType() == 4 && (emptyFilter(rowFilter) || emptyFilter(columnFilter) || rangeFilter(rowFilter) || rangeFilter(columnFilter))) ||
                (_bigt.getType() == 4 && (rangeFilter(rowFilter) || emptyFilter(rowFilter) || emptyFilter(valueFilter) || rangeFilter(columnFilter)))
        ) {
            parseFilter(condExprs, rowFilter, 1);
            parseFilter(condExprs, columnFilter, 2);
            parseFilter(condExprs, valueFilter, 4);

            filescanObject = new FileScan(_bigt.name, attributes, attributeSizes, (short) 4, 4, (FldSpec[]) null, getCondExprArray(condExprs));
            sorter = new Sort(attributes, (short) 4, attributeSizes, filescanObject, orderType, TupleOrder.Ascending, Map.STRING_ATTR_SIZE, MINIBASE_BUFFER_POOL_SIZE );

        } else if(_bigt.getType() == 2) {
            parseFilter(indexExprs, rowFilter, 1);
            parseFilter(condExprs, columnFilter, 2);
            parseFilter(condExprs, valueFilter, 4);

            indexscanObject = new IndexScan(new IndexType(IndexType.Row_Index))

        } else if(_bigt.getType() == 3) {


        } else if(_bigt.getType() == 4) {


        } else if(_bigt.getType() == 5) {


        }
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
}
