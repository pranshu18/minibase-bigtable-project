package index;
import BigT.Map;
import global.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

	/**
	 * class constructor. set up the index scan.
	 * @param index type of the index (B_Index, Hash)
	 * @param relName name of the input relation
	 * @param indName name of the input index
	 * @param types array of types in this relation
	 * @param str_sizes array of string sizes (for attributes that are string)
	 * @param noInFlds number of fields in input tuple
	 * @param noOutFlds number of fields in output tuple
	 * @param outFlds fields to project
	 * @param indexFilters conditions to apply, first one is primary
	 * @param fldNum field number of the indexed field
	 * @param indexOnly whether the answer requires only the key or the tuple
	 * @exception IndexException error from the lower layer
	 * @exception InvalidTypeException tuple type not valid
	 * @exception InvalidTupleSizeException tuple size not valid
	 * @exception UnknownIndexTypeException index type unknown
	 * @exception IOException from the lower layer
	 */
	public IndexScan(
			IndexType     index,
			final String  relName,
			final String  indName,
			AttrType      types[],
			short         str_sizes[],
			int           noInFlds,
			int           noOutFlds,
			FldSpec       outFlds[],
			CondExpr      indexFilters[],
			CondExpr      allFilters[],
			final int     fldNum,
			final boolean indexOnly
	)
			throws IndexException,
			InvalidTypeException,
			InvalidTupleSizeException,
			UnknownIndexTypeException,
			IOException, IteratorException, ConstructPageException, UnknownKeyTypeException, KeyNotMatchException, PinPageException, InvalidSelectionException, UnpinPageException, GetFileEntryException {
		_fldNum = fldNum;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;

		AttrType[] Jtypes = new AttrType[noOutFlds];
		short[] ts_sizes;
		new_map = new Map();

		//Not needed for Map type classes
    /*
    try {
      ts_sizes = TupleUtils.setup_op_tuple(JMap, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
    }
    catch (TupleUtilsException e) {
      throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
    }
    catch (InvalidRelation e) {
      throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
    }
     */

		_index_filters = indexFilters;
		_all_filters = allFilters;
		perm_mat = outFlds;
		_noOutFlds = noOutFlds;
		map1 = new Map();
		try {
			map1.setHdr(null);
		}
		catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile error");
		}

		t1_size = map1.size();
		index_only = indexOnly;  // added by bingjie miao

		try {
			f = new Heapfile(relName);
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile not created");
		}

		if(index.indexType != IndexType.None)
			indScan = IndexUtils.BTree_scan(indexFilters, new BTreeFile(indName));
	}

	/**
	 * returns the next tuple.
	 * if <code>index_only</code>, only returns the key value
	 * (as the first field in a tuple)
	 * otherwise, retrive the tuple and returns the whole tuple
	 * @return the tuple
	 * @exception IndexException error from the lower layer
	 * @exception UnknownKeyTypeException key type unknown
	 * @exception IOException from the lower layer
	 */
	public Map get_next()
			throws IndexException,
			UnknownKeyTypeException,
			IOException
	{
		MID mid;
		int unused;
		KeyDataEntry nextentry = null;

		try {
			nextentry = indScan.get_next();
		}
		catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: BTree error");
		}

		while(nextentry != null) {
			if (index_only) {
				// only need to return the key

				AttrType[] attrType = new AttrType[1];
				short[] s_sizes = new short[1];
				new_map = new Map();

				attrType[0] = new AttrType(AttrType.attrString);
				// calculate string size of _fldNum
				int count = 0;
				for (int i=0; i<_fldNum; i++) {
					if (_types[i].attrType == AttrType.attrString)
						count ++;
				}
				s_sizes[0] = _s_sizes[count-1];

				try {
					new_map.setHdr(null);
				}
				catch (Exception e) {
					throw new IndexException(e, "IndexScan.java: Heapfile error");
				}

				try {
					new_map.setValue(((StringKey)nextentry.key).getKey());
				}
				catch (Exception e) {
					throw new IndexException(e, "IndexScan.java: Heapfile error");
				}
				return new_map;
			}

			// not index_only, need to return the whole tuple
			mid = ((LeafData)nextentry.data).getData();
			try {
				map1 = f.getRecord(mid);
			}
			catch (Exception e) {
				throw new IndexException(e, "IndexScan.java: getRecord failed");
			}

			try {
				map1.setHdr(null);
			}
			catch (Exception e) {
				throw new IndexException(e, "IndexScan.java: Heapfile error");
			}

			boolean eval;
			try {
				eval = PredEval.Eval(_all_filters, map1, null, _types, null);
			}
			catch (Exception e) {
				throw new IndexException(e, "IndexScan.java: Heapfile error");
			}

			if (eval) {
				// need projection.java
				try {
					new_map = map1;
					//Projection.Project(map1, _types, JMap, perm_mat, _noOutFlds);
				}
				catch (Exception e) {
					throw new IndexException(e, "IndexScan.java: Heapfile error");
				}

				return new_map;
			}

			try {
				nextentry = indScan.get_next();
			}
			catch (Exception e) {
				throw new IndexException(e, "IndexScan.java: BTree error");
			}
		}

		return null;
	}

	public MapMidPair getNextMIDPair() throws Exception {

		MID _mid;
		KeyDataEntry next_entry = null;
		next_entry = indScan.get_next();

		while (next_entry != null) {

			if (index_only) {

				AttrType[] attrType = new AttrType[1];
				short[] s_sizes = new short[1];

				MapMidPair Jpair = new MapMidPair();


				attrType[0] = new AttrType(AttrType.attrString);
				int count = 0;
				for (int i=0; i<_fldNum; i++) {
					if (_types[i].attrType == AttrType.attrString)
						count ++;
				}
				s_sizes[0] = _s_sizes[count-1];

				Jpair.indKey = ((StringKey)next_entry.key).getKey();

				_mid = ((LeafData) next_entry.data).getData();
				Jpair.mid = _mid;

				return Jpair;
			}

			_mid = ((LeafData) next_entry.data).getData();
			map1 = f.getRecord(_mid);
			map1.setHdr(null);

			boolean eval;
			eval = PredEval.Eval(_all_filters, map1, null, _types, null);

			if (eval) {
				new_map = map1;
				MapMidPair mpair = new MapMidPair();
				mpair.map = new_map;
				mpair.mid = _mid;
				return mpair;
			}

			next_entry = indScan.get_next();
		}

		return null;
	}


	/**
	 * Cleaning up the index scan, does not remove either the original
	 * relation or the index from the database.
	 * @exception IndexException error from the lower layer
	 * @exception IOException from the lower layer
	 */
	public void close() throws IOException, IndexException
	{
		if (!closeFlag) {
			if (indScan instanceof BTFileScan) {
				try {
					((BTFileScan)indScan).DestroyBTreeFileScan();
				}
				catch(Exception e) {
					throw new IndexException(e, "BTree error in destroying index scan.");
				}
			}

			closeFlag = true;
		}
	}

	public FldSpec[]      perm_mat;
	private IndexFile     indFile;
	private IndexFileScan indScan;
	private AttrType[]    _types;
	private short[]       _s_sizes;
	private CondExpr[]    _index_filters;
	private CondExpr[]    _all_filters;
	private int           _noInFlds;
	private int           _noOutFlds;
	private Heapfile      f;
	private Map map1;
	private Map new_map;
	private int           t1_size;
	private int           _fldNum;
	private boolean       index_only;

}
