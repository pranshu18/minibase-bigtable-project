package iterator;

import heap.*;          
import global.*;
import diskmgr.*;
import bufmgr.*;

import java.io.*;

import BigT.Map;

public class SpoofIbuf implements GlobalConst  {
  
  /**
   *constructor, use the init to initialize
   */
  public void SpoofIbuf()
    {
      
      hf_scan = null;
    }
  
 
  /**
   *Initialize some necessary inormation, call Iobuf to create the
   *object, and call init to finish intantiation
   *@param bufs[][] the I/O buffer
   *@param n_pages the numbers of page of this buffer
   *@param tSize the tuple size
   *@param fd the reference to an Heapfile
   *@param Ntuples the tuple numbers of the page
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public  void init(Heapfile fd, byte bufs[][], int n_pages,
		    int tSize, int Ntuples)
    throws IOException,
	   Exception
    {
      _fd       = fd;       _bufs        = bufs;
      _n_pages  = n_pages;  t_size       = tSize;
      
      t_proc    = 0;        t_in_buf     = 0;
      tot_t_proc= 0;
      curr_page = 0;        t_rd_from_pg = 0;
      done      = false;    t_per_pg     = MINIBASE_PAGESIZE / t_size;
     
      
      n_tuples = Ntuples;
     
      // open a scan
      if (hf_scan != null)  hf_scan = null;
      
      try {
	hf_scan = _fd.openScan();
      }
      catch(Exception e){
	throw e;
      }
      
      
    }
  
   /** 
   *get a tuple from current buffer,pass reference buf to this method
   *usage:temp_tuple = tuple.Get(buf); 
   *@param temp_map write the result to buf
   *@return the result tuple
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public  Map Get(Map  temp_map)throws IOException, Exception
    {
      if (tot_t_proc == n_tuples) done = true;
      
      if (done == true){temp_map = null; return null;}
      if (t_proc == t_in_buf)
	{
	  try {
	    t_in_buf = readin();
	  }
	  catch (Exception e){
	    throw e;
	  }
	  curr_page = 0; t_rd_from_pg = 0; t_proc = 0;
	}
      
      if (t_in_buf == 0)                        // No tuples read in?
	{
	  done = true; temp_map = null;return null;
	}
 
      temp_map.mapSet(_bufs[curr_page],t_rd_from_pg*t_size, t_size);
      tot_t_proc++;
      
      // Setup for next read
      t_rd_from_pg++; t_proc++;
      if (t_rd_from_pg == t_per_pg)
	{
	  t_rd_from_pg = 0; curr_page++;
	}
      return temp_map;
    }
  
   
  /**
   *@return if the buffer is empty,return true. otherwise false
   */
  public  boolean empty()
    {
      if (tot_t_proc == n_tuples) done = true;
      return done;
    }
  
  /**
   *
   *@return the numbers of tuples in the buffer
   *@exception IOException some I/O fault
   *@exception InvalidTupleSizeException Heapfile error
   */
  private int readin()throws IOException,InvalidTupleSizeException
    {
      int   t_read = 0, tot_read = 0;
      Map m      = new Map ();
      byte[] mCopy;
      
      curr_page = 0;
      while (curr_page < _n_pages)
	{
	  while (t_read < t_per_pg)
	    {
	      MID mid =new MID();
	      try {
		if ( (m = hf_scan.getNext(mid)) == null) return tot_read;
		mCopy = m.getMapByteArray();
		System.arraycopy(mCopy,0,_bufs[curr_page],t_read*t_size,t_size);
	      }
	      catch (Exception e) {
		System.err.println (""+e);
	      }
	      t_read++; tot_read++;
	    } 
	  t_read     = 0;
	  curr_page++;
	}
      return tot_read;
    }
  
  
  private  byte[][] _bufs;
  
  private  int   TEST_fd;
  
  private  Heapfile _fd;
  private  Scan hf_scan;
  private  int    _n_pages;
  private  int    t_size;
  
  private  int    t_proc, t_in_buf;
  private  int    tot_t_proc;
  private  int    t_rd_from_pg, curr_page;
  private  int    t_per_pg;
  private  boolean   done;
  private  int    n_tuples;
}


