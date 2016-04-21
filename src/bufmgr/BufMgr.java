package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
	
	Page[] buffer_pool;
	FrameDesc[] frametab; 
	//pageToFrame: to map a disk page number to a frame descriptor 
	//+ to tell if the a disk page is not in the buffer pool
	HashMap<Integer, Integer> pageFrameMap; 
	Clock replPolicy;
	
  public BufMgr(int numframes) {

	  buffer_pool = new Page[numframes];
	  frametab = new FrameDesc[numframes];
	  
	  for (int i = 0; i < buffer_pool.length; i++)
	  {
	      buffer_pool[i] = new Page();
	      frametab[i] = new FrameDesc();
	  }

	  pageFrameMap = new HashMap<Integer, Integer>();
	  replPolicy = new Clock(this);
	  
  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	//frameNo: to get the frame that holds the page if it is exist in the buffer pool 
	  Integer frameNo = pageFrameMap.get(pageno.pid);
	  
	 //System.out.println("In pinPage: " + "pageno is: # " + pageno.pid + "frameNo is: #" + frameNo);
	  
	// If disk page pageno is already in the buffer pool ==> increment pin_count of that frame.  
	  if(frameNo != null){
		  frametab[frameNo].pin_count ++;
	  }
	  else {
		  //uses the "Clock" replacement policy to select a frame to replace
		  int victimFrm = replPolicy.pickVictim();
		  
		  //if we couldn't find an available frame, so return an error
		  if(victimFrm == -1){
			  throw new IllegalStateException("All pages are pinned (pool is full)!");
		  }
		  //in case there is an available frame
		  else{
			  //1. check if frame to use is dirty and update the disk accordingly
			  if(frametab[victimFrm].dirty){
				  flushPage(frametab[victimFrm].pageno);
			  }
			  
			  //2. remove previous page from the frame if any
			  pageFrameMap.remove(pageno.pid);
			  
			  //3. reset the frame details
			  frametab[victimFrm].pin_count = 0;
	          frametab[victimFrm].valid = false; 
	          frametab[victimFrm].dirty = false;
	          frametab[victimFrm].refbit = false;

			  
			  switch (contents)
			  {
			  case PIN_DISKIO: {
				 //read the page from disk into the frame 
				 
		            
		          Minibase.DiskManager.read_page(pageno, buffer_pool[victimFrm]);
		          
		          
				  frametab[victimFrm].pin_count ++;
		          frametab[victimFrm].valid = true; 
		          frametab[victimFrm].dirty = false;
		          
		          //frametab[victimFrm].pageno.copyPageId(pageno);
		          frametab[victimFrm].pageno = new PageId(pageno.pid);
		          
		          frametab[victimFrm].refbit = true;
		          
		          
		          pageFrameMap.put(frametab[victimFrm].pageno.pid, victimFrm);
		          
		          mempage.setData(buffer_pool[victimFrm].getData());
				
	              
				  break;
			  }
			  case PIN_MEMCPY: {
				//copy mempage into the frame
				  
				  frametab[victimFrm].pin_count ++;
		          frametab[victimFrm].valid = true; 
		          frametab[victimFrm].dirty = false;

				  //frametab[victimFrm].pageno = new PageId();
		          //frametab[victimFrm].pageno.copyPageId(pageno);
		          frametab[victimFrm].pageno = new PageId(pageno.pid);
		          
		          frametab[victimFrm].refbit = true;
	              

		          pageFrameMap.put(this.frametab[victimFrm].pageno.pid, victimFrm);
		          
		          buffer_pool[victimFrm].setPage(mempage);
		           
				  break;
			  }
			  case PIN_NOOP: {
				//copy nothing into the frame - the frame contents are irrelevant
				  
				  frametab[victimFrm].pin_count ++;
		          frametab[victimFrm].valid = true; 
		          frametab[victimFrm].dirty = false;

				
		          frametab[victimFrm].pageno = new PageId(pageno.pid);
		          
		          frametab[victimFrm].refbit = true;
	              

		          pageFrameMap.put(this.frametab[victimFrm].pageno.pid, victimFrm);
		          
		          buffer_pool[victimFrm].setPage(mempage);
		          
		          //set page and data
	              mempage.setPage(buffer_pool[victimFrm]);
	              mempage.setData(buffer_pool[victimFrm].getData());
		              
				  break;
			  }
			  }
		  }
		  
	  }
	  

  } // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

	  if(pageFrameMap.containsKey(pageno.pid)){
		  
	  //frameNo: to get the frame that holds the page if it is exist in the buffer pool 
	  Integer frameNo = pageFrameMap.get(pageno.pid);
	  
	  //System.out.println("In unpinPage: " + "pageno is: # " + pageno.pid + "frameNo is: #" + frameNo);
	  
	  
	  //first check if the page is not in the buffer pool OR not pinned
	  if(frameNo == null || frametab[frameNo].pin_count == 0)
		{
		  throw new IllegalArgumentException("Page is not in the buffer pool or not pinned!"+ "P###"
				  + pageno.toString() + ":" + pageno.pid);
		}
		else
		{
			//is page updated or not? 
			//update dirty "field" according to that 
			if(dirty)
				frametab[frameNo].dirty = UNPIN_DIRTY; //UNPIN_DIRTY = true ==> write update to disk
			else
				frametab[frameNo].dirty = UNPIN_CLEAN; //UNPIN_CLEAN = false ==> no update (no need to write back to disk)
			
			//decrease pin_count variable for that frame
			frametab[frameNo].pin_count--;
			
			//set "refbit" to true when pin_count is set to 0
			if (frametab[frameNo].pin_count == 0)
		      {
		        frametab[frameNo].refbit = true;
		      }
		}

	  }
  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {

	  //pageno: "Page Id" of 1st allocated page
	  PageId pageno = new PageId();
	  //frameNo: is used to check if firstpg is already pinned
	  Integer frameNo = pageFrameMap.get(pageno.pid);
	  
	  
	  //System.out.println("In newPage: "+ "frameNo is: #" + frameNo + "run_size is: " + run_size);
	  
  
//	  Allocates a run of new disk pages and pins the first one in the buffer pool.
//	  The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
	  
//	   * @throws IllegalArgumentException if firstpg is already pinned
//	   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)

	  //1. check if there is a free frame in buffer pool
	  if(getNumUnpinned() == 0) {
		  throw new IllegalStateException("All pages are pinned!");
	  }
	  //2. check if first page is allocated or not
	  //also check if the page is pinned or not
	  else if (frameNo != null && frametab[frameNo].pin_count > 0) {
		  throw new IllegalArgumentException("firstpg is already pinned!");
	  }
	  //3. otherwise; "pinPage" 
	  else {
		  pinPage(pageno, firstpg, PIN_MEMCPY);
	  }
	  
	  //return "Page Id" of 1st allocated page
	  return pageno;

  } // public PageId newPage(Page firstpg, int run_size)

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {

	  if(pageFrameMap.containsKey(pageno.pid)){
		  
		//frameNo: is used to check if firstpg is already pinned
		Integer frameNo = pageFrameMap.get(pageno.pid);
		
		//System.out.println("In freePage: " + "pageno is: # " + pageno.pid + "frameNo is: #" + frameNo);
		  
		//1. check if pageno in the buffer pool already
		// and if it is pinned or not
		if(frameNo != null && frametab[frameNo].pin_count > 0) {
			throw new IllegalArgumentException("The page is pinned!");
		}
		//2. otherwise; deallocate the page / free it from the pool
		else {
			Minibase.DiskManager.deallocate_page(pageno);
		}
	  }

  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {

	  for (int i=0 ; i < frametab.length ; ++i) {
		  if (frametab[i].pageno != null) {
			  if (frametab[i].valid && frametab[i].dirty) {

				  flushPage(frametab[i].pageno);
			  }
		  }
	  }

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {
	 
	  //int can not hold "null"!
	  //frameNo: to get the frame that holds the page if it is exist in the buffer pool 
	  Integer frameNo = pageFrameMap.get(pageno.pid);
	  
	 // System.out.println("In flushPage: " + "pageno is: # " + pageno.pid + "frameNo is: #" + frameNo);
	  
	  
		if(frameNo != null)
		{
			if (frametab[frameNo].dirty) {
				//write page to disk
				Minibase.DiskManager.write_page(pageno, buffer_pool[frameNo]);
			}
		}
		else
		{
			 throw new IllegalArgumentException("Page is not in the buffer pool!");
		}
	
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
    
	  return frametab.length;

  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    
	  int unpinned_count = 0;
	  
	    for (int i = 0; i < frametab.length; i++)
	    {
	       if (frametab[i].pin_count == 0)
	       {
	         unpinned_count++;
	       }
	    }
	    
	   // System.out.println("In getNumUnpinned: " + unpinned_count);
		  
	    return unpinned_count;

  }

} // public class BufMgr implements GlobalConst