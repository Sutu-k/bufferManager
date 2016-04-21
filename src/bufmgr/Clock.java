package bufmgr;

import global.GlobalConst;


	public class Clock implements GlobalConst {

	private int current;
	//frametab: copy of the frametab in "BufMgr.java"
	private FrameDesc[] frametab;
	 
	public Clock(BufMgr bm) {
		current = 0;
		this.frametab=bm.frametab;
	}
	
	
	
	public int pickVictim() {
		
		//(frametab.length)*2: as we need to check the buff. pool 2 times
		for(int counter = 0; counter <= (frametab.length)*2; counter++) {
			
			//1. if data in bufpool[current] is not valid, choose current
			if (!frametab[current].valid){
				return current;
			}
			//2. if frametab[current]'s pin count is 0
			else if (frametab[current].pin_count == 0){
				// check if frametab [current] has refbit
				if (frametab[current].refbit){
					frametab[current].refbit = false;
				} else {
					return current;
				}	
			}
			// increment current, mod N
			current = (current+1) % frametab.length;
		}
		
		// (-1) if No frame available in the buff. pool
		return -1;
	}
}
