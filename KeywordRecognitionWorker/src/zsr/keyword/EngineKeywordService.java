package zsr.keyword;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 * @author thinkit
 *
 */
public class EngineKeywordService implements CenterKeywordService, Runnable{
	private EngineKeywordService(){
		
	}
	private static EngineKeywordService only = null;
	public static EngineKeywordService getOnlyInstance(){
		if(only == null) only = new EngineKeywordService();
		return only;
	}
	@Override
	public void addGlobalEnvi(Map<String, String> map) {
		synchronized(globalEnvis) {
			globalEnvis.putAll(map);	
		}
	}

	@Override
	public void removeGlobalEnvi(Set<String> set) {
		synchronized(globalEnvis) {
			globalEnvis.keySet().removeAll(set);	
		}
	}

	@Override
	public Set<String> getGlobalVariable() {
		return  new HashSet<String>(globalEnvis.keySet());
	}

	@Override
	public Map<String, String> getGlobalEnvi(Set<String> varSet) {
		Map<String, String> ret = new HashMap<String, String> ();
		synchronized(globalEnvis){
			for(String k : globalEnvis.keySet()) {
				ret.put(k, globalEnvis.get(k));
			}			
		}
		return ret;
	}
	
	private int genUId = 0;
	private class ServiceChannelImpl implements ServiceChannel{
		private int UId;
		ServiceChannelImpl(){
			UId = ++genUId;
		}
		//附加UId的原因是：toString不能产生对象的唯一标识。
		@Override 
		public String toString(){
			return super.toString()+" "+UId;
		}
		@Override
		public BlockingQueue<KeywordRequestPacket> getRequestQueue() {
			// TODO Auto-generated method stub
			return requestQueue;
		}

		@Override
		public BlockingQueue<KeywordResultPacket> getResultQueue() {
			// TODO Auto-generated method stub
			return resultQueue;
		}
		
		@Override
		public void close(){
			allJobs.remove(this);
		}
		
		final BlockingQueue<KeywordResultPacket> resultQueue = new LinkedBlockingQueue<KeywordResultPacket>();
		final BlockingQueue<KeywordRequestPacket> requestQueue = new LinkedBlockingQueue<KeywordRequestPacket>();
	}
	@Override
	public ServiceChannel allocateOneChannel() {
		ServiceChannel sc = new ServiceChannelImpl();
		allJobs.put(sc.toString(), sc);
		return sc;
	}

	
	Map<String, String> globalEnvis = 
			Collections.synchronizedMap(new HashMap<String, String>());
	
	final Map<String, ServiceChannel> allJobs =
			Collections.synchronizedMap(new HashMap<String, ServiceChannel>());
	
	//TODO 遍历allJobs，处理话单。
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}

class EngineStubb {
	
}
