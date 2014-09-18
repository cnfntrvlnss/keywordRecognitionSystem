package zsr.keyword;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 * @author thinkit
 *
 */
public class EngineKeywordService implements CenterKeywordService{
	private EngineKeywordService(){
		
	}
	private static EngineKeywordService only = null;
	public static EngineKeywordService getOnlyInstance(){
		if(only == null) only = new EngineKeywordService();
		return only;
	}
	@Override
	public void addGlobalEnvi(Map<String, String> map) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeGlobalEnvi(Set<String> set) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<String> getGlobalVariable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getGlobalEnvi(Set<String> s) {
		// TODO Auto-generated method stub
		return null;
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

	
	
	final Map<String, ServiceChannel> allJobs =
			Collections.synchronizedMap(new HashMap<String, ServiceChannel>());
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
