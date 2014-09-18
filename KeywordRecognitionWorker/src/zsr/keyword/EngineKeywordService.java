package zsr.keyword;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
/**
 * 每个对象维护一对request/result队列，再有一个类
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


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	@Override
	public ServiceChannel allocateOneChannel() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void releaseChannel(ServiceChannel allocated) {
		// TODO Auto-generated method stub
		
	}

}
