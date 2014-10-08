package zsr.keyword;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 * @author thinkit
 *
 */
public class EngineKeywordService implements CenterKeywordService, Runnable, IdxfileSupportService{
	private EngineKeywordService(){
		Properties defualtSettings = new Properties();
		defualtSettings.put("idxfile_path", "D:\\worker1\\idxfiles\\");
		defualtSettings.put("audio_url", "localhost");
		Properties settings = new Properties(defualtSettings);
		try {
			settings.load(new FileInputStream("center.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		dataRoot = settings.getProperty("idxfile_path");

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
	
	
	@Override
	public void setMergeProcess(){
		
	}
	@Override
	public boolean isMergeComplete(){
		return true;
	}
	@Override
	public boolean setDataExchanger(IdxfileDataExchanger e){
		this.exchanger = e;
		return false;
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

	/**
	 * 从文件来源处，读取语音数据。
	 * @param path
	 * @return
	 */
	byte[] readAudioFile(String path) {
		return null;
	}
	/**
	 * 任务处理的核心流程，依次在每一个ServiceChannel上处理包任务。
	 * @param req
	 * @return
	 */
	KeywordResultPacket requestToResult(KeywordRequestPacket req){
		KeywordResultPacket ret = new KeywordResultPacket();
		String idxFile;
		ret.reqID = req.id;
		ret.type = req.type;
		ret.loopStack = req.loopStack;
		if(ret.type == KeywordRequestType.OnlineSearch){
			byte[] audioData = readAudioFile(req.audioFile);
			if(audioData == null) {
				ret.res = KeywordResultType.fileUnavailableError;
				return ret;
			}
		}
	/*	else if(ret.type == KeywordRequestType.OfflineSearch){
			
		}
	*/	
		return ret;
	}
	//TODO 遍历allJobs，处理话单。
	//TODO 放入周期性的scheduler 中 。
	@Override
	public void run() {
		// TODO Auto-generated method stub
		List<ServiceChannel> scLi = new ArrayList<ServiceChannel>();
		synchronized(allJobs) {
			for(String k: allJobs.keySet()) {
				scLi.add(allJobs.get(k));
			}
		}
		for(ServiceChannel sc: scLi){
			while(sc.getRequestQueue().peek() != null) {
				KeywordRequestPacket pkt = sc.getRequestQueue().poll();
				
			}
			
		}
	}

	String dataRoot;
	IdxfileDataExchanger exchanger;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}

class NativeEngineStubb {
	static final public int recogConcurrency = 1;
	/**
	 * 
	 * @param idx 0--->(recogConcurrency
	 * @return 0---free; 1---busy; 2---complete.
	 */
	static int getStatus(int idx){
		return 0;
	}
	static boolean handRequestPacket(KeywordRequestPacket req, KeywordResultPacket res){
		return true;
	}
	static KeywordResultPacket retrieveResultPacket(){
		return null;
	}
	static final private int[] statArr;
	static {
		statArr = new int[recogConcurrency];
	}
}



