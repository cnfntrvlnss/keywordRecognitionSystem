package zsr.keyword;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import static zsr.keyword.FuncUtil.*;

public class CenterKeywordServiceImpl implements CenterKeywordService{

	private CenterKeywordServiceImpl() {
		
	}
	public static CenterKeywordServiceImpl onlyOne = new CenterKeywordServiceImpl();
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

	@Override
	public void addGlobalEnvi(Map<String, String> map) {
		synchronized(globalEnvis) {
			envisUpdateRecords.put(++globalEnvisVer, new HashMap<String, String>(map));
			globalEnvis.putAll(map);	
		}
	}

	@Override
	public void removeGlobalEnvi(Set<String> set) {
		Map<String, String> tmpMap = new HashMap<String,String> ();
		for(String e: set){
			tmpMap.put(e, null);
		}
		synchronized(globalEnvis) {
			envisUpdateRecords.put(++globalEnvisVer, tmpMap);
			globalEnvis.keySet().removeAll(tmpMap.keySet());	
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
	
	private Map<String, String> cloneGlobalEnvis(Integer outVer){
		synchronized(globalEnvis) {
			outVer = globalEnvisVer;
			return new HashMap<String, String>(globalEnvis);
		}
	}

	@Override
	public BlockingQueue<KeywordRequestPacket> getRequestQueue() {
		// TODO Auto-generated method stub
		return reqQueue;
	}

	@Override
	public BlockingQueue<KeywordResultPacket> getResultQueue() {
		// TODO Auto-generated method stub
		return resQueue;
	}
	/**
	 * implement client side of worker service protocol.
	 * process idx file transparently.
	 * @author thinkit
	 *
	 */
	class DispatchTaskChannel implements Runnable {
	//	Socket socket;
		ObjectOutputStream out;
		ObjectInputStream in;
		Integer usedSecondAccessGEnvis = 0; //definitely can be zero.
		boolean isValidState = true;
		public DispatchTaskChannel(Socket s) {
			try{
				// send globalEnvis.
				out = new ObjectOutputStream(s.getOutputStream());
				out.writeObject(cloneGlobalEnvis(usedSecondAccessGEnvis));
				String rec = (String) in.readObject();
				if(rec != "OK") {
					throw new MySocketInteractException ("unexpected socket message, after sending gloabl envis.");
				}
			}
			catch(IOException e) {
				e.printStackTrace();
				isValidState = false;
			}
			catch(ClassNotFoundException e) {
				e.printStackTrace();
				isValidState = false;
			}
		}
		@Override
		public void finalize(){
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		/**
		 * 当前，做最简化处理：onlineSearch是建索引，搜索关键词两个完整的过程；
		 * offlineSearch只是搜索关键词的过程，若由于没有索引文件而失败就发起建索引过程。
		 * @param pag
		 * @return
		 */
		WorkerKeywordRequestPacket wrapWorkerPacket(KeywordRequestPacket pag) {
			
			return new WorkerKeywordRequestPacket(pag);	
		}
		/**
		 * 
		 * @param pag
		 * @return
		 */
		KeywordResultPacket unwrapWorkerPacket(WorkerKeywordResultPacket pag) {
			//TODO 若reqID 为 -1 就 保存idx data ，添加任务到同步队列，并返回null.
			if(pag.reqID.equals("-1")) {
				if(pag.idxFilePath!= null && pag.idxData!=null) {
					writeIdxFile(cmder.dataRoot+pag.idxFilePath, pag.idxData);
				}
			}
			//TODO 若onlineSearch 且 成功 就保存idx data, 添加任务到同步队列。
			//TODO 若offlineSearch 且 由于没有idx file而失败，查找center上有没有
			//idx data，若有就发起同步过程，若没有就追加一个ID 为-1的请求包到reqQueue.

			return null;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				while(isValidState) {
					if(envisUpdateRecords.get(usedSecondAccessGEnvis+1) != null){
						out.writeObject(envisUpdateRecords.get(usedSecondAccessGEnvis+1));
						usedSecondAccessGEnvis ++;
					}
					KeywordRequestPacket ret = reqQueue.take();
					out.writeObject(wrapWorkerPacket(ret));
					WorkerKeywordResultPacket rec = (WorkerKeywordResultPacket)in.readObject();
					resQueue.put(unwrapWorkerPacket(rec));
				}
			}
			catch(IOException e) {
				e.printStackTrace();
			}
			catch(InterruptedException e) {
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
	}

	
	
	Map<String, String> globalEnvis = Collections.synchronizedMap(new HashMap<String, String>());
//TODO: how do we synchronize the change of global variables
	//store changing action, for retrieved by Dispatching threads.
	//the modification of the following 2 vars must be locked by object gloablEnvis.
	// the access for var globalEnvisVer may be locked by object globalEnvis.
	int globalEnvisVer = 0;
	Map<Integer, Map<String, String>> envisUpdateRecords
	= Collections.synchronizedMap(new HashMap<Integer, Map<String, String>>());
	
	BlockingQueue<KeywordRequestPacket> reqQueue = new LinkedBlockingQueue<KeywordRequestPacket>(200000);
	BlockingQueue<KeywordResultPacket> resQueue = new LinkedBlockingQueue<KeywordResultPacket>(200000);
	Logger myLogger = Logger.getLogger("zsr.keyword");
	WorkerManagement cmder = WorkerManagement.onlyOne;
}

class MySocketInteractException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public MySocketInteractException() {}
	public MySocketInteractException (String grip) {
		super(grip);
	}
}


