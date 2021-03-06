package zsr.keyword;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static zsr.keyword.FuncUtil.*;
/**
 * 实现关键词识别，检索的统一接口，内部实现包括对识别服务客户线程的实现 与管理。
 * 
 * 当大量的请求包包含少数几个大变量时，可以把大变量映射为大变量标识符，程序会在识别之前从标识符替换回相应的大变量。
 * @author Administrator
 *
 */
public class CenterKeywordServiceImpl implements CenterKeywordService, Runnable{

	private CenterKeywordServiceImpl() {
		scheduler.scheduleAtFixedRate(this, 1, 10, TimeUnit.SECONDS);
	}
	public static CenterKeywordServiceImpl onlyOne = new CenterKeywordServiceImpl();
	
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
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
	
	private Map<String, String> cloneGlobalEnvis(Integer[] outVer){
		synchronized(globalEnvis) {
			outVer[0] = globalEnvisVer;
			return new HashMap<String, String>(globalEnvis);
		}
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
		// TODO Auto-generated method stub
		ServiceChannel newOne = new ServiceChannelImpl();
		allJobs.put(newOne.toString(), newOne);
		return newOne;
	}

	/**
	 * 1。监控workerList,保证每个worker下启动的线程数量（默认为2）
	 * 2。删除终止运行的线程，辨别终止运行的异常，在非sokect异常时，向workerManager释放workerInfo.
	 * 3。监控各线程中的globalEnvis的版本号，并维护累积变量envisUpdateRecords.
	 * 4. 把allTaskChannels的队列集与全局队列reqQueue/resQueue连接起来。
	 */
	@Override
	public void run() {
		//TODO this对象生成时要周期性的运行这部分代码。
		//维护到识别服务的连接通道： glAllTaskConns.
		Set<Integer> allWs =  workerWare.getMachineSet();
		for(Integer m: allWs) {
			List<DispatchTaskChannel> liConns = glAllTaskConns.get(m);
			if(liConns == null) {
				glAllTaskConns.put(m, new LinkedList<DispatchTaskChannel>());
				liConns = glAllTaskConns.get(m);
			}
			for(Iterator<DispatchTaskChannel> it=liConns.iterator(); it.hasNext();){
				if(it.next()==null || ! it.next().isValidState) {
					it.remove();
				}
			}
			while(liConns.size()<connNumPerWorker) {
		//		BlockingQueue<WorkerManagement.IdxFileSynch>[] arrIdxQue = new BlockingQueue[1];
				WorkerInfo w = workerWare.allocateOne(m); //, arrIdxQue
				if(w == null){
					//一次分配失败，就说明以前分配的同一机器下的workerInfo失效了。
					for(DispatchTaskChannel t: liConns){
						t.isValidState = false;
					}
					liConns.clear();
					break;
				}
				//打开sokect, 创建DispatchTaskChannel对象，启动线程，添加对象到管理容器。
				Socket s = null;
				try{
					s = new Socket(w.strIp, w.port);
					DispatchTaskChannel t = new DispatchTaskChannel(s);//, arrIdxQue[0]
					new Thread(t, "DispatchTaskChannel thread").start();
					liConns.add(t);
				}
				catch(Exception e){
					//对收到的所有异常，还是盲目地看作socket异常，并清空所有的机器m相关的连接对象。
					if(s != null){
						try {
							s.close();
						} catch (IOException e1) {
							e1.printStackTrace();
						}						
					}
					for(DispatchTaskChannel t: liConns){
						t.isValidState = false;
					}
					liConns.clear();
					break;
				}
				
			}//while(liConns.size())
		}//for(m)
		//前面的队列集与后面的单个队列串连起来。
		try{
			synchronized(allJobs) {
				for(String s: allJobs.keySet()){
					ServiceChannel sc = allJobs.get(s);
					sc.getRequestQueue();
					KeywordRequestPacket pkt = null;
					do{
						pkt = sc.getRequestQueue().poll();
						if(pkt != null){
							pkt.loopStack.push(s);
							reqQueue.put(pkt);
						}
					}
					while(pkt != null);
				}
				
				KeywordResultPacket pkt = null;
				do{
					pkt = resQueue.poll();
					if(pkt != null ){
						ServiceChannel sc = allJobs.get(pkt.loopStack.pop());
						if(sc != null){
							sc.getResultQueue().put(pkt);
						}
					}
				}
				while(pkt != null);
			}
		
		}
		catch(InterruptedException e){
			e.printStackTrace();
		}
		
		//维护用于保存GlobalEnvis的变动历史的变量 envisUpdateRecords。
		int smallest = Collections.max(envisUpdateRecords.keySet());
		for(Integer i: glAllTaskConns.keySet()) {
			if(glAllTaskConns.get(i) != null){
				for(DispatchTaskChannel t: glAllTaskConns.get(i)) {
					if(t.curGEnviVersion != 0 && t.curGEnviVersion<smallest){
						smallest = t.curGEnviVersion;
					}
				}
			}
		}
		for(Iterator<Map.Entry<Integer, Map<String,String>>> it=envisUpdateRecords.entrySet().iterator();it.hasNext(); ){
			if(it.next().getKey() < smallest){
				it.remove();
			}
		}
	}

	/**
	 * implement client side of keyword service.
	 * 
	 * @author thinkit
	 *
	 */
	class DispatchTaskChannel implements Runnable {
	//	Socket socket;
		ObjectOutputStream out;
		ObjectInputStream in;
	//	BlockingQueue<WorkerManagement.IdxFileSynch> idxFileQueue;
		public volatile int curGEnviVersion = 0; //definitely can be zero.
		volatile boolean isValidState = true; //currently, ignoring the cause for zero setting.
		public DispatchTaskChannel(Socket s/*, BlockingQueue<WorkerManagement.IdxFileSynch> idxQue*/) {
			try{
		//		this.idxFileQueue = idxQue;
				// send globalEnvis.
				out = new ObjectOutputStream(s.getOutputStream());
				Integer[] refVer = new Integer[1];
				out.writeObject(cloneGlobalEnvis(refVer));
				curGEnviVersion = refVer[0];
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
		
		@Override
		public void run() {
			try{
				while(isValidState) {
					if(envisUpdateRecords.get(curGEnviVersion+1) != null){
						out.writeObject(envisUpdateRecords.get(curGEnviVersion+1));
						curGEnviVersion ++;
					}
					//发送一条请求包，等待相应的结果；然后，下一轮的发送---回收。
					//TODO 需要添加错误处理逻辑，比如收到的结果不是有效地结果包，等待超时。
					out.writeObject(reqQueue.take());
					resQueue.put((KeywordResultPacket)in.readObject());
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
			finally{
				isValidState = false;	
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private int connNumPerWorker = 2;
	private Map<Integer, List<DispatchTaskChannel> > glAllTaskConns
	= new HashMap<Integer,List<DispatchTaskChannel>>(); //only used by run method.
	Map<String, String> globalEnvis = Collections.synchronizedMap(new HashMap<String, String>());
	//store changing action, then be used by Dispatching threads.
	//access to var globalEnvisVer, envisUpdateRecords must be locked by the above var
	//globalEnvis.
	int globalEnvisVer = 0;
	Map<Integer, Map<String, String>> envisUpdateRecords
	= Collections.synchronizedMap(new HashMap<Integer, Map<String, String>>());
	//allJobs中各队列汇集成单一的队列reqQueue和resQueue，简化后面的操作。
	final Map<String, ServiceChannel> allJobs = 
			Collections.synchronizedMap(new HashMap<String, ServiceChannel>());
	BlockingQueue<KeywordRequestPacket> reqQueue = new LinkedBlockingQueue<KeywordRequestPacket>(200000);
	BlockingQueue<KeywordResultPacket> resQueue = new LinkedBlockingQueue<KeywordResultPacket>(200000);
	Logger myLogger = Logger.getLogger("zsr.keyword");
	
	private WorkerManagement workerWare = WorkerManagement.onlyOne;

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


