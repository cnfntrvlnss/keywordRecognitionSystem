package zsr.keyword;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static zsr.keyword.FuncUtil.*;

/**
 * 需求： 在随意变更worker的socket服务地址时，center会正常运行。
 * 功能：logic of worker management's socket service. eg: centralized management of worker
 * info, transfer idx file between center and worker. 
 * @author Administrator
 *
 */
public class WorkerManagement implements Runnable{
	private WorkerManagement(){
		myLogger.setLevel(Level.ALL);
		myLogger.setUseParentHandlers(false);
		Handler h= new ConsoleHandler();
		h.setLevel(Level.ALL);
		myLogger.addHandler(h);
		
		manaThread = new Thread(this,"worker management");
		manaThread.start();
	}
	public static WorkerManagement onlyOne = new WorkerManagement();
	
	/**
	 * called before allocateOne.
	 * @return
	 */
	public Set<Integer> getMachineSet(){
		synchronized (currentWorkerSpace) {
			return new HashSet<Integer> (currentWorkerSpace.keySet());
		}
	}
	/**
	 * 一次分配失败，就说明以前分配的同一机器下的workerInfo失效了。
	 * 当多次连续分配超限时，即，对于同一个machine, 在两次AddOnWorkerList的调用中，除去release的个数，allocate
	 * 的次数超过REALLOCNUM时，就会在workerList中删除这条记录。
	 * @param imach
	 * @param refBQue
	 * @return
	 */
	public WorkerInfo allocateOne(int imach, BlockingQueue<String> [] refBQue) {
		WorkerInfo ret = null;
		synchronized(currentWorkerSpace) {
			if(!currentWorkerSpace.containsKey(imach) || currentWorkerSpace.get(imach) == null) {
				return null;
			}
			if (currentWorkerSpace.get(imach).reallocTime == 0) {
				myLogger.info("the workerInfo: "+currentWorkerSpace.get(imach).one+" is out of date, then don't allocate it, and remove it.");
				removeOnWorkerList(currentWorkerSpace.get(imach).one);
				return null;
			}
			if (refBQue != null) {
				ret = currentWorkerSpace.get(imach).one;
				refBQue[0] = idxFileSpace.get(ret.strIp).taskQueue;
				currentWorkerSpace.get(imach).reallocTime --;
			}
		}
		return ret;
	}

	/**
	 * 此函数的语义是之前得到的workerInfo不再需要了。
	 * 只有在不是由于网络通信失败的情况下，才能调用。也可以不调用。总之，不很重要的函数。
	 * @param imach
	 * @param worker
	 * @return
	 */
	public boolean releaseOne(int imach, WorkerInfo worker) {
		synchronized(currentWorkerSpace) {
			if(currentWorkerSpace.get(imach).reallocTime != REALLOCNUM) {
				currentWorkerSpace.get(imach).reallocTime ++;
			}
		}
		return true;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			ClientThreadCab cab = new ClientThreadCab();
			ServerSocket server = new ServerSocket(8828);
			while(! Thread.interrupted()) {
				try{
					if(cab.hasRoom()){
						Socket s = server.accept();
						myLogger.fine("a new socket: local "+s.getLocalSocketAddress()+"; remote "+s.getRemoteSocketAddress());
						Thread t = new Thread(new ClientLogicProcess(s), "client process");
						t.start();
						cab.addThread(t);
					}
					else{
						Thread.sleep(2000);
						myLogger.warning("the client threads alive is counted to max_num, then couldn't accept new one.");
					}		
				}
				catch(IOException e) {
					e.printStackTrace();
				}
				catch(InterruptedException e) {
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
			}
			cab.joinAll();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 若参数中的机器信息不在列表中，就增加一项；
	 * 若参数中的机器信息已在列表中存在，且内容相等，就更新一下实时信息；
	 * 若参数中的机器信息已在列表中存在，但内容不相等，就操作失败。
	 * @param worker
	 * @param synchSpace 返回从跟机器信息相应的同步文件结构中截掉的同步文件信息。
	 * 
	 */
	private boolean addOnWorkerList(WorkerInfo worker, TransferedFileSpace[] refSynchSpace) {
		synchronized(currentWorkerSpace) {
			int iMach = worker.machine;
			if(currentWorkerSpace.containsKey(iMach) && currentWorkerSpace.get(iMach)
					!= null ) {
				if(currentWorkerSpace.get(iMach).one.equals(worker)) {
					//update real time Info.
					currentWorkerSpace.get(iMach).lastActiveTime = new Date();
					currentWorkerSpace.get(iMach).reallocTime  = REALLOCNUM;
					if (refSynchSpace != null) {
						refSynchSpace[0] = idxFileSpace.get(worker.strIp).synchSpace.splitAll();
					}
					return true;
				}
				else {
					myLogger.warning("ignore the coming workerInfo, for not being consistent with the workerInfo being used currently.");
					return false;
				}
			}
			boolean hasAddr = false;
			for(Integer mach : currentWorkerSpace.keySet()) {
				if(worker.strIp.equals(currentWorkerSpace.get(mach).one.strIp)) {
					hasAddr = true;
				}
			}		
	
			if (hasAddr == false) {
				idxFileSpace.put(worker.strIp, new AddressRelatedSpace());
			}
			
			currentWorkerSpace.put(worker.machine, new StoredWorkerInfo(worker));
			return false;
		
		}
	}
	
	/**
	 * the only way to remove worker info from the stored list.
	 * @param worker
	 * @return
	 */
	private boolean removeOnWorkerList(WorkerInfo worker) {
		synchronized(currentWorkerSpace) {
			currentWorkerSpace.remove(worker.machine);
			boolean hasAddr = false;
			for (Integer i: currentWorkerSpace.keySet()) {
				if(worker.strIp.equals(currentWorkerSpace.get(i).one.strIp)) {
					hasAddr = true;
					break;
				}
			}
			if(hasAddr == false) {
				idxFileSpace.remove(worker.strIp);
			}
		}
		
		return false;
	}
	
	/**
	 * worker管理端口（8828端口）的处理逻辑。
	 * protocal 1-1:
	 * 1. c->s: workerInfo
	 * 2. s->c: TransferedFileSpace
	 * 3. c->s: if TranferedFileSpace::upFiles has values, then TransferedFileSpace.
	 * @author thinkit
	 *
	 */
	private class ClientLogicProcess implements Runnable {
		public ClientLogicProcess(Socket socket) {
			this.socket = socket;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			//从这儿，用一种方式，可获取worker/center中缺失的文件，放到任务队列中，
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			try{
				in = new ObjectInputStream(socket.getInputStream());
				out = new ObjectOutputStream(socket.getOutputStream());
				WorkerInfo one = (WorkerInfo)in.readObject();
				myLogger.info("received worker info: "+ one);
				TransferedFileSpace[] refSend = new TransferedFileSpace[1];
				addOnWorkerList(one, refSend);
				TransferedFileSpace send = refSend[0];
				if (send == null) {
					send = new TransferedFileSpace();
				}
				myLogger.info(send.toString());
				for(String key : send.downFiles.keySet()) {
					send.downFiles.put(key, readIdxFile(dataRoot+key));
				}
				out.writeObject(send);
				if (!send.upFiles.isEmpty()){
					TransferedFileSpace receive = (TransferedFileSpace) in.readObject();
					for(String key : receive.upFiles.keySet()) {
						writeIdxFile(dataRoot+key, receive.upFiles.get(key));
					}
				}
			}
			catch(IOException e) {
				e.printStackTrace();
			}
			catch(ClassNotFoundException e) {
				e.printStackTrace();
			}
			finally{
				try{
					in.close();
					out.close();					
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}		
		Socket socket;
	}
	/**
	 * manage batch of threads of ClientLogicProcess. 
	 * eg. maintain max num of threads alive.
	 */
	private class ClientThreadCab {
		
		boolean hasRoom(){
			ListIterator<Thread> it = allThreads.listIterator();
			while(it.hasNext()) {
				if(!it.next().isAlive()){
					it.remove();
				}
			}
			if(allThreads.size() > maxThread){
				return false;
			}
			return true;
		}
		void addThread(Thread t){
			allThreads.add(t);
		}
		void joinAll() {
			for(Thread t : allThreads) {
				try{
					t.join();			
				}
				catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		List<Thread> allThreads = new LinkedList<Thread>();
		static final int maxThread = 20;
	}
	/**
	 * 保存worker信息，保存将要通过8828端口与相应的worker交互的信息。
	 * @author thinkit
	 *
	 */
	private class StoredWorkerInfo{
		/**
		 * 
		 * @param one
		 * @param li not allowed to be null.
		 */
		public StoredWorkerInfo (WorkerInfo one){
			this.one = one;
			lastActiveTime = new Date();
			reallocTime = REALLOCNUM;
		}
		
		WorkerInfo one;
		Date lastActiveTime; // useless temporarily.
		int reallocTime;
		/**
		 * 同一个workerInfo.strIp下共用相同的TransferedFileSpace.
		 */
	//	TransferedFileSpace needSynchroTasks;
	}
	
	private class AddressRelatedSpace{		
		BlockingQueue<String> taskQueue = new LinkedBlockingQueue<String>();
		TransferedFileSpace synchSpace = new TransferedFileSpace();
	}
	
	Thread manaThread;
	int centerPort = 8828;
	String dataRoot = "D:\\keywordCenter\\idxData\\";
	private final int REALLOCNUM = 5;
	/**
	 * 对两个Map对象的操作基本上是同步的：
	 * 当在currentWorkerSpace中添加一项时，且出现了新的地址，就在idxFileSpace中添加新的地址。
	 * 当在currentWorkerSpace中删除某项时，且相应的地址也失效了，就在idxFileSpace中删除相应的项目。
	 * 当在currentWorkerSpace中更新某项的workerInfo信息时，---暂时不允许这样的情况存在，可以通过内部的交互分解成
	 * 先删除再添加。
	 */
	private Map<Integer, StoredWorkerInfo> currentWorkerSpace = 
			Collections.synchronizedMap(new HashMap<Integer, StoredWorkerInfo>());
	private Map<String, AddressRelatedSpace> idxFileSpace = 
			Collections.synchronizedMap(new HashMap<String, AddressRelatedSpace>());
//	Map<String, List<TransferedFile> > synchroTasks; 
	Logger myLogger = Logger.getLogger("zsr.keyword");
	
	//for test...
	public static void main(String[] args) {
		try{
			WorkerManagement.onlyOne.manaThread.join();	
		}
		catch(InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}

//server related info of worker.。
class WorkerInfo implements Serializable {
	public WorkerInfo(){
		
	}
	public WorkerInfo(int iMachine, String strIp, int port){
		this.machine = iMachine;
		this.strIp = strIp;
		this.port = port;
	}
	@Override
	public String toString() {
		String allFields = "strIp:" + strIp+"; port:"+port+"; machine:"+machine;
		return super.toString()+"  "+allFields;
	}
	@Override
	public boolean equals(Object oth) {
		if(oth == null) return false;
		if(getClass() == oth.getClass() ) {
			if(super.equals(oth) &&
					strIp.equals(((WorkerInfo) oth).strIp) &&
					port.equals(((WorkerInfo) oth).port) &&
					machine.equals(((WorkerInfo) oth).machine)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String strIp;
	Integer port;
	Integer machine;
	
}

/**
 * used for synchronization of distributed files。
 */
class TransferedFileSpace implements Serializable {
	public TransferedFileSpace() {
		
	}
	synchronized public void pushOne(String file, boolean isDown) {
		if (isDown){
			downFiles.put(file, null);
		}
		else {
			upFiles.put(file, null);
		}
	}
	@Override
	synchronized public String toString(){
		String add = "downFiles:";
		int cnt = 0;
		for (String ky : downFiles.keySet()) {
			if(cnt !=0) add += ",";
			add += ky;
		}
		add += ";upFiles:";
		cnt = 0;
		for(String ky : upFiles.keySet()) {
			if(cnt != 0) add += ",";
			add +=ky;
		}
		add += ";";
		return super.toString()+" "+add;
	}
	synchronized public TransferedFileSpace splitAll() {
		TransferedFileSpace ret = new TransferedFileSpace();
		if (!downFiles.isEmpty()){
			Map<String,byte[]> tmp = downFiles;
			downFiles = ret.downFiles;
			ret.downFiles = tmp;
		}
		if (!upFiles.isEmpty()) {
			Map<String, byte[]> tmp = upFiles;
			upFiles = ret.upFiles;
			ret.upFiles = tmp;
		}
		return ret;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, byte[]> downFiles = new HashMap<String, byte[]>();
	Map<String, byte[]> upFiles = new HashMap<String, byte[]>();
	
	public static void main(String[] args) {
		
	}
}










