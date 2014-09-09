package zsr.keyword;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
	
	public Set<Integer> getMachineSet(){
		return null;
	}
	
	public WorkerInfo allocateOne(int imach, BlockingQueue<String> [] refBQue) {
		return null;
	}

	/**
	 * 此函数的语义是之前得到的workerInfo，我不再需要它了。
	 * 只有在不是由于网络通信失败的情况下，才能调用。也可以不调用。总之，不很重要的函数。
	 * 切记，在网络通信失败的情况下调用，是严厉禁止的。
	 * @param imach
	 * @param worker
	 * @return
	 */
	public boolean releaseOne(int imach, WorkerInfo worker) {
		return false;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		/*
		 * 监听端口的连接，对于建立的每个连接，用单独的线程交互。
		 */
		
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
 * 更新相应机器信息. 若之前没有机器号，就添加新的信息，并复用同一个待同步文件列表；
 * 若已存在相应的文件列表，就修改相应的信息，若ip变了就把待同步文件列表设为相同Ip的。
 * @param worker
 */
	private void updateWorkerList(WorkerInfo worker) {
		TransferedFileSpace tmpLi = null;
		Boolean isFound = false;
		boolean ipIsChanged = false;
		synchronized (currentWorkerSpace) {
			for (Integer mach : currentWorkerSpace.keySet()) {
			if(mach.intValue() == worker.machine.intValue()) {
				if (!currentWorkerSpace.get(mach).one.strIp.equals(worker.strIp)){
					ipIsChanged = true;
				}
				currentWorkerSpace.get(mach).lastActiveTime = new Date();
				currentWorkerSpace.get(mach).reallocTime = REALLOCNUM;
				isFound = true;
			}
			if (currentWorkerSpace.get(mach).one.strIp.equals(worker.strIp)) {
				tmpLi = currentWorkerSpace.get(mach).needSynchroTasks;
			}
			}
			if (tmpLi == null) tmpLi = new TransferedFileSpace();
			if (isFound == false) {
				currentWorkerSpace.put(worker.machine, new StoredWorkerInfo(worker, tmpLi));
			}
			else if (ipIsChanged) {
				//ip换了，与ip相关的同步任务空间要变更。
				currentWorkerSpace.get(worker.machine).needSynchroTasks = tmpLi;
			}	
		}
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
				updateWorkerList(one);
				TransferedFileSpace send = currentWorkerSpace.get(one.machine).needSynchroTasks.splitAll();
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
		public StoredWorkerInfo (WorkerInfo one, TransferedFileSpace li){
			this.one = one;
			lastActiveTime = new Date();
			reallocTime = REALLOCNUM;
			needSynchroTasks = li;
		}
		
		WorkerInfo one;
		Date lastActiveTime;
		int reallocTime;
		/**
		 * 同一个workerInfo.strIp下共用相同的TransferedFileSpace.
		 */
	//	TransferedFileSpace needSynchroTasks;
	}
	
	private class AddressRelatedSpace{		
		BlockingQueue<String> allocatedQueue = new LinkedBlockingQueue<String>();
		TransferedFileSpace neededSuchTasks = new TransferedFileSpace();
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










