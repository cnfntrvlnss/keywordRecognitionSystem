package zsr.keyword;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
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
		Properties defaultSettings = new Properties();
		defaultSettings.put("idxfile_path", "D:\\dataRoot\\idxfiles\\");
		defaultSettings.put("socket_port", "8828");
	/*	defaultSettings.put("ftp_url", "localhost");
		defaultSettings.put("ftp_usr", "root");
		defaultSettings.put("ftp_pwd", "thinkit"); */
		Properties settings = new Properties(defaultSettings);
		try {
			settings.load(new FileInputStream("center.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	/*	GE = new GlobalEnviroment(settings.getProperty("ftp_url"), settings.getProperty("ftp_usr"),
				settings.getProperty("ftp_pwd"));
				*/
		dataRoot = settings.getProperty("idxfile_path");
		centerPort = Integer.parseInt(settings.getProperty("socket_port"));
		/*
		myLogger.setLevel(Level.ALL);
		myLogger.setUseParentHandlers(false);
		Handler h= new ConsoleHandler();
		h.setLevel(Level.ALL);
		myLogger.addHandler(h);
		*/	
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
	public WorkerInfo allocateOne(int imach /*, BlockingQueue<IdxFileSynch> [] refBQue*/) {
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
			/*
			if (refBQue != null) {
				ret = currentWorkerSpace.get(imach).one;
				refBQue[0] = idxFileSpace.get(ret.strIp).taskQueue;
				currentWorkerSpace.get(imach).reallocTime --;
			}
			*/
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
		ClientThreadCab cab = new ClientThreadCab();
		try{
			ServerSocket server = new ServerSocket(centerPort);
			while(! Thread.currentThread().isInterrupted()) {
				try{
					if(cab.hasRoom()){
						Socket s = server.accept();
						myLogger.info("a new socket: local "+s.getLocalSocketAddress()+"; remote "+s.getRemoteSocketAddress());
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
	/*
	private TransferedFileSpace getSynchSpace(String strIp) {
		//附加逻辑: 把taskQueue中信息转移到synchSpace中。
		//遍历idxFileSpace要枷锁
		Set<String> allIps = null;
		synchronized(idxFileSpace) {
			allIps = new HashSet<String>(idxFileSpace.keySet());
		}
		for(String curIp: allIps) {
			BlockingQueue<IdxFileSynch> bQ = null;
			synchronized(idxFileSpace) {
				if(idxFileSpace.get(curIp)!= null){
					bQ = idxFileSpace.get(curIp).taskQueue;
				}
			}
			if(bQ != null) {
				while(true){
					IdxFileSynch synch = bQ.poll();
					if(synch == null) break;
					//追加到除当前ip之外的，其他ip下的同步文件空间中。
					if(synch.hasOrNot ==true){
						synchronized(idxFileSpace){
							for(String s: idxFileSpace.keySet()){
								if(s != curIp && idxFileSpace.get(s)!=null){
									idxFileSpace.get(s).synchSpace.pushOne(synch.idxFile, true);
								}
							}
						}
					}
					//追加到当前ip下的同步文件空间中。
					else{
						synchronized(idxFileSpace){
							if(idxFileSpace.get(curIp)!=null){
								idxFileSpace.get(curIp).synchSpace.pushOne(synch.idxFile, true);
							}
						}
					}
				}
				
			}
		}
		TransferedFileSpace ret = null;
		synchronized(idxFileSpace){
			ret = idxFileSpace.get(strIp).synchSpace.splitAll();
		}
		return 	ret;
	}
	*/
	
	/**
	 * 若参数中的机器信息不在列表中，就增加一项；
	 * 若参数中的机器信息已在列表中存在，且内容相等，就更新一下实时信息；
	 * 若参数中的机器信息已在列表中存在，但内容不相等，就操作失败。
	 * **即，只有等到机器信息自行失败删除后，才能添加新的有变动的机器信息。
	 * @param worker
	 * @param refOut 若首次添加，返回null；若非首次，就返回可能有的索引文件同步信息。
	 * @return 参数信息丢弃，返回false. 参数信息有用，返回true;
	 */
	private boolean addOnWorkerList(WorkerInfo worker /*, Object[] refOut*/) {
		synchronized(currentWorkerSpace) {
			int iMach = worker.machine;
			if(currentWorkerSpace.containsKey(iMach) && currentWorkerSpace.get(iMach)
					!= null ) {
				if(currentWorkerSpace.get(iMach).one.equals(worker)) {
					//update real time Info.
					currentWorkerSpace.get(iMach).lastActiveTime = new Date();
					currentWorkerSpace.get(iMach).reallocTime  = REALLOCNUM;
					/*
					if (refOut != null) {					
						refOut[0] = getSynchSpace(worker.strIp);
					}
					*/
					return true;
				}
				else {
					myLogger.warning("ignore the coming workerInfo, for not being consistent with the workerInfo being used currently.");
					return false;
				}
			}
/*			boolean hasAddr = false;
			for(Integer mach : currentWorkerSpace.keySet()) {
				if(worker.strIp.equals(currentWorkerSpace.get(mach).one.strIp)) {
					hasAddr = true;
				}
			}		
	
			if (hasAddr == false) {
				idxFileSpace.put(worker.strIp, new AddressRelatedSpace());
			}
			
			currentWorkerSpace.put(worker.machine, new StoredWorkerInfo(worker));
	/*		if(refOut != null) {
				refOut[0] = GE;
			} */
			return true;
		
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
//			boolean hasAddr = false;
			for (Integer i: currentWorkerSpace.keySet()) {
				if(worker.strIp.equals(currentWorkerSpace.get(i).one.strIp)) {
//					hasAddr = true;
					break;
				}
			}
//			if(hasAddr == false) {
//				idxFileSpace.remove(worker.strIp);
//			}
		}
		
		return false;
	}
	
	/**
	 * worker管理端口（8828端口）的处理逻辑。
	 * protocal 1-1:
	 * 1. c->s: workerInfo
	 * 2. s->c: TransferedFileSpace or GlobalEnviroment.
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
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			try{
				in = new ObjectInputStream(socket.getInputStream());
				out = new ObjectOutputStream(socket.getOutputStream());
				WorkerInfo one = (WorkerInfo)in.readObject();
				myLogger.info("received worker info: "+ one);
				Object[] refSend = new Object[1];
				addOnWorkerList(one /*, refSend*/);
				if(refSend[0] ==null){
					out.writeObject(new Object());
				}
				/*
				else if(refSend[0] instanceof TransferedFileSpace){
					TransferedFileSpace send = (TransferedFileSpace) refSend[0];
					myLogger.info("sending to worker: "+ send.toString());
					for(String key: ((TransferedFileSpace)send).downFiles.keySet()) {
						send.downFiles.put(key, readIdxFile(dataRoot+key));
					}
					out.writeObject(send);
					if(!send.upFiles.isEmpty()){
						TransferedFileSpace receive = (TransferedFileSpace) in.readObject();
						for(String key : receive.upFiles.keySet()) {
							writeIdxFile(dataRoot+key, receive.upFiles.get(key));
						}				
					}
				}
			else if(refSend[0] instanceof GlobalEnviroment) {
					out.writeObject(refSend[0]);
				} */
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
	/**
	 * hasOrNot为0，表示我没有此文件；hasOrNot为1，表示我有此文件。
	 * @author Administrator
	 *
	 */
	
/*	static class IdxFileSynch{
		public IdxFileSynch(boolean hasOrNot, String idxFile) {
			this.hasOrNot = hasOrNot;
			this.idxFile = idxFile;
		}
		final boolean hasOrNot;
		final String idxFile;
	}
	private static class AddressRelatedSpace{	
		final BlockingQueue<IdxFileSynch> taskQueue = new LinkedBlockingQueue<IdxFileSynch>();
		final TransferedFileSpace synchSpace = new TransferedFileSpace();
	}
*/	
	private Thread manaThread;
	private int centerPort;
//	final GlobalEnviroment GE;// = new GlobalEnviroment("localhost","root","toor");
	String dataRoot;
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
//	private Map<String, AddressRelatedSpace> idxFileSpace = 
//			Collections.synchronizedMap(new HashMap<String, AddressRelatedSpace>());
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

/**
 * 
 * @author Administrator
 *
 */
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
	@Override
	public int hashCode(){
		return machine.hashCode()+strIp.hashCode()+port.hashCode();
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
/*
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
	private static final long serialVersionUID = 1L;
	Map<String, byte[]> downFiles = new HashMap<String, byte[]>();
	Map<String, byte[]> upFiles = new HashMap<String, byte[]>();
	
	public static void main(String[] args) {
		
	}
}
*/










