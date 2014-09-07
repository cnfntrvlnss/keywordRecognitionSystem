package zsr.keyword;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
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
						myLogger.info("the client threads alive is counted to max_num, then couldn't accept new one.");
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
 * 更新保存的机器号列表，若之前没有机器号，就添加新的信息，并复用同一个待同步文件列表；
 * 若已存在相应的文件列表，就修改相应的信息，若ip变了就把待同步文件列表设为相同Ip的。
 * @param worker
 */
	private void updateWorkerList(WorkerInfo worker) {
		TransferedFileSpace tmpLi = null;
		Boolean isFound = false;
		boolean ipIsChanged = false;
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
		if (isFound == false) {
			if (tmpLi == null) tmpLi = new TransferedFileSpace();
			currentWorkerSpace.put(worker.machine, new StoredWorkerInfo(worker, tmpLi));
		}
		else if (ipIsChanged){
			if (tmpLi == null) tmpLi = new TransferedFileSpace();
			currentWorkerSpace.get(worker.machine).needSynchroTasks = tmpLi;
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
	 * manage batch of threads executing ClientLogicProcess.
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
	class StoredWorkerInfo{

		public StoredWorkerInfo (WorkerInfo one, TransferedFileSpace li){
			this.one = one;
			lastActiveTime = new Date();
			reallocTime = REALLOCNUM;
			needSynchroTasks = li;
		}
		
		WorkerInfo one;
		Date lastActiveTime;
		int reallocTime;
		TransferedFileSpace needSynchroTasks;
	}
	
	Thread manaThread;
	int centerPort = 8828;
	String dataRoot = "D:\\keywordCenter\\idxData\\";
	private final int REALLOCNUM = 5;
	private Map<Integer, StoredWorkerInfo> currentWorkerSpace = new HashMap<Integer, StoredWorkerInfo>();
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










