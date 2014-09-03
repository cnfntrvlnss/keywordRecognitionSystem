package zsr.keyword;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class WorkerManagement implements Runnable{
	private WorkerManagement(){
		
	}
	public static WorkerManagement onlyOne = new WorkerManagement();
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		/*
		 * 监听端口的连接，对于建立的每个连接，用单独的线程交互。
		 */
		try{
			while(true) {
				ServerSocket server = new ServerSocket(8828);
				Socket s = server.accept();
				new Thread(new ClientLogicProcess(s)).start();				
			}
		}
		catch (IOException e) {
			
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
	 * @author thinkit
	 *
	 */
	class ClientLogicProcess implements Runnable {
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
				updateWorkerList(one);
				TransferedFileSpace send = currentWorkerSpace.get(one.machine).needSynchroTasks.splitAll();
				//TODO: 访问磁盘，填充文件内容。
				out.writeObject(send);
				if (!send.upFiles.isEmpty()){
					//TODO: 访问磁盘，把文件内容写到磁盘。
					TransferedFileSpace receive = (TransferedFileSpace) in.readObject();
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
	
	private final int REALLOCNUM = 5;
	private Map<Integer, StoredWorkerInfo> currentWorkerSpace = new HashMap<Integer, StoredWorkerInfo>();
//	Map<String, List<TransferedFile> > synchroTasks; 
}

//worker 的服务地址等相关信息。
class WorkerInfo implements Serializable {
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
 * 下发的文件在放到此类中下发。
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
}










