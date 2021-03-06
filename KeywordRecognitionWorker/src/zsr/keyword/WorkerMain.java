package zsr.keyword;

import static zsr.keyword.FuncUtil.readFile;
import static zsr.keyword.FuncUtil.writeFile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * 提供系统内部的kewordService通讯的服务端。
 * 作为一个节点的主控程序
 * 连接workerManageService通信的服务端。
 * 
 * @author Administrator
 *
 */
public class WorkerMain implements Runnable{

	private WorkerMain(){
		Properties defualtSettings = new Properties();
		defualtSettings.put("uid", "1");
		defualtSettings.put("center_ip", "localhost");
		defualtSettings.put("center_port", "8828");
		Properties settings = new Properties(defualtSettings);
		try {
			settings.load(new FileInputStream("worker.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		iMachine = Integer.parseInt(settings.getProperty("uid"));
		notifier = new WorkerParticipation(settings.getProperty("center_ip"),
				Integer.parseInt(settings.getProperty("center_port")));
		
	}
	public static WorkerMain getWorkerObj(){
		if(onlyOrNot == null) onlyOrNot = new WorkerMain();
		return onlyOrNot;
	}
	private static WorkerMain onlyOrNot; 
	
	/**
	 * 监听连接线程。并在此线程里面，启动通信线程。
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Integer[] refStart = new Integer[1];
		refStart[0] = startPort;
		ServerSocket server= null;
		while(server != null) {
			try{
				server = new ServerSocket(refStart[0]++);
			}
			catch(IOException e) {
				e.printStackTrace();
			}
		}
		myLogger.info("WorkerMain "+iMachine+" start keywordSerices at port " + refStart[0]);
		while(! Thread.currentThread().isInterrupted()) {
			
			try{
					Socket s = server.accept();
					
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * 与中心机管理端口的通信逻辑，包括读写需要同步的idx文件。
	 * 需要周期执行。
	 * @author Administrator
	 *
	 */
	private class WorkerParticipation implements Runnable{
		 WorkerParticipation(String centerIp, int centerPort){
			 this.centerIp = centerIp;
			 this.centerPort = centerPort;
			connThread = new Thread(this, "worker participation");
			connThread.start();
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(! Thread.currentThread().isInterrupted()){
				Socket s = null;
				try{
					if(recogServer == null){
						Thread.sleep(1000);
						continue;
					}
					s = new Socket(centerIp, centerPort);
					myLogger.fine("new socket: "+"local "+s.getLocalSocketAddress()+"remote "+s.getRemoteSocketAddress());
					ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
					out.writeObject(recogServer);
					myLogger.info("have notified keyword server: "+recogServer);
					ObjectInputStream in = new ObjectInputStream(s.getInputStream());
					Object ret = in.readObject();
				/*	if(ret instanceof GlobalEnviroment){
						GE = (GlobalEnviroment) ret;
					}
					else if(ret instanceof TransferedFileSpace){
						TransferedFileSpace tf = (TransferedFileSpace)ret;
						myLogger.info("have received feedback: " + ret.toString());
						if (tf.downFiles.size()>0){
							Map<String, byte[]> allFiles = tf.downFiles;
							tf.downFiles = new HashMap<String, byte[]>();
							for(String key : allFiles.keySet()) {
								String filePath = dataRoot+ key;
								writeIdxFile(filePath, allFiles.get(key));
							}
						}
						if (tf.upFiles.size()>0) {
							//TODO: 填充upfiles中的文件内容。
							for(String key : tf.upFiles.keySet()) {
								tf.upFiles.put(key, readIdxFile(dataRoot+key));
							}
							out.writeObject(tf);
						}				
					}
					else {
						//ignore this branch.
					}
					*/
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				catch(InterruptedException e){
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
				finally{
					try{
						if(s != null) s.close();	
						Thread.sleep(60000);
					}
					catch(IOException e){
						e.printStackTrace();
					}
					catch(InterruptedException e){
						e.printStackTrace();
						Thread.currentThread().interrupt();
					}
				}
			}
		}
		
		Thread connThread;
		String centerIp;// = "localhost";
		int centerPort;// = 8828;
	}

	/**
	 * 一路workerService的通讯线程。在里面：调用keywordService接口函数完成通信逻辑。
	 * @author thinkit
	 *
	 */
	private class WorkerServiceChannel implements Runnable {
		ObjectOutputStream out;
		ObjectInputStream in;
		volatile boolean isValidState = true; //currently, ignoring the cause for zero setting.
		
		public WorkerServiceChannel(Socket s){
			try{
				in = new ObjectInputStream(s.getInputStream());
				glEnvis = (Map<String, String>) in.readObject();
				out = new ObjectOutputStream(s.getOutputStream());
				out.writeObject(new String("OK"));
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
		public void run() {
			// TODO Auto-generated method stub
			CenterKeywordService.ServiceChannel jobChannel = es.allocateOneChannel();
			try{
				while(isValidState) {
					Object obj = in.readObject();
					if(obj instanceof Map){
						@SuppressWarnings("unchecked")
						Map<String, String> m = (Map<String, String>) obj;
						for(String s: m.keySet()){
							if(m.get(s) == null){
								glEnvis.remove(s);
							}
							else{
								glEnvis.put(s, m.get(s));
							}
						}
					}
					else if(obj instanceof KeywordRequestPacket){
						//替换大变量标识符为大变量本身。
						//当前是只有关键词列表需要替换。
						KeywordRequestPacket pkt = (KeywordRequestPacket)obj;
						if(pkt.keywords != null && pkt.keywords.length()>0 
								&& pkt.keywords.charAt(0) =='$'){
							pkt.keywords = glEnvis.get(pkt.keywords);
						}
						jobChannel.getRequestQueue().put(pkt);
						out.writeObject(jobChannel.getResultQueue().take());
					}
					else {
						isValidState  = false;
						throw new MySocketInteractException("recieve unexpected object " +
								"at worker side of keyword service.");
					}
				}
			}
			catch(ClassNotFoundException e){
				e.printStackTrace();
			}
			catch(IOException e){
				e.printStackTrace();
			}
			catch(InterruptedException e){
				e.printStackTrace();
			}
			finally{
				jobChannel.close();
				isValidState = false;
				try {
					out.close();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		Map<String, String> glEnvis;
	}
	
	int iMachine;
	int startPort = 8899;
	Thread mainThread;
	WorkerInfo recogServer;
//	volatile GlobalEnviroment GE;
	Logger myLogger = Logger.getLogger("zsr.keyword");
	WorkerParticipation notifier;//
	
	EngineKeywordService es = EngineKeywordService.getOnlyInstance();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO log management would be considered later.
		/*
		myLogger.setLevel(Level.ALL);
		myLogger.setUseParentHandlers(false);
		Handler h= new ConsoleHandler();
		h.setLevel(Level.ALL);
		myLogger.addHandler(h);
		*/
		WorkerMain worker = WorkerMain.getWorkerObj();
	}

}
