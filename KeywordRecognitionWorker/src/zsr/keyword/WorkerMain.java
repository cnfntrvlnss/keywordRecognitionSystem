package zsr.keyword;

import static zsr.keyword.FuncUtil.readIdxFile;
import static zsr.keyword.FuncUtil.writeIdxFile;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerMain implements Runnable{

	private WorkerMain(){
		
	}
	public static WorkerMain getWorkerObj(){
		if(onlyOrNot == null) onlyOrNot = new WorkerMain();
		return onlyOrNot;
	}
	private static WorkerMain onlyOrNot; 
	
	/**
	 * 监听连接，启动识别任务处理线程。
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 与中心机管理端口的通信逻辑，包括读写需要同步的idx文件。
	 * 需要定期执行。
	 * @author Administrator
	 *
	 */
	private class WorkerParticipation implements Runnable{
		 WorkerParticipation(){
		
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
					if(ret instanceof GlobalEnviroment){
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
		String centerIp = "localhost";
		int centerPort = 8828;
	}

	private class KeywordLogicThread implements Runnable {
		
	}
	Thread mainThread;
	WorkerInfo recogServer;
	GlobalEnviroment GE;
	String dataRoot = "D:\\keywordRecognition\\idxData\\";
	Logger myLogger = Logger.getLogger("zsr.keyword");
	WorkerParticipation notifier = new WorkerParticipation();
	
	
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
