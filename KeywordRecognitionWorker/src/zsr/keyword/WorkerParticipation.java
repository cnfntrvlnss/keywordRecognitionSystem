package zsr.keyword;

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

import static zsr.keyword.FuncUtil.*;

/**
 * 连接management端口，发送本机识别服务的端口号，并处理与center的文件同步。
 * @author thinkit
 *
 */
public class WorkerParticipation implements Runnable{
	private WorkerParticipation(){
		myLogger.setLevel(Level.ALL);
		myLogger.setUseParentHandlers(false);
		Handler h= new ConsoleHandler();
		h.setLevel(Level.ALL);
		myLogger.addHandler(h);
		
		
		connThread = new Thread(this, "worker participation");
		connThread.start();
	}
	public static WorkerParticipation onlyOne = new WorkerParticipation();
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			Socket s = null;
			try{
				Thread.sleep(5000);
				//TODO: make sure that recogServer is valid. or skip following .
				if(recogServer == null){
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
			}
			finally{
				try{
					if(s != null) s.close();		
				}
				catch(IOException e){
					e.printStackTrace();
				}
			}
		}
	}

	
	void setRecogServer(WorkerInfo worker) {
		this.recogServer = worker;
	}
	WorkerInfo getRecogServer() {
		return recogServer;
	}
	
	Thread connThread;
	String centerIp = "localhost";
	int centerPort = 8828;
	WorkerInfo recogServer;
	GlobalEnviroment GE;
	String dataRoot = "D:\\keywordRecognition\\idxData\\";
	Logger myLogger = Logger.getLogger("zsr.keyword");

	
	public static void main(String[] args) {
		WorkerParticipation.onlyOne.setRecogServer(new WorkerInfo(1, "localhost", 7878));
		try{
			WorkerParticipation.onlyOne.clientThread.join();	
		}
		catch(InterruptedException e){
			e.printStackTrace();
		}
	}
}
