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
		
		
		clientThread = new Thread(this, "worker participation");
		clientThread.start();
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
				myLogger.info("have notified keyword server, "+recogServer);
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				TransferedFileSpace ret = (TransferedFileSpace)in.readObject();
				myLogger.info("have received feedback, " + ret.toString());
				if (ret.downFiles.size()>0){
					Map<String, byte[]> allFiles = ret.downFiles;
					ret.downFiles = new HashMap<String, byte[]>();
					for(String key : allFiles.keySet()) {
						String filePath = dataRoot+ key;
						writeIdxFile(filePath, allFiles.get(key));
					}
				}
				if (ret.upFiles.size()>0) {
					//TODO: 填充upfiles中的文件内容。
					for(String key : ret.upFiles.keySet()) {
						ret.upFiles.put(key, readIdxFile(dataRoot+key));
					}
					out.writeObject(ret);
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
	
	Thread clientThread;
	String centerIp = "localhost";
	int centerPort = 8828;
	WorkerInfo recogServer;
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
