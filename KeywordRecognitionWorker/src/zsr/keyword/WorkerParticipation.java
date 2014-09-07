package zsr.keyword;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * 连接management端口，发送本机识别服务的端口号，并处理与center的文件同步。
 * @author thinkit
 *
 */
public class WorkerParticipation implements Runnable{
	private WorkerParticipation(){
		
	}
	public static WorkerParticipation onlyOne = new WorkerParticipation();
	/**
	 * 要确保写文件时，其他的程序不能获取
	 * @param file
	 */
	void writeIdxFile(String file) {
		
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try{
				Socket s = new Socket("localhost", 8828);
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
				out.writeObject(recogServer);
				TransferedFileSpace ret = (TransferedFileSpace)in.readObject();
				//TODO: 把 downfiles写到磁盘，并删除内存。
				if (ret.downFiles.size()>0){
					Map<String, Byte[]> allFiles = ret.downFiles;
					ret.downFiles = new HashMap<String, Byte[]>();
					for(String key : allFiles.keySet()) {
						String filePath = dataRoot+ key;
						writeIdxFile(filePath);
					}
				}
				if (ret.upFiles.size()>0) {
					//TODO: 填充upfiles中的文件内容。
				
					out.writeObject(ret);
				}
				
				
				
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	
	void setRecogServer(WorkerInfo worker) {
		this.recogServer = worker;
	}
	WorkerInfo getRecogServer() {
		return recogServer;
	}
	
	WorkerInfo recogServer;
	String dataRoot = "D:\\keywordRecognition\\idxData\\";
}
