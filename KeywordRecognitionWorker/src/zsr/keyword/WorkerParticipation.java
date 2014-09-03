package zsr.keyword;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
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
	 * 要确保写文件时，其他的程序不能读写。
	 * @param file
	 */
	void writeIdxFile(String file, byte[] data) {
		File f = new File(file);
		if(f.exists()) {
			//log: warning, while the exact file exists, receive a creat-file
			//command.
			return;
		}
		File tF = new File(file + ".tmp");
		try{
			OutputStream out = new FileOutputStream(tF);
			out.write(data);
			out.close();
			tF.renameTo(f);
		}
		catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally{
			if(tF.exists()){
				tF.delete();
			}
		}
	}
	byte[] readIdxFile(String file) {
		File f = new File(file);
		if(!f.exists()) {
			//log: warning, while the exact file doesn't exist, receive
			//a read command.
			return null;
		}
		byte[] res = null;
		try{
			InputStream in = new FileInputStream(f);
			byte[] tmpArr = new byte[1000];
			int readNum = in.read(tmpArr);
			while(readNum == 1000){
				if(res == null){
					res = tmpArr;
				}
				else {
					FuncUtil.concat(res, tmpArr);
				}
			}
			in.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
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
