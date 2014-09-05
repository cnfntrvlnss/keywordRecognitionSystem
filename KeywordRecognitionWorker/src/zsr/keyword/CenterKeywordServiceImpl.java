package zsr.keyword;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CenterKeywordServiceImpl implements CenterKeywordService{

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addGlobalEnvi(Map<String, String> map) {
		// TODO Auto-generated method stub
		globalEnvis.putAll(map);
	}

	@Override
	public void removeGlobalEnvi(Set<String> set) {
		// TODO Auto-generated method stub
		for(String e : set) {
			globalEnvis.remove(e);
		}
	}

	@Override
	public Set<String> getGlobalVariable() {
		return  new HashSet<String>(globalEnvis.keySet());
	}

	@Override
	public Map<String, String> getGlobalEnvi(Set<String> varSet) {
		Map<String, String> ret = new HashMap<String, String> ();
		synchronized(globalEnvis){
			for(String k : globalEnvis.keySet()) {
				ret.put(k, globalEnvis.get(k));
			}			
		}
		return ret;
	}

	@Override
	public BlockingQueue<KeywordRequestPacket> getRequestQueue() {
		// TODO Auto-generated method stub
		return reqQueue;
	}

	@Override
	public BlockingQueue<KeywordResultPacket> getResultQueue() {
		// TODO Auto-generated method stub
		return resQueue;
	}
//TODO: how do we synchronize the change of global variables
	Map<String, String> globalEnvis = Collections.synchronizedMap(new HashMap<String, String>());
	BlockingQueue<KeywordRequestPacket> reqQueue = new LinkedBlockingQueue<KeywordRequestPacket>(200000);
	BlockingQueue<KeywordResultPacket> resQueue = new LinkedBlockingQueue<KeywordResultPacket>(200000);
}

/**
 * implement client side of worker service protocol.
 * process idx file transparently.
 * @author thinkit
 *
 */
class DispatchTaskChannel implements Runnable {
	Socket socket;
	Map<String, String> changed
	public DispatchTaskChannel(Socket s) {
		this.socket = s;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}

