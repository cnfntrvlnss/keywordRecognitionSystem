package zsr.keyword;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
/**
 * 关键词识别系统对外提供的接口，提供一个socket服务端口，并处理客户端连接，处理请求作业。
 * 作业及作业结果的格式是通用的格式JSON? xml? 或 其它。
 * 要处理的作业的类型：在线识别与建立历史，历史检索，删除历史。
 * ---删除历史的功能是后考虑进去的，留在前两个实现之后。
 * 
 * @author thinkit
 *
 */
public class CenterMain implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	private class JobChannel implements Runnable{
		
		String globName = "$keywords"+ Thread.currentThread().getId();
		boolean hasUsedGlobName = false;
		String kwTmpStoredForOnline;//上一个Job用到的keywords变量。若为null，
		                           //相应的keywords变量就用的是globEnvis中了。
		private InputStream in;
		private OutputStream out;
		volatile boolean isValidState = true;
		
		public JobChannel(Socket s){
			try {
				in =  s.getInputStream();
				out = s.getOutputStream();
		} catch (IOException e) {
				e.printStackTrace();
				isValidState = false;
			}
		}
		private void toService(JobStruct js, CenterKeywordService.ServiceChannel sc) {
			//当job.id为1（代表在线识别）时，若keywords为空，就复用前面的keywords；若不为空，就用当前的keywords.
			//当job.id为2(历史检索)时，都用当前的keywords，不论为空不为空。
			//若keywords.length()大于20，就借用globName, 否则，一概用原始keywords.
			String pktKw;
			if(js.id.equals("1")){
				if( js.keywords.equals("")){
					if(kwTmpStoredForOnline != null){
						pktKw = kwTmpStoredForOnline;
					}
					else if(hasUsedGlobName == false){
						kwTmpStoredForOnline = "";
						pktKw = kwTmpStoredForOnline;
					}
					else {
						pktKw = globName;
					}
					
				}
				else if( js.keywords.length()<=20){
					kwTmpStoredForOnline = js.keywords;
					pktKw = kwTmpStoredForOnline;
				}
				else {
					kwTmpStoredForOnline = null;
					Map<String, String> tmpMap = new HashMap<String, String> ();
					tmpMap.put(globName, js.keywords);
					centerService.addGlobalEnvi(tmpMap);
					pktKw = globName;
				}
				
				for(JobStruct.AudioEntity ae: js.audioFiles){
					KeywordRequestPacket pkt = new KeywordRequestPacket();
					pkt.id = ae.id;
					pkt.type = KeywordRequestType.OnlineSearch;
					pkt.keywords = pktKw;
					pkt.audioFile = ae.file;
					try {
						sc.getRequestQueue().put(pkt);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			else {
				 if(js.keywords.length()<=20){
					pktKw= js.keywords;
				 }
				 else {
					Map<String, String> tmpMap = new HashMap<String, String> ();
					tmpMap.put(globName, js.keywords);
					centerService.addGlobalEnvi(tmpMap);
					pktKw = globName;
				 }
				 
				 for(JobStruct.AudioEntity ae: js.audioFiles){
					 KeywordRequestPacket pkt = new KeywordRequestPacket();
					 pkt.id = ae.id;
					 pkt.type = KeywordRequestType.OfflineSearch;
					 pkt.keywords = pktKw;
					 pkt.audioFile = ae.file;
					 pkt.loopStack.add(js.id);
					 try {
						sc.getRequestQueue().put(pkt);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				 }	
			}
			
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			CenterKeywordService.ServiceChannel sc = centerService.allocateOneChannel();
			
			try{
				while(isValidState){
					
				}
			}
			finally{
				sc.close();
				Set<String> tmpSet = new HashSet<String>();
				tmpSet.add(globName);
				centerService.removeGlobalEnvi(tmpSet);
					try {
						if(in != null) in.close();
						if(out != null) out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				
			}
		}
		
	}
	
	CenterKeywordService centerService;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		JobStruct one = new JobStruct();
		one.addTestData();
		String json = one.toJson();
		System.out.println("one = " + one);
		System.out.println("one's json = " + json);
		JobStruct two = JobStruct.fromJson(json);
		System.out.println("two = " + two);
		
	}

}

class JobStruct {
	
	void addTestData (){
		id = "string";
		type = "1 or 2 or 3";
		keywords = "";
		audioFiles = new LinkedList<AudioEntity>();
		audioFiles.add(new AudioEntity("1", "xxx/xxx/file1.wav") );
		audioFiles.add(new AudioEntity("2", "xxx/xxx/file2.wav"));
	}
	String toJson(){
		Gson gson = new Gson();
		return gson.toJson(this	);
	}
	static JobStruct fromJson(String json){
		Gson gson = new Gson();
		Type type =  new TypeToken<JobStruct>(){}.getType();
		return gson.fromJson(json, type);
	}
	
	@Override
	public String toString(){
		String add = " $id:" + id+" $type:"+type+" $keywords:"+
	keywords + " $audioFiles:"+audioFiles;
		return super.toString() + add;
	}
	 static class AudioEntity{
		AudioEntity(String id, String file){
			this.id = id;
			this.file = file;
		}
		@Override
		public String toString(){
			return super.toString()+" $id:"+id+" $file:"+file;
		}
		String id;
		String file;
	}
	String id;
	String type;
	String keywords;
	List<AudioEntity> audioFiles;
}
class JobResultStruct {
	
}


























