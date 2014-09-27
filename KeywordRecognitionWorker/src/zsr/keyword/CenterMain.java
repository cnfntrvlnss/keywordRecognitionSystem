package zsr.keyword;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
/**
 * 关键词识别系统对外提供的接口，提供一个socket服务端口，并处理客户端连接，处理请求作业。
 * 作业及作业结果的格式是JSON。
 * 要处理的作业的类型：在线识别与建立历史，历史检索，删除历史。
 * ---删除历史的功能是后考虑进去的，留在前两个实现之后。
 * 
 * @author thinkit
 *
 */
public class CenterMain implements Runnable {

	private CenterMain(){
		Properties defaultSettings = new Properties();
		defaultSettings.put("ftp_url", "localhost");
		defaultSettings.put("ftp_usr", "root");
		defaultSettings.put("ftp_pwd", "thinkit");
		Properties settings = new Properties(defaultSettings);
		try {
			settings.load(new FileInputStream("center.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
/*		GE = new GlobalEnviroment(settings.getProperty("ftp_url"), settings.getProperty("ftp_usr"),
				settings.getProperty("ftp_pwd")); */
		Map<String, String> map = new HashMap<String, String>();
		map.put("ftp_url", settings.getProperty("ftp_url"));
		map.put("ftp_usr", settings.getProperty("ftp_usr"));
		map.put("ftp_pwd", settings.getProperty("ftp_pwd"));
		centerService.addGlobalEnvi(map);
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	private class JobChannel implements Runnable{
		
		String globName = "$keywords"+ Thread.currentThread().getId();
		boolean hasUsedGlobName = false;
		String kwTmpStoredForOnline;//上一个Job用到的keywords变量。若为null，
		                           //相应的keywords变量就用的是globEnvis中了。
		private InputStreamReader in;
		private OutputStreamWriter out;
		volatile boolean isValidState = true;
		
		public JobChannel(Socket s){
			try {
				in =  new InputStreamReader(s.getInputStream(), "UTF-8");
				out = new OutputStreamWriter(s.getOutputStream(), "UTF-8");
		} catch (IOException e) {
				e.printStackTrace();
				isValidState = false;
			}
		}
		/**
		 * 把作业元信息存入返回对象中。
		 * 抽出冗余大变量，放到globEnvis中，组装出reqPkt,发送到channel中。
		 * 
		 * @param js
		 * @param sc
		 * @return 结果对象，后续结果的收集是通过对此结果对象的操作。
		 */
		private JobResultStruct toService(JobStruct js,
				CenterKeywordService.ServiceChannel sc) {
			//当job.type为1（代表在线识别）时，若keywords为空，就复用前面的keywords；若不为空，就用当前的keywords.
			//当job.type为2(历史检索)时，都用当前的keywords，不论为空不为空。
			//若keywords.length()大于20，就借用globName, 否则，一概用原始keywords.
			String pktKw;
			int totalNum = 0;
			if(js.type.equals("1")){
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
						totalNum ++;
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
					 pkt.loopStack.push(js.id);
					 try {
						sc.getRequestQueue().put(pkt);
						totalNum ++;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				 }	
			}
			 if(totalNum > 0){
				 JobResultStruct jrs = new JobResultStruct(js.id,js.type, totalNum);
				 return jrs;
			 }
			 else{
				 return null;
			 }
			
		}
		/**
		 * 查询结果序列，若有结果就取结果包，并解析；然后， 返回。
		 * @param jrs
		 * @param sc
		 * @return
		 */
		private JobResultStruct fromService(JobResultStruct jrs,
				CenterKeywordService.ServiceChannel sc){
			while(jrs.getStatistic().cursor+jrs.getStatistic().length < jrs.getStatistic().totalNum || sc.getResultQueue().peek()!=null){
				KeywordResultPacket pkt = sc.getResultQueue().poll();
				if(pkt == null) continue;
				//在offlineSearch下，要验证job标识，保证结果与请求的一致性。
				if(pkt.type == KeywordRequestType.OfflineSearch 
						&& !jrs.id.equals(pkt.loopStack.pop())){
				}
				else{
					jrs.appendResult(pkt.reqID, pkt.res.toString());
				}
			}
			return jrs;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			CenterKeywordService.ServiceChannel sc = centerService.allocateOneChannel();
			Gson gson = new Gson();
			try{
				while(isValidState){
					// read job of string format.
					StringBuffer sb = new StringBuffer();
					char[] tmpBuf = new char[1024];
					do{
						int readNum = in.read(tmpBuf, 0, 1024);
						sb.append(tmpBuf, 0, readNum);
					}while(in.ready());
					JobStruct js = JobStruct.fromJson(sb.toString());
					if(js != null){
						out.write("{feedback:\"ok\"}");
						JobResultStruct jrs = toService(js, sc);
						while(isValidState){
							sb.delete(0, sb.length());
							int readNum = in.read(tmpBuf, 0, 1024);
							sb.append(tmpBuf, 0, readNum);
							JobIncomeMessage jim = JobIncomeMessage.fromJson(sb.toString());
							if(jim == null) {
								out.write("{feedback:\"Invalid Message\"}");
								continue;
							}
							if(jim.isQueryProgress()){
								fromService(jrs, sc);
								out.write(gson.toJson(jrs.getStatistic()));
							}
							else if(jim.isQueryResultAll()){
								/**
								 * 若10分钟之内没有更新结果，就超时退出。
								 */
								int clen = jrs.getStatistic().length;
								int llen = clen;
								int circleSecs = 5;
								int accuSecs = 0;
								try{
									while(jrs.getStatistic().cursor+jrs.getStatistic().length<
										jrs.getStatistic().totalNum && (llen != clen || accuSecs <10*60)) {
									fromService(jrs, sc);
									if(clen != jrs.getStatistic().length){
										accuSecs = 0;
										llen = clen;
										clen = jrs.getStatistic().length;
									}
									else {
										accuSecs +=circleSecs;	
									}
									Thread.sleep(circleSecs * 1000);
									}
								}
								catch(InterruptedException e) {
									e.printStackTrace();
									isValidState = false;
								}
								finally{
									out.write(gson.toJson(jrs));
								}
							}
							else if(jim.isQueryResultPart()){
								/**
								 * 若10分钟没有更新结果，就超时退出。
								 */
								int clen = jrs.getStatistic().length;
								int llen = clen;
								int circleSecs = 5;
								int accuSecs = 0;
								try{
									while(jrs.getStatistic().cursor+jrs.getStatistic().length<
											jrs.getStatistic().totalNum && (llen != clen || accuSecs <10*60)) {
										fromService(jrs, sc);
										if(clen != jrs.getStatistic().length){
											break;
										}
										else {
											accuSecs +=circleSecs;	
										}
										Thread.sleep(circleSecs * 1000);
									}
								
								}
								catch(InterruptedException e) {
									e.printStackTrace();
									isValidState = false;
								}
								finally {
									out.write(gson.toJson(jrs.splitStruct()));
								}
							}
						}
						
					}
					else {
						out.write("{feedback:\"fail\"}");
					}
				}
			}
			catch(IOException e) {
				e.printStackTrace();
			}
			
			finally{
				//释放2类资源，[重要]
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
	
	Logger myLogger = Logger.getLogger("zsr.keyword");	
	CenterKeywordService centerService = EngineKeywordService.getOnlyInstance();
	//GlobalEnviroment GE;//
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
	
	private Gson getGsonObj(){
		if(gson == null){
			gson = new Gson();
		}
		return gson;
	}
	public String toJson(){
		Gson gson = getGsonObj();
		return gson.toJson(this);
	}
	public static JobStruct fromJson(String json){
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
	private transient Gson gson;
}

/**
 * 适应通信交互过程处理逻辑的定制的收集结果的类。
 * @author Administrator
 *
 */
class JobResultStruct {
	private static class AudioResultEntity{
		AudioResultEntity(String id, String result){
			this.id = id;
			this.result = result;
		}
		@Override
		public String toString(){
			return super.toString()+" $id"+id+" $file:"+result;
		}
		String id;
		String result;
	}
	static class StatisticStruct{
		int totalNum;
		int cursor;
		int length;
	}
	public JobResultStruct(String id, String type, int totalNum) {
		this.id = id;
		this.type = type;
		this.jss = new StatisticStruct();
		this.jss.totalNum = totalNum;
		this.jss.cursor = 0;
		this.jss.length = 0;
	}
	public void appendResult(String id, String res){
		allResults.add(new AudioResultEntity(id, res));
		this.jss.length ++;
	}
	public StatisticStruct getStatistic(){
		return jss;
	}

	public JobResultStruct splitStruct(){
		JobResultStruct ret = new JobResultStruct(this.id, this.type, this.jss.totalNum);
		ret.jss.cursor = this.jss.cursor;
		for(int i=0; i<ret.jss.length; i++){
			this.jss.length --;
			this.jss.cursor ++;	
			AudioResultEntity tmp = this.allResults.remove(0);
			ret.appendResult(tmp.id, tmp.result);
		}
		return ret;
	}
	
	public String id;
	public String type;
	private StatisticStruct jss;
	private List<AudioResultEntity> allResults;
}

class JobIncomeMessage{
	private Gson getGsonObj(){
		if(gson==null){
			gson = new Gson();
		}
		return gson;
	}
	public String toJson(){
		Gson g = getGsonObj();
		return g.toJson(this);
	}
	public static JobIncomeMessage fromJson(String str){
		Gson g = new Gson();
		Type type = new TypeToken<JobIncomeMessage>(){}.getType();
		return g.fromJson(str, type);
	}
	
	boolean isQueryProgress(){
		if(name.equals("query progress")){
			return true;
		}
		return false;
	}
	boolean isQueryResultPart(){
		if(name.equals("query result") && value.equals("part")){
			return true;
		}
		return false;
	}
	boolean isQueryResultAll(){
		if(name.equals("query result") && value.equals("all")){
			return true;
		}
		return false;
	}
	private String name;
	private String value;
	private transient Gson gson;
}
/*
class GlobalEnviroment implements Serializable {

	private static final long serialVersionUID = 1L;
	public GlobalEnviroment(String root, String usr, String pwd) {
		this.ftpRoot = root;
		this.ftpUsr = usr;
		this.ftpPwd = pwd;
	}
	@Override
	public String toString() {
		String add = " ftp:"+ftpUsr+"|"+ftpPwd+"@"+ftpRoot;
		return super.toString()+add;
	}
	String ftpRoot;
	String ftpUsr;
	String ftpPwd;
}
*/

























