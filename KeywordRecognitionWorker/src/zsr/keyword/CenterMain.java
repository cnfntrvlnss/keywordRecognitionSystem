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
 * 要处理的作业的类型：在线识别，历史检索.
 * 
 * @author thinkit
 *
 */
public class CenterMain implements Runnable {

	private CenterMain(){
		Properties defaultSettings = new Properties();
		defaultSettings.put("audio_url", "localhost");
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
		map.put("audio_url", settings.getProperty("audio_url"));
		centerService.addGlobalEnvi(map);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	/**
	 * 一线流程，作业处理的全部逻辑。通过socket输入输出。
	 * 数据格式：参照JobStruct, JobResultStruct, 
	 * 另外的几个信令：JobIncomeMessage， {feedback: ok or fail or ...}
	 * 交互协议：
	 * 1. 发送一个JobStruct的json字符串，若回答{feedback: ok}， 继续。
	 * 2. 发送JobIncomeMessage的一个json字符串，根据其内容，返回统计信息，部分结果，全部结果等。
	 * 3. 全部结果的返回也许不完整，有可能通过超时强制作业完成的。
	 * 期待一个JobResultStruct的json字符串。
	 * 两个相邻JobStruct的Id不能相等。
	 * 同一线上可处理在线识别和历史检索两种作业，当前的关键词列表没有指定，就复用前面的关键词列表。
	 * 
	 * @author Administrator
	 *
	 */
	private class JobChannel implements Runnable{
		
		String globNameOnline = "$keywords1"+ Thread.currentThread().getId();
		String globNameOffline = "$Keywords2"+ Thread.currentThread().getId();
		
		String kwTmpStoredOnline = "";
		String kwTmpStoredOffline = "";
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
			String pktKw;
			int totalNum = 0; //返回对象要保存的信息。

			if(js.type.equals("1")){
				//若keywords为空，就复用之前的keywords；若不为空，就用当前的keywords.
				//若keywords.length()大于20，就借用globName..., 否则，一概用原始keywords.				
				if(js.keywords.equals("")){
					if(kwTmpStoredOnline.length()<=20){
						pktKw = kwTmpStoredOnline;
					}
					else{
						pktKw = globNameOnline;
					}
				}
				else if(js.keywords.length()<20){
					kwTmpStoredOnline = js.keywords;
					pktKw = kwTmpStoredOnline;
				}
				else {
					kwTmpStoredOnline = js.keywords;
					pktKw = globNameOnline;
					Map<String, String> tmpMap = new HashMap<String, String> ();
					tmpMap.put(globNameOnline, js.keywords);
					centerService.addGlobalEnvi(tmpMap);
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
				if(js.keywords.equals("") && kwTmpStoredOffline.equals("")){
					return null;
				}
				if(js.keywords.equals("")){
					if(kwTmpStoredOffline.length()<=20){
						pktKw = kwTmpStoredOffline;
					}
					else{
						pktKw = globNameOffline;
					}
				}
				else if(js.keywords.length()<20){
					kwTmpStoredOffline = js.keywords;
					pktKw = kwTmpStoredOffline;
				}
				else {
					kwTmpStoredOffline = js.keywords;
					pktKw = globNameOffline;
					Map<String, String> tmpMap = new HashMap<String, String> ();
					tmpMap.put(globNameOffline, js.keywords);
					centerService.addGlobalEnvi(tmpMap);
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
			// TODO 流程还不完整。
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
						JobResultStruct jrs = toService(js, sc);
						if(jrs == null){
							//TODO 记录log, 发送fail 
							out.write("{feeback:\"fail\"}");
							continue;
						}
						out.write("{feedback:\"ok\"}");
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
				tmpSet.add(globNameOnline);
				tmpSet.add(globNameOffline);
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

























