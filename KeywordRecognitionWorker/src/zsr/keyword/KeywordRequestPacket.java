package zsr.keyword;

/**
 * packet between client and center.
 * @author Administrator
 *
 */
public class KeywordRequestPacket {
	
	public KeywordRequestPacket(KeywordRequestPacket p) {
		this.id = p.id;
		this.type = p.type;
		this.keywords = p.keywords;
		this.audioFile = p.audioFile;
	}
	
	@Override
	public String toString() {
		String addStr = "id:"+id+";type:"+type.toString()+";keywords:"
				+keywords+";audioFile:"+audioFile;
		return super.toString()+" "+addStr;
	}
	/**
	 * OnlineSearch假设不存在索引文件的搜索，处理流程就是先建立索引，再返回搜索结果；同时伴随着索引的上传与同步过程，上传成功
	 * 则表明结果成功，同步过程再后续执行。
	 * OfflineSearch假设索引文件是存在，若索引文件确实没在某个worker上存在，就记录缺失情况，直接返回。返回的信息由center截获并
	 * 处理：发起索引建立/同步过程。
	 * @author thinkit
	 *
	 */
	public String id;
	KeywordRequestType type;
	/**
	 * when storing global variable name, format $xxx.
	 */
	public String keywords;
	public String audioFile;	

}
/**
 * packet between center and worker.
 * @author Administrator
 *
 */
class WorkerKeywordRequestPacket extends KeywordRequestPacket {
	public WorkerKeywordRequestPacket(KeywordRequestPacket p){
		super(p);
	}
	@Override
	public String toString() {
		String addStr = "";
		return super.toString() + " " + addStr;
	}
}

enum KeywordRequestType{
	OfflineSearch, OnlineSearch
}
