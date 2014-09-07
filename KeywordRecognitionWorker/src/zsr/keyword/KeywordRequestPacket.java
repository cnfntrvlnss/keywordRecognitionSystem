package zsr.keyword;


public class KeywordRequestPacket {
	/**
	 * OnlineSearch假设不存在索引文件的搜索，处理流程就是先建立索引，再返回搜索结果；同时伴随着索引的上传与同步过程，上传成功
	 * 则表明结果成功，同步过程再后续执行。
	 * OfflineSearch假设索引文件是存在，若索引文件确实没在某个worker上存在，就记录缺失情况，直接返回。返回的信息由center截获并
	 * 处理：若center上存在索引，就由 center发起同步流程，若center上不存在索引，就记录日志到特定介质。
	 * 至于center上存在文件破坏的情况，就单独处理把，留在以后。
	 * @author thinkit
	 *
	 */
	public enum RequestType{
		OfflineSearch, OnlineSearch
	}
	public String id;
	RequestType type;
	/**
	 * when storing global variable name, format $xxx.
	 */
	public String keywords;
	public String audioFile;	
}
class WorkerKeywordRequestPacket extends KeywordRequestPacket {
	
}