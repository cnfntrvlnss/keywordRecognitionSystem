package zsr.keyword;

import java.util.Deque;

public class KeywordResultPacket {
	public KeywordResultPacket(){}
	public KeywordResultPacket(KeywordResultPacket p) {
		this.reqID = p.reqID;
		this.res = p.res;
		this.comment = p.comment;
	}
	@Override
	public String toString() {
		String addStr = "reqID:"+reqID+";res:"+res+";comment:"+comment;
		return super.toString()+" "+addStr;
	}
	public String reqID;
	KeywordRequestType type;//affect to how to process the specified packet.
	public Deque<String> loopStack;
	KeywordResultType res;
	public String comment;
}

class WorkerKeywordResultPacket extends KeywordResultPacket{
	
	@Override
	public String toString(){
		return super.toString();
	}
	// 用这三个字段在一个中心机下的集群中传递索引文件。
	public byte[] idxData;
	public String workerID;
	public String idxFilePath;
}
enum KeywordResultType{
	success, fileMissError, InternalError, recognitionError
}
