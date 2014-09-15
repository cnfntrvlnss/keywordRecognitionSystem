package zsr.keyword;

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
	KeywordRequestType type;
	KeywordResultType res;
	public String comment;
}

class WorkerKeywordResultPacket extends KeywordResultPacket{
	
	@Override
	public String toString(){
		return super.toString();
	}
	public byte[] idxData;
	public String workerID;
	public String idxFilePath;
}
enum KeywordResultType{
	success, fileMissError, InternalError, recognitionError
}
