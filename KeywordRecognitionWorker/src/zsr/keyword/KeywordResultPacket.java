package zsr.keyword;

public class KeywordResultPacket {
	public String reqID;
	public String res;
}
class WorkerKeywordResultPacket extends KeywordResultPacket{
	public String outFile;
//	public String workerID;
}