/**
 * 大致流程是CenterKeywordService接口提供关键词识别功能；它的实现会通过分割任务，汇总结果的形式完成任务。分割后的任务，待汇总的结果
 * 会通过WorkerKeywordService接口传递。
 */
 package zsr.keyword;

import java.util.List;
/**
 * center的实现内容是根据管理端口提供的服务的地址，连接服务，分发任务。
 * @author thinkit
 *
 */
public interface CenterKeywordService extends KeywordGlobalEnviroment{
	int handReuqests(List<KeywordRequestPacket> requests);
	List<KeywordResultPacket> retieveResults();
}
/**
 * 功能机提供的接口，比中心机提供的接口复杂得多，会有文件的传递。
 * 从这个接口通过自定义的通讯协议，把结果和文件发送到中心机。
 * @author thinkit
 *
 */
interface WorkerKeywordService extends KeywordGlobalEnviroment{
	int handRequest(List<WorkerKeywordRequestPacket> requests);
	List<WorkerKeywordResultPacket> retieveResults();
}