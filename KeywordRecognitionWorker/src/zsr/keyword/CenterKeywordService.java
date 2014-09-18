/**
 * 大致流程是CenterKeywordService接口提供关键词识别功能；它的实现会通过分割任务，汇总结果的形式完成任务。分割后的任务，待汇总的结果
 * 会通过WorkerKeywordService接口传递。
 */
 package zsr.keyword;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
/**
 * when storing global variable name, format $xxx.
 * 提供并行处理任务的API
 * @author thinkit
 *
 */
public interface CenterKeywordService {

	interface ServiceChannel{
		BlockingQueue<KeywordRequestPacket> getRequestQueue();
		BlockingQueue<KeywordResultPacket> getResultQueue();
		void close();
	}
	void addGlobalEnvi(Map<String, String> map);
	void removeGlobalEnvi(Set<String> set);
	Set<String> getGlobalVariable();
	Map<String, String> getGlobalEnvi(Set<String> s); 
	//唯一产生ServiceChannel的工厂函数。
	ServiceChannel allocateOneChannel();
}
