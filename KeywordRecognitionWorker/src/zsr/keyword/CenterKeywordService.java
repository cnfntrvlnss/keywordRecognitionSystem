/**
 * 大致流程是CenterKeywordService接口提供关键词识别功能；它的实现会通过分割任务，汇总结果的形式完成任务。分割后的任务，待汇总的结果
 * 会通过WorkerKeywordService接口传递。
 */
 package zsr.keyword;

import java.util.Iterator;
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

	/**
	 * 提供一路的服务通道，即，由此送入的请求，再由此吐出结果。
	 * @author Administrator
	 *
	 */
	interface ServiceChannel{
		BlockingQueue<KeywordRequestPacket> getRequestQueue();
		BlockingQueue<KeywordResultPacket> getResultQueue();
		void close();
	}
	ServiceChannel allocateOneChannel();
	
	/**
	 * 下面的4个函数是全局变量的更新函数，对于每个位于下游的keywordService节点继承一份。
	 * 这4个函数，要求是线程安全的。
	 * @param map
	 */
	void addGlobalEnvi(Map<String, String> map);
	void removeGlobalEnvi(Set<String> set);
	Set<String> getGlobalVariable();
	Map<String, String> getGlobalEnvi(Set<String> s);
		
}
/**
 * *一个节点是实现了这个接口的类和实现了下个接口的类的集合。
 * *下游是指向引擎方向的，上游是指向centerMain方向的。
 * *有 EngineKeywordService 和 WorkerManagement 实现这个接口。
 * 功能是，传递在合并过程中的指令与状态信息。
 * @author Administrator
 *
 */
interface IdxfileSupportService {
	/**
	 * 启动从相邻下游节点向本节点合并索引文件。由上向下传递此命令。
	 */
	void setMergeProcess();
	/**
	 * 由调用者查询本节点上的所有索引文件已从下游节点合并完毕。表明此节点的索引文件是完整的。
	 * @return
	 */
	boolean isMergeComplete();
	/**
	 * *对于EngineKeywordService, 没有下游节点，不需要exchanger的download逻辑。
	 * *对于centerMain上实现此接口的类，由于没有workerMain,所以就没有调用此函数，就不再传递索引文件了。
	 * @param e
	 * @return true,需要download逻辑; false, 不需要download逻辑。
	 */
	boolean setDataExchanger(IdxfileDataExchanger e);	
}

/**
 * 由所有的 WorkerMain 实现这个接口。
 * 传递相邻节点索引文件上传下载的信息。
 * @author Administrator
 *
 */
interface IdxfileDataExchanger{
	/**
	 * 从本节点向相邻上游节点传递索引文件。
	 * @param filePath
	 */
	void addUploadFile(String filePath);
	/*
	 * 向本节点的相邻下游节点传递索引文件。
	 */
	String getDownloadFile();
}

