package zsr.keyword;

import java.util.Map;

public interface KeywordGlobalEnviroment {
	void SetGlobalEnvi(Map<String, String> map);
	Map<String, String> getGlobalEnviAll(boolean hasValue);
	Map<String, String> getGlobalEnviOne(String key, boolean hasValue);
	Map<String, String> getGlobalEnviOne(String key);
	
}
