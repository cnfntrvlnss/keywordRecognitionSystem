package zsr.keyword;

import java.util.Map;
import java.util.Set;

/**
 * set predefined variable for search task being deployed later.
 * be sure the implement support synchronization
 * 
 * @author thinkit
 *
 */
public interface KeywordGlobalEnviroment {
	void addGlobalEnvi(Map<String, String> map);
	void removeGlobalEnvi(Set<String> set);
	Set<String> getGlobalVariable();
	Map<String, String> getGlobalEnvi(Set<String> s); 
}
