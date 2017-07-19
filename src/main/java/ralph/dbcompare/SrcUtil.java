package ralph.dbcompare;

import java.util.List;

public class SrcUtil extends MyUtil {
	List<String> srcTables;
	@Override	
	protected String getConnString() {
		return "jdbc:oracle:thin:@192.168.0.9:1521:ORCL";
	}
	@Override
	protected List<String> getTableList(){
		return srcTables;
	}
	@Override
	protected void setTableList(List<String> tableList){
		srcTables = tableList;
	}
	@Override
	protected String getTableName(String tableName) {
		return tableName;
	}
}
