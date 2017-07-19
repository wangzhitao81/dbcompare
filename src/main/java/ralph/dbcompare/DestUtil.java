package ralph.dbcompare;


public class DestUtil extends MyUtil {
	@Override	
	protected String getConnString() {
		return "jdbc:oracle:thin:@192.168.0.100:1521:ORCL";
	}
	@Override
	protected String getTableName(String tableName) {
		return tableName+"@DEST";
	}
}
