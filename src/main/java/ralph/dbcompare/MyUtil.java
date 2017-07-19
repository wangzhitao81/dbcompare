package ralph.dbcompare;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MyUtil {
	Connection myConn;
	Statement myStmt;
	Map<String,List<String>> columnMap = new HashMap<String,List<String>>();
	Map<String,Map<String,String>> columnDataTypeMap = new HashMap<String,Map<String,String>>();
	Map<String,Map<String,String>> columnTypeLengthMap = new HashMap<String,Map<String,String>>();
	Map<String,List<String>> triggerMap = new HashMap<String,List<String>>();
	Map<String,String> triggerTextMap = new HashMap<String,String>();
	List<String> seqList = new ArrayList<String>();
	private static final String OWNER="MYTEST";
	protected String getConnString() {
		return "";
	}
	protected List<String> getTableList(){
		return null;
	}
	protected void setTableList(List<String> tableList){
	}
	
	protected String getTableName(String tableName) {
		return null;
	}
	private Connection getConnection() {
		if(myConn !=null)
			return myConn;
		 try {
			 Class.forName("oracle.jdbc.driver.OracleDriver");
			 myConn = DriverManager.getConnection(getConnString(),"username","password");
			 return myConn;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null;
	}

	public void releaseConn() {
		if(myConn == null)
			return;
		
			try {
				myConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public ResultSet executSql(String sql) {
		Connection conn = getConnection();
		//Statement stmt = null;
		try {
			if(myStmt == null)
				myStmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			ResultSet rs =myStmt.executeQuery(sql);
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public List<String> getTables(){
		if(getTableList() != null) {
			return getTableList();
		}
		String sql = "select TABLE_NAME from user_tables  ORDER BY TABLE_NAME";
		Connection conn = getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);			
			List<String> list = new ArrayList<String>();
			while(rs.next()) {
				list.add(rs.getString("TABLE_NAME"));
			}
			rs.close();			
			setTableList(list);
			return list;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			if(stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return null;
	}
	public void initColumnMap(){
		
		String sql = "select TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH from user_TAB_COLUMNS order by TABLE_NAME";
		Connection conn = getConnection();
		
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);			
			
			while(rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				List<String> colList = columnMap.get(tableName);
				if(colList == null)
					colList = new ArrayList<String>();
				colList.add(rs.getString("COLUMN_NAME"));
				columnMap.put(tableName, colList);
				Map<String,String> dataTypeMap = columnDataTypeMap.get(tableName);
				if(dataTypeMap == null) {
					dataTypeMap = new HashMap<String,String>();
					columnDataTypeMap.put(tableName, dataTypeMap);
				}
				dataTypeMap.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
				
				Map<String,String> typeLengthMap = columnTypeLengthMap.get(tableName);
				if(typeLengthMap == null) {
					typeLengthMap = new HashMap<String,String>();
					columnTypeLengthMap.put(tableName, typeLengthMap);
				}
				typeLengthMap.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_LENGTH"));
			}
			rs.close();
			stmt.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void initTrigger() {
			String sql = "select TRIGGERING_EVENT,TRIGGER_NAME,TABLE_NAME from user_triggers";
			Connection conn = getConnection();
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				ResultSet rs =stmt.executeQuery(sql);	
				while(rs.next()) {
					String tableName = rs.getString("TABLE_NAME");
					List<String> list = triggerMap.get(tableName);
					if(list == null)
						list = new ArrayList<String>();
					list.add(rs.getString("TRIGGER_NAME"));
					triggerMap.put(tableName, list);
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				if(stmt != null)
					try {
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
	}
	
	public void initTriggerText() {

			String sql = "select NAME,TEXT from all_source where OWNER='MYTEST' and LINE=1 and type='TRIGGER' ";
			Connection conn = getConnection();
			try {
				Statement stmt = conn.createStatement();
				ResultSet rs =stmt.executeQuery(sql);	
				while(rs.next()) {
					triggerTextMap.put(rs.getString("NAME"), rs.getString("TEXT"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public List<String> initSeq(){
		if(seqList != null)
			return seqList;
		String sql = "select * from user_sequences";
		Connection conn = getConnection();
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);	
			seqList  = new ArrayList<String>();
			while(rs.next()) {
				seqList.add(rs.getString("SEQUENCE_NAME"));
			}
			return seqList;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public List<String> getColumnListByTableName(String tableName){
		return columnMap.get(tableName);
	}
	public String getColumnDataType(String tableName,String columnName){
		Map<String,String> map = columnDataTypeMap.get(tableName);
		if(map == null)
			return null;
		return map.get(columnName);
	}
	public String getColumnTypeLength(String tableName,String columnName){
		Map<String,String> map = columnTypeLengthMap.get(tableName);
		if(map == null)
			return null;
		return map.get(columnName);
	}
	public List<String> getTriggerByTableName(String tableName){
		return triggerMap.get(tableName);
	}
	public String getTriggerTextByTriggerName(String triggerName){
		return triggerTextMap.get(triggerName);
	}
	
	
	
	public static Boolean ignoreColumn(String colName) {
//		if("ID".equals(colName))
//			return true;
		if("CREATEDATE".equals(colName))
			return true;
		if("CREATETIME".equals(colName))
			return true;
		if("MODIFYDATE".equals(colName))
			return true;
		if("MODIFYTIME".equals(colName))
			return true;
		
		if("MODEDATACREATEDATE".equals(colName))
			return true;
		if("MODEDATACREATETIME".equals(colName))
			return true;
		
		//default 
		return false;
	}
	public static Boolean ignoreTable(String tableName) {
		
		if(tableName.toUpperCase().startsWith("BILL"))
			return true;
		if(tableName.toUpperCase().startsWith("BLOG"))
			return true;
		if(tableName.toUpperCase().startsWith("CAR"))
			return true;
		if(tableName.toUpperCase().startsWith("CRM"))
			return true;
		if(tableName.toUpperCase().startsWith("DOC"))
			return true;
		if(tableName.toUpperCase().startsWith("DOWNLOAD"))
			return true;
		if(tableName.toUpperCase().startsWith("ECOLOGY_LOG"))
			return true;
		if(tableName.toUpperCase().startsWith("EXP_"))
			return true;
		if(tableName.toUpperCase().startsWith("FNA"))
			return true;
		if(tableName.toUpperCase().startsWith("FULLSEARCH_"))
			return true;
		if(tableName.toUpperCase().startsWith("GM_"))
			return true;
		if(tableName.toUpperCase().startsWith("GP_ACCESS"))
			return true;
		if(tableName.toUpperCase().startsWith("HRM"))
			return true;
		if(tableName.toUpperCase().startsWith("MAIL"))
			return true;
		if(tableName.toUpperCase().startsWith("LGC"))
			return true;
		if(tableName.toUpperCase().startsWith("MEETING"))
			return true;
		if(tableName.toUpperCase().startsWith("MOBILE"))
			return true;
		if(tableName.toUpperCase().startsWith("MODEVIEWLOG"))
			return true;
		if(tableName.toUpperCase().startsWith("PR_"))
			return true;
		if(tableName.toUpperCase().startsWith("PRJ_"))
			return true;
		if(tableName.toUpperCase().startsWith("RTX"))
			return true;
		if(tableName.toUpperCase().startsWith("SAP"))
			return true;
		if(tableName.toUpperCase().startsWith("SMS"))
			return true;
		if(tableName.toUpperCase().startsWith("SOCIAL_"))
			return true;
		if(tableName.toUpperCase().startsWith("TEMP_"))
			return true;
		if(tableName.toUpperCase().startsWith("TASK_"))
			return true;
		if(tableName.toUpperCase().startsWith("TB_"))
			return true;
		if(tableName.toUpperCase().startsWith("T_INPUTREPORT"))
			return true;
		if(tableName.toUpperCase().startsWith("T_OUTREPORT"))
			return true;
		if(tableName.toUpperCase().startsWith("TAKS_"))
			return true;
		if(tableName.toUpperCase().startsWith("UF_"))
			return true;
		if(tableName.toUpperCase().startsWith("TM_"))
			return true;
		if(tableName.toUpperCase().startsWith("TRACEPR"))
			return true;
		if(tableName.toUpperCase().startsWith("VOTING"))
			return true;
		if(tableName.toUpperCase().startsWith("WEATHER"))
			return true;
		if(tableName.toUpperCase().startsWith("WEB"))
			return true;
		if(tableName.toUpperCase().startsWith("WECHAT_"))
			return true;
		if(tableName.toUpperCase().startsWith("WORKFLOW_REQUESTLOG"))
			return true;
		if(tableName.toUpperCase().startsWith("WORKPLAN"))
			return true;
		if(tableName.toUpperCase().startsWith("WORKTASK"))
			return true;
		if(tableName.toUpperCase().startsWith("BAOBIAO"))
			return true;
		if(tableName.toUpperCase().startsWith("WORKFLOW_REQUEST"))
			return true;
		if(tableName.toUpperCase().startsWith("MODEDATASHARE"))
			return true;
		if(tableName.toUpperCase().startsWith("LDAP"))
			return true;
		
		if(tableName.toUpperCase().equals("INDEXUPDATELOG"))
			return true;
		if(tableName.toUpperCase().equals("CHECKDATE"))
			return true;
		if(tableName.toUpperCase().equals("APPUSEINFO"))
			return true;
		if(tableName.toUpperCase().equals("APPDATACOUNT"))
			return true;
		if(tableName.toUpperCase().equals("FORMMODELOG"))
			return true;
		if(tableName.toUpperCase().equals("SYSMAINTENANCELOG"))
			return true;
		if(tableName.toUpperCase().equals("WORKFLOWUSECOUNT"))
			return true;
		if(tableName.toUpperCase().equals("ACTIONEXECUTELOG"))
			return true;
		if(tableName.toUpperCase().equals("EMOBILELOGINKEY"))
			return true;
		if(tableName.toUpperCase().equals("WORKFLOW_CURRENTOPERATOR"))
			return true;
		if(tableName.toUpperCase().equals("SYSPOPPUPREMINDINFONEW"))
			return true;
		if(tableName.toUpperCase().equals("PORTALLOGINFO"))
			return true;
		if(tableName.toUpperCase().equals("OFROUTEMESSAGE"))
			return true;
		if(tableName.toUpperCase().equals("OFOFFLINE"))
			return true;
		if(tableName.toUpperCase().equals("HTMLFIELDTOCLOBLOG"))
			return true;
		if(tableName.toUpperCase().equals("LMMEETINGFORTOP"))
			return true;
		if(tableName.toUpperCase().equals("LINSHI1"))
			return true;
		
		//default 
		return false;
	}
}
