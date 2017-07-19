package ralph.dbcompare;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
	static SrcUtil srcUtil = new SrcUtil();
    static DestUtil destUtil  = new DestUtil();
    static Pattern pattern = Pattern.compile("[a-z]+");
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        long start = System.currentTimeMillis();
        
        try {
	        List<String> tableDiffs = tableDiff();
            System.out.println("table compared over! take "+(System.currentTimeMillis()-start)/1000+"s");
            
            start = System.currentTimeMillis();
	        seqDiff();
	        System.out.println("sequence compared over! take "+(System.currentTimeMillis()-start)/1000+"s");
	        
	        start = System.currentTimeMillis();
	        srcUtil.initColumnMap();
	        srcUtil.initTrigger();
	        srcUtil.initTriggerText();
	        
	        destUtil.initColumnMap();
	        destUtil.initTrigger();
	        destUtil.initTriggerText();
	        System.out.println("init metadata over! take "+(System.currentTimeMillis()-start)/1000+"s");

	        start = System.currentTimeMillis();
	        List<String> columnDiffTables = columnDiff(tableDiffs);
	        System.out.println("column compared over! take "+(System.currentTimeMillis()-start)/1000+"s");
	        
	        start = System.currentTimeMillis();
	        triggerDiff();
	        System.out.println("trigger compared over! take "+(System.currentTimeMillis()-start)/1000+"s");
	        
	        start = System.currentTimeMillis();
	        dataDiff(columnDiffTables);
	        System.out.println("data compared over! take "+(System.currentTimeMillis()-start)/1000+"s");
        }catch(Exception ex) {
        	ex.printStackTrace();
        }finally {
	        srcUtil.releaseConn();
	        destUtil.releaseConn();
        }
    }
    private static List<String> tableDiff() throws IOException, SQLException {
    	
    		List<String> diffList = new ArrayList<String>();
    		List<String> srcTables = srcUtil.getTables();
    		List<String> destTables = destUtil.getTables();
    		for(String src :srcTables) {
    			if((destTables.indexOf(src)  <0)) {
    				diffList.add(src);
    			}
    		}
    		
        		FileOutputStream fos = new FileOutputStream("D:\\diffs-table.txt");
        		StringBuilder sqlBuilder = new StringBuilder();
    			for(String tableName:diffList) {
    				fos.write("\r\n--".getBytes());
    				fos.write(tableName.getBytes());
    				fos.write("\r\n".getBytes());
    				sqlBuilder.delete(0, sqlBuilder.length());
    				sqlBuilder.append("SELECT DBMS_METADATA.GET_DDL('TABLE','").append(tableName).append("') AS DES FROM DUAL");
    				ResultSet rs =srcUtil.executSql(sqlBuilder.toString());
    				if(rs.next()) {
    					String tableDesc = rs.getString("DES");
    					fos.write(tableDesc.getBytes());
    					fos.write(";\r\n".getBytes());
    				}
    			}
    			fos.close();
    		

    		return diffList;
    }
    private static void dataDiff(List<String> columnDiffTables) throws IOException, SQLException {
    	List<String> srcTables = srcUtil.getTables();
    	List<String> destTables = destUtil.getTables();
    	
			FileOutputStream fos = new FileOutputStream("D:\\diffs-data.txt");
		
		    	for(String tableName:srcTables) {
		    		if(MyUtil.ignoreTable(tableName))
		    			continue;
		    		if(destTables.indexOf(tableName)<0) {
		    			fos.write(("--table "+tableName+" not exists in dest\r\n").getBytes());
		    			continue;
		    		}
		    		if(columnDiffTables.indexOf(tableName)>=0) {
		    			fos.write(("--table "+tableName+" has column diffs between src and dest\r\n").getBytes());
		    			continue;
		    		}
		    		String hashColumns = getHashColumns(tableName,srcUtil,"SRC");
		    		if(hashColumns == null || "".equals(hashColumns)) {
		    			continue;
		    		}
	    			StringBuilder rowUpdate =generateUpdateSql(tableName);
	    			StringBuilder rowInsert =generateInsertSql(tableName);
	    			if(rowUpdate !=null)  
	    				fos.write(rowUpdate.toString().getBytes());
	    			if(rowInsert != null)
	    				fos.write(rowInsert.toString().getBytes());
				}
		    	fos.flush();
		    	fos.close();
			
    }
    private static StringBuilder generateUpdateSql(String tableName) throws SQLException {
    	StringBuilder rowBuilder = new StringBuilder();
    	List<String> srcColList = srcUtil.getColumnListByTableName(tableName);
		List<String> destColList = destUtil.getColumnListByTableName(tableName);
		
				
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT SRC.* FROM ").append(tableName).append(" SRC");
		if(srcColList.indexOf("ID")>=0) {
			sqlBuilder.append(",").append(tableName).append("@TEST_FW DEST");
			sqlBuilder.append(" WHERE SRC.ID=DEST.ID AND ORA_HASH(");
		}else {
			sqlBuilder.append(" WHERE ORA_HASH(");
		}
		String hashColumns = getHashColumns(tableName,srcUtil,"SRC");
		sqlBuilder.append(hashColumns).append(") NOT IN");
		hashColumns = getHashColumns(tableName,destUtil,"");
		sqlBuilder.append("(SELECT ORA_HASH(").append(hashColumns);
		sqlBuilder.append(") FROM ").append(tableName).append("@TEST_FW)");

		System.out.println(sqlBuilder);
		ResultSet rs = srcUtil.executSql(sqlBuilder.toString());
		rs.last();
		if(rs.getRow()<1)
			return null;
		rs.beforeFirst();
		rowBuilder.append("-----").append(tableName).append("\r\n");
		while (rs.next()) {
			rowBuilder.append("UPDATE ").append(destUtil.getTableName(tableName)).append(" DEST SET ");
						
			for(String col:destColList) {
				String dataType = destUtil.getColumnDataType(tableName,col);
				if("BLOB".equals(dataType))
					continue;
				if("CLOB".equals(dataType))
					continue;
				if("CHAR".equals(dataType) || "VARCHAR".equals(dataType) || "VARCHAR2".equals(dataType)
						||"NVARCHAR2".equals(dataType) ) {
					if( rs.getString(col) == null || "null".equals(rs.getString(col)) || "NULL".equals(rs.getString(col)))
						rowBuilder.append(col).append("=").append(rs.getString(col)).append(",");
					else
						rowBuilder.append(col).append("=").append("'").append(rs.getString(col)).append("'").append(",");
				}
				else {
					rowBuilder.append(col).append("=").append(rs.getString(col)).append(",");
				}
			}
			rowBuilder.replace(rowBuilder.length()-1, rowBuilder.length(),"");
			if(srcColList.indexOf("ID")>=0)
				rowBuilder.append(" WHERE DEST.ID=").append(rs.getString("ID"));
			rowBuilder.append(";\r\n");
		}
		rs.close();
		return rowBuilder;
    }
    private static StringBuilder generateInsertSql(String tableName) throws SQLException {
    	StringBuilder rowBuilder  = new StringBuilder();
    	rowBuilder.append("-----").append(tableName).append("\r\n");
    	List<String> srcColList = srcUtil.getColumnListByTableName(tableName);
				
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT SRC.* FROM ").append(tableName).append(" SRC");
		if(srcColList.indexOf("ID")>=0) {
			sqlBuilder.append(" WHERE SRC.ID NOT IN (SELECT ID ");
		}
		else {
			sqlBuilder.append(" WHERE ORA_HASH(");
			String hashColumns = getHashColumns(tableName,srcUtil,"SRC");
			sqlBuilder.append(hashColumns).append(") NOT IN");
			
			hashColumns = getHashColumns(tableName,destUtil,"");
			sqlBuilder.append("(SELECT ORA_HASH(").append(hashColumns).append(")");
		}
		sqlBuilder.append(" FROM ").append(tableName).append("@TEST_FW)");

		System.out.println(sqlBuilder);
		ResultSet rs = srcUtil.executSql(sqlBuilder.toString());
		rs.last();
		if(rs.getRow()<1)
			return null;
		rs.beforeFirst();
		
		while (rs.next()) {
			rowBuilder.append("INSERT INTO ").append(tableName).append("(");
			
			for(String col:srcColList) {
				rowBuilder.append(col).append(",");
			}
			rowBuilder.replace(rowBuilder.length()-1, rowBuilder.length(),"");
			rowBuilder.append(")values(\r\n");
			
			for(String col:srcColList) {
				String srcDataType = srcUtil.getColumnDataType(tableName,col);
				if("BLOB".equals(srcDataType))
					continue;
				if("CLOB".equals(srcDataType))
					continue;
				if("CHAR".equals(srcDataType) || "VARCHAR".equals(srcDataType) || "VARCHAR2".equals(srcDataType)
						||"NVARCHAR2".equals(srcDataType) ) {
					if( rs.getString(col) == null || "null".equals(rs.getString(col)) || "NULL".equals(rs.getString(col)))
						rowBuilder.append(rs.getString(col)).append(",");
					else
						rowBuilder.append("'").append(rs.getString(col)).append("'").append(",");
				}
				else {
					rowBuilder.append(rs.getString(col)).append(",");
				}
			}
			rowBuilder.replace(rowBuilder.length()-1, rowBuilder.length(),"");
			rowBuilder.append(");\r\n");
		}
		rs.close();
		return rowBuilder;
    }
    private static String getHashColumns(String tableName,MyUtil myUtil,String tableAliasName) {
    	StringBuilder sb = new StringBuilder();
    	List<String> colList = myUtil.getColumnListByTableName(tableName);
    	for(String colName:colList) {
			
			if(MyUtil.ignoreColumn(colName))
				continue;
			String srcDataType = myUtil.getColumnDataType(tableName,colName);
			if("CLOB".equals(srcDataType.toUpperCase()))
				continue;
			if("BLOB".equals(srcDataType.toUpperCase()))
				continue;
			if("LONG".equals(srcDataType.toUpperCase()))
				continue;
			Matcher matcher = pattern.matcher(colName);
			if(!CommonUtil.IsNullOrEmpty(tableAliasName))
				sb.append(tableAliasName).append(".");
			if(matcher.find()) {
				sb.append("\"").append(colName).append("\"").append("||'-'||");
			}else {
				sb.append(colName).append("||'-'||");
			}
		}
    	if(sb.length()<7)
    		return "";
    	sb.replace(sb.length()-7, sb.length(), "");
    	return sb.toString();
    }
    private static void seqDiff() {
    	List<String> srcSeq = srcUtil.initSeq();
    	List<String> destSeq = destUtil.initSeq();
    	List<String> diffList = new ArrayList<String>();
    	for(String s:srcSeq) {
    		if(destSeq.indexOf(s)<0)
    			diffList.add(s);
    	}
    	try {
			FileOutputStream fos = new FileOutputStream("D:\\diffs-sequence.txt");
			fos.write("--sequence diff".getBytes());
			fos.write("\r\n".getBytes());	
			for(String s:diffList) {
				fos.write(s.getBytes());
				fos.write("\r\n".getBytes());				
			}
			fos.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    /*
alter table HTMLLABELINFOCHANGE rename column indexid to INDEXID1;
alter table HTMLLABELINFOCHANGE modify indexid number(10);
alter table HTMLLABELINFOCHANGE add aaa varchar2(4);
alter table HTMLLABELINFOCHANGE drop column indexid;
     */
    private static List<String> columnDiff(List<String> tableDiffs) {
    	 Map<String,List<String>> diffColMap = new HashMap<String,List<String>>();
    	 List<String> columnDiffTables = new ArrayList<String>();
    	 List<String> srcTables = srcUtil.getTables();
	        for(String tableName:srcTables) {
	        	if(!(tableDiffs.indexOf(tableName)<0)) {
	        		System.out.println("columnDiff,"+tableName +" not exists in dest db");
	        		continue;
	        	}
	        	
	    			List<String> srcColList =  srcUtil.getColumnListByTableName(tableName);
	    			List<String> destColList = destUtil.getColumnListByTableName(tableName);
	    			List<String> columnDiffList = new ArrayList<String>();
    				StringBuilder sqlBuilder = new StringBuilder();
	    			for(String columnName:srcColList) {
	    				sqlBuilder.replace(0, sqlBuilder.length(), "");
	    				if(destColList.indexOf(columnName)<0) {
	    					sqlBuilder.append("alter table ").append(tableName).append(" add ").append(columnName);
	    				}else{
	    					String srcDataType = srcUtil.getColumnDataType(tableName,columnName);
	    					String srcTypeLength = srcUtil.getColumnTypeLength(tableName,columnName);
	    					String destDataType = destUtil.getColumnDataType(tableName,columnName);
	    					String destTypeLength = destUtil.getColumnTypeLength(tableName,columnName);
	    					if(!CommonUtil.isEqual(srcDataType, destDataType) || !CommonUtil.isEqual(srcTypeLength, destTypeLength)){
	    						sqlBuilder.append("alter table ").append(tableName).append(" modify ").append(columnName);
	    					}else{
	    						continue;
	    					}
	    				}
	    				String typeLen = srcUtil.getColumnTypeLength(tableName,columnName);
    					if(typeLen !=null && typeLen != ""){
    						sqlBuilder.append(" ").append(srcUtil.getColumnDataType(tableName,columnName)).append("(");
    						sqlBuilder.append(typeLen).append(");\r\n");
    					}else{
    						sqlBuilder.append(" ").append(srcUtil.getColumnDataType(tableName,columnName)).append("\r\n");
    					}
    					columnDiffList.add(sqlBuilder.toString());
	    			}
	    			
	    		
	        	if(!columnDiffList.isEmpty()) {
	        		diffColMap.put(tableName, columnDiffList);
	        		columnDiffTables.add(tableName);
	        	}
	        	
	        }
	        outputCollumnDiff("D:\\diffs-collumn.txt",diffColMap);
	        System.out.println("column compared over");
	        return columnDiffTables;
    }
    private static void triggerDiff() {
    		List<String> diffTrigger = new ArrayList<String>();
    		List<String> srcTables = srcUtil.getTables();
	        List<String> destTables = destUtil.getTables();
	        
	        for(String tableName:srcTables) {
	        	if((destTables.indexOf(tableName)<0)) {
	        		System.out.println("triggerDiff,"+tableName +" not exists in dest db");
	        		continue;
	        	}
	        	List<String> srcTriggers = srcUtil.getTriggerByTableName(tableName);
	        	List<String> destTriggers  = destUtil.getTriggerByTableName(tableName);
	        	if(srcTriggers == null && destTriggers !=null) {
	        		diffTrigger.add(tableName+" trigger does'nt exsist in src but in dest");
	        	}
	        	if(srcTriggers == null)
	        		continue;
	        	for(String trigger : srcTriggers) {
	        		if(destTriggers== null || destTriggers.indexOf(trigger)<0) {
	        			diffTrigger.add(tableName+" trigger does'nt exsist in dest db");
	        		}
		        	String srcTriggerText = srcUtil.getTriggerTextByTriggerName(trigger);
		        	String destTriggerText = destUtil.getTriggerTextByTriggerName(trigger);
		        	
		        	if( !srcTriggerText.equals(destTriggerText))
		        		diffTrigger.add(tableName+" trigger text has difference,src trigger= "+trigger);	
	        	}
	        }
	        
	        for(String tableName:destTables) {
	        	if((srcTables.indexOf(tableName)<0)) {
	        		System.out.println("triggerDiff,"+tableName +" not exists in src db");
	        		continue;
	        	}
	        	
	        	List<String> srcTriggers = srcUtil.getTriggerByTableName(tableName);
	        	List<String> destTriggers = destUtil.getTriggerByTableName(tableName);
	        	if(destTriggers == null && srcTriggers !=null) {
	        		diffTrigger.add(tableName+" trigger does'nt exsist in dest but in src");
	        	}
	        	if(destTriggers == null)
	        		continue;
	        	for(String trigger : destTriggers) {
	        		if(srcTriggers == null || srcTriggers.indexOf(trigger)<0) {
	        			diffTrigger.add(tableName+" trigger does'nt exsist in src db");
	        		}
		        	String srcTriggerText = srcUtil.getTriggerTextByTriggerName(trigger);
		        	String destTriggerText = destUtil.getTriggerTextByTriggerName(trigger);
		        	
		        	if( !destTriggerText.equals(srcTriggerText))
		        		diffTrigger.add(tableName+" trigger text has difference,dest trigger= "+trigger);	
	        	}
	        }
	        
	        outputTriggerNameDiff("D:\\diffs-trigger.txt",diffTrigger);
    }
    
    
    
    private static void outputCollumnDiff(String fileName,Map<String,List<String>> diffMap) {
    	Iterator<String> it = diffMap.keySet().iterator();
    	try {
			FileOutputStream fos = new FileOutputStream(fileName);
			while(it.hasNext()) {
				String tableName = it.next();
				List<String> collDiffs = diffMap.get(tableName);
				String s1="--talbe:"+tableName;
				fos.write(s1.getBytes());
				fos.write("\r\n".getBytes());
				for(String s:collDiffs) {
					fos.write(s.getBytes());
					fos.write("\r\n".getBytes());
				}
			}
			fos.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
    private static void outputTriggerNameDiff(String fileName,List<String> diffs) {
    	
    	try {
    		FileOutputStream fos = new FileOutputStream(fileName);
			for(String s:diffs) {
				fos.write(s.getBytes());
				fos.write("\r\n".getBytes());
			}
			fos.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("trigger name compared over!");
    }
}
