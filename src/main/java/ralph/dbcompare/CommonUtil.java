package ralph.dbcompare;

public class CommonUtil {
	
	public static final boolean IsNullOrEmpty(String s){
		if(s == null)
			return true;
		if("".equals(s))
			return true;
		return false;
	}
	public static final boolean isEqual(Object src,Object dest){
		if(src == null && dest == null)
			return true;
		if(src !=null && dest!=null){
			if(src.equals(dest))
				return true;
		}
		return false;
	}
}
