package ralph.dbcompare;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class MyTest {
	
	@Test
	public void testRegular() {
		Pattern pattern = Pattern.compile("[a-z]+");
		String[] ss = {"fiD","FID","fid","FiD","FId","Fid"};
		for(String s:ss) {
			Matcher matcher = pattern.matcher(s);
			if(matcher.find()) {
				System.out.println(s+" contains lower letter");
			}
		}
		
		
		
    }
}
