package stop_words;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StopWords {

	private Set<String> stopwordsset;
	
	public StopWords()
	{
		stopwordsset = new HashSet<>();
		String stop_wordfile;
		BufferedReader buff_read;
		String         line;

		try{
			stop_wordfile="stopwords.txt";
			buff_read = new BufferedReader(new FileReader(stop_wordfile));
			while ((line = buff_read.readLine()) != null) {
				stopwordsset.add(line);
			}
			buff_read.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		buff_read = null;
	}
	
	public boolean isnotstopword(StringBuilder str){		
		return (!stopwordsset.contains(str));		
	}
	public boolean isnotstopword(String str){		
		return (!stopwordsset.contains(str));		
	}
	
}
