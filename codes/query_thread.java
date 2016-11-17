package search;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

import driver.index_secondary;
import frequency_counter.count_frequency;;

public class query_thread extends Thread{
	
	private int flag_set;
	private List<index_secondary> secondary_index;
	private Map<String, Double> tf_idmap;
	private double N;
	private RandomAccessFile random_access_file;
	private String query_str;
	public query_thread(){		
	}
	
	public query_thread(String str, List<index_secondary> temp_second_index, Map<String, Double> temp_tf_idmap, String index_path, int tempflag) throws FileNotFoundException{
		
		query_str = str;
		N = 16.4575;
		secondary_index = temp_second_index;
		tf_idmap = temp_tf_idmap;
		flag_set = tempflag;		
		random_access_file = new RandomAccessFile(index_path+"/index_after_merging", "r");
		
	}
	// overriding thread's run
	@Override
	public void run(){
		if(query_str.length() != 0)
		{
			try {
				tokenize_keyword(query_str);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("Exception occured while tokenizing");
				e.printStackTrace();
			}
		}
	}
	
	public void tokenize_keyword(String str) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println(str+ "  "+flag_set);
		long[] index = new long[2];
		index[0] = index[1] = -1;
		//binary_search in secondary index
		int start, end;
		start = 0;
		end = secondary_index.size()-1;
		int middle;
		middle = (start+end)/2;
		while(start < (end-1)){
			if(str.compareTo(secondary_index.get(middle).getKey()) < 0){
				end = middle;
			}
			else if(str.compareTo(secondary_index.get(middle).getKey()) > 0){
				start = middle;
			}
			else{
				index[0] = secondary_index.get(middle).getOffset();
				index[1] = secondary_index.get(middle+1).getOffset();
				return;
			}
			middle = (start+end)/2;
		}
			
		if(str.compareTo(secondary_index.get(secondary_index.size()-1).getKey()) < 0){
			index[0] = secondary_index.get(start).getOffset();			
			index[1] = secondary_index.get(start+1).getOffset();
		}
		else{
			index[0] = secondary_index.get(start+1).getOffset();			
			index[1] = -1;
			
		}//binary search ended
		byte[] value;
		int diffval = (int) (index[1]-index[0]);
		if(index[1] != -1)
			value = new byte[(int)(diffval+1)];
		else 
			value = new byte[(int) (928)];
		try {
			random_access_file.seek(index[0]);
			if(index[1] != -1)
				random_access_file.read(value, 0, (int)(index[1]-index[0]));
			else
			{
				random_access_file.read(value);
				//System.out.println("value is::::"+new String(value));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception occured");
			e.printStackTrace();
		}
		String result = new String(value);
		try{
			//System.out.println("lengggg:"+result.length());
			tokenize_string1(str, result);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return;
		}
		
	}	
	private void tokenize_string1(String str, String result1) throws Exception{
//		System.out.println("ans:"+result1);
//		System.out.println("res1.len is::"+result1.length());
		String[] temp_str = result1.split("\n");
		//System.out.println("str is::"+temp_str.length);
		for(String result : temp_str){
			//System.out.println("result si ::"+result);
			int index = result.indexOf(':');
			if(index==-1)
				index=0;
			String query_str = result.substring(0, index);
			if(query_str.compareToIgnoreCase(str) > 0)
				break;
			int i = index+1;
			if(query_str.matches(str)){
				int previous = 0;
				String document_id = "";
				int no_of_doc = 0;
				
				index = result.indexOf(':', i);
				no_of_doc = Integer.parseInt(result.substring(i, index));
				//System.out.println("no of doc::"+no_of_doc);
				i = index+1;
				int page_count = no_of_doc;
				//System.out.println("res len is::"+result.length());
				for(int k = i;k<result.length() && page_count >= 0;k++){
					page_count--;
					index = result.indexOf('@', k);
					document_id = String.valueOf(Integer.parseInt(result.substring(k, index))+previous);
					previous = Integer.parseInt(document_id);
					count_frequency freq_count = new count_frequency(document_id);
					
					k = index+1;
					
					while(result.charAt(k) != ';'){
						if(k+1 == result.length()){
							break;
						}
						if(result.charAt(k) == '@') {k++;continue;}
						int flag_set1 = 0;
						
						switch(result.charAt(k)){
						
								case 't' : flag_set1 = 1;
											break;
								case 'i' : flag_set1 = 2;
											break;
								case 'b' : flag_set1 = 3;
								 			break;
								case 'r' : flag_set1 = 6;
											break;
								case 'L' : flag_set1 = 5;
											break;
								case 'c' : flag_set1 = 4;
											break;
								case 'l' : flag_set1 = 7;
											break;
								default : flag_set1 = 0;
						}
						
						if(flag_set1 != 0){
							index = Math.min(result.indexOf('@', k), result.indexOf(';', k));
							if(index == -1){
								index = result.indexOf(';', k);
						}
						try{
							int count = Integer.parseInt(result.substring(k+1, index));
							//System.out.print(count +" ");
							freq_count.count_incrementbyvalue(flag_set1, count);
							k = index;
						}
						catch(Exception e){e.printStackTrace();
							System.out.println("EXCEPTION : "+e.getMessage());
						}
						}
					}
				//System.out.println("calllinggggggg..........");
				adding_to_temp_map(str,document_id, freq_count, no_of_doc);
				}//System.out.println("page_count is:"+page_count);
				break;
			}
			
			
		}
		
	}

	private void adding_to_temp_map(String str, String document_id, count_frequency freq_count,
			int no_of_doc) throws Exception {
		
		Double count = 0.0;
		switch(flag_set){
		
			case 1 : {
						count += (freq_count.getTitle());	
						//System.out.println("count of title is::"+count);
						break;
			}
			case 2 : {
						count += (freq_count.getInfo());
						break;
			}
			case 3 : {
						count += freq_count.getBody();
						break;
			}
			case 4 : {
						count += freq_count.getCategory();
						break;
			}
			case 5 : {
						count += (freq_count.getLink()+freq_count.getinlinks());
						break;
			}
			case 6 : {	
						count += freq_count.getreferences();
						break;
			}
			case 0 : {
						count += freq_count.getScore();	
						break;
			}
		}
		if(count == 0.0) return;
		//nod->number of document
		Double nod=Math.log(no_of_doc);
		Double value = (Double)count*(N - nod);
			//System.out.println("tfid map is :"+tf_idmap);
		if(!tf_idmap.containsKey(document_id)){
			//System.out.println("not present :"+document_id+" "+str);
			tf_idmap.put(document_id, value);
		}
		else{ 
			//System.out.println("present :"+document_id+" "+str);
				tf_idmap.put(document_id, tf_idmap.get(document_id) * value);
			}		
		return;
		
		
	}
	
}
