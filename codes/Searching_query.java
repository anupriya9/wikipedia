package search;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import driver.*;
import porter_stemmer.Stemmer;
import stop_words.StopWords;
import frequency_counter.*;

public class Searching_query {
	
	public RandomAccessFile rand_access_file,rand_access_file_map;
	public Map<String, Double> tf_idmap;
	private StopWords stop_words;
	private String index_path;
	private double N;
	private Stemmer stemmer;
	public int flag_set;
	public List<index_secondary> secondary_index;
	public List<index_secondary> secondary_map_index;	
	public Searching_query() {
	}
	public Searching_query(String path_to_index) throws Exception{
	
		secondary_map_index = new ArrayList<index_secondary>();
		index_path = path_to_index;
		N = Math.log(15345381);
		flag_set = -1;
		secondary_index = new ArrayList<index_secondary>();
		load_secondary_index(path_to_index);
		stemmer = new Stemmer();
		stop_words = new StopWords();
		rand_access_file_map = new RandomAccessFile(path_to_index+"/map_id_title", "r");
			
	}
	protected void finalize() throws Throwable {
		rand_access_file.close();
		rand_access_file_map.close();
	}
	public void searching_query(String query) throws InterruptedException{
		tf_idmap = new ConcurrentHashMap<String, Double>();
		List<query_thread> search_thread_list = new ArrayList<query_thread>();
		query_thread search_thread;
		String delims="[-?; ()!@#,+=<>{}$_./]";
		String []query_split=query.split(delims);
		//System.out.println("query is:"+query_split[1]);
		for(String temp_str : query_split){
			flag_set = 0;
			int index = temp_str.indexOf(':');
			if(index >-1){
				switch(temp_str.charAt(0)){				
					case 't' : flag_set = 1;
								break;
					case 'b' : flag_set = 3;
								break;
					case 'i' : flag_set = 2;
								break;
					case 'l' : flag_set = 5;
								break;
					case 'c' : flag_set = 4;
								break;
					case 'r' : flag_set = 6;
								break;
					default  : flag_set = 0;					
				}
				temp_str = temp_str.substring(index+1);
				
			}
			temp_str = temp_str.toLowerCase();
			//System.out.println("b4 temp is::"+temp_str+"flag_set"+flag_set);
			if((!stop_words.isnotstopword(temp_str)&&flag_set!=1)){
				//System.out.println("temp ::"+temp_str+" "+flag_set);
				continue;
			}
			stemmer.add(temp_str.toCharArray(), temp_str.length());
			stemmer.stem();
			temp_str = stemmer.toString();
			if(!stop_words.isnotstopword(temp_str)&&flag_set!=1){
				continue;
			}
			try {
				search_thread = new query_thread(temp_str,secondary_index, tf_idmap, index_path, flag_set);
				search_thread.start();
				search_thread_list.add(search_thread);
				//System.out.println("search thread is::"+search_thread_list);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("thread exception");
				e.printStackTrace();
			}
			
		}
		
			for(query_thread temp_st : search_thread_list){
					temp_st.join();
			}
		long size = tf_idmap.size();
		if(size == 0){
			System.out.println("Sorry : Result not found");
		}
		
		for(int i = 0;(i<10 && i<size);i++){
			Double maximum = 0.0;
			String document_id = "";
			Set<Entry<String, Double>> set = tf_idmap.entrySet();
			
			for(Map.Entry<String, Double> entry : set){				
				if(maximum < entry.getValue()){
					maximum = entry.getValue();
					document_id = entry.getKey();
				}				
			}
			try {
				print_result(document_id);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			tf_idmap.remove(document_id);
		}
		tf_idmap.clear();
	}
	
	private void print_result(String entry) throws Exception {
		
		String temp_str = entry;
		long[] index = new long[2];
		index[0] = index[1] = -1; 
		//binarysearch_in_secondary_map(temp_str, index);
		int start, end;
		start = 0;
		end = secondary_map_index.size()-1;
		int middle;
		middle = (start+end)/2;
		while(start < (end-1)){
			if(Integer.parseInt(temp_str) < Integer.parseInt(secondary_map_index.get(middle).getKey())){
				end = middle;
			}
			else if(Integer.parseInt(temp_str) > Integer.parseInt(secondary_map_index.get(middle).getKey())){
				start = middle;
			}
			else{
				index[0] = secondary_map_index.get(middle).getOffset();
				index[1] = secondary_map_index.get(middle+1).getOffset();
				return;
			}
			middle = (start+end)/2;
		}
		if(Integer.parseInt(temp_str) < Integer.parseInt(secondary_map_index.get(secondary_map_index.size()-1).getKey())){
			index[0] = secondary_map_index.get(start).getOffset();			
			index[1] = secondary_map_index.get(start+1).getOffset();
		}
		else{
			index[0] = secondary_map_index.get(start+1).getOffset();			
			index[1] = -1;
		}
		//binary search in secondary map ended
		byte[] value;
		if(index[1] != -1)
		{
			value = new byte[(int)(index[1]-index[0]+1)];
			//System.out.println("if part::"+index[1]);
		}
		else 
		{
			value = new byte[(int)(499703044-index[0])];
			System.out.println("in else part"+index[1]);
		}
		
			rand_access_file_map.seek((int)index[0]);
			if(index[1] != -1)
			{
				rand_access_file_map.read(value, 0, (int)(index[1]-index[0]));
				//System.out.println("if part:::"+value);
			}
			else
			{
				rand_access_file_map.read(value);
				//System.out.println("random access::"+value);
			}
		
		String result = new String(value);
		
		mapstring_parsing(temp_str,result);
		
		//System.out.println(result);
		
	}
	private void mapstring_parsing(String temp_str, String result) throws Exception{
		// TODO Auto-generated method stub
		String key;
		int length = result.length();
		for(int i = 0;i<length;){
			if(result.charAt(i) == '\n')
				break;
			int index = result.indexOf(':', i);
			key = result.substring(i, index);
			i = index+1;

			if(temp_str.matches(key.toString())){
				System.out.print(temp_str +" -> ");
				index = result.indexOf('\n', i);
				String output = result.substring(i, index);
				i = index;
				System.out.println(output);
				return;
			}
			else{
				while(result.charAt(i) != '\n'){
					i++;
					if(i == length){
						return;
					}
				}
				i++;
			}			
			
		}
		
	}
	private void load_secondary_index(String path_to_index) throws Exception{
		FileInputStream file_input_stream;
		DataInputStream input_stream;
		BufferedReader buff_read;
		
		try{
			file_input_stream = new FileInputStream(path_to_index+"/secondary_index");
			input_stream = new DataInputStream(file_input_stream);
			buff_read = new BufferedReader(new InputStreamReader(input_stream));
			
			String temp_str;
			while((temp_str = buff_read.readLine())!=null)
			{
				long offset;
				int index;
				String key;
				index = temp_str.indexOf(':');
				key = temp_str.substring(0, index);
				offset = Long.parseLong(temp_str.substring(index+1));
				index_secondary temp = new index_secondary(key, offset);
				//System.out.println("temp is::"+key+"::"+offset);
				secondary_index.add(temp);
			}
		}
		catch(Exception e){
			System.out.println("Exception occurred while loading the data");
			e.printStackTrace();
		}
		
			file_input_stream = new FileInputStream(path_to_index+"/secondary_map_index");
			input_stream = new DataInputStream(file_input_stream);
			buff_read = new BufferedReader(new InputStreamReader(input_stream));
			
			String temp_str;
			while((temp_str = buff_read.readLine())!=null)
			{
				String key;
				long offset;
				int index;
				index = temp_str.indexOf(':');
				key = temp_str.substring(0, index);
				offset = Long.parseLong(temp_str.substring(index+1));
				index_secondary temp = new index_secondary(key, offset);
				//System.out.println("second map temp::"+key+"::"+offset);
				secondary_map_index.add(temp);
			}
		
	}

}
