package merge_files;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import merge_files.*;
import frequency_counter.*;

public class merges_files {

	
	private Path dir_path,write_to_file;
	private BufferedWriter buff_write;
	
	private Map<Integer, String> map_file;
	
	private String arguments, counter;
	private FileInputStream []file_stream;
	private DataInputStream []data_input;
	private BufferedReader []buff_read;
	private StringBuilder content;
	private List<count_frequency> freq_count;
	private Map<String, merge_utility> tree_map;
	private FileWriter file_writter;
	
	private long lines;
		public void mergingFunc(String path_to_index_folder, long no_of_files) throws IOException{
			
			
			map_file = new HashMap<Integer, String>();
			data_input=new DataInputStream[(int) no_of_files] ;
			lines = 0;
			String name;
			file_stream =new FileInputStream[(int) no_of_files];
			buff_read = new BufferedReader[(int) no_of_files] ;
			int merge_count = 1;
			File file = new File(path_to_index_folder + "/index_after_merging");
			if(!file.exists()){
				try {
					file.createNewFile();
				} 
				catch (IOException e) {
					e.printStackTrace();
					System.out.println("Error in creating new file : "+e.getMessage());
				}
			}
			
			try{
				file_writter = new FileWriter(file.getAbsoluteFile());
			
				dir_path = Paths.get(file.getParentFile().toURI());
			
				write_to_file = dir_path.resolve(file.getName());
				buff_write = Files.newBufferedWriter(write_to_file,               
	                Charset.forName("UTF-8"), 
	                new OpenOption[] {StandardOpenOption.WRITE});
			}
			catch(IOException e){e.printStackTrace();
				System.out.println("Error in writing to file : "+e.getMessage());
			}
			
	        path_to_index_folder = path_to_index_folder + "/file_";
			tree_map = new TreeMap<String, merge_utility>();
			content = new StringBuilder();
			freq_count = new ArrayList<count_frequency>();
			arguments = new String();
			
			try
		    {
				System.out.println("no_of_files"+no_of_files);
			for(int i=0;i<no_of_files;i++)
			{
				name = path_to_index_folder + merge_count;
				map_file.put(i, name);
				merge_count++;
				file_stream[i] = new FileInputStream(name);
				data_input[i] = new DataInputStream(file_stream[i]);
				buff_read[i] = new BufferedReader(new InputStreamReader(data_input[i]));
				
				String str;
				if((str = buff_read[i].readLine())!=null)
				{
					arguments = "";
					try{
					scan_line(str);
					if(tree_map.containsKey(arguments)){
						tree_map.get(arguments).file.add(i);
						tree_map.get(arguments).setCounter(counter);
						for(int k = 0;k<freq_count.size();k++){
							tree_map.get(arguments).list_freq_count.add(freq_count.get(k));
						}
					}
					else
					{
						merge_utility merge_class = new merge_utility();
						merge_class.file.add(i);
						merge_class.setCounter(counter);
						for(int k = 0;k<freq_count.size();k++){
							
							merge_class.list_freq_count.add(freq_count.get(k));						
						}	
						tree_map.put(arguments,merge_class);
						//System.out.println(arguments);
					}
					}
					catch(Exception e){e.printStackTrace();
						System.out.println("EXCEPTION : "+e.getMessage());
					}
					freq_count.clear();
				}
			  }
		    }
			catch(Exception e){e.printStackTrace();
				System.out.println("EXCEPTION : "+e.getMessage());
			}
			
			while(tree_map.size() != 0){
				List<Integer>file_freq_count = null;
				try {
					file_freq_count = printing_top_values();
				for(Integer i : file_freq_count){
					String str;
					if((str = buff_read[i].readLine())!=null)
					{
						arguments = "";
						scan_line(str);
						if(tree_map.containsKey(arguments)){
							tree_map.get(arguments).file.add(i);
							tree_map.get(arguments).setCounter(counter);
							for(int k = 0;k<freq_count.size();k++){
									tree_map.get(arguments).list_freq_count.add(freq_count.get(k));		
							}
						}
						else
						{
							merge_utility merge_class = new merge_utility();
							merge_class.file.add(i);
							merge_class.setCounter(counter);
							for(int k = 0;k<freq_count.size();k++){
								
								merge_class.list_freq_count.add(freq_count.get(k));						
							}	
							tree_map.put(arguments,merge_class);
							
						}
					}
					else
					{
						buff_read[i].close();
						System.out.println("Delete file "+map_file.get(i));
						Files.deleteIfExists(Paths.get(map_file.get(i)));
					}
					freq_count.clear();
				   }
				}
				catch(Exception e){e.printStackTrace();
					System.out.println("EXCEPTION : "+e.getMessage());
				}
			}
				
			
					 if(content.length() != 0)
					 {
						 //System.out.println("content :::"+content);
						 buff_write.write(content.toString());
					 }
					 buff_write.close();
				 
			System.out.println("Index File Created of line : "+lines);
		}
		
		private List<Integer> printing_top_values() throws IOException {
			 lines++;
			 Iterator<Entry<String, merge_utility>> iterator = tree_map.entrySet().iterator();
			 Entry<String, merge_utility> merge_entry = iterator.next();
	         content.append(merge_entry.getKey() + ":");
	         content.append(merge_entry.getValue().getCounter() + ":");
	         int previous_id = 0;
	         merge_utility merge_class = merge_entry.getValue();
	         List<count_frequency> freq_count = merge_class.list_freq_count;
	         List<Integer> file_freq_count = merge_class.file;
	         for(count_frequency temp_freq : freq_count){
	        	 content.append(Integer.parseInt(temp_freq.getId())-previous_id);
	        	 previous_id = Integer.parseInt(temp_freq.getId());
	        	 if(temp_freq.getTitle() != 0){
	        		 content.append("@");
	        		 content.append("t"+temp_freq.getTitle());
	        	 }
	        	 if(temp_freq.getInfo() != 0){
	        		 content.append("@");
	        		 content.append("i"+temp_freq.getInfo());
	        	 }
	        	 if(temp_freq.getBody() != 0){
	        		 content.append("@");
	        		 content.append("b"+temp_freq.getBody());
	        	 }
	        	 if(temp_freq.getreferences() != 0){
	        		 content.append("@");
	        		 content.append("r"+temp_freq.getreferences());
	        	 }
	        	 if(temp_freq.getLink() != 0){
	        		 content.append("@");
	        		 content.append("L"+temp_freq.getLink());
	        	 }
	        	 if(temp_freq.getCategory() != 0){
	        		 content.append("@");
	        		 content.append("c"+temp_freq.getCategory());
	        	 }
	        	 if(temp_freq.getinlinks() != 0){
	        		 content.append("@");
	        		 content.append("l"+temp_freq.getinlinks());
	        	 }
	        	 content.append(";");
	        	 if(content.length() > 600){
	        		 //System.out.println("be con....."+content);
	            	 buff_write.write(content.toString());
	            	 content = new StringBuilder("");
	            	 
	             }			        	
	         }
	         content.append("\n");
	         if(content.length() > 600){
	        	 //System.out.println("be con writiing..................."+content);
	        	 buff_write.write(content.toString());
	        	 content = new StringBuilder("");
	         }	
	         tree_map.remove(merge_entry.getKey());
	         return file_freq_count;
		}

		private void scan_line(String str) {
			// TODO Auto-generated method stub
			count_frequency temp_freq = null ;
			int prev = 0;
			String document_id = "";
			int flag = 0,flag1 = 0;
			for(int k = 0;k<str.length();k++)
			{
				if(str.charAt(k) == ':'){
					flag = 1;k++;
				}
				
				if(flag == 1){
					int indexid;
					if(flag1 == 0){
						counter = "";
						indexid = str.indexOf(':', k);
						counter = str.substring(k, indexid);
						k = indexid+1;
						flag1 = 1;
					}
					indexid = str.indexOf('@', k);
					document_id = String.valueOf(Integer.parseInt(str.substring(k, indexid))+prev);
					prev = Integer.parseInt(document_id);
					
					temp_freq = new count_frequency(document_id);
					//System.out.println("writting the doc "+str+" "+document_id+" "+f.getScore());

					k = indexid+1;
					while(str.charAt(k) != ';'){
						if(k+1 == str.length()){
							return;
						}
						if(str.charAt(k) == '@') {k++;continue;}
						int flag_set = 0;
						
						switch(str.charAt(k)){
						
							case 't' : flag_set = 1;
										 break;
							case 'i' : flag_set = 2;
										break;
							case 'b' : flag_set = 3;
					 		 			break;
							case 'r' : flag_set = 6;
										break;
							case 'L' : flag_set = 5;
										break;
							case 'c' : flag_set = 4;
										break;
							case 'l' : flag_set = 7;
										break;
							default : flag_set = 0;
						}
						
						if(flag_set != 0){
							int rate_index=str.indexOf("@");
							int colon_index=str.indexOf(";");
							if(rate_index>colon_index)
								indexid=colon_index;
							else
								indexid=rate_index;
							if(indexid == -1){
								indexid = str.indexOf(';', k);
							}
							try{
							int count = Integer.parseInt(str.substring(k+1, indexid));
							for(int j = 0;j<count;j++)
								temp_freq.count_increment(flag_set);
							k = indexid;
							}
							catch(Exception e){e.printStackTrace();
								System.out.println("EXCEPTION : "+e.getMessage());
							}
						}
					}
					freq_count.add(temp_freq);
				}
				else{
					arguments = arguments+str.charAt(k);
				}		
			}
		}	
	}
