package driver;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;

import porter_stemmer.Stemmer;
import stop_words.StopWords;
import frequency_counter.count_frequency;;
public class XMLParsing {

	Path writeFilemap,map_directory;
	BufferedWriter buff_read_map;
	public static String xml_file, index_to_folder;
	File map_to_file;
	String mapping_parent;
	public static StringBuilder id_title_map;
	private long no_of_files = 0;
	
	
	public long parse_content(String xml_file_name, String index_path) throws IOException
	{
		
		xml_file = 	xml_file_name;	
		index_to_folder = index_path;
		mapping_parent = index_to_folder + "//map_id_title";
		map_to_file = new File(mapping_parent);
		if(!map_to_file.exists()){
				map_to_file.createNewFile();
		}
		id_title_map = new StringBuilder();
			map_directory = Paths.get(map_to_file.getParentFile().toURI());
			
		    writeFilemap = map_directory.resolve(map_to_file.getName());
		    buff_read_map = Files.newBufferedWriter(writeFilemap,               
		            Charset.forName("UTF-8"), 
		            new OpenOption[] {StandardOpenOption.WRITE});
		
		
		try{
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			
			DefaultHandler sax_handler = new DefaultHandler() {
				
				boolean page_start = false;
				boolean title_start = false;
				boolean flag = false;
				String temp_file;
				boolean id_start = false;
				boolean text_start = false;
				StringBuilder str_title,str_id,str_text;
				String document_id;
				boolean var_start = false;
				long page_count=0;
				int flag_set;
				Map<String, ArrayList<count_frequency>> pimary_index;
				StopWords stop_words;
				Stemmer stemmer;
				
				public void startElement(String xml_file, String localName,String qName, 
		                 Attributes attributes) throws SAXException {
				if (qName.equalsIgnoreCase("page")) {
					page_count++;
					page_start = true;
				}
			 		
				if (qName.equalsIgnoreCase("title")) {
					str_title = new StringBuilder();
					title_start = true;
					
				}
		 
				if (qName.equalsIgnoreCase("id")) {
					str_id =  new StringBuilder();
					id_start = true;
					
				}
		 
				if (qName.equalsIgnoreCase("text")) {
					text_start = true;
					flag_set = 3;
					str_text = new StringBuilder();
				}
				if (qName.equalsIgnoreCase("mediawiki") || qName.equalsIgnoreCase("file")) {
					pimary_index = new TreeMap<String, ArrayList<count_frequency>>();
					temp_file =XMLParsing.xml_file;
					stop_words = new StopWords();
					stemmer = new Stemmer();
					
				}
		 	}
		 
			public void endElement(String xml_file, String localName,
				String qName) throws SAXException {
				
				if(qName.equalsIgnoreCase("page")){
					if(page_count % 9000 == 0)
					{
						write_data_to_file();
					}
					flag_set = 0;
					flag = false;
					page_start = false;
				}
				
				if(qName.equalsIgnoreCase("title")){
					title_start = false;
					if(str_title.length() > 10)
						if(str_title.substring(0, 10).equalsIgnoreCase("wikipedia:")){
							var_start = false;
					}
				}
				if(qName.equalsIgnoreCase("id") && flag == false){
					flag = true;
					id_start = false;
					document_id = new String(str_id.toString());
					try{
						if(var_start)
							buff_read_map.write(document_id+":"+str_title+"\n");
					}
					catch(Exception e){
							try {
								buff_read_map.close();
							} catch (IOException e1) {
								System.out.println("Closing error");
								e1.printStackTrace();
							}
						
					}
					if(id_title_map.length() == 9900){
							try {
								buff_read_map.write(id_title_map.toString());
							} catch (IOException e) {
								e.printStackTrace();
							}
							id_title_map.setLength(0);
						
					}
					
				}
				if(qName.equalsIgnoreCase("text")){
					title_start = true;
					if(var_start)
						parsing_title(str_title);
					title_start = false;
					text_start = false;
					
				}
				if( qName.equalsIgnoreCase("file")||qName.equalsIgnoreCase("mediawiki")){
					if(id_title_map.length() != 0 ){
								try {
									buff_read_map.write(id_title_map.toString());
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							
					}
						try {
							buff_read_map.close();
						} catch (IOException e) {
							System.out.println("unable to close");
						}
					if(!pimary_index.isEmpty())
					{
						System.out.println("writing");
						write_data_to_file();
					}
					System.out.println("Num of pages : "+page_count);	
										
				}
		 
			}
			
			public void characters(char temp_char[], int start, int length) throws SAXException {
				if (title_start) {
					str_title.append(temp_char,start,length);
					var_start = true;
					
				}
		 
				if (id_start) {
					str_id.append(temp_char,start,length);
				}
		 
				
				if (text_start && var_start) {
						parsing_data(temp_char, start,length);
						return;
					
				}	 				
			}
			
			private void parsing_data(char[] char_arr, int start, int length) {
				Stack<Character> bracket = new Stack<Character>();
				String previous = new String();
				previous = "";
				char c1='{',c2 = '{';
				String temp_str = new String();
				temp_str = "";
				if(title_start == true)
					flag_set = 1;
				
				for(int i = start;(i+start)<length;i++){
					char temp_char = char_arr[i];
					if((temp_char >= 'A' && temp_char <= 'Z') || (temp_char >= 'a' && temp_char <= 'z'))
					{
						temp_str = temp_str + Character.toLowerCase(temp_char);
//						if(((i+7)<length)&&(char_arr[i]=='i' && char_arr[i+1]=='n' && char_arr[i+2]=='f' && char_arr[i+3]=='o' && char_arr[i+4]=='b' && char_arr[i+5]=='o' && char_arr[i+6]=='x') && (c1=='{') && (c2=='{') )
//						{	
//								flag_set = 2;
//								i=i+7;
//						}
						
//						if(temp_str.equals("infobox") && (c1=='{') && (c2=='{') )
//							flag_set = 2;
						if(temp_str.equals("references") && (c1 == '=') && (c2 == '=') && (char_arr[i+1] == '=') && (char_arr[i+2] == '='))
						{ temp_str = "";	flag_set = 6;}
						if((i+2 < length)  && (title_start == false)){
							
							if(temp_str.equals("links") && previous.equals("external") && (c1 == '=') && (c2 == '=') && (char_arr[i+1] == '=') && (char_arr[i+2] == '='))
							{temp_str = "";	flag_set = 5;}
							if(temp_str.equals("category") && (c1 == '[') && (c2 == '[') && (char_arr[i+1] == ':'))
							{
								temp_str = "";	flag_set = 4;
							}
							if(temp_str.length()==2 && (c1 == '[') && (c2 == '[') && (char_arr[i+1] == ':'))
							{
								temp_str = "";	flag_set = 7;
							}
							
						}
					}
					else if(char_arr[i]=='<') 
					{
			   			 if (i+4<length&&char_arr.toString().substring(i+1,i+4).equals("!--")) 
			   			 {
			   				 //Check and Eliminate comments
			   				 i=i+4;
			   				 int locate_close = char_arr.toString().indexOf("-->",i);
			   				 if(locate_close+2<length&&locate_close>0)
			 					 i=locate_close+2;
			    			 }
			    			 
			    			 else if(i+8<length&&char_arr.toString().substring(i+1,i+8).equalsIgnoreCase("gallery"))
			    			 {
			    				 //Check and eliminate gallery
			    				 i=i+8;
			    				 int locate_close = char_arr.toString().indexOf("</gallery>" , i+1);
			    				 if(locate_close+10<length&&locate_close>0)
			    					 i=locate_close+10;
			    				
			    			 }
			                     else if(i+5<length&&char_arr.toString().substring(i+1,i+5).equalsIgnoreCase("ref>"))
			                     {
			                          i=i+5;
			    				 int locate_close = char_arr.toString().indexOf("</ref>" , i+1);  
			                     if(locate_close+5<length&&locate_close>0)
			    					 i=locate_close+6;
			    				
			                     }
			                        
					}
					else{
						if(temp_char != ' '){
							c2 = c1;
							c1 = temp_char;
						}
						if(temp_char == '{'){
							bracket.push(temp_char);							
						}
						if(temp_char == '}'){
							if(!bracket.empty())
								bracket.pop();
							if(bracket.empty() && flag_set == 2){
								flag_set = 3;								
							}
						}
						if((flag_set == 7 || flag_set == 4) && c1 ==']' && c2 ==']'){
							flag_set = 3;
						}
						if(flag_set == 5 && c1 =='{' && c2 =='{'){
							flag_set = 3;
						}
						if(!stop_words.isnotstopword(temp_str)){
							temp_str = "";
							continue;
						}
						if(!temp_str.isEmpty()  ){
							previous = temp_str;
							if(flag_set != 0)
								tokenize_keywords(temp_str, flag_set);
							temp_str = "";
						}						
					}
					
									
				}
				
				if(!temp_str.isEmpty() & stop_words.isnotstopword(temp_str) ){
					previous = temp_str;
					if(flag_set != 0)
						tokenize_keywords(temp_str, flag_set);
					temp_str = "";
				}

			}

			private void parsing_title(StringBuilder str_temp) {
				Stack<Character> bracket = new Stack<Character>();
				String temp_str = new String();
				temp_str = "";
				char c1='{',c2 = '{';
				String previous = new String();
				previous = "";
				if(title_start == true)
					flag_set = 1;
				
				for(int i = 0;i<str_temp.length();i++){
					char temp_char = str_temp.charAt(i);
					if((temp_char >= 'A' && temp_char <= 'Z') || (temp_char >= 'a' && temp_char <= 'z'))
					{
						temp_str = temp_str + Character.toLowerCase(temp_char);
					}
					else{
					
				if(!temp_str.isEmpty()){
							previous = temp_str;
							if(flag_set != 0)
								tokenize_keywords(temp_str, flag_set);
							temp_str = "";
						}						
					}
					
									
				}
				if(!temp_str.isEmpty()){
					previous = temp_str;
					if(flag_set != 0)
						tokenize_keywords(temp_str, flag_set);
					temp_str = "";
				}
			}
			private void write_data_to_file() {	
				no_of_files++;
				File file = new File(index_to_folder);
				String temp_str;
				if(!file.exists())
					file.mkdirs();
				temp_str = index_to_folder + "/file_" + no_of_files;
				file = new File(temp_str);
				System.out.print(temp_str + " : ");
				
				if(!file.exists()){
					try {
						file.createNewFile();
					} 
					catch (IOException e) {
						e.printStackTrace();
						System.out.println("EXCEPTION : "+e.getMessage());
					}
					writing_into_file(file);
				}
				
				
				pimary_index = new TreeMap<String, ArrayList<count_frequency>>();
			}

				private void writing_into_file(File file) {
				StringBuilder content = new StringBuilder();
			    Iterator<Entry<String, ArrayList<count_frequency>>> i = pimary_index.entrySet().iterator();
			    System.out.println(pimary_index.size());
			    try{
			    	FileWriter file_writter = new FileWriter(file.getAbsoluteFile());
					BufferedWriter buff_write = new BufferedWriter(file_writter);
					
					Path dir_path = Paths.get(file.getParentFile().toURI());
					
			        Path write_file = dir_path.resolve(file.getName());
			        BufferedWriter buff_read = Files.newBufferedWriter(write_file,               
			                Charset.forName("UTF-8"), 
			                new OpenOption[] {StandardOpenOption.WRITE});
			        
					int previous_id = 0;
			    while(i.hasNext()) {
			 		Entry<String, ArrayList<count_frequency>> map_entry = i.next();
			         content.append(map_entry.getKey() + ":");
			         
			         previous_id = 0;
			         ArrayList<count_frequency> freq_count = map_entry.getValue();
			         content.append(freq_count.size() + ":");
			         for(count_frequency freq_counter : freq_count){
			        	 
			        	 content.append(Integer.parseInt(freq_counter.getId())-previous_id);
			        	 previous_id = Integer.parseInt(freq_counter.getId());
			        	 if(freq_counter.getTitle() != 0){
			        		 content.append("@");
			        		 content.append("t"+freq_counter.getTitle());
			        	 }
			        	 if(freq_counter.getInfo() != 0){
			        		 content.append("@");
			        		 content.append("i"+freq_counter.getInfo());
			        	 }
			        	 if(freq_counter.getBody() != 0){
			        		 content.append("@");
			        		 content.append("b"+freq_counter.getBody());
			        	 }
			        	 if(freq_counter.getreferences() != 0){
			        		 content.append("@");
			        		 content.append("r"+freq_counter.getreferences());
			        	 }
			        	 if(freq_counter.getLink() != 0){
			        		 content.append("@");
			        		 content.append("L"+freq_counter.getLink());
			        	 }
			        	 if(freq_counter.getCategory() != 0){
			        		 content.append("@");
			        		 content.append("c"+freq_counter.getCategory());
			        	 }
			        	 if(freq_counter.getinlinks() != 0){
			        		 content.append("@");
			        		 content.append("l"+freq_counter.getinlinks());
			        	 }
			        	 content.append(";");
			         }
			         content.append("\n");
			         if(content.length() > 2000){
			        	 buff_read.write(content.toString());
			        	 content = new StringBuilder(""); 	 
			         }
		
			      }
			        if(content.length() != 0)
			       {
			        	buff_read.write(content.toString());}
			        buff_read.close();
					buff_write.close();
			      }
			      catch(IOException e){
			    	  e.printStackTrace();
			    	  System.out.println("EXCEPTION : "+e.getMessage());
			      }		
			}
			
			private void tokenize_keywords(String temp_str, int flag){
				stemmer.add(temp_str.toCharArray(), temp_str.length());
				stemmer.stem();
				temp_str = stemmer.toString();
				if((stop_words.isnotstopword(temp_str)&&flag!=1)||flag_set==1){
					if(!pimary_index.containsKey(temp_str)){
						count_frequency freq_count = new count_frequency(document_id);
						freq_count.count_increment(flag);
						pimary_index.put(temp_str, new ArrayList<count_frequency>());
						pimary_index.get(temp_str).add(freq_count);
					}
					else
					{
						ArrayList<count_frequency> temp_list = pimary_index.get(temp_str);
						int flag1 = 0;
						int size = temp_list.size()-1;
						if(temp_list.get(size).getId().equalsIgnoreCase(document_id)){
							pimary_index.get(temp_str).get(size).count_increment(flag);								
							flag1 = 1;
						}										
						if(flag1 == 0){
							count_frequency freq_count = new count_frequency(document_id);
							freq_count.count_increment(flag);
							pimary_index.get(temp_str).add(freq_count);
						}
					}
				}
			}
			
			};
			saxParser.parse(xml_file, sax_handler);
		}
		
		
		
        catch (SAXException e) 
        {
        	e.printStackTrace();
           System.out.println("EXCEPTION : "+e.getMessage());
        }  
		catch(Exception e){
			e.printStackTrace();
			System.out.println("EXCEPTION : "+e.getMessage());
		}
		
		return no_of_files;
	}
		
}


