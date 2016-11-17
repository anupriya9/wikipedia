package driver;

import java.io.FileNotFoundException;
import java.io.IOException;

import merge_files.merges_files;
import secondary_index.secondary_indexing;

public class main_index {
	public static void main(String args[]) throws IOException
	{
		String xml_file=new String();
		String path_to_index_folder=new String();
		//String xml_file="/home/anupriya/ire/wikidata2.xml";
		//String path_to_index_folder="/home/anupriya/ire/index1";
		xml_file=args[0];
		path_to_index_folder=args[1];
		XMLParsing parse=new XMLParsing();
		long no_of_files=0;
		double start_time=System.currentTimeMillis();
		//System.out.println("start time is:"+start_time);
		//System.out.println("going..."+xml_file+" "+path_to_index_folder);
		try {
			no_of_files=parse.parse_content(xml_file,path_to_index_folder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("number of files is: "+no_of_files);
		if(no_of_files>0)
		{
			//if no of files are more than one then call merging function
			merges_files merger=new merges_files();
				
				merger.mergingFunc(path_to_index_folder,no_of_files);
				
			
			
		}
		//building secondary index
		
		secondary_indexing sec_index=new secondary_indexing();
		try {
			sec_index.createsecondaryindex(path_to_index_folder,1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			sec_index.createsecondaryindex(path_to_index_folder, 2);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		double end_time=System.currentTimeMillis();
		//System.out.println("end time is:"+end_time);
		double total_time=(end_time-start_time)/1000;
		System.out.println("total time taken for indexing is:"+total_time+"seconds");
		return;
	}

}
