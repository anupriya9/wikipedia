package secondary_index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.io.DataInputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class secondary_indexing {
	private DataInputStream input_stream;
	private BufferedReader buff_reader;
	
	private FileWriter file_writter;
	private FileInputStream file_stream;
	
	private Path directory_path,write_into_file;
	private BufferedWriter buff_writter;

	public void createsecondaryindex(String path_to_index_folder, int flag) throws IOException {

		
		File file;
		if(flag == 1)
			file = new File(path_to_index_folder+"/secondary_index");
		else
			file = new File(path_to_index_folder+"/secondary_map_index");
		
			if(flag == 1)
			{
				file_stream = new FileInputStream(path_to_index_folder+"/index_after_merging");
			}
			else if(flag==2)
				file_stream = new FileInputStream(path_to_index_folder+"/map_id_title");
			
			input_stream = new DataInputStream(file_stream);
			buff_reader = new BufferedReader(new InputStreamReader(input_stream));
		
		
		
			file_writter = new FileWriter(file.getAbsoluteFile());
		
			directory_path = Paths.get(file.getParentFile().toURI());
		
			write_into_file = directory_path.resolve(file.getName());
			buff_writter = Files.newBufferedWriter(write_into_file,               
                Charset.forName("UTF-8"), 
                new OpenOption[] {StandardOpenOption.WRITE});
		
		final int difference = 20;
		StringBuilder content = new StringBuilder("");
		String arguments;
		int count = 0;
		String str;
		long offset = 0;
			while((str = buff_reader.readLine())!=null){
				if(count == 0){
					int index_id;
					index_id = str.indexOf(':');
					arguments = str.substring(0, index_id);
					content = content.append(arguments+":"+offset+"\n");
					if(content.length() > 600){
		            	 buff_writter.write(content.toString());
		            	 content = new StringBuilder("");		            	 
		             }
					count = difference;
					
				}
				else{
					count--;
				}
				offset += str.getBytes().length;
				offset++;		
			}
			if(content.length() != 0){
				 buff_writter.write(content.toString());
			}
			//System.out.println("OFFSET : "+offset);
			buff_writter.close();
			buff_reader.close();
	}

}
