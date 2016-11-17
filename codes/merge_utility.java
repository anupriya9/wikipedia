package merge_files;

import java.util.ArrayList;
import java.util.List;

import frequency_counter.*;

public class merge_utility {
	public List<count_frequency> list_freq_count;
	public List<Integer> file;
	public int counter;
	
	merge_utility(){
		list_freq_count = new ArrayList<count_frequency>();
		file = new ArrayList<Integer>();
	}
	public void setCounter(String temp_count){
		counter += Integer.parseInt(temp_count);
	}

	public int getCounter(){
		return counter;
	}

}
