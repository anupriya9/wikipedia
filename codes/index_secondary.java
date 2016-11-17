package driver;

public class index_secondary {
	
	private long offset;
	private String key;
	public index_secondary()
	{
		key = new String();
		offset = -1;
	
	}
	public index_secondary(String temp_str, long temp_offset){
		key = temp_str;
		offset = temp_offset;
	}
	public long getOffset(){
		return offset;
	}
	
	public String getKey(){
		return key;
	}
	

}
