package frequency_counter;

public class count_frequency {
	private String document_id;
	private int count_title;
	private int count_info;
	private int count_body;
	private int count_category;
	private int count_link;
	private int count_references;
	private int count_internallinks;
	private int total_value;
	
	public count_frequency(String id)
	{
		document_id = id;
		count_title =0;
		count_info = 0;
		count_body =0;
		count_category =0;
		count_link =0;
		count_references =0;
		count_internallinks =0;
		total_value = 0;
	}
	public String getId(){
		return document_id;
	}
	
	public void count_increment(int flag){
		total_value++;
		switch(flag){
		
			case 1 : count_title++;
					 break;
			case 2 : count_info++;
					 break;
			case 3 : count_body++;
			 		 break;
			case 4 : count_category++;
					 break;
			case 5 : count_link++;
			 		 break;
			case 6 : count_references++;
			 		 break;
			case 7 : count_internallinks++;
	 		 		 break;
		}		
	}
	public void count_incrementbyvalue(int flag, int value){
		
		total_value += value;
		switch(flag){
		
			case 1 : count_title += value;
					//System.out.println("value is:"+value);
					 break;
			case 2 : count_info += value;
					 break;
			case 3 : //count_body += value;
					count_body++;
			 		 break;
			case 4 : count_category += value;
					 break;
			case 5 : count_link += value;
			 		 break;
			case 6 : count_references += value;
			 		 break;
			case 7 : count_internallinks += value;
	 		 		 break;
		}		
	}
	
	
	
	public int getTitle(){
		return count_title;
	}
	public int getInfo(){
		return count_info;
	}
	public int getBody(){
		return count_body;
	}
	public int getCategory(){
		return count_category;
	}
	public int getLink(){
		return count_link;
	}
	public int getreferences(){
		return count_references;
	}
	public int getinlinks(){
		return count_internallinks;
	}
	public int gettotal(){
		return total_value;
	}
	public Double getScore(){
		return (count_title*20.0 + count_info*5.0 + total_value) ;
		
	}

}
