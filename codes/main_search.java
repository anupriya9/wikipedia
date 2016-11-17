package driver;

import java.io.BufferedReader;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyStore.LoadStoreParameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import search.Searching_query;

public class main_search {

	private static Scanner input_query;
		public static void main(String args[]) throws Exception{
		
		//String path_to_index = "/home/anupriya/ire/index_56";
		String path_to_index;
		try{
			path_to_index = args[0];
			Searching_query search = new Searching_query(path_to_index);
			String query;
			boolean flag = true;
			while(flag==true){
			 input_query = new Scanner(System.in);
			 System.out.println("ENTER QUERY : ");
			 query = input_query.nextLine();
			 System.out.println("YOUR QUERY IS: " + query);
			 /*calculating the start time*/
			double start_time_of_query = System.currentTimeMillis();
				search.searching_query(query);
				/*calculating the end time*/
			double end_time_of_query   = System.currentTimeMillis();
			double timetaken = end_time_of_query - start_time_of_query;
			System.out.println("SEARCH TIME : "+ timetaken/1000 + " sec");
			
			 System.out.print("Want next Query (1 for YES,0 for NO)? : ");
			 if(!(input_query.nextLine()).equalsIgnoreCase("1")){
				 flag = false;
				 System.out.println();
			 }
			}	
			
			}
		catch(ArrayIndexOutOfBoundsException e){
			e.printStackTrace();
			System.out.println("EXCEPTION : Enter path of the index folder.");
		}
		
		
	}	

}
