package vps.mapreduce.test;

public class test_CompareTo {

	   public static void main(String args[]) {
	      String str1 = "aaa";
		  String str2 = "bbb";
	      String str3 = "ccc";

	      int result = str1.compareTo( str2 );
	      System.out.println(result);
		  
	      result = str2.compareTo( str3 );
	      System.out.println(result);
		 
	      result = str3.compareTo( str1 );
	      System.out.println(result);
	   }
	}