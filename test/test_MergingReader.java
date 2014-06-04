package vps.mapreduce.test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import vps.mapreduce.Configuration;
import vps.mapreduce.reader.FileInput;
import vps.mapreduce.reader.InputReader;
import vps.mapreduce.reader.MergingReader;

public class test_MergingReader {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final int parts = 3;
		final String filename = "/home/michael/test/test.txt";
		int count = 0;
		
		try {
			count = Configuration.getLineCount(filename);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		InputReader[] iR = new InputReader[parts];
		
		try {
			for (int i = 0; i < parts; i++) {
				iR[i] = new InputReader(new FileInput(filename, i*(count/parts), parts));
				System.out.println(i*(count/parts) + " " + parts);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		MergingReader<String> mR = new MergingReader<String>(iR);
		
		String test = "";
		int i = 1;
		while ((test = mR.read()) != null) {
			System.out.println(test + " " + i);
			i++;
		}
		
		mR.close();
	}
	
	public static int count(String filename)
    {
     	try {
    		File file = new File(filename);
    		
    		if (file.exists()) {
    			int linecount = 0;
    			
    			Scanner in = new Scanner(file, "UTF-8");
    			while (in.hasNextLine()) {
    				linecount += in.nextLine().length() + 1;
    			}
    			in.close();
    			
    		    return linecount;
    		} else {
    			 System.out.println("File does not exists!");
    		}
    	} catch(IOException e) {
    		e.printStackTrace();
    	}

     	return 0;
    }
}