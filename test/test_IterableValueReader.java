package vps.mapreduce.test;

import java.io.IOException;
import java.util.Iterator;

import vps.mapreduce.reader.FileInput;
import vps.mapreduce.reader.InputReader;
import vps.mapreduce.reader.IterableValueReader;
import vps.mapreduce.reader.KeyValueReader;
import vps.mapreduce.util.KeyValuePair;

public class test_IterableValueReader {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		IterableValueReader<String,String> iVR = null;
		
		try {
			iVR = new IterableValueReader<String,String>
			(
				new KeyValueReader
				(
					new InputReader
					(
						new FileInput("E:\\test\\example_wc.txt")
					)
				)
			);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test KeyValuePair
		KeyValuePair<String, Iterable<String>> kVP = iVR.read();
		System.out.println(kVP.toString());
		
		//Test Iterator
		Iterator<String> iter = iVR.iterator();
		String test = null;
		
		while (iter.hasNext()) {
			test = iter.next();
			System.out.println(test);
		}
		
		//Close reader
		iVR.close();
	}

}
