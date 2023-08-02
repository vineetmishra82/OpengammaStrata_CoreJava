package com.kpts.port23;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

	static List<String> headers = new ArrayList<String>();
	static double loopCount = -1;

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		 long projectStartTime = System.currentTimeMillis();
		 
		 ExecutorService executorService = null;
		 
		 Map<String,List<String>> finalResult = new HashMap<>();

		List<Map<String, String>> itemList = new ArrayList<Map<String, String>>();
		System.out.println("Running strata project");

		try {
			FileReader fileReader = new FileReader(args[0]);

			if (args.length == 2) {
				loopCount = Double.valueOf(args[1]);
				System.out.println("Per row loop count is " + (int) loopCount);
			} else {
				System.out.println(
						"No loop count provided in argument, so per line loop will be taken into consideration.");
			}

			if (fileReader != null)
				System.out.println("File found..start processing.. ");

			BufferedReader buffReader = new BufferedReader(fileReader);

			String line = "";
			int count = 0;
			
			readLines:
			while ((line = buffReader.readLine()) != null) {
				String[] values = line.split(",");						

				if (count == 0) {
					for (String string : values) {
						headers.add(string);

					}

					System.out.println("headers size is " + headers.size());
				}

				else if (values.length > 0) {

					Map<String, String> item = new TreeMap<String, String>();

					item.put("RowNo", String.valueOf(count));
					
					//Checking empty line
					
					int blankCount = 0;
					for (String string : values) {
						
						if(string.equals(""))
						{
							blankCount++;
						}
					}
					
					if(blankCount >= headers.size())
					{
						break readLines;
					}
					
					
					for (int i = 0; i < headers.size(); i++) {
						item.put(headers.get(i), values[i]);
					}

					itemList.add(item);
				}

				count++;

			}
			buffReader.close();
			
			System.out.println("\nTotal rows are - "+itemList.size());
			
			System.out.print("\nContinue ? - ");
			
			String response = scan.nextLine();
			
			if(response.toUpperCase().equals("N"))
			{
				System.exit(0);
			}
			
			
			
					

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("File not found at location...terminating");
			System.exit(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("File reading error - " + e.getLocalizedMessage());
		} catch (NumberFormatException e) {
			loopCount = -1;
		}
		
		int threadCount = 8;
		
		System.out.print("\nNo of threads - ");
		
		try {
			threadCount = Integer.parseInt(scan.nextLine());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Thread count is 8 only as u entered wrong");
		}
		finally {
scan.close();
		}

		int lineNo = 1;
		 long startTime = System.currentTimeMillis();
		for (Map<String, String> item : itemList) {
			
						
			final String lineNum = String.valueOf(lineNo);
			List<String> resultList = new ArrayList<String>();
			
			finalResult.put(lineNum, resultList);
			
			Product product = new Product(item.get("SECURITY_SCHEME") + "," + item.get("SECURITY_VALUE"),
					item.get("SECURITY_SCHEME") + "," + item.get("ISSUER_VALUE"),
					Long.valueOf(item.get("QUANTITY").replace("L", "")), Double.valueOf(item.get("NOTIONAL")),
					Double.valueOf(item.get("FIXED_RATE")), item.get("START_DATE"), item.get("END_DATE"),
					item.get("SETTLEMENT"), Double.valueOf(item.get("CLEAN_PRICE")), item.get("VAL_DATE"),
					String.valueOf(lineNo));

			double loopSize = loopCount == -1 ? Double.valueOf(item.get("Loops")) : loopCount;
		
			executorService = Executors.newFixedThreadPool(threadCount);
			
			Runnable calculateValue = () -> {
				
				if(lineNum.equals(String.valueOf(328)))
				{
					for (double i = 0; i < loopSize; i++) {
						
						
						String value = "";
						
						try {
							value = product.calculatePresentValue();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							continue;
						}
						
						if(!value.equals(null))
						{
							List<String> list = finalResult.get(lineNum);
							
							if(list.equals(null))
							{
								list = new ArrayList<>();
							}
							
							list.add(value);
							
							finalResult.put(lineNum, list);
							
							if(i==loopSize-1)
							{
								System.out.println("For row "+lineNum+" result list size is "+list.size());
								if(itemList.indexOf(item)==itemList.size()-1)
								{
									System.out.printf("\nTime taken for calculations only : %s ms%n", System.currentTimeMillis() - startTime);
									System.out.printf("Time taken for Entire Project with File Reading & storing results : %s ms%n", System.currentTimeMillis() - projectStartTime);
									 
								}
							}
							
							
							
						}
						
										
					}
				}

			
			};
			
			executorService.submit(calculateValue);

	        // Shutdown the executor service.
			executorService.shutdown();
			
			 List<String> list = finalResult.get(String.valueOf(lineNo));
				System.out.println("Processed Row " + lineNo + " for " + String.valueOf(loopSize) + " times\n");

				lineNo++;	
				
				if(itemList.indexOf(item)==itemList.size()-1)
				{
					System.out.printf("\nTime taken for calculations only : %s ms%n", System.currentTimeMillis() - startTime);
					System.out.printf("Time taken for Entire Project with File Reading & storing results : %s ms%n", System.currentTimeMillis() - projectStartTime);
					 
				}
	       
			
		}
		
			
		
	}
		
		
}
	
