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
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {

	static List<String> headers = new ArrayList<String>();
	static double loopCount = -1;

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		long projectStartTime = System.currentTimeMillis();

		ExecutorService executorService = null;

		Map<String, List<StringBuilder>> finalResult = new HashMap<>();
		
		  CountDownLatch latch = new CountDownLatch(10);

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

			int noOfRows = -1;

			System.out.print("\nNo of rows to read - ");

			try {
				noOfRows = Integer.parseInt(scan.nextLine());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("All rows will be read as u entered wrong");
			}

			readLines: while ((line = buffReader.readLine()) != null) {
				String[] values = line.split(",");

				if (count == noOfRows) {
					break readLines;
				}

				if (count == 0) {
					for (String string : values) {
						headers.add(string);

					}

					System.out.println("headers size is " + headers.size());
				}

				else if (values.length > 0) {

					Map<String, String> item = new TreeMap<String, String>();

					item.put("RowNo", String.valueOf(count));

					// Checking empty line

					int blankCount = 0;
					for (String string : values) {

						if (string.equals("")) {
							blankCount++;
						}
					}

					if (blankCount >= headers.size()) {
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

			System.out.println("\nTotal rows are - " + itemList.size());

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
		} finally {
			scan.close();
		}

		int lineNo = 1;
		long startTime = System.currentTimeMillis();
		for (Map<String, String> item : itemList) {

			final String lineNum = String.valueOf(lineNo);
			List<StringBuilder> resultList = new ArrayList<StringBuilder>();

			double loopSize = loopCount == -1 ? Double.valueOf(item.get("Loops")) : loopCount;

			executorService = Executors.newFixedThreadPool(threadCount);

//			Product product = new Product(item.get("SECURITY_SCHEME") + "," + item.get("SECURITY_VALUE"),
//					item.get("SECURITY_SCHEME") + "," + item.get("ISSUER_VALUE"),
//					Long.valueOf(item.get("QUANTITY").replace("L", "")), Double.valueOf(item.get("NOTIONAL")),
//					Double.valueOf(item.get("FIXED_RATE")), item.get("START_DATE"), item.get("END_DATE"),
//					item.get("SETTLEMENT"), Double.valueOf(item.get("CLEAN_PRICE")), item.get("VAL_DATE"),
//					String.valueOf(lineNo));

			Runnable calculate = new Runnable() {

				@Override
				public void run() {

					for (double i = 0; i < loopSize; i++) {

						resultList.add(getFactorial(i));
					}
					latch.countDown();	
					finalResult.put(lineNum, resultList);
					
				}

				
			};

			executorService.submit(calculate);
//
//			System.out.println("Processed Row " + lineNo + " for " + String.format("%.0f", loopSize)
//					+ " times with result size " + finalResult.get(lineNum).size() + "\n");

			lineNo++;

		}

		executorService.shutdown();

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long duration = System.currentTimeMillis() - startTime;
		// checking results

		for (int i = 0; i < finalResult.size(); i++) {

			System.out.println("\nFor row " + (i + 1) + " the answer size is " + finalResult.get(String.valueOf(i + 1)).size());
		}

		System.out.printf("\nTime taken for calculations only : %s ms%n",duration );
		System.out.printf("Time taken for Entire Project with File Reading & storing results : %s ms%n",
				System.currentTimeMillis() - projectStartTime);
		
		System.exit(0);

	}
	
	private static StringBuilder getFactorial(double i) {

		StringBuilder stB = new StringBuilder();
		
		stB.append(String.valueOf(i*i));
		return stB;
	}

}
