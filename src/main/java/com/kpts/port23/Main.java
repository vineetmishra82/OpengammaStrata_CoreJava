package com.kpts.port23;

import static com.opengamma.strata.basics.currency.Currency.EUR;
import static com.opengamma.strata.basics.date.DayCounts.ACT_365F;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
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

import com.google.common.collect.ImmutableMap;
import com.opengamma.strata.basics.ReferenceData;
import com.opengamma.strata.basics.currency.Currency;
import com.opengamma.strata.basics.currency.CurrencyAmount;
import com.opengamma.strata.basics.currency.Payment;
import com.opengamma.strata.basics.date.BusinessDayAdjustment;
import com.opengamma.strata.basics.date.BusinessDayConventions;
import com.opengamma.strata.basics.date.DayCount;
import com.opengamma.strata.basics.date.DayCounts;
import com.opengamma.strata.basics.date.DaysAdjustment;
import com.opengamma.strata.basics.date.HolidayCalendarId;
import com.opengamma.strata.basics.date.HolidayCalendarIds;
import com.opengamma.strata.basics.schedule.Frequency;
import com.opengamma.strata.basics.schedule.PeriodicSchedule;
import com.opengamma.strata.basics.schedule.StubConvention;
import com.opengamma.strata.collect.array.DoubleArray;
import com.opengamma.strata.collect.tuple.Pair;
import com.opengamma.strata.market.curve.CurveMetadata;
import com.opengamma.strata.market.curve.CurveName;
import com.opengamma.strata.market.curve.Curves;
import com.opengamma.strata.market.curve.InterpolatedNodalCurve;
import com.opengamma.strata.market.curve.LegalEntityGroup;
import com.opengamma.strata.market.curve.RepoGroup;
import com.opengamma.strata.market.curve.interpolator.CurveInterpolator;
import com.opengamma.strata.market.curve.interpolator.CurveInterpolators;
import com.opengamma.strata.pricer.DiscountFactors;
import com.opengamma.strata.pricer.DiscountingPaymentPricer;
import com.opengamma.strata.pricer.ZeroRateDiscountFactors;
import com.opengamma.strata.pricer.bond.DiscountingFixedCouponBondProductPricer;
import com.opengamma.strata.pricer.bond.DiscountingFixedCouponBondTradePricer;
import com.opengamma.strata.pricer.bond.ImmutableLegalEntityDiscountingProvider;
import com.opengamma.strata.pricer.bond.LegalEntityDiscountingProvider;
import com.opengamma.strata.product.LegalEntityId;
import com.opengamma.strata.product.SecurityId;
import com.opengamma.strata.product.bond.FixedCouponBond;
import com.opengamma.strata.product.bond.FixedCouponBondYieldConvention;
import com.opengamma.strata.product.bond.ResolvedFixedCouponBond;
import com.opengamma.strata.product.bond.ResolvedFixedCouponBondSettlement;
import com.opengamma.strata.product.bond.ResolvedFixedCouponBondTrade;

public class Main {

	static List<String> headers = new ArrayList<String>();
	static double loopCount = -1;
	static CountDownLatch latch;
	static BigInteger totalSum = new BigInteger("0");
	

	public static void main(String[] args) {
		// Opengamma variables
		ReferenceData REF_DATA;
		SecurityId SECURITY_ID;
		LegalEntityId ISSUER_ID;
		long QUANTITY;
		FixedCouponBondYieldConvention YIELD_CONVENTION;
		double NOTIONAL;
		double FIXED_RATE;
		HolidayCalendarId EUR_CALENDAR;
		DaysAdjustment DATE_OFFSET;
		DayCount DAY_COUNT;
		LocalDate START_DATE;
		LocalDate END_DATE;

		LocalDate SETTLEMENT;
		BusinessDayAdjustment BUSINESS_ADJUST;
		PeriodicSchedule PERIOD_SCHEDULE;
		DaysAdjustment EX_COUPON;
		double CLEAN_PRICE;

		double DIRTY_PRICE;

		CurveInterpolator INTERPOLATOR = CurveInterpolators.LINEAR;
		CurveName NAME_REPO = CurveName.of("TestRepoCurve");
		CurveMetadata METADATA_REPO = Curves.zeroRates(NAME_REPO, ACT_365F);
		InterpolatedNodalCurve CURVE_REPO = InterpolatedNodalCurve.of(METADATA_REPO, DoubleArray.of(0.1, 2.0, 10.0),
				DoubleArray.of(0.05, 0.06, 0.09), INTERPOLATOR);
		RepoGroup GROUP_REPO = RepoGroup.of("GOVT1 BOND1");
		LegalEntityGroup GROUP_ISSUER = LegalEntityGroup.of("GOVT1");
		CurveName NAME_ISSUER = CurveName.of("TestIssuerCurve");
		CurveMetadata METADATA_ISSUER = Curves.zeroRates(NAME_ISSUER, ACT_365F);
		InterpolatedNodalCurve CURVE_ISSUER = InterpolatedNodalCurve.of(METADATA_ISSUER, DoubleArray.of(0.2, 9.0, 15.0),
				DoubleArray.of(0.03, 0.05, 0.13), INTERPOLATOR);
		Scanner scan = new Scanner(System.in);
		long projectStartTime = System.currentTimeMillis();

		ExecutorService executorService = null;

		Map<String, List<StringBuilder>> finalResult = new HashMap<>();

		List<Map<String, String>> itemList = new ArrayList<Map<String, String>>();
		System.out.println("Running strata project");
		
		int processors = Runtime.getRuntime ().availableProcessors();
		System.out.println("No of cores detected - "+processors);

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

//		int threadCount = 8;
//
//		System.out.print("\nNo of threads - ");
//
//		try {
//			threadCount = Integer.parseInt(scan.nextLine());
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			System.out.println("Thread count is 8 only as u entered wrong");
//		} finally {
//			scan.close();
//		}

		int lineNo = 1;
		long startTime = System.currentTimeMillis();
		
		int fileSize = itemList.size();
		
		int threadLoops = fileSize <= processors ? 1 : (fileSize/processors)+(fileSize%processors==0 ? 0 : 1);
		
		if(fileSize<=processors)
		{
			System.out.println("\nSince no of lines are less than available cores, only 1 cycle will be executed.."); 
		}
		else {
			System.out.println("Since no of lines are more than the cores, a total of "+threadLoops+" cycles will be executed..."); 
		}
		
	
		int rowsProcessedUpto = fileSize <= processors ? fileSize : processors;
		
		for(int f = 0;f<threadLoops;f++)
		{
			executorService = Executors.newFixedThreadPool(processors);			
			
			latch = new CountDownLatch(rowsProcessedUpto);
			
			System.out.println("Rows Processed upto = "+rowsProcessedUpto);
			
			for(int th = 0;th<rowsProcessedUpto;th++)
			{
				Map<String, String> item = itemList.get(lineNo-1);
				
				SECURITY_ID = getSecurityID(item.get("SECURITY_SCHEME") + "," + item.get("SECURITY_VALUE"));
				ISSUER_ID = getLegalEntityID(item.get("SECURITY_SCHEME") + "," + item.get("ISSUER_VALUE"));
				QUANTITY = Long.valueOf(item.get("QUANTITY").replace("L", ""));
				YIELD_CONVENTION = FixedCouponBondYieldConvention.DE_BONDS;
				NOTIONAL = Double.valueOf(item.get("NOTIONAL"));
				FIXED_RATE = Double.valueOf(item.get("FIXED_RATE"));
				EUR_CALENDAR = HolidayCalendarIds.EUTA;
				DATE_OFFSET = DaysAdjustment.ofBusinessDays(3, EUR_CALENDAR);
				DAY_COUNT = DayCounts.ACT_365F;
				START_DATE = getLocalDate(item.get("START_DATE"));
				END_DATE = getLocalDate(item.get("END_DATE"));
				SETTLEMENT = getLocalDate(item.get("SETTLEMENT"));
				BUSINESS_ADJUST = BusinessDayAdjustment.of(BusinessDayConventions.MODIFIED_FOLLOWING, EUR_CALENDAR);
				PERIOD_SCHEDULE = PeriodicSchedule.of(START_DATE, END_DATE, Frequency.P6M, BUSINESS_ADJUST,
						StubConvention.SHORT_INITIAL, false);
				EX_COUPON = DaysAdjustment.ofCalendarDays(-5, BUSINESS_ADJUST);
				CLEAN_PRICE = Double.valueOf(item.get("CLEAN_PRICE"));
				final LocalDate VAL_DATE = getLocalDate(item.get("VAL_DATE"));
				final DiscountingFixedCouponBondTradePricer TRADE_PRICER = DiscountingFixedCouponBondTradePricer.DEFAULT;
				final DiscountingFixedCouponBondProductPricer PRODUCT_PRICER = TRADE_PRICER.getProductPricer();
				REF_DATA = ReferenceData.standard();
				final DiscountingPaymentPricer PRICER_NOMINAL = DiscountingPaymentPricer.DEFAULT;

				// CalculateProduct();

				final ResolvedFixedCouponBond PRODUCT = FixedCouponBond.builder().securityId(SECURITY_ID)
						.dayCount(DAY_COUNT).fixedRate(FIXED_RATE).legalEntityId(ISSUER_ID).currency(EUR).notional(NOTIONAL)
						.accrualSchedule(PERIOD_SCHEDULE).settlementDateOffset(DATE_OFFSET)
						.yieldConvention(YIELD_CONVENTION).exCouponPeriod(EX_COUPON).build().resolve(REF_DATA);

				DIRTY_PRICE = PRODUCT_PRICER.dirtyPriceFromCleanPrice(PRODUCT, SETTLEMENT, CLEAN_PRICE);
				final Payment UPFRONT_PAYMENT = Payment.of(CurrencyAmount.of(EUR, -QUANTITY * NOTIONAL * DIRTY_PRICE),
						SETTLEMENT);
				// CalculateTrade();
				final ResolvedFixedCouponBondTrade TRADE = ResolvedFixedCouponBondTrade.builder().product(PRODUCT)
						.quantity(QUANTITY).settlement(ResolvedFixedCouponBondSettlement.of(SETTLEMENT, CLEAN_PRICE))
						.build();

				DiscountFactors dscRepo = ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_REPO);
				DiscountFactors dscIssuer = ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_ISSUER);
				final LegalEntityDiscountingProvider PROVIDER = ImmutableLegalEntityDiscountingProvider.builder()
						.issuerCurves(ImmutableMap.of(Pair.of(GROUP_ISSUER, EUR), dscIssuer))
						.issuerCurveGroups(ImmutableMap.of(ISSUER_ID, GROUP_ISSUER))
						.repoCurves(ImmutableMap.of(Pair.of(GROUP_REPO, EUR), dscRepo))
						.repoCurveSecurityGroups(ImmutableMap.of(SECURITY_ID, GROUP_REPO)).valuationDate(VAL_DATE).build();

				final String lineNum = String.valueOf(lineNo);
				List<StringBuilder> resultList = new ArrayList<StringBuilder>();

				double loopSize = loopCount == -1 ? Double.valueOf(item.get("Loops")) : loopCount;
			
				Runnable calculate = new Runnable() {

					@Override
					public void run() {
						
						for (double i = 0; i < loopSize; i++) {
							
							CurrencyAmount computedTrade = TRADE_PRICER.presentValue(TRADE, PROVIDER);
							CurrencyAmount computedProduct = PRODUCT_PRICER.presentValue(PRODUCT, PROVIDER);
							CurrencyAmount pvPayment = PRICER_NOMINAL.presentValue(UPFRONT_PAYMENT,
									ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_REPO));

							synchronized (resultList) {
								resultList
										.add(new StringBuilder(computedTrade.getCurrency() + ":" + computedTrade.getAmount()
												+ "," + computedTrade.getCurrency() + ":" + computedProduct.getAmount()
												+ "," + computedTrade.getCurrency() + ":" + pvPayment.getAmount()));
							}
							
						}

						latch.countDown();
						
						synchronized (finalResult) {
							finalResult.put(lineNum, resultList);

						}


					}

				};
				executorService.submit(calculate);
				
				lineNo++;
			}
			
			executorService.shutdown();

			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("Processed "+rowsProcessedUpto+" rows from file....");
			
			rowsProcessedUpto = fileSize-lineNo >= processors ? processors : fileSize-lineNo;
			
			
		}
//		for (Map<String, String> item : itemList) {
//
//			// Setting opengamma variables
//
//			SECURITY_ID = getSecurityID(item.get("SECURITY_SCHEME") + "," + item.get("SECURITY_VALUE"));
//			ISSUER_ID = getLegalEntityID(item.get("SECURITY_SCHEME") + "," + item.get("ISSUER_VALUE"));
//			QUANTITY = Long.valueOf(item.get("QUANTITY").replace("L", ""));
//			YIELD_CONVENTION = FixedCouponBondYieldConvention.DE_BONDS;
//			NOTIONAL = Double.valueOf(item.get("NOTIONAL"));
//			FIXED_RATE = Double.valueOf(item.get("FIXED_RATE"));
//			EUR_CALENDAR = HolidayCalendarIds.EUTA;
//			DATE_OFFSET = DaysAdjustment.ofBusinessDays(3, EUR_CALENDAR);
//			DAY_COUNT = DayCounts.ACT_365F;
//			START_DATE = getLocalDate(item.get("START_DATE"));
//			END_DATE = getLocalDate(item.get("END_DATE"));
//			SETTLEMENT = getLocalDate(item.get("SETTLEMENT"));
//			BUSINESS_ADJUST = BusinessDayAdjustment.of(BusinessDayConventions.MODIFIED_FOLLOWING, EUR_CALENDAR);
//			PERIOD_SCHEDULE = PeriodicSchedule.of(START_DATE, END_DATE, Frequency.P6M, BUSINESS_ADJUST,
//					StubConvention.SHORT_INITIAL, false);
//			EX_COUPON = DaysAdjustment.ofCalendarDays(-5, BUSINESS_ADJUST);
//			CLEAN_PRICE = Double.valueOf(item.get("CLEAN_PRICE"));
//			final LocalDate VAL_DATE = getLocalDate(item.get("VAL_DATE"));
//			final DiscountingFixedCouponBondTradePricer TRADE_PRICER = DiscountingFixedCouponBondTradePricer.DEFAULT;
//			final DiscountingFixedCouponBondProductPricer PRODUCT_PRICER = TRADE_PRICER.getProductPricer();
//			REF_DATA = ReferenceData.standard();
//			final DiscountingPaymentPricer PRICER_NOMINAL = DiscountingPaymentPricer.DEFAULT;
//
//			// CalculateProduct();
//
//			final ResolvedFixedCouponBond PRODUCT = FixedCouponBond.builder().securityId(SECURITY_ID)
//					.dayCount(DAY_COUNT).fixedRate(FIXED_RATE).legalEntityId(ISSUER_ID).currency(EUR).notional(NOTIONAL)
//					.accrualSchedule(PERIOD_SCHEDULE).settlementDateOffset(DATE_OFFSET)
//					.yieldConvention(YIELD_CONVENTION).exCouponPeriod(EX_COUPON).build().resolve(REF_DATA);
//
//			DIRTY_PRICE = PRODUCT_PRICER.dirtyPriceFromCleanPrice(PRODUCT, SETTLEMENT, CLEAN_PRICE);
//			final Payment UPFRONT_PAYMENT = Payment.of(CurrencyAmount.of(EUR, -QUANTITY * NOTIONAL * DIRTY_PRICE),
//					SETTLEMENT);
//			// CalculateTrade();
//			final ResolvedFixedCouponBondTrade TRADE = ResolvedFixedCouponBondTrade.builder().product(PRODUCT)
//					.quantity(QUANTITY).settlement(ResolvedFixedCouponBondSettlement.of(SETTLEMENT, CLEAN_PRICE))
//					.build();
//
//			DiscountFactors dscRepo = ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_REPO);
//			DiscountFactors dscIssuer = ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_ISSUER);
//			final LegalEntityDiscountingProvider PROVIDER = ImmutableLegalEntityDiscountingProvider.builder()
//					.issuerCurves(ImmutableMap.of(Pair.of(GROUP_ISSUER, EUR), dscIssuer))
//					.issuerCurveGroups(ImmutableMap.of(ISSUER_ID, GROUP_ISSUER))
//					.repoCurves(ImmutableMap.of(Pair.of(GROUP_REPO, EUR), dscRepo))
//					.repoCurveSecurityGroups(ImmutableMap.of(SECURITY_ID, GROUP_REPO)).valuationDate(VAL_DATE).build();
//
//			final String lineNum = String.valueOf(lineNo);
//			List<StringBuilder> resultList = new ArrayList<StringBuilder>();
//
//			double loopSize = loopCount == -1 ? Double.valueOf(item.get("Loops")) : loopCount;
//
//			executorService = Executors.newFixedThreadPool(threadCount);
//
////			Product product = new Product(item.get("SECURITY_SCHEME") + "," + item.get("SECURITY_VALUE"),
////					item.get("SECURITY_SCHEME") + "," + item.get("ISSUER_VALUE"),
////					Long.valueOf(item.get("QUANTITY").replace("L", "")), Double.valueOf(item.get("NOTIONAL")),
////					Double.valueOf(item.get("FIXED_RATE")), item.get("START_DATE"), item.get("END_DATE"),
////					item.get("SETTLEMENT"), Double.valueOf(item.get("CLEAN_PRICE")), item.get("VAL_DATE"),
////					String.valueOf(lineNo));
//
//			
//				Runnable calculate = new Runnable() {
//
//					@Override
//					public void run() {
//						
//						for (double i = 0; i < loopSize; i++) {
//							
//							CurrencyAmount computedTrade = TRADE_PRICER.presentValue(TRADE, PROVIDER);
//							CurrencyAmount computedProduct = PRODUCT_PRICER.presentValue(PRODUCT, PROVIDER);
//							CurrencyAmount pvPayment = PRICER_NOMINAL.presentValue(UPFRONT_PAYMENT,
//									ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_REPO));
//
//							synchronized (resultList) {
//								resultList
//										.add(new StringBuilder(computedTrade.getCurrency() + ":" + computedTrade.getAmount()
//												+ "," + computedTrade.getCurrency() + ":" + computedProduct.getAmount()
//												+ "," + computedTrade.getCurrency() + ":" + pvPayment.getAmount()));
//							}
//							
//						}
//
//						latch.countDown();
//						
//						synchronized (finalResult) {
//							finalResult.put(lineNum, resultList);
//
//						}
//
//
//					}
//
//				};
//				executorService.submit(calculate);
//			
//		
//
//			
//			lineNo++;
//
//		}
//
//		executorService.shutdown();
//
//		try {
//			latch.await();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		long duration = System.currentTimeMillis() - startTime;
		// checking results

		for (int i = 0; i < finalResult.size(); i++) {

			int size = finalResult.get(String.valueOf(i + 1)).size();
			System.out.println("\nFor row " + (i + 1) + " the answer size is " + size);

		}

		System.out.printf("\nTime taken for calculations only : %s ms%n", duration);
		System.out.printf("Time taken for Entire Project with File Reading & storing results : %s ms%n",
				System.currentTimeMillis() - projectStartTime);

		System.exit(0);

	}

	private static SecurityId getSecurityID(String sECURITY_ID2) {
		String[] values = sECURITY_ID2.split(",");

		return SecurityId.of(values[0], values[1]);
	}

	private static LegalEntityId getLegalEntityID(String iSSUER_ID2) {
		String[] values = iSSUER_ID2.split(",");

		return LegalEntityId.of(values[0], values[1]);
	}

	private static LocalDate getLocalDate(String sTART_DATE2) {

		String[] values = sTART_DATE2.split("-");
		return LocalDate.of(Integer.valueOf(values[2]), Integer.valueOf(values[1]), Integer.valueOf(values[0]));
	}

}
