package com.kpts.port23;

import static com.opengamma.strata.basics.currency.Currency.EUR;
import static com.opengamma.strata.basics.date.DayCounts.ACT_365F;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.opengamma.strata.basics.ReferenceData;
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

public class Product {

	 private float[] input;
	    private float[] output;

	 private ReferenceData REF_DATA;	
	  private SecurityId SECURITY_ID;
	  private LegalEntityId ISSUER_ID;
	  private long QUANTITY;
	  private FixedCouponBondYieldConvention YIELD_CONVENTION;
	  private double NOTIONAL;
	  private double FIXED_RATE;
	  private HolidayCalendarId EUR_CALENDAR;
	  private DaysAdjustment DATE_OFFSET;
	  private DayCount DAY_COUNT;
	  private LocalDate START_DATE;
	  private LocalDate END_DATE;
	  private LocalDate VAL_DATE;
	  private LocalDate SETTLEMENT;
	  private BusinessDayAdjustment BUSINESS_ADJUST;
	  private PeriodicSchedule PERIOD_SCHEDULE;
	  private DaysAdjustment EX_COUPON;
	  private double CLEAN_PRICE;
	  private DiscountingFixedCouponBondTradePricer TRADE_PRICER;
	  private DiscountingFixedCouponBondProductPricer PRODUCT_PRICER;
	  private DiscountingPaymentPricer PRICER_NOMINAL;
	  private ResolvedFixedCouponBond PRODUCT;
	  double DIRTY_PRICE;
	  private Payment UPFRONT_PAYMENT;
	  private ResolvedFixedCouponBondTrade TRADE;
	  private LegalEntityDiscountingProvider PROVIDER;
	  private CurveInterpolator INTERPOLATOR = CurveInterpolators.LINEAR;
	  private CurveName NAME_REPO = CurveName.of("TestRepoCurve");
	  private CurveMetadata METADATA_REPO = Curves.zeroRates(NAME_REPO, ACT_365F);
	  private InterpolatedNodalCurve CURVE_REPO = InterpolatedNodalCurve.of(
	      METADATA_REPO, DoubleArray.of(0.1, 2.0, 10.0), DoubleArray.of(0.05, 0.06, 0.09), INTERPOLATOR);
	  private RepoGroup GROUP_REPO = RepoGroup.of("GOVT1 BOND1");
	  private LegalEntityGroup GROUP_ISSUER = LegalEntityGroup.of("GOVT1");
	  private CurveName NAME_ISSUER = CurveName.of("TestIssuerCurve");
	  private CurveMetadata METADATA_ISSUER = Curves.zeroRates(NAME_ISSUER, ACT_365F);
	  private InterpolatedNodalCurve CURVE_ISSUER = InterpolatedNodalCurve.of(
		      METADATA_ISSUER, DoubleArray.of(0.2, 9.0, 15.0), DoubleArray.of(0.03, 0.05, 0.13), INTERPOLATOR);
	  
	  public String rowNum;
	  
	  public boolean isprinted = false;
	  
	  public static double computeCount;
	  
	  public Product(String sECURITY_ID, String iSSUER_ID, long qUANTITY,
			double nOTIONAL, double fIXED_RATE,
			String sTART_DATE,
			String eND_DATE,String settlementDate,double cleanPrice,String valDate,String rowNum) {
		SECURITY_ID = getSecurityID(sECURITY_ID);
		ISSUER_ID = getLegalEntityID(iSSUER_ID);
		QUANTITY = qUANTITY;
		YIELD_CONVENTION  = FixedCouponBondYieldConvention.DE_BONDS;
		NOTIONAL = nOTIONAL;
		FIXED_RATE = fIXED_RATE;
		EUR_CALENDAR = HolidayCalendarIds.EUTA;
		DATE_OFFSET = DaysAdjustment.ofBusinessDays(3, EUR_CALENDAR);
		DAY_COUNT = DayCounts.ACT_365F;
		START_DATE = getLocalDate(sTART_DATE);
		END_DATE = getLocalDate(eND_DATE);
		SETTLEMENT = getLocalDate(settlementDate);
		BUSINESS_ADJUST =
			      BusinessDayAdjustment.of(BusinessDayConventions.MODIFIED_FOLLOWING, EUR_CALENDAR);
		PERIOD_SCHEDULE = PeriodicSchedule.of(
			      START_DATE, END_DATE, Frequency.P6M, BUSINESS_ADJUST, StubConvention.SHORT_INITIAL, false);
		EX_COUPON = DaysAdjustment.ofCalendarDays(-5, BUSINESS_ADJUST);
		CLEAN_PRICE = cleanPrice;
		VAL_DATE = getLocalDate(valDate);
		TRADE_PRICER  = DiscountingFixedCouponBondTradePricer.DEFAULT;
		PRODUCT_PRICER = TRADE_PRICER.getProductPricer();
		REF_DATA = ReferenceData.standard();
		PRICER_NOMINAL = DiscountingPaymentPricer.DEFAULT;
		CalculateProduct();
		DIRTY_PRICE  = PRODUCT_PRICER.dirtyPriceFromCleanPrice(PRODUCT, SETTLEMENT, CLEAN_PRICE);
		UPFRONT_PAYMENT = Payment.of(
			      CurrencyAmount.of(EUR, -QUANTITY * NOTIONAL * DIRTY_PRICE), SETTLEMENT);
		CalculateTrade();
		PROVIDER  = createRatesProvider(VAL_DATE);
		this.rowNum = rowNum;
	  }

	public Product(float[] input, float[] output) {
        this.input = input;
        this.output = output;
    }

	private LegalEntityDiscountingProvider createRatesProvider(LocalDate valuationDate) {
	    DiscountFactors dscRepo = ZeroRateDiscountFactors.of(EUR, valuationDate, CURVE_REPO);
	    DiscountFactors dscIssuer = ZeroRateDiscountFactors.of(EUR, valuationDate, CURVE_ISSUER);
	    LegalEntityDiscountingProvider provider = ImmutableLegalEntityDiscountingProvider.builder()
	        .issuerCurves(ImmutableMap.of(Pair.of(GROUP_ISSUER, EUR), dscIssuer))
	        .issuerCurveGroups(ImmutableMap.of(ISSUER_ID, GROUP_ISSUER))
	        .repoCurves(ImmutableMap.of(Pair.of(GROUP_REPO, EUR), dscRepo))
	        .repoCurveSecurityGroups(ImmutableMap.of(SECURITY_ID, GROUP_REPO))
	        .valuationDate(valuationDate)
	        .build();
	    return provider;
	  }

	private void CalculateTrade() {
		TRADE = ResolvedFixedCouponBondTrade.builder()
			      .product(PRODUCT)
			      .quantity(QUANTITY)
			      .settlement(ResolvedFixedCouponBondSettlement.of(SETTLEMENT, CLEAN_PRICE))
			      .build();
		
	}

	private void CalculateProduct() {
		
		PRODUCT = FixedCouponBond.builder()
			      .securityId(SECURITY_ID)
			      .dayCount(DAY_COUNT)
			      .fixedRate(FIXED_RATE)
			      .legalEntityId(ISSUER_ID)
			      .currency(EUR)
			      .notional(NOTIONAL)
			      .accrualSchedule(PERIOD_SCHEDULE)
			      .settlementDateOffset(DATE_OFFSET)
			      .yieldConvention(YIELD_CONVENTION)
			      .exCouponPeriod(EX_COUPON)
			      .build()
			      .resolve(REF_DATA);
		
	}

	private LocalDate getLocalDate(String sTART_DATE2) {
		
		String[] values = sTART_DATE2.split("-");
		return LocalDate.of(Integer.valueOf(values[2]),
				Integer.valueOf(values[1]),
				Integer.valueOf(values[0]));
	}

	private LegalEntityId getLegalEntityID(String iSSUER_ID2) {
		String[] values = iSSUER_ID2.split(",");
		
		return LegalEntityId.of(values[0],values[1]);
	}

	private SecurityId getSecurityID(String sECURITY_ID2) {
		
		String[] values = sECURITY_ID2.split(",");
		
		return SecurityId.of(values[0],values[1]);
	}
	
	public String calculatePresentValue() {
	    
		 CurrencyAmount computedTrade = TRADE_PRICER.presentValue(TRADE, PROVIDER);
		    CurrencyAmount computedProduct = PRODUCT_PRICER.presentValue(PRODUCT, PROVIDER);
		    CurrencyAmount pvPayment =
		    PRICER_NOMINAL.presentValue(UPFRONT_PAYMENT, ZeroRateDiscountFactors.of(EUR, VAL_DATE, CURVE_REPO));
		
	    
	    String str = "";
	    str+="Present Values ->";
	    str+="\nComputed Trade - "+computedTrade.getCurrency()+" : "+computedTrade.getAmount();
	    str+="\nComputed Product - "+computedProduct.getCurrency()+" : "+computedProduct.getAmount();
	    str+="\nPv Payment - "+pvPayment.getCurrency()+" : "+pvPayment.getAmount();
	    
	   return str;
	   
	}

	private void WriteToFile(String str) {
		  try {
		    	
		    	BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("resultData.txt",true));
		    	
		    	bufferedWriter.append(str);
		    	
		    	bufferedWriter.close();
		    	
		    }catch(FileNotFoundException ex)
		    {
		    	System.out.println("Target file not found to write");
		    }
		    catch(IOException ex)
		    {
		    	System.out.println("Could not write to file...");
		    }
		
	}
	
	 
	  
}
