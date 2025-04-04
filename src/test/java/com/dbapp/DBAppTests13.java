package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests13
{

	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ku9", cols0);
		String [][] records_ku9 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_ku9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ku9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ku9", records_ku9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record13: [a13, b1, c1]
		//record14: [a14, b0, c2]
		//record15: [a15, b1, c0]
		//record16: [a16, b0, c1]
		//record17: [a17, b1, c2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ku9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		int pageCount0 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("ku9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("d9r6", cols1);
		String [][] records_d9r6 = new String[271][cols1.length];
		for(int i=0;i<271;i++)
		{
			records_d9r6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d9r6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d9r6", records_d9r6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record266: [a266, b0, c2, d2]
		//record267: [a267, b1, c0, d3]
		//record268: [a268, b0, c1, d0]
		//record269: [a269, b1, c2, d1]
		//record270: [a270, b0, c0, d2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("d9r6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 136, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		int pageCount1 = (int)Math.ceil(271.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("d9r6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+136));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zhs83", cols2);
		String [][] records_zhs83 = new String[225][cols2.length];
		for(int i=0;i<225;i++)
		{
			records_zhs83[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zhs83[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zhs83", records_zhs83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3]
		String[] ConditionColumns2 = {"j","b","m","i","g","h"};
		String[] ConditionColumnsValues2 = {"j0","b0","m11","i5","g1","h2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("zhs83", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11]
		int pageCount2 = (int)Math.ceil(225.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("zhs83");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sa6m", cols3);
		String [][] records_sa6m = new String[499][cols3.length];
		for(int i=0;i<499;i++)
		{
			records_sa6m[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_sa6m[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("sa6m", records_sa6m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		//record497: [a497, b1, c2, d1, e2, f5, g0]
		//record498: [a498, b0, c0, d2, e3, f0, g1]
		String[] ConditionColumns3 = {"f","a"};
		String[] ConditionColumnsValues3 = {"f2","a194"};
		ArrayList<String[]> tableSelect3 = DBApp.select("sa6m", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a194, b0, c2, d2, e4, f2, g5]
		int pageCount3 = (int)Math.ceil(499.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("sa6m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wg38", cols4);
		String [][] records_wg38 = new String[216][cols4.length];
		for(int i=0;i<216;i++)
		{
			records_wg38[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wg38[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wg38", records_wg38[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8, m4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7]
		String[] ConditionColumns4 = {"i"};
		String[] ConditionColumnsValues4 = {"i6"};
		ArrayList<String[]> tableSelect4 = DBApp.select("wg38", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 24, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		int pageCount4 = (int)Math.ceil(216.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("wg38");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i6zct", cols0);
		String [][] records_i6zct = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_i6zct[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i6zct[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i6zct", records_i6zct[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		String[] ConditionColumns0 = {"d","h","i","g"};
		String[] ConditionColumnsValues0 = {"d2","h6","i6","g6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i6zct", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("i6zct");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("q15", cols1);
		String [][] records_q15 = new String[230][cols1.length];
		for(int i=0;i<230;i++)
		{
			records_q15[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q15[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q15", records_q15[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0]
		//record226: [a226, b0, c1, d2, e1]
		//record227: [a227, b1, c2, d3, e2]
		//record228: [a228, b0, c0, d0, e3]
		//record229: [a229, b1, c1, d1, e4]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q15", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 57, records:
		//record0: [a3, b1, c0, d3, e3]
		//record1: [a7, b1, c1, d3, e2]
		//record2: [a11, b1, c2, d3, e1]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a19, b1, c1, d3, e4]
		int pageCount1 = (int)Math.ceil(230.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("q15");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+57));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i2m", cols2);
		String [][] records_i2m = new String[373][cols2.length];
		for(int i=0;i<373;i++)
		{
			records_i2m[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i2m[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i2m", records_i2m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("i2m", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 62, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		int pageCount2 = (int)Math.ceil(373.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("i2m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+62));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("is1", cols3);
		String [][] records_is1 = new String[220][cols3.length];
		for(int i=0;i<220;i++)
		{
			records_is1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_is1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("is1", records_is1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("is1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 37, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		int pageCount3 = (int)Math.ceil(220.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("is1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("en1", cols4);
		String [][] records_en1 = new String[364][cols4.length];
		for(int i=0;i<364;i++)
		{
			records_en1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_en1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("en1", records_en1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0]
		String[] ConditionColumns4 = {"h"};
		String[] ConditionColumnsValues4 = {"h3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("en1", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 46, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		int pageCount4 = (int)Math.ceil(364.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("en1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+46));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w41c8", cols0);
		String [][] records_w41c8 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			records_w41c8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w41c8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w41c8", records_w41c8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w41c8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 18, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		int pageCount0 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("w41c8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+18));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yy97", cols1);
		String [][] records_yy97 = new String[491][cols1.length];
		for(int i=0;i<491;i++)
		{
			records_yy97[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yy97[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yy97", records_yy97[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10]
		String[] ConditionColumns1 = {"l","f"};
		String[] ConditionColumnsValues1 = {"l8","f2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("yy97", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 41, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8]
		int pageCount1 = (int)Math.ceil(491.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("yy97");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+41));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("x9k", cols0);
		String [][] records_x9k = new String[408][cols0.length];
		for(int i=0;i<408;i++)
		{
			records_x9k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x9k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x9k", records_x9k[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record403: [a403]
		//record404: [a404]
		//record405: [a405]
		//record406: [a406]
		//record407: [a407]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a120"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x9k", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a120]
		int pageCount0 = (int)Math.ceil(408.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("x9k");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("w59", cols1);
		String [][] records_w59 = new String[131][cols1.length];
		for(int i=0;i<131;i++)
		{
			records_w59[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w59[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w59", records_w59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w59", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		int pageCount1 = (int)Math.ceil(131.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("w59");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("s67", cols2);
		String [][] records_s67 = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			records_s67[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s67[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s67", records_s67[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record67: [a67, b1, c1, d3]
		//record68: [a68, b0, c2, d0]
		//record69: [a69, b1, c0, d1]
		//record70: [a70, b0, c1, d2]
		//record71: [a71, b1, c2, d3]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s67", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 36, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		int pageCount2 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("s67");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+36));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("t832x", cols0);
		String [][] records_t832x = new String[175][cols0.length];
		for(int i=0;i<175;i++)
		{
			records_t832x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t832x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t832x", records_t832x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0]
		//record171: [a171, b1, c0, d3, e1]
		//record172: [a172, b0, c1, d0, e2]
		//record173: [a173, b1, c2, d1, e3]
		//record174: [a174, b0, c0, d2, e4]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("t832x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 35, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a20, b0, c2, d0, e0]
		int pageCount0 = (int)Math.ceil(175.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("t832x");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+35));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("w3h", cols1);
		String [][] records_w3h = new String[167][cols1.length];
		for(int i=0;i<167;i++)
		{
			records_w3h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w3h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w3h", records_w3h[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record162: [a162]
		//record163: [a163]
		//record164: [a164]
		//record165: [a165]
		//record166: [a166]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a74"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w3h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a74]
		int pageCount1 = (int)Math.ceil(167.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("w3h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("d97", cols2);
		String [][] records_d97 = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_d97[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d97[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d97", records_d97[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record20: [a20, b0, c2, d0, e0]
		//record21: [a21, b1, c0, d1, e1]
		//record22: [a22, b0, c1, d2, e2]
		//record23: [a23, b1, c2, d3, e3]
		//record24: [a24, b0, c0, d0, e4]
		String[] ConditionColumns2 = {"e","d"};
		String[] ConditionColumnsValues2 = {"e2","d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d97", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a7, b1, c1, d3, e2]
		int pageCount2 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("d97");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("ux", cols0);
		String [][] records_ux = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			records_ux[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ux[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ux", records_ux[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record79: [a79, b1]
		//record80: [a80, b0]
		//record81: [a81, b1]
		//record82: [a82, b0]
		//record83: [a83, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ux", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 42, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("ux");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("nw", cols0);
		String [][] records_nw = new String[403][cols0.length];
		for(int i=0;i<403;i++)
		{
			records_nw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nw", records_nw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record398: [a398, b0, c2, d2]
		//record399: [a399, b1, c0, d3]
		//record400: [a400, b0, c1, d0]
		//record401: [a401, b1, c2, d1]
		//record402: [a402, b0, c0, d2]
		String[] ConditionColumns0 = {"c","d"};
		String[] ConditionColumnsValues0 = {"c0","d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 34, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a12, b0, c0, d0]
		//record2: [a24, b0, c0, d0]
		//record3: [a36, b0, c0, d0]
		//record4: [a48, b0, c0, d0]
		int pageCount0 = (int)Math.ceil(403.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("nw");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+34));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ng", cols0);
		String [][] records_ng = new String[141][cols0.length];
		for(int i=0;i<141;i++)
		{
			records_ng[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ng[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ng", records_ng[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ng", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		int pageCount0 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("ng");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xz2", cols1);
		String [][] records_xz2 = new String[414][cols1.length];
		for(int i=0;i<414;i++)
		{
			records_xz2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xz2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xz2", records_xz2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xz2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 83, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record3: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record4: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		int pageCount1 = (int)Math.ceil(414.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("xz2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+83));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bu", cols2);
		String [][] records_bu = new String[81][cols2.length];
		for(int i=0;i<81;i++)
		{
			records_bu[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bu[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bu", records_bu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("bu", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		int pageCount2 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("bu");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("uj", cols3);
		String [][] records_uj = new String[196][cols3.length];
		for(int i=0;i<196;i++)
		{
			records_uj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_uj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("uj", records_uj[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record191: [a191, b1]
		//record192: [a192, b0]
		//record193: [a193, b1]
		//record194: [a194, b0]
		//record195: [a195, b1]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a166"};
		ArrayList<String[]> tableSelect3 = DBApp.select("uj", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a166, b0]
		int pageCount3 = (int)Math.ceil(196.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("uj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("sc2", cols0);
		String [][] records_sc2 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			records_sc2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sc2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sc2", records_sc2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record18: [a18, b0, c0]
		//record19: [a19, b1, c1]
		//record20: [a20, b0, c2]
		//record21: [a21, b1, c0]
		//record22: [a22, b0, c1]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a9"};
		ArrayList<String[]> tableSelect0 = DBApp.select("sc2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a9, b1, c0]
		int pageCount0 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("sc2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("uqtn", cols1);
		String [][] records_uqtn = new String[448][cols1.length];
		for(int i=0;i<448;i++)
		{
			records_uqtn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uqtn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uqtn", records_uqtn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("uqtn", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 224, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		int pageCount1 = (int)Math.ceil(448.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("uqtn");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+224));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("f40", cols2);
		String [][] records_f40 = new String[355][cols2.length];
		for(int i=0;i<355;i++)
		{
			records_f40[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f40[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f40", records_f40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record350: [a350, b0, c2]
		//record351: [a351, b1, c0]
		//record352: [a352, b0, c1]
		//record353: [a353, b1, c2]
		//record354: [a354, b0, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("f40", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 178, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount2 = (int)Math.ceil(355.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("f40");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+178));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dkc", cols0);
		String [][] records_dkc = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_dkc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dkc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dkc", records_dkc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9, n10, o13]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11, o14]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11, n12, o0]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13, o1]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0, o2]
		String[] ConditionColumns0 = {"h","j","i","a","m","d"};
		String[] ConditionColumnsValues0 = {"h6","j8","i1","a118","m1","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("dkc", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1, n6, o13]
		int pageCount0 = (int)Math.ceil(183.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("dkc");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zzh4", cols1);
		String [][] records_zzh4 = new String[420][cols1.length];
		for(int i=0;i<420;i++)
		{
			records_zzh4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zzh4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zzh4", records_zzh4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		//record416: [a416, b0, c2, d0, e1, f2, g3]
		//record417: [a417, b1, c0, d1, e2, f3, g4]
		//record418: [a418, b0, c1, d2, e3, f4, g5]
		//record419: [a419, b1, c2, d3, e4, f5, g6]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("zzh4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 105, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		int pageCount1 = (int)Math.ceil(420.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("zzh4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+105));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("vna2", cols0);
		String [][] records_vna2 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			records_vna2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vna2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vna2", records_vna2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record30: [a30, b0, c0, d2]
		//record31: [a31, b1, c1, d3]
		//record32: [a32, b0, c2, d0]
		//record33: [a33, b1, c0, d1]
		//record34: [a34, b0, c1, d2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vna2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a4, b0, c1, d0]
		//record2: [a7, b1, c1, d3]
		//record3: [a10, b0, c1, d2]
		//record4: [a13, b1, c1, d1]
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("vna2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u80", cols0);
		String [][] records_u80 = new String[128][cols0.length];
		for(int i=0;i<128;i++)
		{
			records_u80[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u80[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u80", records_u80[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("u80", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record4: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1]
		int pageCount0 = (int)Math.ceil(128.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("u80");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("z217", cols1);
		String [][] records_z217 = new String[497][cols1.length];
		for(int i=0;i<497;i++)
		{
			records_z217[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z217[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z217", records_z217[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record492: [a492, b0]
		//record493: [a493, b1]
		//record494: [a494, b0]
		//record495: [a495, b1]
		//record496: [a496, b0]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a106"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z217", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a106, b0]
		int pageCount1 = (int)Math.ceil(497.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("z217");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ly0", cols2);
		String [][] records_ly0 = new String[347][cols2.length];
		for(int i=0;i<347;i++)
		{
			records_ly0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ly0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ly0", records_ly0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6]
		String[] ConditionColumns2 = {"d","e","c"};
		String[] ConditionColumnsValues2 = {"d3","e1","c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ly0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1]
		//record1: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1]
		//record2: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1]
		//record3: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1]
		//record4: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1]
		int pageCount2 = (int)Math.ceil(347.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("ly0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("eb22", cols0);
		String [][] records_eb22 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_eb22[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eb22[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eb22", records_eb22[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("eb22", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		int pageCount0 = (int)Math.ceil(28.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("eb22");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q9z2", cols1);
		String [][] records_q9z2 = new String[271][cols1.length];
		for(int i=0;i<271;i++)
		{
			records_q9z2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q9z2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q9z2", records_q9z2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q9z2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 33, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7]
		int pageCount1 = (int)Math.ceil(271.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("q9z2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+33));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("s3b", cols2);
		String [][] records_s3b = new String[382][cols2.length];
		for(int i=0;i<382;i++)
		{
			records_s3b[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s3b[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s3b", records_s3b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record377: [a377, b1, c2, d1, e2]
		//record378: [a378, b0, c0, d2, e3]
		//record379: [a379, b1, c1, d3, e4]
		//record380: [a380, b0, c2, d0, e0]
		//record381: [a381, b1, c0, d1, e1]
		String[] ConditionColumns2 = {"a","d"};
		String[] ConditionColumnsValues2 = {"a102","d2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s3b", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a102, b0, c0, d2, e2]
		int pageCount2 = (int)Math.ceil(382.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("s3b");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("if", cols3);
		String [][] records_if = new String[39][cols3.length];
		for(int i=0;i<39;i++)
		{
			records_if[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_if[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("if", records_if[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g6"};
		ArrayList<String[]> tableSelect3 = DBApp.select("if", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 5, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a20, b0, c2, d0, e0, f2, g6]
		//record3: [a27, b1, c0, d3, e2, f3, g6]
		//record4: [a34, b0, c1, d2, e4, f4, g6]
		int pageCount3 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("if");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("i0o", cols0);
		String [][] records_i0o = new String[383][cols0.length];
		for(int i=0;i<383;i++)
		{
			records_i0o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i0o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i0o", records_i0o[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record378: [a378]
		//record379: [a379]
		//record380: [a380]
		//record381: [a381]
		//record382: [a382]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a77"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i0o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a77]
		int pageCount0 = (int)Math.ceil(383.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("i0o");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xw", cols0);
		String [][] records_xw = new String[206][cols0.length];
		for(int i=0;i<206;i++)
		{
			records_xw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xw", records_xw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3, l9, m6, n5, o6]
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2, i4, j2, k4, l10, m7, n6, o7]
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5, l11, m8, n7, o8]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4, k6, l0, m9, n8, o9]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10, n9, o10]
		String[] ConditionColumns0 = {"j","o","m","b","k"};
		String[] ConditionColumnsValues0 = {"j7","o2","m7","b1","k5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2]
		int pageCount0 = (int)Math.ceil(206.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("xw");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("o7", cols1);
		String [][] records_o7 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_o7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o7", records_o7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("o7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a2, b0, c2, d2, e2]
		//record2: [a4, b0, c1, d0, e4]
		int pageCount1 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("o7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+3));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("csk", cols2);
		String [][] records_csk = new String[198][cols2.length];
		for(int i=0;i<198;i++)
		{
			records_csk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_csk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("csk", records_csk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3]
		//record194: [a194, b0, c2, d2, e4]
		//record195: [a195, b1, c0, d3, e0]
		//record196: [a196, b0, c1, d0, e1]
		//record197: [a197, b1, c2, d1, e2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("csk", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 66, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a6, b0, c0, d2, e1]
		//record3: [a9, b1, c0, d1, e4]
		//record4: [a12, b0, c0, d0, e2]
		int pageCount2 = (int)Math.ceil(198.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("csk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+66));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bd", cols0);
		String [][] records_bd = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_bd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bd", records_bd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1]
		String[] ConditionColumns0 = {"f","g"};
		String[] ConditionColumnsValues0 = {"f3","g1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bd", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record1: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7]
		//record2: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9]
		//record3: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1]
		//record4: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3]
		int pageCount0 = (int)Math.ceil(212.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("bd");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ho9a", cols1);
		String [][] records_ho9a = new String[67][cols1.length];
		for(int i=0;i<67;i++)
		{
			records_ho9a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ho9a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ho9a", records_ho9a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ho9a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		int pageCount1 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("ho9a");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qva6r", cols2);
		String [][] records_qva6r = new String[434][cols2.length];
		for(int i=0;i<434;i++)
		{
			records_qva6r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qva6r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qva6r", records_qva6r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1]
		String[] ConditionColumns2 = {"f","g"};
		String[] ConditionColumnsValues2 = {"f5","g2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qva6r", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 10, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record1: [a65, b1, c2, d1, e0, f5, g2, h1]
		//record2: [a107, b1, c2, d3, e2, f5, g2, h3]
		//record3: [a149, b1, c2, d1, e4, f5, g2, h5]
		//record4: [a191, b1, c2, d3, e1, f5, g2, h7]
		int pageCount2 = (int)Math.ceil(434.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("qva6r");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sc", cols0);
		String [][] records_sc = new String[439][cols0.length];
		for(int i=0;i<439;i++)
		{
			records_sc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sc", records_sc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6]
		String[] ConditionColumns0 = {"d","g","b","f","e"};
		String[] ConditionColumnsValues0 = {"d0","g3","b0","f2","e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("sc", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8]
		int pageCount0 = (int)Math.ceil(439.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("sc");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vj3", cols1);
		String [][] records_vj3 = new String[352][cols1.length];
		for(int i=0;i<352;i++)
		{
			records_vj3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vj3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vj3", records_vj3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("vj3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 71, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		int pageCount1 = (int)Math.ceil(352.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("vj3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+71));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("fzb", cols0);
		String [][] records_fzb = new String[485][cols0.length];
		for(int i=0;i<485;i++)
		{
			records_fzb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fzb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fzb", records_fzb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1]
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fzb", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 81, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3]
		int pageCount0 = (int)Math.ceil(485.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("fzb");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+81));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("q70", cols1);
		String [][] records_q70 = new String[499][cols1.length];
		for(int i=0;i<499;i++)
		{
			records_q70[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q70[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q70", records_q70[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2, n6]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3, n7]
		//record498: [a498, b0, c0, d2, e3, f0, g1, h2, i3, j8, k3, l6, m4, n8]
		String[] ConditionColumns1 = {"l"};
		String[] ConditionColumnsValues1 = {"l4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q70", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 42, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		int pageCount1 = (int)Math.ceil(499.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("q70");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tjq", cols2);
		String [][] records_tjq = new String[165][cols2.length];
		for(int i=0;i<165;i++)
		{
			records_tjq[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tjq[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tjq", records_tjq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4]
		String[] ConditionColumns2 = {"d","g"};
		String[] ConditionColumnsValues2 = {"d2","g1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("tjq", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record1: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0]
		//record2: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8]
		//record3: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6]
		//record4: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4]
		int pageCount2 = (int)Math.ceil(165.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("tjq");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("p8v", cols3);
		String [][] records_p8v = new String[191][cols3.length];
		for(int i=0;i<191;i++)
		{
			records_p8v[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p8v[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p8v", records_p8v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record186: [a186, b0, c0, d2, e1, f0]
		//record187: [a187, b1, c1, d3, e2, f1]
		//record188: [a188, b0, c2, d0, e3, f2]
		//record189: [a189, b1, c0, d1, e4, f3]
		//record190: [a190, b0, c1, d2, e0, f4]
		String[] ConditionColumns3 = {"b","c","e"};
		String[] ConditionColumnsValues3 = {"b1","c0","e2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p8v", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 6, records:
		//record0: [a27, b1, c0, d3, e2, f3]
		//record1: [a57, b1, c0, d1, e2, f3]
		//record2: [a87, b1, c0, d3, e2, f3]
		//record3: [a117, b1, c0, d1, e2, f3]
		//record4: [a147, b1, c0, d3, e2, f3]
		int pageCount3 = (int)Math.ceil(191.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("p8v");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("a8", cols4);
		String [][] records_a8 = new String[56][cols4.length];
		for(int i=0;i<56;i++)
		{
			records_a8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_a8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("a8", records_a8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1]
		//record52: [a52, b0, c1, d0, e2]
		//record53: [a53, b1, c2, d1, e3]
		//record54: [a54, b0, c0, d2, e4]
		//record55: [a55, b1, c1, d3, e0]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("a8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 19, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		int pageCount4 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("a8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+19));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("q35o0", cols0);
		String [][] records_q35o0 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			records_q35o0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q35o0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q35o0", records_q35o0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("q35o0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		int pageCount0 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("q35o0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wcf1d", cols1);
		String [][] records_wcf1d = new String[492][cols1.length];
		for(int i=0;i<492;i++)
		{
			records_wcf1d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wcf1d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wcf1d", records_wcf1d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1]
		String[] ConditionColumns1 = {"n"};
		String[] ConditionColumnsValues1 = {"n3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wcf1d", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 35, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7, n3]
		int pageCount1 = (int)Math.ceil(492.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("wcf1d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+35));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v2q", cols2);
		String [][] records_v2q = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			records_v2q[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v2q[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v2q", records_v2q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5]
		String[] ConditionColumns2 = {"i","h","e"};
		String[] ConditionColumnsValues2 = {"i4","h1","e4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("v2q", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5]
		int pageCount2 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("v2q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("n7g", cols3);
		String [][] records_n7g = new String[237][cols3.length];
		for(int i=0;i<237;i++)
		{
			records_n7g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n7g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n7g", records_n7g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("n7g", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 118, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		int pageCount3 = (int)Math.ceil(237.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("n7g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+118));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("e5l", cols4);
		String [][] records_e5l = new String[232][cols4.length];
		for(int i=0;i<232;i++)
		{
			records_e5l[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_e5l[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("e5l", records_e5l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record227: [a227, b1, c2, d3, e2]
		//record228: [a228, b0, c0, d0, e3]
		//record229: [a229, b1, c1, d1, e4]
		//record230: [a230, b0, c2, d2, e0]
		//record231: [a231, b1, c0, d3, e1]
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("e5l", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 47, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a6, b0, c0, d2, e1]
		//record2: [a11, b1, c2, d3, e1]
		//record3: [a16, b0, c1, d0, e1]
		//record4: [a21, b1, c0, d1, e1]
		int pageCount4 = (int)Math.ceil(232.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("e5l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+47));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("kpyg", cols0);
		String [][] records_kpyg = new String[346][cols0.length];
		for(int i=0;i<346;i++)
		{
			records_kpyg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kpyg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kpyg", records_kpyg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1]
		//record342: [a342, b0, c0, d2, e2]
		//record343: [a343, b1, c1, d3, e3]
		//record344: [a344, b0, c2, d0, e4]
		//record345: [a345, b1, c0, d1, e0]
		String[] ConditionColumns0 = {"c","d"};
		String[] ConditionColumnsValues0 = {"c2","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("kpyg", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a14, b0, c2, d2, e4]
		//record2: [a26, b0, c2, d2, e1]
		//record3: [a38, b0, c2, d2, e3]
		//record4: [a50, b0, c2, d2, e0]
		int pageCount0 = (int)Math.ceil(346.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("kpyg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+29));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("k15fg", cols1);
		String [][] records_k15fg = new String[182][cols1.length];
		for(int i=0;i<182;i++)
		{
			records_k15fg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k15fg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k15fg", records_k15fg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record177: [a177, b1, c0, d1, e2]
		//record178: [a178, b0, c1, d2, e3]
		//record179: [a179, b1, c2, d3, e4]
		//record180: [a180, b0, c0, d0, e0]
		//record181: [a181, b1, c1, d1, e1]
		String[] ConditionColumns1 = {"b","c"};
		String[] ConditionColumnsValues1 = {"b0","c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k15fg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 31, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a6, b0, c0, d2, e1]
		//record2: [a12, b0, c0, d0, e2]
		//record3: [a18, b0, c0, d2, e3]
		//record4: [a24, b0, c0, d0, e4]
		int pageCount1 = (int)Math.ceil(182.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("k15fg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("y9d2", cols2);
		String [][] records_y9d2 = new String[479][cols2.length];
		for(int i=0;i<479;i++)
		{
			records_y9d2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y9d2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y9d2", records_y9d2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record474: [a474, b0, c0, d2, e4, f0]
		//record475: [a475, b1, c1, d3, e0, f1]
		//record476: [a476, b0, c2, d0, e1, f2]
		//record477: [a477, b1, c0, d1, e2, f3]
		//record478: [a478, b0, c1, d2, e3, f4]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y9d2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 119, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a11, b1, c2, d3, e1, f5]
		//record3: [a15, b1, c0, d3, e0, f3]
		//record4: [a19, b1, c1, d3, e4, f1]
		int pageCount2 = (int)Math.ceil(479.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("y9d2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+119));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e33qk", cols3);
		String [][] records_e33qk = new String[160][cols3.length];
		for(int i=0;i<160;i++)
		{
			records_e33qk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e33qk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e33qk", records_e33qk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12, n1]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5]
		String[] ConditionColumns3 = {"m"};
		String[] ConditionColumnsValues3 = {"m10"};
		ArrayList<String[]> tableSelect3 = DBApp.select("e33qk", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 12, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record2: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record3: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		//record4: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6]
		int pageCount3 = (int)Math.ceil(160.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("e33qk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i1m3m", cols0);
		String [][] records_i1m3m = new String[188][cols0.length];
		for(int i=0;i<188;i++)
		{
			records_i1m3m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i1m3m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i1m3m", records_i1m3m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i1m3m", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 63, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4]
		int pageCount0 = (int)Math.ceil(188.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("i1m3m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+63));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n240", cols1);
		String [][] records_n240 = new String[134][cols1.length];
		for(int i=0;i<134;i++)
		{
			records_n240[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n240[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n240", records_n240[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		String[] ConditionColumns1 = {"m","f"};
		String[] ConditionColumnsValues1 = {"m10","f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n240", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10]
		//record1: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10]
		int pageCount1 = (int)Math.ceil(134.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("n240");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("zx9", cols2);
		String [][] records_zx9 = new String[412][cols2.length];
		for(int i=0;i<412;i++)
		{
			records_zx9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zx9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zx9", records_zx9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record407: [a407, b1, c2, d3]
		//record408: [a408, b0, c0, d0]
		//record409: [a409, b1, c1, d1]
		//record410: [a410, b0, c2, d2]
		//record411: [a411, b1, c0, d3]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("zx9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 206, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		int pageCount2 = (int)Math.ceil(412.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("zx9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+206));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("u1c", cols3);
		String [][] records_u1c = new String[391][cols3.length];
		for(int i=0;i<391;i++)
		{
			records_u1c[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u1c[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u1c", records_u1c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record386: [a386, b0, c2, d2, e1, f2]
		//record387: [a387, b1, c0, d3, e2, f3]
		//record388: [a388, b0, c1, d0, e3, f4]
		//record389: [a389, b1, c2, d1, e4, f5]
		//record390: [a390, b0, c0, d2, e0, f0]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("u1c", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 66, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a6, b0, c0, d2, e1, f0]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a24, b0, c0, d0, e4, f0]
		int pageCount3 = (int)Math.ceil(391.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("u1c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+66));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("xk4", cols4);
		String [][] records_xk4 = new String[134][cols4.length];
		for(int i=0;i<134;i++)
		{
			records_xk4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xk4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xk4", records_xk4[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record129: [a129, b1]
		//record130: [a130, b0]
		//record131: [a131, b1]
		//record132: [a132, b0]
		//record133: [a133, b1]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("xk4", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 67, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount4 = (int)Math.ceil(134.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("xk4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+67));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("op1", cols0);
		String [][] records_op1 = new String[155][cols0.length];
		for(int i=0;i<155;i++)
		{
			records_op1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_op1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("op1", records_op1[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record150: [a150, b0]
		//record151: [a151, b1]
		//record152: [a152, b0]
		//record153: [a153, b1]
		//record154: [a154, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("op1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 78, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(155.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("op1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+78));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("n878", cols1);
		String [][] records_n878 = new String[495][cols1.length];
		for(int i=0;i<495;i++)
		{
			records_n878[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n878[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n878", records_n878[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n878", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 71, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		int pageCount1 = (int)Math.ceil(495.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("n878");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+71));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("q61", cols2);
		String [][] records_q61 = new String[101][cols2.length];
		for(int i=0;i<101;i++)
		{
			records_q61[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q61[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q61", records_q61[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record96: [a96, b0]
		//record97: [a97, b1]
		//record98: [a98, b0]
		//record99: [a99, b1]
		//record100: [a100, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("q61", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 50, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount2 = (int)Math.ceil(101.0/DBApp.dataPageSize);
		int[] pageFrequency2 = new int[pageCount2];
		for(int i = 0; i<tableSelect2.size(); i++)
		{
			pageFrequency2[Integer.parseInt((tableSelect2.get(i)[0]).substring(1,tableSelect2.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts2 = new ArrayList<String>();
		for(int i=0;i<pageFrequency2.length;i++)
		{
			if(pageFrequency2[i]!=0)
				pagesCounts2.add("["+i+", "+pageFrequency2[i]+"]");
		}
		String selectTrace2 = DBApp.getLastTrace("q61");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+50));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e1", cols3);
		String [][] records_e1 = new String[457][cols3.length];
		for(int i=0;i<457;i++)
		{
			records_e1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e1", records_e1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("e1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 152, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		int pageCount3 = (int)Math.ceil(457.0/DBApp.dataPageSize);
		int[] pageFrequency3 = new int[pageCount3];
		for(int i = 0; i<tableSelect3.size(); i++)
		{
			pageFrequency3[Integer.parseInt((tableSelect3.get(i)[0]).substring(1,tableSelect3.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts3 = new ArrayList<String>();
		for(int i=0;i<pageFrequency3.length;i++)
		{
			if(pageFrequency3[i]!=0)
				pagesCounts3.add("["+i+", "+pageFrequency3[i]+"]");
		}
		String selectTrace3 = DBApp.getLastTrace("e1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+152));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("ft0", cols4);
		String [][] records_ft0 = new String[328][cols4.length];
		for(int i=0;i<328;i++)
		{
			records_ft0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ft0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ft0", records_ft0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record323: [a323, b1, c2, d3]
		//record324: [a324, b0, c0, d0]
		//record325: [a325, b1, c1, d1]
		//record326: [a326, b0, c2, d2]
		//record327: [a327, b1, c0, d3]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ft0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 109, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a4, b0, c1, d0]
		//record2: [a7, b1, c1, d3]
		//record3: [a10, b0, c1, d2]
		//record4: [a13, b1, c1, d1]
		int pageCount4 = (int)Math.ceil(328.0/DBApp.dataPageSize);
		int[] pageFrequency4 = new int[pageCount4];
		for(int i = 0; i<tableSelect4.size(); i++)
		{
			pageFrequency4[Integer.parseInt((tableSelect4.get(i)[0]).substring(1,tableSelect4.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts4 = new ArrayList<String>();
		for(int i=0;i<pageFrequency4.length;i++)
		{
			if(pageFrequency4[i]!=0)
				pagesCounts4.add("["+i+", "+pageFrequency4[i]+"]");
		}
		String selectTrace4 = DBApp.getLastTrace("ft0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+109));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("v3o", cols0);
		String [][] records_v3o = new String[211][cols0.length];
		for(int i=0;i<211;i++)
		{
			records_v3o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v3o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v3o", records_v3o[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record206: [a206]
		//record207: [a207]
		//record208: [a208]
		//record209: [a209]
		//record210: [a210]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a93"};
		ArrayList<String[]> tableSelect0 = DBApp.select("v3o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a93]
		int pageCount0 = (int)Math.ceil(211.0/DBApp.dataPageSize);
		int[] pageFrequency0 = new int[pageCount0];
		for(int i = 0; i<tableSelect0.size(); i++)
		{
			pageFrequency0[Integer.parseInt((tableSelect0.get(i)[0]).substring(1,tableSelect0.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts0 = new ArrayList<String>();
		for(int i=0;i<pageFrequency0.length;i++)
		{
			if(pageFrequency0[i]!=0)
				pagesCounts0.add("["+i+", "+pageFrequency0[i]+"]");
		}
		String selectTrace0 = DBApp.getLastTrace("v3o");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("c5554", cols1);
		String [][] records_c5554 = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_c5554[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c5554[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c5554", records_c5554[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("c5554", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 23, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		int pageCount1 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		int[] pageFrequency1 = new int[pageCount1];
		for(int i = 0; i<tableSelect1.size(); i++)
		{
			pageFrequency1[Integer.parseInt((tableSelect1.get(i)[0]).substring(1,tableSelect1.get(i)[0].length()))/DBApp.dataPageSize]++;
		}
		ArrayList<String> pagesCounts1 = new ArrayList<String>();
		for(int i=0;i<pageFrequency1.length;i++)
		{
			if(pageFrequency1[i]!=0)
				pagesCounts1.add("["+i+", "+pageFrequency1[i]+"]");
		}
		String selectTrace1 = DBApp.getLastTrace("c5554");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+23));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p3e", cols0);
		String [][] records_p3e = new String[399][cols0.length];
		for(int i=0;i<399;i++)
		{
			records_p3e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p3e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p3e", records_p3e[i]);
		}
	
		int pageCount0 = (int)Math.ceil(399.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns00 = {"h","c","n","f","i","e"};
		String[] ConditionColumnsValues00 = {"h3","c0","n7","f3","i3","e2"};
		DBApp.select("p3e", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"j","e","b"};
		String[] ConditionColumnsValues01 = {"j2","e2","b0"};
		DBApp.select("p3e", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("p3e", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("p3e");
	
		String selectTrace0 = DBApp.getFullTrace("p3e");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 405);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 399"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("r82", cols1);
		String [][] records_r82 = new String[378][cols1.length];
		for(int i=0;i<378;i++)
		{
			records_r82[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r82[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r82", records_r82[i]);
		}
	
		int pageCount1 = (int)Math.ceil(378.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("r82");
	
		String[] ConditionColumns11 = {"a"};
		String[] ConditionColumnsValues11 = {"a30"};
		DBApp.select("r82", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"a"};
		String[] ConditionColumnsValues14 = {"a19"};
		DBApp.select("r82", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("r82");
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a246"};
		DBApp.select("r82", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"a"};
		String[] ConditionColumnsValues17 = {"a34"};
		DBApp.select("r82", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a250"};
		DBApp.select("r82", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer111,recordSelectPointer111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a90"};
		DBApp.select("r82", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"a"};
		String[] ConditionColumnsValues113 = {"a223"};
		DBApp.select("r82", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a81"};
		DBApp.select("r82", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"a"};
		String[] ConditionColumnsValues115 = {"a189"};
		DBApp.select("r82", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("r82");
	
		String[] ConditionColumns117 = {"a"};
		String[] ConditionColumnsValues117 = {"a63"};
		DBApp.select("r82", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((378%DBApp.dataPageSize))-1;
		DBApp.select("r82", pageSelectPointer118,recordSelectPointer118);
	
		String selectTrace1 = DBApp.getFullTrace("r82");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 399);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 378"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("iw", cols2);
		String [][] records_iw = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			records_iw[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_iw[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("iw", records_iw[i]);
		}
	
		int pageCount2 = (int)Math.ceil(90.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a61"};
		DBApp.select("iw", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("iw");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("iw");
	
		String[] ConditionColumns25 = {"b"};
		String[] ConditionColumnsValues25 = {"b1"};
		DBApp.select("iw", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("iw");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("iw");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"b"};
		String[] ConditionColumnsValues212 = {"b0"};
		DBApp.select("iw", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("iw");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a15"};
		DBApp.select("iw", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"b"};
		String[] ConditionColumnsValues216 = {"b0"};
		DBApp.select("iw", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"b"};
		String[] ConditionColumnsValues217 = {"b1"};
		DBApp.select("iw", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("iw");
	
		DBApp.select("iw");
	
		DBApp.select("iw");
	
		String selectTrace2 = DBApp.getFullTrace("iw");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 113);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 90"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z59s", cols3);
		String [][] records_z59s = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			records_z59s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z59s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z59s", records_z59s[i]);
		}
	
		int pageCount3 = (int)Math.ceil(1.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("z59s");
	
		String[] ConditionColumns31 = {"g"};
		String[] ConditionColumnsValues31 = {"g0"};
		DBApp.select("z59s", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		String[] ConditionColumns34 = {"e","b","i","j","g"};
		String[] ConditionColumnsValues34 = {"e0","b0","i0","j0","g0"};
		DBApp.select("z59s", ConditionColumns34, ConditionColumnsValues34);
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((1%DBApp.dataPageSize))-1;
		DBApp.select("z59s", pageSelectPointer39,recordSelectPointer39);
	
		String[] ConditionColumns310 = {"b"};
		String[] ConditionColumnsValues310 = {"b0"};
		DBApp.select("z59s", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"k"};
		String[] ConditionColumnsValues311 = {"k0"};
		DBApp.select("z59s", ConditionColumns311, ConditionColumnsValues311);
	
		DBApp.select("z59s");
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((1%DBApp.dataPageSize))-1;
		DBApp.select("z59s", pageSelectPointer313,recordSelectPointer313);
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((1%DBApp.dataPageSize))-1;
		DBApp.select("z59s", pageSelectPointer317,recordSelectPointer317);
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		String[] ConditionColumns321 = {"i","b","l"};
		String[] ConditionColumnsValues321 = {"i0","b0","l0"};
		DBApp.select("z59s", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"i"};
		String[] ConditionColumnsValues322 = {"i0"};
		DBApp.select("z59s", ConditionColumns322, ConditionColumnsValues322);
	
		String[] ConditionColumns323 = {"h","i","d","g"};
		String[] ConditionColumnsValues323 = {"h0","i0","d0","g0"};
		DBApp.select("z59s", ConditionColumns323, ConditionColumnsValues323);
	
		DBApp.select("z59s");
	
		String[] ConditionColumns325 = {"m"};
		String[] ConditionColumnsValues325 = {"m0"};
		DBApp.select("z59s", ConditionColumns325, ConditionColumnsValues325);
	
		DBApp.select("z59s");
	
		DBApp.select("z59s");
	
		String[] ConditionColumns328 = {"b","a","j","i"};
		String[] ConditionColumnsValues328 = {"b0","a0","j0","i0"};
		DBApp.select("z59s", ConditionColumns328, ConditionColumnsValues328);
	
		String selectTrace3 = DBApp.getFullTrace("z59s");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 32);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 1"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ts8g3", cols0);
		String [][] records_ts8g3 = new String[442][cols0.length];
		for(int i=0;i<442;i++)
		{
			records_ts8g3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ts8g3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ts8g3", records_ts8g3[i]);
		}
	
		int pageCount0 = (int)Math.ceil(442.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("ts8g3");
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b0"};
		DBApp.select("ts8g3", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("ts8g3");
	
		DBApp.select("ts8g3");
	
		DBApp.select("ts8g3");
	
		DBApp.select("ts8g3");
	
		String[] ConditionColumns08 = {"c"};
		String[] ConditionColumnsValues08 = {"c0"};
		DBApp.select("ts8g3", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a171"};
		DBApp.select("ts8g3", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"d"};
		String[] ConditionColumnsValues012 = {"d1"};
		DBApp.select("ts8g3", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d1"};
		DBApp.select("ts8g3", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"a","b"};
		String[] ConditionColumnsValues014 = {"a4","b0"};
		DBApp.select("ts8g3", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"d"};
		String[] ConditionColumnsValues015 = {"d0"};
		DBApp.select("ts8g3", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer016,recordSelectPointer016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"b"};
		String[] ConditionColumnsValues018 = {"b0"};
		DBApp.select("ts8g3", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"b"};
		String[] ConditionColumnsValues019 = {"b0"};
		DBApp.select("ts8g3", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"a"};
		String[] ConditionColumnsValues020 = {"a419"};
		DBApp.select("ts8g3", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer021,recordSelectPointer021);
	
		String[] ConditionColumns022 = {"d"};
		String[] ConditionColumnsValues022 = {"d1"};
		DBApp.select("ts8g3", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("ts8g3");
	
		String[] ConditionColumns025 = {"c","a"};
		String[] ConditionColumnsValues025 = {"c2","a47"};
		DBApp.select("ts8g3", ConditionColumns025, ConditionColumnsValues025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer026,recordSelectPointer026);
	
		String[] ConditionColumns027 = {"c"};
		String[] ConditionColumnsValues027 = {"c1"};
		DBApp.select("ts8g3", ConditionColumns027, ConditionColumnsValues027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("ts8g3", pageSelectPointer028,recordSelectPointer028);
	
		String[] ConditionColumns029 = {"b"};
		String[] ConditionColumnsValues029 = {"b0"};
		DBApp.select("ts8g3", ConditionColumns029, ConditionColumnsValues029);
	
		String selectTrace0 = DBApp.getFullTrace("ts8g3");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 474);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 442"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("s795", cols1);
		String [][] records_s795 = new String[293][cols1.length];
		for(int i=0;i<293;i++)
		{
			records_s795[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s795[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s795", records_s795[i]);
		}
	
		int pageCount1 = (int)Math.ceil(293.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("s795");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"b","e"};
		String[] ConditionColumnsValues15 = {"b1","e0"};
		DBApp.select("s795", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("s795");
	
		String[] ConditionColumns17 = {"c"};
		String[] ConditionColumnsValues17 = {"c2"};
		DBApp.select("s795", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"c"};
		String[] ConditionColumnsValues18 = {"c0"};
		DBApp.select("s795", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("s795");
	
		String[] ConditionColumns112 = {"d"};
		String[] ConditionColumnsValues112 = {"d0"};
		DBApp.select("s795", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("s795");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("s795");
	
		String[] ConditionColumns118 = {"d"};
		String[] ConditionColumnsValues118 = {"d0"};
		DBApp.select("s795", ConditionColumns118, ConditionColumnsValues118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"c","b"};
		String[] ConditionColumnsValues120 = {"c0","b0"};
		DBApp.select("s795", ConditionColumns120, ConditionColumnsValues120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer121,recordSelectPointer121);
	
		DBApp.select("s795");
	
		String[] ConditionColumns123 = {"d","c"};
		String[] ConditionColumnsValues123 = {"d3","c1"};
		DBApp.select("s795", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"a","e"};
		String[] ConditionColumnsValues124 = {"a36","e1"};
		DBApp.select("s795", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("s795");
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((293%DBApp.dataPageSize))-1;
		DBApp.select("s795", pageSelectPointer126,recordSelectPointer126);
	
		DBApp.select("s795");
	
		String[] ConditionColumns128 = {"e"};
		String[] ConditionColumnsValues128 = {"e4"};
		DBApp.select("s795", ConditionColumns128, ConditionColumnsValues128);
	
		String[] ConditionColumns129 = {"a"};
		String[] ConditionColumnsValues129 = {"a291"};
		DBApp.select("s795", ConditionColumns129, ConditionColumnsValues129);
	
		String selectTrace1 = DBApp.getFullTrace("s795");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 325);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 293"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ukxs", cols2);
		String [][] records_ukxs = new String[191][cols2.length];
		for(int i=0;i<191;i++)
		{
			records_ukxs[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ukxs[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ukxs", records_ukxs[i]);
		}
	
		int pageCount2 = (int)Math.ceil(191.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"m","j","l","o","f","c"};
		String[] ConditionColumnsValues21 = {"m0","j9","l3","o9","f3","c0"};
		DBApp.select("ukxs", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"k","m","g","o","c","l","i"};
		String[] ConditionColumnsValues24 = {"k7","m6","g0","o9","c0","l0","i3"};
		DBApp.select("ukxs", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"m"};
		String[] ConditionColumnsValues25 = {"m7"};
		DBApp.select("ukxs", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"l"};
		String[] ConditionColumnsValues27 = {"l5"};
		DBApp.select("ukxs", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("ukxs");
	
		String[] ConditionColumns29 = {"d"};
		String[] ConditionColumnsValues29 = {"d2"};
		DBApp.select("ukxs", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"f","h","k","b"};
		String[] ConditionColumnsValues212 = {"f5","h5","k0","b1"};
		DBApp.select("ukxs", ConditionColumns212, ConditionColumnsValues212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("ukxs", pageSelectPointer214,recordSelectPointer214);
	
		DBApp.select("ukxs");
	
		String selectTrace2 = DBApp.getFullTrace("ukxs");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 209);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 191"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("g52", cols3);
		String [][] records_g52 = new String[132][cols3.length];
		for(int i=0;i<132;i++)
		{
			records_g52[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g52[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g52", records_g52[i]);
		}
	
		int pageCount3 = (int)Math.ceil(132.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("g52");
	
		DBApp.select("g52");
	
		DBApp.select("g52");
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer36,recordSelectPointer36);
	
		String[] ConditionColumns37 = {"d","b"};
		String[] ConditionColumnsValues37 = {"d1","b1"};
		DBApp.select("g52", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"b"};
		String[] ConditionColumnsValues38 = {"b1"};
		DBApp.select("g52", ConditionColumns38, ConditionColumnsValues38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("g52");
	
		String[] ConditionColumns311 = {"b"};
		String[] ConditionColumnsValues311 = {"b0"};
		DBApp.select("g52", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"b"};
		String[] ConditionColumnsValues312 = {"b1"};
		DBApp.select("g52", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"d","c"};
		String[] ConditionColumnsValues313 = {"d0","c1"};
		DBApp.select("g52", ConditionColumns313, ConditionColumnsValues313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"d"};
		String[] ConditionColumnsValues315 = {"d2"};
		DBApp.select("g52", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("g52");
	
		DBApp.select("g52");
	
		String[] ConditionColumns318 = {"c"};
		String[] ConditionColumnsValues318 = {"c1"};
		DBApp.select("g52", ConditionColumns318, ConditionColumnsValues318);
	
		int pageSelectPointer319 = genRandNum(pageCount3)-1;
		int recordSelectPointer319 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer319==pageCount3-1)
			recordSelectPointer319 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer319,recordSelectPointer319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer320,recordSelectPointer320);
	
		String[] ConditionColumns321 = {"b"};
		String[] ConditionColumnsValues321 = {"b0"};
		DBApp.select("g52", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"a"};
		String[] ConditionColumnsValues322 = {"a111"};
		DBApp.select("g52", ConditionColumns322, ConditionColumnsValues322);
	
		String[] ConditionColumns323 = {"d","c"};
		String[] ConditionColumnsValues323 = {"d3","c0"};
		DBApp.select("g52", ConditionColumns323, ConditionColumnsValues323);
	
		int pageSelectPointer324 = genRandNum(pageCount3)-1;
		int recordSelectPointer324 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer324==pageCount3-1)
			recordSelectPointer324 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer324,recordSelectPointer324);
	
		String[] ConditionColumns325 = {"a"};
		String[] ConditionColumnsValues325 = {"a55"};
		DBApp.select("g52", ConditionColumns325, ConditionColumnsValues325);
	
		int pageSelectPointer326 = genRandNum(pageCount3)-1;
		int recordSelectPointer326 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer326==pageCount3-1)
			recordSelectPointer326 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer326,recordSelectPointer326);
	
		int pageSelectPointer327 = genRandNum(pageCount3)-1;
		int recordSelectPointer327 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer327==pageCount3-1)
			recordSelectPointer327 = genRandNum((132%DBApp.dataPageSize))-1;
		DBApp.select("g52", pageSelectPointer327,recordSelectPointer327);
	
		String selectTrace3 = DBApp.getFullTrace("g52");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 162);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 132"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jz3kv", cols4);
		String [][] records_jz3kv = new String[104][cols4.length];
		for(int i=0;i<104;i++)
		{
			records_jz3kv[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_jz3kv[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("jz3kv", records_jz3kv[i]);
		}
	
		int pageCount4 = (int)Math.ceil(104.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer41,recordSelectPointer41);
	
		DBApp.select("jz3kv");
	
		String[] ConditionColumns43 = {"a"};
		String[] ConditionColumnsValues43 = {"a21"};
		DBApp.select("jz3kv", ConditionColumns43, ConditionColumnsValues43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer44,recordSelectPointer44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer45,recordSelectPointer45);
	
		String[] ConditionColumns46 = {"l","d","i"};
		String[] ConditionColumnsValues46 = {"l9","d1","i3"};
		DBApp.select("jz3kv", ConditionColumns46, ConditionColumnsValues46);
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer47,recordSelectPointer47);
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer412,recordSelectPointer412);
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer413,recordSelectPointer413);
	
		DBApp.select("jz3kv");
	
		String[] ConditionColumns415 = {"d"};
		String[] ConditionColumnsValues415 = {"d1"};
		DBApp.select("jz3kv", ConditionColumns415, ConditionColumnsValues415);
	
		String[] ConditionColumns416 = {"e","k"};
		String[] ConditionColumnsValues416 = {"e4","k9"};
		DBApp.select("jz3kv", ConditionColumns416, ConditionColumnsValues416);
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer417,recordSelectPointer417);
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		String[] ConditionColumns421 = {"d"};
		String[] ConditionColumnsValues421 = {"d1"};
		DBApp.select("jz3kv", ConditionColumns421, ConditionColumnsValues421);
	
		String[] ConditionColumns422 = {"b"};
		String[] ConditionColumnsValues422 = {"b1"};
		DBApp.select("jz3kv", ConditionColumns422, ConditionColumnsValues422);
	
		int pageSelectPointer423 = genRandNum(pageCount4)-1;
		int recordSelectPointer423 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer423==pageCount4-1)
			recordSelectPointer423 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer423,recordSelectPointer423);
	
		DBApp.select("jz3kv");
	
		int pageSelectPointer425 = genRandNum(pageCount4)-1;
		int recordSelectPointer425 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer425==pageCount4-1)
			recordSelectPointer425 = genRandNum((104%DBApp.dataPageSize))-1;
		DBApp.select("jz3kv", pageSelectPointer425,recordSelectPointer425);
	
		DBApp.select("jz3kv");
	
		DBApp.select("jz3kv");
	
		String[] ConditionColumns428 = {"h"};
		String[] ConditionColumnsValues428 = {"h7"};
		DBApp.select("jz3kv", ConditionColumns428, ConditionColumnsValues428);
	
		DBApp.select("jz3kv");
	
		String selectTrace4 = DBApp.getFullTrace("jz3kv");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 136);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 104"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("js5", cols0);
		String [][] records_js5 = new String[430][cols0.length];
		for(int i=0;i<430;i++)
		{
			records_js5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_js5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("js5", records_js5[i]);
		}
	
		int pageCount0 = (int)Math.ceil(430.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns00 = {"d","f","c","k","h"};
		String[] ConditionColumnsValues00 = {"d2","f0","c0","k7","h6"};
		DBApp.select("js5", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("js5", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("js5");
	
		String[] ConditionColumns03 = {"d"};
		String[] ConditionColumnsValues03 = {"d1"};
		DBApp.select("js5", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("js5");
	
		DBApp.select("js5");
	
		DBApp.select("js5");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("js5", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("js5", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"i","e","g","f","a"};
		String[] ConditionColumnsValues09 = {"i1","e2","g6","f1","a307"};
		DBApp.select("js5", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("js5");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("js5", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"i","f","k","c","j"};
		String[] ConditionColumnsValues012 = {"i7","f4","k2","c1","j6"};
		DBApp.select("js5", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"c","a","j","e","i"};
		String[] ConditionColumnsValues013 = {"c2","a56","j6","e1","i2"};
		DBApp.select("js5", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"h","g","e"};
		String[] ConditionColumnsValues014 = {"h1","g4","e0"};
		DBApp.select("js5", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"d"};
		String[] ConditionColumnsValues015 = {"d3"};
		DBApp.select("js5", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("js5");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("js5", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"k","f","c","e","h"};
		String[] ConditionColumnsValues018 = {"k4","f3","c0","e3","h5"};
		DBApp.select("js5", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"b"};
		String[] ConditionColumnsValues019 = {"b0"};
		DBApp.select("js5", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("js5");
	
		DBApp.select("js5");
	
		String selectTrace0 = DBApp.getFullTrace("js5");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 454);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 430"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z65", cols1);
		String [][] records_z65 = new String[434][cols1.length];
		for(int i=0;i<434;i++)
		{
			records_z65[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z65[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z65", records_z65[i]);
		}
	
		int pageCount1 = (int)Math.ceil(434.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a4"};
		DBApp.select("z65", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("z65");
	
		DBApp.select("z65");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b0"};
		DBApp.select("z65", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c2"};
		DBApp.select("z65", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("z65", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"f","b","h"};
		String[] ConditionColumnsValues16 = {"f3","b1","h7"};
		DBApp.select("z65", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"f","b"};
		String[] ConditionColumnsValues17 = {"f1","b1"};
		DBApp.select("z65", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("z65", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("z65");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("z65", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"g"};
		String[] ConditionColumnsValues111 = {"g3"};
		DBApp.select("z65", ConditionColumns111, ConditionColumnsValues111);
	
		String selectTrace1 = DBApp.getFullTrace("z65");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 448);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 434"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("yx5", cols2);
		String [][] records_yx5 = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			records_yx5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yx5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yx5", records_yx5[i]);
		}
	
		int pageCount2 = (int)Math.ceil(70.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("yx5");
	
		String[] ConditionColumns21 = {"d","i","e","g","k"};
		String[] ConditionColumnsValues21 = {"d1","i6","e3","g5","k0"};
		DBApp.select("yx5", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("yx5");
	
		String[] ConditionColumns23 = {"i","j","e","m"};
		String[] ConditionColumnsValues23 = {"i6","j0","e0","m8"};
		DBApp.select("yx5", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("yx5");
	
		DBApp.select("yx5");
	
		DBApp.select("yx5");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("yx5");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("yx5", pageSelectPointer215,recordSelectPointer215);
	
		String selectTrace2 = DBApp.getFullTrace("yx5");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 88);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 70"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("j5cg", cols0);
		String [][] records_j5cg = new String[202][cols0.length];
		for(int i=0;i<202;i++)
		{
			records_j5cg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j5cg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j5cg", records_j5cg[i]);
		}
	
		int pageCount0 = (int)Math.ceil(202.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		DBApp.select("j5cg");
	
		DBApp.select("j5cg");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("j5cg", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("j5cg", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("j5cg");
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a85"};
		DBApp.select("j5cg", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a81"};
		DBApp.select("j5cg", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a95"};
		DBApp.select("j5cg", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("j5cg", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("j5cg");
	
		String[] ConditionColumns010 = {"a"};
		String[] ConditionColumnsValues010 = {"a83"};
		DBApp.select("j5cg", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("j5cg", pageSelectPointer011,recordSelectPointer011);
	
		String selectTrace0 = DBApp.getFullTrace("j5cg");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 216);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 202"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("rhd", cols1);
		String [][] records_rhd = new String[220][cols1.length];
		for(int i=0;i<220;i++)
		{
			records_rhd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rhd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rhd", records_rhd[i]);
		}
	
		int pageCount1 = (int)Math.ceil(220.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		String[] ConditionColumns10 = {"f"};
		String[] ConditionColumnsValues10 = {"f0"};
		DBApp.select("rhd", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("rhd");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("rhd", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("rhd");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("rhd", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("rhd", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("rhd");
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b1"};
		DBApp.select("rhd", ConditionColumns17, ConditionColumnsValues17);
	
		String selectTrace1 = DBApp.getFullTrace("rhd");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 230);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 220"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("p4c4", cols0);
		String [][] records_p4c4 = new String[473][cols0.length];
		for(int i=0;i<473;i++)
		{
			records_p4c4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p4c4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p4c4", records_p4c4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(473.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		String[] ConditionColumns00 = {"c"};
		String[] ConditionColumnsValues00 = {"c1"};
		DBApp.select("p4c4", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a179"};
		DBApp.select("p4c4", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("p4c4", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("p4c4");
	
		String[] ConditionColumns04 = {"b"};
		String[] ConditionColumnsValues04 = {"b1"};
		DBApp.select("p4c4", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("p4c4");
	
		String[] ConditionColumns06 = {"c"};
		String[] ConditionColumnsValues06 = {"c2"};
		DBApp.select("p4c4", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c2"};
		DBApp.select("p4c4", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("p4c4");
	
		DBApp.select("p4c4");
	
		String selectTrace0 = DBApp.getFullTrace("p4c4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 485);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 473"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b1e", cols1);
		String [][] records_b1e = new String[319][cols1.length];
		for(int i=0;i<319;i++)
		{
			records_b1e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1e", records_b1e[i]);
		}
	
		int pageCount1 = (int)Math.ceil(319.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("b1e");
	
		String[] ConditionColumns11 = {"e"};
		String[] ConditionColumnsValues11 = {"e3"};
		DBApp.select("b1e", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"b","e"};
		String[] ConditionColumnsValues13 = {"b0","e3"};
		DBApp.select("b1e", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"g"};
		String[] ConditionColumnsValues14 = {"g1"};
		DBApp.select("b1e", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"b"};
		String[] ConditionColumnsValues15 = {"b1"};
		DBApp.select("b1e", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("b1e");
	
		String[] ConditionColumns17 = {"d","b"};
		String[] ConditionColumnsValues17 = {"d2","b0"};
		DBApp.select("b1e", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"e","c"};
		String[] ConditionColumnsValues111 = {"e1","c1"};
		DBApp.select("b1e", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("b1e");
	
		String[] ConditionColumns114 = {"f"};
		String[] ConditionColumnsValues114 = {"f0"};
		DBApp.select("b1e", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("b1e");
	
		DBApp.select("b1e");
	
		String[] ConditionColumns118 = {"d"};
		String[] ConditionColumnsValues118 = {"d3"};
		DBApp.select("b1e", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"f"};
		String[] ConditionColumnsValues119 = {"f4"};
		DBApp.select("b1e", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer120,recordSelectPointer120);
	
		DBApp.select("b1e");
	
		String[] ConditionColumns122 = {"f","e"};
		String[] ConditionColumnsValues122 = {"f2","e4"};
		DBApp.select("b1e", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"f"};
		String[] ConditionColumnsValues123 = {"f3"};
		DBApp.select("b1e", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("b1e");
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer125,recordSelectPointer125);
	
		DBApp.select("b1e");
	
		String[] ConditionColumns127 = {"e"};
		String[] ConditionColumnsValues127 = {"e4"};
		DBApp.select("b1e", ConditionColumns127, ConditionColumnsValues127);
	
		String[] ConditionColumns128 = {"d"};
		String[] ConditionColumnsValues128 = {"d1"};
		DBApp.select("b1e", ConditionColumns128, ConditionColumnsValues128);
	
		int pageSelectPointer129 = genRandNum(pageCount1)-1;
		int recordSelectPointer129 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer129==pageCount1-1)
			recordSelectPointer129 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer129,recordSelectPointer129);
	
		String selectTrace1 = DBApp.getFullTrace("b1e");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 351);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 319"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vwg78", cols2);
		String [][] records_vwg78 = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			records_vwg78[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vwg78[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vwg78", records_vwg78[i]);
		}
	
		int pageCount2 = (int)Math.ceil(47.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns20 = {"k"};
		String[] ConditionColumnsValues20 = {"k3"};
		DBApp.select("vwg78", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("vwg78");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"g"};
		String[] ConditionColumnsValues25 = {"g5"};
		DBApp.select("vwg78", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"e"};
		String[] ConditionColumnsValues26 = {"e0"};
		DBApp.select("vwg78", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("vwg78");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("vwg78");
	
		String[] ConditionColumns214 = {"d","b","j","c","h"};
		String[] ConditionColumnsValues214 = {"d1","b1","j1","c2","h1"};
		DBApp.select("vwg78", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"e"};
		String[] ConditionColumnsValues215 = {"e0"};
		DBApp.select("vwg78", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"k"};
		String[] ConditionColumnsValues216 = {"k7"};
		DBApp.select("vwg78", ConditionColumns216, ConditionColumnsValues216);
	
		DBApp.select("vwg78");
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer218,recordSelectPointer218);
	
		DBApp.select("vwg78");
	
		DBApp.select("vwg78");
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("vwg78", pageSelectPointer221,recordSelectPointer221);
	
		String selectTrace2 = DBApp.getFullTrace("vwg78");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 71);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 47"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("tf", cols3);
		String [][] records_tf = new String[358][cols3.length];
		for(int i=0;i<358;i++)
		{
			records_tf[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tf[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tf", records_tf[i]);
		}
	
		int pageCount3 = (int)Math.ceil(358.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer32,recordSelectPointer32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer33,recordSelectPointer33);
	
		DBApp.select("tf");
	
		String[] ConditionColumns35 = {"c"};
		String[] ConditionColumnsValues35 = {"c1"};
		DBApp.select("tf", ConditionColumns35, ConditionColumnsValues35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("tf");
	
		String[] ConditionColumns38 = {"c"};
		String[] ConditionColumnsValues38 = {"c0"};
		DBApp.select("tf", ConditionColumns38, ConditionColumnsValues38);
	
		String[] ConditionColumns39 = {"d"};
		String[] ConditionColumnsValues39 = {"d1"};
		DBApp.select("tf", ConditionColumns39, ConditionColumnsValues39);
	
		DBApp.select("tf");
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer311,recordSelectPointer311);
	
		DBApp.select("tf");
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer313,recordSelectPointer313);
	
		DBApp.select("tf");
	
		DBApp.select("tf");
	
		DBApp.select("tf");
	
		DBApp.select("tf");
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer318,recordSelectPointer318);
	
		DBApp.select("tf");
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("tf", pageSelectPointer320,recordSelectPointer320);
	
		DBApp.select("tf");
	
		String[] ConditionColumns322 = {"d"};
		String[] ConditionColumnsValues322 = {"d2"};
		DBApp.select("tf", ConditionColumns322, ConditionColumnsValues322);
	
		String[] ConditionColumns323 = {"c"};
		String[] ConditionColumnsValues323 = {"c2"};
		DBApp.select("tf", ConditionColumns323, ConditionColumnsValues323);
	
		String[] ConditionColumns324 = {"c"};
		String[] ConditionColumnsValues324 = {"c2"};
		DBApp.select("tf", ConditionColumns324, ConditionColumnsValues324);
	
		DBApp.select("tf");
	
		DBApp.select("tf");
	
		String selectTrace3 = DBApp.getFullTrace("tf");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 387);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 358"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jdn", cols4);
		String [][] records_jdn = new String[300][cols4.length];
		for(int i=0;i<300;i++)
		{
			records_jdn[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_jdn[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("jdn", records_jdn[i]);
		}
	
		int pageCount4 = (int)Math.ceil(300.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("jdn");
	
		DBApp.select("jdn");
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("jdn", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"b"};
		String[] ConditionColumnsValues43 = {"b1"};
		DBApp.select("jdn", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"j"};
		String[] ConditionColumnsValues44 = {"j6"};
		DBApp.select("jdn", ConditionColumns44, ConditionColumnsValues44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("jdn", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("jdn");
	
		String[] ConditionColumns47 = {"e"};
		String[] ConditionColumnsValues47 = {"e3"};
		DBApp.select("jdn", ConditionColumns47, ConditionColumnsValues47);
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("jdn", pageSelectPointer48,recordSelectPointer48);
	
		String selectTrace4 = DBApp.getFullTrace("jdn");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 311);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 300"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("jlfqj", cols0);
		String [][] records_jlfqj = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_jlfqj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jlfqj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jlfqj", records_jlfqj[i]);
		}
	
		int pageCount0 = (int)Math.ceil(28.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		String[] ConditionColumns00 = {"d","a"};
		String[] ConditionColumnsValues00 = {"d0","a8"};
		DBApp.select("jlfqj", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("jlfqj");
	
		String[] ConditionColumns04 = {"b","f","a","d"};
		String[] ConditionColumnsValues04 = {"b1","f3","a27","d3"};
		DBApp.select("jlfqj", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a19"};
		DBApp.select("jlfqj", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("jlfqj");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"h"};
		String[] ConditionColumnsValues08 = {"h1"};
		DBApp.select("jlfqj", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("jlfqj");
	
		DBApp.select("jlfqj");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"e","h","d"};
		String[] ConditionColumnsValues012 = {"e1","h0","d0"};
		DBApp.select("jlfqj", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"c","d","e"};
		String[] ConditionColumnsValues013 = {"c2","d0","e0"};
		DBApp.select("jlfqj", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"b","f"};
		String[] ConditionColumnsValues014 = {"b0","f2"};
		DBApp.select("jlfqj", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"c"};
		String[] ConditionColumnsValues015 = {"c1"};
		DBApp.select("jlfqj", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer016,recordSelectPointer016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer017,recordSelectPointer017);
	
		DBApp.select("jlfqj");
	
		DBApp.select("jlfqj");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer020,recordSelectPointer020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("jlfqj");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("jlfqj");
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("jlfqj", pageSelectPointer025,recordSelectPointer025);
	
		String selectTrace0 = DBApp.getFullTrace("jlfqj");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 56);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 28"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("l6xc", cols1);
		String [][] records_l6xc = new String[437][cols1.length];
		for(int i=0;i<437;i++)
		{
			records_l6xc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l6xc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l6xc", records_l6xc[i]);
		}
	
		int pageCount1 = (int)Math.ceil(437.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("l6xc");
	
		String[] ConditionColumns12 = {"c"};
		String[] ConditionColumnsValues12 = {"c1"};
		DBApp.select("l6xc", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("l6xc");
	
		DBApp.select("l6xc");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"a"};
		String[] ConditionColumnsValues111 = {"a276"};
		DBApp.select("l6xc", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l6xc", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"c","h"};
		String[] ConditionColumnsValues113 = {"c1","h5"};
		DBApp.select("l6xc", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("l6xc");
	
		String[] ConditionColumns115 = {"a","e","c","f"};
		String[] ConditionColumnsValues115 = {"a151","e1","c1","f1"};
		DBApp.select("l6xc", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("l6xc");
	
		String selectTrace1 = DBApp.getFullTrace("l6xc");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 456);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 437"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("qr", cols2);
		String [][] records_qr = new String[425][cols2.length];
		for(int i=0;i<425;i++)
		{
			records_qr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qr", records_qr[i]);
		}
	
		int pageCount2 = (int)Math.ceil(425.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns20 = {"i","g","j","d","f"};
		String[] ConditionColumnsValues20 = {"i4","g3","j9","d1","f1"};
		DBApp.select("qr", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"h"};
		String[] ConditionColumnsValues23 = {"h0"};
		DBApp.select("qr", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"j"};
		String[] ConditionColumnsValues25 = {"j6"};
		DBApp.select("qr", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"i"};
		String[] ConditionColumnsValues26 = {"i0"};
		DBApp.select("qr", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("qr");
	
		String[] ConditionColumns28 = {"g"};
		String[] ConditionColumnsValues28 = {"g6"};
		DBApp.select("qr", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"c","b","d"};
		String[] ConditionColumnsValues29 = {"c0","b0","d0"};
		DBApp.select("qr", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("qr");
	
		String[] ConditionColumns211 = {"a","f","c"};
		String[] ConditionColumnsValues211 = {"a278","f2","c2"};
		DBApp.select("qr", ConditionColumns211, ConditionColumnsValues211);
	
		DBApp.select("qr");
	
		String[] ConditionColumns213 = {"b"};
		String[] ConditionColumnsValues213 = {"b1"};
		DBApp.select("qr", ConditionColumns213, ConditionColumnsValues213);
	
		DBApp.select("qr");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer215,recordSelectPointer215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer216,recordSelectPointer216);
	
		DBApp.select("qr");
	
		DBApp.select("qr");
	
		String[] ConditionColumns219 = {"c","f","h"};
		String[] ConditionColumnsValues219 = {"c1","f1","h7"};
		DBApp.select("qr", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("qr");
	
		String[] ConditionColumns221 = {"c"};
		String[] ConditionColumnsValues221 = {"c0"};
		DBApp.select("qr", ConditionColumns221, ConditionColumnsValues221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("qr", pageSelectPointer222,recordSelectPointer222);
	
		String selectTrace2 = DBApp.getFullTrace("qr");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 450);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 425"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k5", cols3);
		String [][] records_k5 = new String[346][cols3.length];
		for(int i=0;i<346;i++)
		{
			records_k5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k5", records_k5[i]);
		}
	
		int pageCount3 = (int)Math.ceil(346.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer30,recordSelectPointer30);
	
		DBApp.select("k5");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("k5");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("k5");
	
		DBApp.select("k5");
	
		DBApp.select("k5");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("k5");
	
		String[] ConditionColumns310 = {"a"};
		String[] ConditionColumnsValues310 = {"a74"};
		DBApp.select("k5", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"k"};
		String[] ConditionColumnsValues311 = {"k4"};
		DBApp.select("k5", ConditionColumns311, ConditionColumnsValues311);
	
		DBApp.select("k5");
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("k5");
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer316,recordSelectPointer316);
	
		DBApp.select("k5");
	
		String[] ConditionColumns318 = {"g"};
		String[] ConditionColumnsValues318 = {"g2"};
		DBApp.select("k5", ConditionColumns318, ConditionColumnsValues318);
	
		int pageSelectPointer319 = genRandNum(pageCount3)-1;
		int recordSelectPointer319 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer319==pageCount3-1)
			recordSelectPointer319 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer319,recordSelectPointer319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((346%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer320,recordSelectPointer320);
	
		String selectTrace3 = DBApp.getFullTrace("k5");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 369);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 346"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("q02", cols0);
		String [][] records_q02 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_q02[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q02[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q02", records_q02[i]);
		}
	
		int pageCount0 = (int)Math.ceil(8.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("q02");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"n"};
		String[] ConditionColumnsValues03 = {"n3"};
		DBApp.select("q02", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("q02");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("q02");
	
		String[] ConditionColumns010 = {"e","h","n","g"};
		String[] ConditionColumnsValues010 = {"e2","h2","n2","g2"};
		DBApp.select("q02", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("q02");
	
		String[] ConditionColumns012 = {"g"};
		String[] ConditionColumnsValues012 = {"g0"};
		DBApp.select("q02", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"l"};
		String[] ConditionColumnsValues013 = {"l7"};
		DBApp.select("q02", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("q02");
	
		DBApp.select("q02");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("q02");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("q02");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer022,recordSelectPointer022);
	
		String[] ConditionColumns023 = {"m"};
		String[] ConditionColumnsValues023 = {"m4"};
		DBApp.select("q02", ConditionColumns023, ConditionColumnsValues023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q02", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("q02");
	
		String selectTrace0 = DBApp.getFullTrace("q02");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 36);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 8"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yi", cols1);
		String [][] records_yi = new String[390][cols1.length];
		for(int i=0;i<390;i++)
		{
			records_yi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yi", records_yi[i]);
		}
	
		int pageCount1 = (int)Math.ceil(390.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b1"};
		DBApp.select("yi", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"f","b","l","j"};
		String[] ConditionColumnsValues15 = {"f5","b1","l11","j3"};
		DBApp.select("yi", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"b"};
		String[] ConditionColumnsValues18 = {"b1"};
		DBApp.select("yi", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("yi");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer111,recordSelectPointer111);
	
		String[] ConditionColumns112 = {"f","i","j","k","d","l"};
		String[] ConditionColumnsValues112 = {"f2","i2","j8","k9","d2","l2"};
		DBApp.select("yi", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("yi");
	
		String[] ConditionColumns114 = {"k","b"};
		String[] ConditionColumnsValues114 = {"k5","b1"};
		DBApp.select("yi", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((390%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer119,recordSelectPointer119);
	
		String selectTrace1 = DBApp.getFullTrace("yi");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 412);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 390"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("fw252", cols2);
		String [][] records_fw252 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_fw252[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fw252[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fw252", records_fw252[i]);
		}
	
		int pageCount2 = (int)Math.ceil(37.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("fw252");
	
		String[] ConditionColumns21 = {"i"};
		String[] ConditionColumnsValues21 = {"i8"};
		DBApp.select("fw252", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"h"};
		String[] ConditionColumnsValues23 = {"h2"};
		DBApp.select("fw252", ConditionColumns23, ConditionColumnsValues23);
	
		DBApp.select("fw252");
	
		DBApp.select("fw252");
	
		DBApp.select("fw252");
	
		String[] ConditionColumns27 = {"i","g","j","f","e","m"};
		String[] ConditionColumnsValues27 = {"i7","g0","j7","f1","e2","m7"};
		DBApp.select("fw252", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("fw252");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("fw252");
	
		String[] ConditionColumns211 = {"m"};
		String[] ConditionColumnsValues211 = {"m12"};
		DBApp.select("fw252", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"d"};
		String[] ConditionColumnsValues212 = {"d2"};
		DBApp.select("fw252", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("fw252");
	
		DBApp.select("fw252");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"d","h"};
		String[] ConditionColumnsValues216 = {"d3","h7"};
		DBApp.select("fw252", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"i"};
		String[] ConditionColumnsValues217 = {"i8"};
		DBApp.select("fw252", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer218,recordSelectPointer218);
	
		DBApp.select("fw252");
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer220,recordSelectPointer220);
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer221,recordSelectPointer221);
	
		String[] ConditionColumns222 = {"j"};
		String[] ConditionColumnsValues222 = {"j0"};
		DBApp.select("fw252", ConditionColumns222, ConditionColumnsValues222);
	
		DBApp.select("fw252");
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer224,recordSelectPointer224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer225,recordSelectPointer225);
	
		int pageSelectPointer226 = genRandNum(pageCount2)-1;
		int recordSelectPointer226 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer226==pageCount2-1)
			recordSelectPointer226 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer226,recordSelectPointer226);
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((37%DBApp.dataPageSize))-1;
		DBApp.select("fw252", pageSelectPointer227,recordSelectPointer227);
	
		String[] ConditionColumns228 = {"k"};
		String[] ConditionColumnsValues228 = {"k9"};
		DBApp.select("fw252", ConditionColumns228, ConditionColumnsValues228);
	
		DBApp.select("fw252");
	
		String selectTrace2 = DBApp.getFullTrace("fw252");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 69);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 37"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y3f3", cols3);
		String [][] records_y3f3 = new String[490][cols3.length];
		for(int i=0;i<490;i++)
		{
			records_y3f3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y3f3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y3f3", records_y3f3[i]);
		}
	
		int pageCount3 = (int)Math.ceil(490.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("y3f3");
	
		DBApp.select("y3f3");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer32,recordSelectPointer32);
	
		String[] ConditionColumns33 = {"n"};
		String[] ConditionColumnsValues33 = {"n3"};
		DBApp.select("y3f3", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"n","g","c","e"};
		String[] ConditionColumnsValues35 = {"n13","g6","c2","e1"};
		DBApp.select("y3f3", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"k","f","n","j"};
		String[] ConditionColumnsValues36 = {"k7","f5","n3","j7"};
		DBApp.select("y3f3", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("y3f3");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("y3f3");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer310,recordSelectPointer310);
	
		String[] ConditionColumns311 = {"m"};
		String[] ConditionColumnsValues311 = {"m3"};
		DBApp.select("y3f3", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"k"};
		String[] ConditionColumnsValues312 = {"k9"};
		DBApp.select("y3f3", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"m","l","e"};
		String[] ConditionColumnsValues313 = {"m4","l4","e2"};
		DBApp.select("y3f3", ConditionColumns313, ConditionColumnsValues313);
	
		DBApp.select("y3f3");
	
		String[] ConditionColumns315 = {"o","c","i","g","k","a"};
		String[] ConditionColumnsValues315 = {"o12","c0","i0","g4","k9","a207"};
		DBApp.select("y3f3", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("y3f3");
	
		DBApp.select("y3f3");
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer318,recordSelectPointer318);
	
		DBApp.select("y3f3");
	
		String[] ConditionColumns320 = {"i"};
		String[] ConditionColumnsValues320 = {"i4"};
		DBApp.select("y3f3", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"d","i","h"};
		String[] ConditionColumnsValues321 = {"d2","i0","h6"};
		DBApp.select("y3f3", ConditionColumns321, ConditionColumnsValues321);
	
		DBApp.select("y3f3");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer323,recordSelectPointer323);
	
		String[] ConditionColumns324 = {"i","k"};
		String[] ConditionColumnsValues324 = {"i3","k3"};
		DBApp.select("y3f3", ConditionColumns324, ConditionColumnsValues324);
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer325,recordSelectPointer325);
	
		int pageSelectPointer326 = genRandNum(pageCount3)-1;
		int recordSelectPointer326 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer326==pageCount3-1)
			recordSelectPointer326 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer326,recordSelectPointer326);
	
		int pageSelectPointer327 = genRandNum(pageCount3)-1;
		int recordSelectPointer327 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer327==pageCount3-1)
			recordSelectPointer327 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer327,recordSelectPointer327);
	
		int pageSelectPointer328 = genRandNum(pageCount3)-1;
		int recordSelectPointer328 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer328==pageCount3-1)
			recordSelectPointer328 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("y3f3", pageSelectPointer328,recordSelectPointer328);
	
		String selectTrace3 = DBApp.getFullTrace("y3f3");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 521);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 490"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qdv", cols0);
		String [][] records_qdv = new String[330][cols0.length];
		for(int i=0;i<330;i++)
		{
			records_qdv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qdv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qdv", records_qdv[i]);
		}
	
		int pageCount0 = (int)Math.ceil(330.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("qdv", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("qdv", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("qdv", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"a","h","k","f","l"};
		String[] ConditionColumnsValues03 = {"a192","h0","k5","f0","l0"};
		DBApp.select("qdv", ConditionColumns03, ConditionColumnsValues03);
	
		String selectTrace0 = DBApp.getFullTrace("qdv");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 336);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 330"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("d87", cols1);
		String [][] records_d87 = new String[112][cols1.length];
		for(int i=0;i<112;i++)
		{
			records_d87[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d87[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d87", records_d87[i]);
		}
	
		int pageCount1 = (int)Math.ceil(112.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("d87");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("d87", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("d87");
	
		String[] ConditionColumns13 = {"d","e"};
		String[] ConditionColumnsValues13 = {"d0","e2"};
		DBApp.select("d87", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"e"};
		String[] ConditionColumnsValues14 = {"e1"};
		DBApp.select("d87", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("d87", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("d87", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"a"};
		String[] ConditionColumnsValues17 = {"a103"};
		DBApp.select("d87", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"e"};
		String[] ConditionColumnsValues18 = {"e1"};
		DBApp.select("d87", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"c"};
		String[] ConditionColumnsValues19 = {"c0"};
		DBApp.select("d87", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("d87", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"c","e"};
		String[] ConditionColumnsValues111 = {"c0","e2"};
		DBApp.select("d87", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("d87");
	
		DBApp.select("d87");
	
		DBApp.select("d87");
	
		String[] ConditionColumns115 = {"c"};
		String[] ConditionColumnsValues115 = {"c2"};
		DBApp.select("d87", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("d87");
	
		DBApp.select("d87");
	
		DBApp.select("d87");
	
		String selectTrace1 = DBApp.getFullTrace("d87");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 133);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 112"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("zai", cols2);
		String [][] records_zai = new String[477][cols2.length];
		for(int i=0;i<477;i++)
		{
			records_zai[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zai[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zai", records_zai[i]);
		}
	
		int pageCount2 = (int)Math.ceil(477.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		String[] ConditionColumns20 = {"f"};
		String[] ConditionColumnsValues20 = {"f5"};
		DBApp.select("zai", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((477%DBApp.dataPageSize))-1;
		DBApp.select("zai", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"g","f","a","c"};
		String[] ConditionColumnsValues22 = {"g6","f2","a146","c2"};
		DBApp.select("zai", ConditionColumns22, ConditionColumnsValues22);
	
		String selectTrace2 = DBApp.getFullTrace("zai");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 482);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 477"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("knul", cols3);
		String [][] records_knul = new String[239][cols3.length];
		for(int i=0;i<239;i++)
		{
			records_knul[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_knul[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("knul", records_knul[i]);
		}
	
		int pageCount3 = (int)Math.ceil(239.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"a","e"};
		String[] ConditionColumnsValues31 = {"a177","e2"};
		DBApp.select("knul", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("knul");
	
		String[] ConditionColumns33 = {"d"};
		String[] ConditionColumnsValues33 = {"d3"};
		DBApp.select("knul", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("knul");
	
		DBApp.select("knul");
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("knul");
	
		String[] ConditionColumns310 = {"e"};
		String[] ConditionColumnsValues310 = {"e0"};
		DBApp.select("knul", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer311,recordSelectPointer311);
	
		DBApp.select("knul");
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"e"};
		String[] ConditionColumnsValues315 = {"e1"};
		DBApp.select("knul", ConditionColumns315, ConditionColumnsValues315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer316,recordSelectPointer316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"a","b"};
		String[] ConditionColumnsValues318 = {"a143","b1"};
		DBApp.select("knul", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"e"};
		String[] ConditionColumnsValues319 = {"e0"};
		DBApp.select("knul", ConditionColumns319, ConditionColumnsValues319);
	
		String[] ConditionColumns320 = {"e"};
		String[] ConditionColumnsValues320 = {"e1"};
		DBApp.select("knul", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"e","c"};
		String[] ConditionColumnsValues321 = {"e3","c2"};
		DBApp.select("knul", ConditionColumns321, ConditionColumnsValues321);
	
		DBApp.select("knul");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer323,recordSelectPointer323);
	
		String[] ConditionColumns324 = {"e","b"};
		String[] ConditionColumnsValues324 = {"e4","b1"};
		DBApp.select("knul", ConditionColumns324, ConditionColumnsValues324);
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer325,recordSelectPointer325);
	
		int pageSelectPointer326 = genRandNum(pageCount3)-1;
		int recordSelectPointer326 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer326==pageCount3-1)
			recordSelectPointer326 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer326,recordSelectPointer326);
	
		String[] ConditionColumns327 = {"b"};
		String[] ConditionColumnsValues327 = {"b1"};
		DBApp.select("knul", ConditionColumns327, ConditionColumnsValues327);
	
		String[] ConditionColumns328 = {"e"};
		String[] ConditionColumnsValues328 = {"e1"};
		DBApp.select("knul", ConditionColumns328, ConditionColumnsValues328);
	
		int pageSelectPointer329 = genRandNum(pageCount3)-1;
		int recordSelectPointer329 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer329==pageCount3-1)
			recordSelectPointer329 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("knul", pageSelectPointer329,recordSelectPointer329);
	
		String selectTrace3 = DBApp.getFullTrace("knul");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 271);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 239"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("zp1", cols4);
		String [][] records_zp1 = new String[394][cols4.length];
		for(int i=0;i<394;i++)
		{
			records_zp1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_zp1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("zp1", records_zp1[i]);
		}
	
		int pageCount4 = (int)Math.ceil(394.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("zp1");
	
		DBApp.select("zp1");
	
		String[] ConditionColumns42 = {"d"};
		String[] ConditionColumnsValues42 = {"d2"};
		DBApp.select("zp1", ConditionColumns42, ConditionColumnsValues42);
	
		String[] ConditionColumns43 = {"c"};
		String[] ConditionColumnsValues43 = {"c1"};
		DBApp.select("zp1", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"b"};
		String[] ConditionColumnsValues44 = {"b0"};
		DBApp.select("zp1", ConditionColumns44, ConditionColumnsValues44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("zp1", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("zp1");
	
		String selectTrace4 = DBApp.getFullTrace("zp1");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 403);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 394"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("a79", cols0);
		String [][] records_a79 = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			records_a79[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a79[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a79", records_a79[i]);
		}
	
		int pageCount0 = (int)Math.ceil(60.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("a79");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"b"};
		String[] ConditionColumnsValues02 = {"b0"};
		DBApp.select("a79", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"a","c"};
		String[] ConditionColumnsValues03 = {"a12","c0"};
		DBApp.select("a79", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("a79");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"b","c"};
		String[] ConditionColumnsValues09 = {"b0","c0"};
		DBApp.select("a79", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("a79");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d0"};
		DBApp.select("a79", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"c"};
		String[] ConditionColumnsValues015 = {"c1"};
		DBApp.select("a79", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("a79");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"c","d"};
		String[] ConditionColumnsValues019 = {"c1","d0"};
		DBApp.select("a79", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("a79");
	
		DBApp.select("a79");
	
		DBApp.select("a79");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer024,recordSelectPointer024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((60%DBApp.dataPageSize))-1;
		DBApp.select("a79", pageSelectPointer025,recordSelectPointer025);
	
		String[] ConditionColumns026 = {"a"};
		String[] ConditionColumnsValues026 = {"a3"};
		DBApp.select("a79", ConditionColumns026, ConditionColumnsValues026);
	
		DBApp.select("a79");
	
		String selectTrace0 = DBApp.getFullTrace("a79");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 90);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 60"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hj72a", cols1);
		String [][] records_hj72a = new String[477][cols1.length];
		for(int i=0;i<477;i++)
		{
			records_hj72a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hj72a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hj72a", records_hj72a[i]);
		}
	
		int pageCount1 = (int)Math.ceil(477.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((477%DBApp.dataPageSize))-1;
		DBApp.select("hj72a", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("hj72a");
	
		DBApp.select("hj72a");
	
		String[] ConditionColumns13 = {"i"};
		String[] ConditionColumnsValues13 = {"i0"};
		DBApp.select("hj72a", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((477%DBApp.dataPageSize))-1;
		DBApp.select("hj72a", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("hj72a");
	
		DBApp.select("hj72a");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((477%DBApp.dataPageSize))-1;
		DBApp.select("hj72a", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((477%DBApp.dataPageSize))-1;
		DBApp.select("hj72a", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("hj72a");
	
		DBApp.select("hj72a");
	
		String selectTrace1 = DBApp.getFullTrace("hj72a");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 490);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 477"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("a2q1", cols2);
		String [][] records_a2q1 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_a2q1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a2q1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a2q1", records_a2q1[i]);
		}
	
		int pageCount2 = (int)Math.ceil(40.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"c"};
		String[] ConditionColumnsValues22 = {"c2"};
		DBApp.select("a2q1", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("a2q1");
	
		String[] ConditionColumns24 = {"e"};
		String[] ConditionColumnsValues24 = {"e2"};
		DBApp.select("a2q1", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("a2q1");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"d","c"};
		String[] ConditionColumnsValues29 = {"d1","c2"};
		DBApp.select("a2q1", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"d"};
		String[] ConditionColumnsValues210 = {"d3"};
		DBApp.select("a2q1", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("a2q1");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((40%DBApp.dataPageSize))-1;
		DBApp.select("a2q1", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"b","c"};
		String[] ConditionColumnsValues214 = {"b1","c0"};
		DBApp.select("a2q1", ConditionColumns214, ConditionColumnsValues214);
	
		String selectTrace2 = DBApp.getFullTrace("a2q1");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 57);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 40"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("a17n", cols3);
		String [][] records_a17n = new String[147][cols3.length];
		for(int i=0;i<147;i++)
		{
			records_a17n[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_a17n[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("a17n", records_a17n[i]);
		}
	
		int pageCount3 = (int)Math.ceil(147.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns30 = {"d"};
		String[] ConditionColumnsValues30 = {"d2"};
		DBApp.select("a17n", ConditionColumns30, ConditionColumnsValues30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("a17n");
	
		String[] ConditionColumns34 = {"l"};
		String[] ConditionColumnsValues34 = {"l7"};
		DBApp.select("a17n", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer36,recordSelectPointer36);
	
		String[] ConditionColumns37 = {"h","c","j"};
		String[] ConditionColumnsValues37 = {"h6","c1","j8"};
		DBApp.select("a17n", ConditionColumns37, ConditionColumnsValues37);
	
		DBApp.select("a17n");
	
		DBApp.select("a17n");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer310,recordSelectPointer310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer311,recordSelectPointer311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer312,recordSelectPointer312);
	
		String[] ConditionColumns313 = {"b"};
		String[] ConditionColumnsValues313 = {"b1"};
		DBApp.select("a17n", ConditionColumns313, ConditionColumnsValues313);
	
		DBApp.select("a17n");
	
		DBApp.select("a17n");
	
		DBApp.select("a17n");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("a17n", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"c","d","g","j","l"};
		String[] ConditionColumnsValues318 = {"c1","d2","g2","j2","l10"};
		DBApp.select("a17n", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"f"};
		String[] ConditionColumnsValues319 = {"f1"};
		DBApp.select("a17n", ConditionColumns319, ConditionColumnsValues319);
	
		DBApp.select("a17n");
	
		DBApp.select("a17n");
	
		String selectTrace3 = DBApp.getFullTrace("a17n");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 171);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 147"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o4jn", cols4);
		String [][] records_o4jn = new String[416][cols4.length];
		for(int i=0;i<416;i++)
		{
			records_o4jn[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o4jn[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o4jn", records_o4jn[i]);
		}
	
		int pageCount4 = (int)Math.ceil(416.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		String[] ConditionColumns40 = {"f"};
		String[] ConditionColumnsValues40 = {"f1"};
		DBApp.select("o4jn", ConditionColumns40, ConditionColumnsValues40);
	
		DBApp.select("o4jn");
	
		DBApp.select("o4jn");
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((416%DBApp.dataPageSize))-1;
		DBApp.select("o4jn", pageSelectPointer43,recordSelectPointer43);
	
		DBApp.select("o4jn");
	
		String selectTrace4 = DBApp.getFullTrace("o4jn");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 423);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 416"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("ql185", cols0);
		String [][] records_ql185 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			records_ql185[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ql185[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ql185", records_ql185[i]);
		}
	
		int pageCount0 = (int)Math.ceil(20.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		String[] ConditionColumns00 = {"e","c","a"};
		String[] ConditionColumnsValues00 = {"e1","c1","a1"};
		DBApp.select("ql185", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("ql185");
	
		String[] ConditionColumns02 = {"c"};
		String[] ConditionColumnsValues02 = {"c2"};
		DBApp.select("ql185", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"e"};
		String[] ConditionColumnsValues05 = {"e0"};
		DBApp.select("ql185", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("ql185");
	
		String[] ConditionColumns09 = {"c"};
		String[] ConditionColumnsValues09 = {"c1"};
		DBApp.select("ql185", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"f"};
		String[] ConditionColumnsValues011 = {"f1"};
		DBApp.select("ql185", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("ql185");
	
		DBApp.select("ql185");
	
		String[] ConditionColumns014 = {"c"};
		String[] ConditionColumnsValues014 = {"c0"};
		DBApp.select("ql185", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"b"};
		String[] ConditionColumnsValues015 = {"b1"};
		DBApp.select("ql185", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("ql185");
	
		String[] ConditionColumns017 = {"b"};
		String[] ConditionColumnsValues017 = {"b0"};
		DBApp.select("ql185", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"c","e"};
		String[] ConditionColumnsValues018 = {"c0","e3"};
		DBApp.select("ql185", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"c"};
		String[] ConditionColumnsValues020 = {"c2"};
		DBApp.select("ql185", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((20%DBApp.dataPageSize))-1;
		DBApp.select("ql185", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("ql185");
	
		DBApp.select("ql185");
	
		String selectTrace0 = DBApp.getFullTrace("ql185");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 46);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 20"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("kym", cols1);
		String [][] records_kym = new String[140][cols1.length];
		for(int i=0;i<140;i++)
		{
			records_kym[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kym[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kym", records_kym[i]);
		}
	
		int pageCount1 = (int)Math.ceil(140.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("kym");
	
		DBApp.select("kym");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("kym");
	
		DBApp.select("kym");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a42"};
		DBApp.select("kym", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("kym");
	
		String[] ConditionColumns117 = {"a"};
		String[] ConditionColumnsValues117 = {"a30"};
		DBApp.select("kym", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"a"};
		String[] ConditionColumnsValues120 = {"a24"};
		DBApp.select("kym", ConditionColumns120, ConditionColumnsValues120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("kym", pageSelectPointer121,recordSelectPointer121);
	
		DBApp.select("kym");
	
		String[] ConditionColumns123 = {"a"};
		String[] ConditionColumnsValues123 = {"a132"};
		DBApp.select("kym", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"a"};
		String[] ConditionColumnsValues124 = {"a68"};
		DBApp.select("kym", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("kym");
	
		String selectTrace1 = DBApp.getFullTrace("kym");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 168);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 140"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z2", cols2);
		String [][] records_z2 = new String[290][cols2.length];
		for(int i=0;i<290;i++)
		{
			records_z2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z2", records_z2[i]);
		}
	
		int pageCount2 = (int)Math.ceil(290.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns20 = {"f"};
		String[] ConditionColumnsValues20 = {"f2"};
		DBApp.select("z2", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"e"};
		String[] ConditionColumnsValues21 = {"e4"};
		DBApp.select("z2", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("z2");
	
		String[] ConditionColumns23 = {"f","g","c"};
		String[] ConditionColumnsValues23 = {"f0","g6","c0"};
		DBApp.select("z2", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((290%DBApp.dataPageSize))-1;
		DBApp.select("z2", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("z2");
	
		DBApp.select("z2");
	
		String[] ConditionColumns27 = {"g"};
		String[] ConditionColumnsValues27 = {"g1"};
		DBApp.select("z2", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"d"};
		String[] ConditionColumnsValues28 = {"d1"};
		DBApp.select("z2", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("z2");
	
		String[] ConditionColumns210 = {"f"};
		String[] ConditionColumnsValues210 = {"f5"};
		DBApp.select("z2", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"b"};
		String[] ConditionColumnsValues211 = {"b1"};
		DBApp.select("z2", ConditionColumns211, ConditionColumnsValues211);
	
		DBApp.select("z2");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((290%DBApp.dataPageSize))-1;
		DBApp.select("z2", pageSelectPointer213,recordSelectPointer213);
	
		String selectTrace2 = DBApp.getFullTrace("z2");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 306);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 290"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rg5gv", cols3);
		String [][] records_rg5gv = new String[386][cols3.length];
		for(int i=0;i<386;i++)
		{
			records_rg5gv[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_rg5gv[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("rg5gv", records_rg5gv[i]);
		}
	
		int pageCount3 = (int)Math.ceil(386.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("rg5gv");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"j","g","n","l","h"};
		String[] ConditionColumnsValues32 = {"j5","g3","n3","l3","h7"};
		DBApp.select("rg5gv", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("rg5gv");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"g"};
		String[] ConditionColumnsValues35 = {"g6"};
		DBApp.select("rg5gv", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("rg5gv");
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"m","f","l","b","c","n","d"};
		String[] ConditionColumnsValues39 = {"m7","f5","l11","b1","c2","n3","d3"};
		DBApp.select("rg5gv", ConditionColumns39, ConditionColumnsValues39);
	
		DBApp.select("rg5gv");
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer311,recordSelectPointer311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer312,recordSelectPointer312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer313,recordSelectPointer313);
	
		String[] ConditionColumns314 = {"o","f"};
		String[] ConditionColumnsValues314 = {"o4","f4"};
		DBApp.select("rg5gv", ConditionColumns314, ConditionColumnsValues314);
	
		DBApp.select("rg5gv");
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("rg5gv", pageSelectPointer316,recordSelectPointer316);
	
		String selectTrace3 = DBApp.getFullTrace("rg5gv");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 405);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 386"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("c7", cols0);
		String [][] records_c7 = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_c7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c7", records_c7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(183.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("c7");
	
		DBApp.select("c7");
	
		DBApp.select("c7");
	
		String[] ConditionColumns03 = {"f","o","e","n","l"};
		String[] ConditionColumnsValues03 = {"f5","o11","e1","n5","l11"};
		DBApp.select("c7", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"b","o","a","e","g"};
		String[] ConditionColumnsValues05 = {"b1","o2","a17","e2","g3"};
		DBApp.select("c7", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("c7");
	
		String[] ConditionColumns07 = {"b","k","c","d"};
		String[] ConditionColumnsValues07 = {"b0","k6","c1","d2"};
		DBApp.select("c7", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"d"};
		String[] ConditionColumnsValues09 = {"d3"};
		DBApp.select("c7", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("c7");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"g","b","c","l","i","j","n"};
		String[] ConditionColumnsValues013 = {"g1","b0","c0","l6","i0","j2","n8"};
		DBApp.select("c7", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("c7");
	
		String[] ConditionColumns015 = {"c","n","i","b"};
		String[] ConditionColumnsValues015 = {"c0","n9","i3","b1"};
		DBApp.select("c7", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"h"};
		String[] ConditionColumnsValues017 = {"h5"};
		DBApp.select("c7", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("c7");
	
		String[] ConditionColumns020 = {"n","i","m","l","a"};
		String[] ConditionColumnsValues020 = {"n12","i8","m0","l2","a26"};
		DBApp.select("c7", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("c7", pageSelectPointer021,recordSelectPointer021);
	
		String[] ConditionColumns022 = {"i","h","e","m","l","c"};
		String[] ConditionColumnsValues022 = {"i4","h0","e0","m1","l4","c1"};
		DBApp.select("c7", ConditionColumns022, ConditionColumnsValues022);
	
		DBApp.select("c7");
	
		String[] ConditionColumns024 = {"i","j","h","a","k"};
		String[] ConditionColumnsValues024 = {"i8","j1","h1","a161","k7"};
		DBApp.select("c7", ConditionColumns024, ConditionColumnsValues024);
	
		String selectTrace0 = DBApp.getFullTrace("c7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 210);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 183"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("o68k8", cols1);
		String [][] records_o68k8 = new String[168][cols1.length];
		for(int i=0;i<168;i++)
		{
			records_o68k8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o68k8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o68k8", records_o68k8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(168.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("o68k8", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("o68k8");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("o68k8", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("o68k8");
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b0"};
		DBApp.select("o68k8", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("o68k8", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("o68k8", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("o68k8");
	
		DBApp.select("o68k8");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("o68k8", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b0"};
		DBApp.select("o68k8", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"a"};
		String[] ConditionColumnsValues111 = {"a16"};
		DBApp.select("o68k8", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("o68k8");
	
		String selectTrace1 = DBApp.getFullTrace("o68k8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 183);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 168"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cnp", cols2);
		String [][] records_cnp = new String[451][cols2.length];
		for(int i=0;i<451;i++)
		{
			records_cnp[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cnp[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cnp", records_cnp[i]);
		}
	
		int pageCount2 = (int)Math.ceil(451.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("cnp");
	
		DBApp.select("cnp");
	
		DBApp.select("cnp");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((451%DBApp.dataPageSize))-1;
		DBApp.select("cnp", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("cnp");
	
		String[] ConditionColumns25 = {"g"};
		String[] ConditionColumnsValues25 = {"g1"};
		DBApp.select("cnp", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"g"};
		String[] ConditionColumnsValues26 = {"g1"};
		DBApp.select("cnp", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("cnp");
	
		DBApp.select("cnp");
	
		String selectTrace2 = DBApp.getFullTrace("cnp");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 462);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 451"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("b1i", cols3);
		String [][] records_b1i = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			records_b1i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b1i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b1i", records_b1i[i]);
		}
	
		int pageCount3 = (int)Math.ceil(42.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns30 = {"b","d","a"};
		String[] ConditionColumnsValues30 = {"b1","d1","a25"};
		DBApp.select("b1i", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("b1i");
	
		DBApp.select("b1i");
	
		DBApp.select("b1i");
	
		String[] ConditionColumns34 = {"d"};
		String[] ConditionColumnsValues34 = {"d1"};
		DBApp.select("b1i", ConditionColumns34, ConditionColumnsValues34);
	
		String[] ConditionColumns35 = {"c"};
		String[] ConditionColumnsValues35 = {"c1"};
		DBApp.select("b1i", ConditionColumns35, ConditionColumnsValues35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer36,recordSelectPointer36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"f"};
		String[] ConditionColumnsValues39 = {"f0"};
		DBApp.select("b1i", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"b"};
		String[] ConditionColumnsValues310 = {"b1"};
		DBApp.select("b1i", ConditionColumns310, ConditionColumnsValues310);
	
		DBApp.select("b1i");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer312,recordSelectPointer312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer313,recordSelectPointer313);
	
		DBApp.select("b1i");
	
		DBApp.select("b1i");
	
		String[] ConditionColumns316 = {"f","e"};
		String[] ConditionColumnsValues316 = {"f2","e3"};
		DBApp.select("b1i", ConditionColumns316, ConditionColumnsValues316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("b1i", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"f"};
		String[] ConditionColumnsValues318 = {"f4"};
		DBApp.select("b1i", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"e"};
		String[] ConditionColumnsValues319 = {"e1"};
		DBApp.select("b1i", ConditionColumns319, ConditionColumnsValues319);
	
		DBApp.select("b1i");
	
		DBApp.select("b1i");
	
		String[] ConditionColumns322 = {"f"};
		String[] ConditionColumnsValues322 = {"f1"};
		DBApp.select("b1i", ConditionColumns322, ConditionColumnsValues322);
	
		String selectTrace3 = DBApp.getFullTrace("b1i");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 67);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 42"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("swv8", cols4);
		String [][] records_swv8 = new String[196][cols4.length];
		for(int i=0;i<196;i++)
		{
			records_swv8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_swv8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("swv8", records_swv8[i]);
		}
	
		int pageCount4 = (int)Math.ceil(196.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer40,recordSelectPointer40);
	
		String[] ConditionColumns41 = {"a"};
		String[] ConditionColumnsValues41 = {"a120"};
		DBApp.select("swv8", ConditionColumns41, ConditionColumnsValues41);
	
		String[] ConditionColumns42 = {"a"};
		String[] ConditionColumnsValues42 = {"a118"};
		DBApp.select("swv8", ConditionColumns42, ConditionColumnsValues42);
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer43,recordSelectPointer43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer44,recordSelectPointer44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer45,recordSelectPointer45);
	
		String[] ConditionColumns46 = {"a"};
		String[] ConditionColumnsValues46 = {"a63"};
		DBApp.select("swv8", ConditionColumns46, ConditionColumnsValues46);
	
		String[] ConditionColumns47 = {"a"};
		String[] ConditionColumnsValues47 = {"a108"};
		DBApp.select("swv8", ConditionColumns47, ConditionColumnsValues47);
	
		String[] ConditionColumns48 = {"a"};
		String[] ConditionColumnsValues48 = {"a44"};
		DBApp.select("swv8", ConditionColumns48, ConditionColumnsValues48);
	
		DBApp.select("swv8");
	
		int pageSelectPointer410 = genRandNum(pageCount4)-1;
		int recordSelectPointer410 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer410==pageCount4-1)
			recordSelectPointer410 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer410,recordSelectPointer410);
	
		String[] ConditionColumns411 = {"a"};
		String[] ConditionColumnsValues411 = {"a121"};
		DBApp.select("swv8", ConditionColumns411, ConditionColumnsValues411);
	
		DBApp.select("swv8");
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer413,recordSelectPointer413);
	
		int pageSelectPointer414 = genRandNum(pageCount4)-1;
		int recordSelectPointer414 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer414==pageCount4-1)
			recordSelectPointer414 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer414,recordSelectPointer414);
	
		String[] ConditionColumns415 = {"a"};
		String[] ConditionColumnsValues415 = {"a30"};
		DBApp.select("swv8", ConditionColumns415, ConditionColumnsValues415);
	
		DBApp.select("swv8");
	
		DBApp.select("swv8");
	
		String[] ConditionColumns418 = {"a"};
		String[] ConditionColumnsValues418 = {"a116"};
		DBApp.select("swv8", ConditionColumns418, ConditionColumnsValues418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer419,recordSelectPointer419);
	
		DBApp.select("swv8");
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer421,recordSelectPointer421);
	
		int pageSelectPointer422 = genRandNum(pageCount4)-1;
		int recordSelectPointer422 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer422==pageCount4-1)
			recordSelectPointer422 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("swv8", pageSelectPointer422,recordSelectPointer422);
	
		DBApp.select("swv8");
	
		String selectTrace4 = DBApp.getFullTrace("swv8");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 222);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 196"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vz", cols0);
		String [][] records_vz = new String[331][cols0.length];
		for(int i=0;i<331;i++)
		{
			records_vz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vz", records_vz[i]);
		}
	
		int pageCount0 = (int)Math.ceil(331.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("vz");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"b"};
		String[] ConditionColumnsValues04 = {"b1"};
		DBApp.select("vz", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"f"};
		String[] ConditionColumnsValues05 = {"f4"};
		DBApp.select("vz", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"b","g","e"};
		String[] ConditionColumnsValues08 = {"b1","g6","e1"};
		DBApp.select("vz", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"f"};
		String[] ConditionColumnsValues09 = {"f2"};
		DBApp.select("vz", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"g","d"};
		String[] ConditionColumnsValues010 = {"g6","d3"};
		DBApp.select("vz", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"d"};
		String[] ConditionColumnsValues011 = {"d2"};
		DBApp.select("vz", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("vz", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("vz");
	
		String[] ConditionColumns016 = {"b","e"};
		String[] ConditionColumnsValues016 = {"b0","e0"};
		DBApp.select("vz", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"g","e"};
		String[] ConditionColumnsValues017 = {"g2","e2"};
		DBApp.select("vz", ConditionColumns017, ConditionColumnsValues017);
	
		String selectTrace0 = DBApp.getFullTrace("vz");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 351);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 331"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w5t", cols1);
		String [][] records_w5t = new String[167][cols1.length];
		for(int i=0;i<167;i++)
		{
			records_w5t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w5t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w5t", records_w5t[i]);
		}
	
		int pageCount1 = (int)Math.ceil(167.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("w5t", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("w5t");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("w5t", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"h","f"};
		String[] ConditionColumnsValues13 = {"h6","f4"};
		DBApp.select("w5t", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("w5t");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("w5t", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("w5t", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("w5t", pageSelectPointer17,recordSelectPointer17);
	
		String selectTrace1 = DBApp.getFullTrace("w5t");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 177);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 167"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("l656", cols2);
		String [][] records_l656 = new String[126][cols2.length];
		for(int i=0;i<126;i++)
		{
			records_l656[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l656[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l656", records_l656[i]);
		}
	
		int pageCount2 = (int)Math.ceil(126.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a47"};
		DBApp.select("l656", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("l656");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((126%DBApp.dataPageSize))-1;
		DBApp.select("l656", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"d"};
		String[] ConditionColumnsValues23 = {"d2"};
		DBApp.select("l656", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"c","a"};
		String[] ConditionColumnsValues24 = {"c0","a102"};
		DBApp.select("l656", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("l656");
	
		DBApp.select("l656");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((126%DBApp.dataPageSize))-1;
		DBApp.select("l656", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("l656");
	
		String[] ConditionColumns29 = {"b"};
		String[] ConditionColumnsValues29 = {"b1"};
		DBApp.select("l656", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("l656");
	
		String selectTrace2 = DBApp.getFullTrace("l656");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 139);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 126"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p5", cols3);
		String [][] records_p5 = new String[460][cols3.length];
		for(int i=0;i<460;i++)
		{
			records_p5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p5", records_p5[i]);
		}
	
		int pageCount3 = (int)Math.ceil(460.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer30,recordSelectPointer30);
	
		DBApp.select("p5");
	
		String[] ConditionColumns32 = {"j","b","g"};
		String[] ConditionColumnsValues32 = {"j5","b1","g1"};
		DBApp.select("p5", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"c"};
		String[] ConditionColumnsValues33 = {"c1"};
		DBApp.select("p5", ConditionColumns33, ConditionColumnsValues33);
	
		DBApp.select("p5");
	
		String[] ConditionColumns35 = {"i","a","b"};
		String[] ConditionColumnsValues35 = {"i1","a235","b1"};
		DBApp.select("p5", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("p5");
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer37,recordSelectPointer37);
	
		DBApp.select("p5");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer39,recordSelectPointer39);
	
		String[] ConditionColumns310 = {"g"};
		String[] ConditionColumnsValues310 = {"g5"};
		DBApp.select("p5", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer311,recordSelectPointer311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer312,recordSelectPointer312);
	
		String[] ConditionColumns313 = {"d","k","e","f"};
		String[] ConditionColumnsValues313 = {"d0","k6","e3","f4"};
		DBApp.select("p5", ConditionColumns313, ConditionColumnsValues313);
	
		String[] ConditionColumns314 = {"k"};
		String[] ConditionColumnsValues314 = {"k3"};
		DBApp.select("p5", ConditionColumns314, ConditionColumnsValues314);
	
		String[] ConditionColumns315 = {"j"};
		String[] ConditionColumnsValues315 = {"j6"};
		DBApp.select("p5", ConditionColumns315, ConditionColumnsValues315);
	
		String[] ConditionColumns316 = {"d"};
		String[] ConditionColumnsValues316 = {"d2"};
		DBApp.select("p5", ConditionColumns316, ConditionColumnsValues316);
	
		DBApp.select("p5");
	
		DBApp.select("p5");
	
		int pageSelectPointer319 = genRandNum(pageCount3)-1;
		int recordSelectPointer319 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer319==pageCount3-1)
			recordSelectPointer319 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer319,recordSelectPointer319);
	
		String[] ConditionColumns320 = {"l"};
		String[] ConditionColumnsValues320 = {"l3"};
		DBApp.select("p5", ConditionColumns320, ConditionColumnsValues320);
	
		int pageSelectPointer321 = genRandNum(pageCount3)-1;
		int recordSelectPointer321 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer321==pageCount3-1)
			recordSelectPointer321 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer321,recordSelectPointer321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer322,recordSelectPointer322);
	
		DBApp.select("p5");
	
		int pageSelectPointer324 = genRandNum(pageCount3)-1;
		int recordSelectPointer324 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer324==pageCount3-1)
			recordSelectPointer324 = genRandNum((460%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer324,recordSelectPointer324);
	
		DBApp.select("p5");
	
		String selectTrace3 = DBApp.getFullTrace("p5");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 488);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 460"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("n2r9j", cols4);
		String [][] records_n2r9j = new String[286][cols4.length];
		for(int i=0;i<286;i++)
		{
			records_n2r9j[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_n2r9j[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("n2r9j", records_n2r9j[i]);
		}
	
		int pageCount4 = (int)Math.ceil(286.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("n2r9j");
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer41,recordSelectPointer41);
	
		String[] ConditionColumns42 = {"c"};
		String[] ConditionColumnsValues42 = {"c0"};
		DBApp.select("n2r9j", ConditionColumns42, ConditionColumnsValues42);
	
		DBApp.select("n2r9j");
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer44,recordSelectPointer44);
	
		String[] ConditionColumns45 = {"c"};
		String[] ConditionColumnsValues45 = {"c2"};
		DBApp.select("n2r9j", ConditionColumns45, ConditionColumnsValues45);
	
		int pageSelectPointer46 = genRandNum(pageCount4)-1;
		int recordSelectPointer46 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer46==pageCount4-1)
			recordSelectPointer46 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer46,recordSelectPointer46);
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer47,recordSelectPointer47);
	
		String[] ConditionColumns48 = {"a"};
		String[] ConditionColumnsValues48 = {"a123"};
		DBApp.select("n2r9j", ConditionColumns48, ConditionColumnsValues48);
	
		DBApp.select("n2r9j");
	
		String[] ConditionColumns410 = {"c"};
		String[] ConditionColumnsValues410 = {"c2"};
		DBApp.select("n2r9j", ConditionColumns410, ConditionColumnsValues410);
	
		DBApp.select("n2r9j");
	
		String[] ConditionColumns412 = {"c"};
		String[] ConditionColumnsValues412 = {"c0"};
		DBApp.select("n2r9j", ConditionColumns412, ConditionColumnsValues412);
	
		String[] ConditionColumns413 = {"b"};
		String[] ConditionColumnsValues413 = {"b0"};
		DBApp.select("n2r9j", ConditionColumns413, ConditionColumnsValues413);
	
		String[] ConditionColumns414 = {"a"};
		String[] ConditionColumnsValues414 = {"a119"};
		DBApp.select("n2r9j", ConditionColumns414, ConditionColumnsValues414);
	
		DBApp.select("n2r9j");
	
		String[] ConditionColumns416 = {"b"};
		String[] ConditionColumnsValues416 = {"b0"};
		DBApp.select("n2r9j", ConditionColumns416, ConditionColumnsValues416);
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer417,recordSelectPointer417);
	
		int pageSelectPointer418 = genRandNum(pageCount4)-1;
		int recordSelectPointer418 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer418==pageCount4-1)
			recordSelectPointer418 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer418,recordSelectPointer418);
	
		String[] ConditionColumns419 = {"b"};
		String[] ConditionColumnsValues419 = {"b0"};
		DBApp.select("n2r9j", ConditionColumns419, ConditionColumnsValues419);
	
		String[] ConditionColumns420 = {"c"};
		String[] ConditionColumnsValues420 = {"c0"};
		DBApp.select("n2r9j", ConditionColumns420, ConditionColumnsValues420);
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer421,recordSelectPointer421);
	
		DBApp.select("n2r9j");
	
		DBApp.select("n2r9j");
	
		int pageSelectPointer424 = genRandNum(pageCount4)-1;
		int recordSelectPointer424 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer424==pageCount4-1)
			recordSelectPointer424 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("n2r9j", pageSelectPointer424,recordSelectPointer424);
	
		String[] ConditionColumns425 = {"c"};
		String[] ConditionColumnsValues425 = {"c0"};
		DBApp.select("n2r9j", ConditionColumns425, ConditionColumnsValues425);
	
		String[] ConditionColumns426 = {"c"};
		String[] ConditionColumnsValues426 = {"c0"};
		DBApp.select("n2r9j", ConditionColumns426, ConditionColumnsValues426);
	
		String[] ConditionColumns427 = {"a"};
		String[] ConditionColumnsValues427 = {"a268"};
		DBApp.select("n2r9j", ConditionColumns427, ConditionColumnsValues427);
	
		DBApp.select("n2r9j");
	
		String selectTrace4 = DBApp.getFullTrace("n2r9j");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 317);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 286"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lkitc", cols0);
		String [][] records_lkitc = new String[403][cols0.length];
		for(int i=0;i<403;i++)
		{
			records_lkitc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lkitc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lkitc", records_lkitc[i]);
		}
	
		int pageCount0 = (int)Math.ceil(403.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((403%DBApp.dataPageSize))-1;
		DBApp.select("lkitc", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((403%DBApp.dataPageSize))-1;
		DBApp.select("lkitc", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"f"};
		String[] ConditionColumnsValues05 = {"f4"};
		DBApp.select("lkitc", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"e","f","b"};
		String[] ConditionColumnsValues06 = {"e0","f4","b0"};
		DBApp.select("lkitc", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"b"};
		String[] ConditionColumnsValues07 = {"b0"};
		DBApp.select("lkitc", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b0"};
		DBApp.select("lkitc", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"f"};
		String[] ConditionColumnsValues013 = {"f5"};
		DBApp.select("lkitc", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("lkitc");
	
		String[] ConditionColumns015 = {"d","e","c"};
		String[] ConditionColumnsValues015 = {"d0","e3","c0"};
		DBApp.select("lkitc", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("lkitc");
	
		String[] ConditionColumns017 = {"e"};
		String[] ConditionColumnsValues017 = {"e2"};
		DBApp.select("lkitc", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("lkitc");
	
		DBApp.select("lkitc");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((403%DBApp.dataPageSize))-1;
		DBApp.select("lkitc", pageSelectPointer020,recordSelectPointer020);
	
		String selectTrace0 = DBApp.getFullTrace("lkitc");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 426);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 403"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b97yx", cols1);
		String [][] records_b97yx = new String[167][cols1.length];
		for(int i=0;i<167;i++)
		{
			records_b97yx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b97yx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b97yx", records_b97yx[i]);
		}
	
		int pageCount1 = (int)Math.ceil(167.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("b97yx");
	
		String[] ConditionColumns11 = {"e"};
		String[] ConditionColumnsValues11 = {"e1"};
		DBApp.select("b97yx", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("b97yx");
	
		String[] ConditionColumns13 = {"e","f"};
		String[] ConditionColumnsValues13 = {"e2","f4"};
		DBApp.select("b97yx", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("b97yx");
	
		String[] ConditionColumns15 = {"g"};
		String[] ConditionColumnsValues15 = {"g6"};
		DBApp.select("b97yx", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("b97yx", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("b97yx");
	
		String[] ConditionColumns18 = {"a","b"};
		String[] ConditionColumnsValues18 = {"a138","b0"};
		DBApp.select("b97yx", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("b97yx");
	
		String[] ConditionColumns110 = {"c","f","h","g"};
		String[] ConditionColumnsValues110 = {"c0","f0","h6","g0"};
		DBApp.select("b97yx", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"d"};
		String[] ConditionColumnsValues111 = {"d0"};
		DBApp.select("b97yx", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("b97yx", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("b97yx", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"e"};
		String[] ConditionColumnsValues114 = {"e4"};
		DBApp.select("b97yx", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((167%DBApp.dataPageSize))-1;
		DBApp.select("b97yx", pageSelectPointer115,recordSelectPointer115);
	
		String selectTrace1 = DBApp.getFullTrace("b97yx");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 185);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 167"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("t3", cols2);
		String [][] records_t3 = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			records_t3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t3", records_t3[i]);
		}
	
		int pageCount2 = (int)Math.ceil(45.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("t3");
	
		String[] ConditionColumns21 = {"c"};
		String[] ConditionColumnsValues21 = {"c1"};
		DBApp.select("t3", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("t3");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("t3");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"b"};
		String[] ConditionColumnsValues28 = {"b1"};
		DBApp.select("t3", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"c"};
		String[] ConditionColumnsValues29 = {"c2"};
		DBApp.select("t3", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("t3");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("t3");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer214,recordSelectPointer214);
	
		DBApp.select("t3");
	
		DBApp.select("t3");
	
		String[] ConditionColumns217 = {"b"};
		String[] ConditionColumnsValues217 = {"b1"};
		DBApp.select("t3", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("t3");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("t3", pageSelectPointer219,recordSelectPointer219);
	
		String selectTrace2 = DBApp.getFullTrace("t3");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 67);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 45"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ap6", cols3);
		String [][] records_ap6 = new String[421][cols3.length];
		for(int i=0;i<421;i++)
		{
			records_ap6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ap6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ap6", records_ap6[i]);
		}
	
		int pageCount3 = (int)Math.ceil(421.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns30 = {"f"};
		String[] ConditionColumnsValues30 = {"f0"};
		DBApp.select("ap6", ConditionColumns30, ConditionColumnsValues30);
	
		String selectTrace3 = DBApp.getFullTrace("ap6");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 424);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 421"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dw", cols4);
		String [][] records_dw = new String[391][cols4.length];
		for(int i=0;i<391;i++)
		{
			records_dw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dw", records_dw[i]);
		}
	
		int pageCount4 = (int)Math.ceil(391.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("dw");
	
		DBApp.select("dw");
	
		DBApp.select("dw");
	
		String[] ConditionColumns43 = {"l"};
		String[] ConditionColumnsValues43 = {"l0"};
		DBApp.select("dw", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"c","i","b","g","e","f"};
		String[] ConditionColumnsValues44 = {"c1","i7","b1","g0","e4","f1"};
		DBApp.select("dw", ConditionColumns44, ConditionColumnsValues44);
	
		String[] ConditionColumns45 = {"h","f","m","k"};
		String[] ConditionColumnsValues45 = {"h2","f0","m7","k9"};
		DBApp.select("dw", ConditionColumns45, ConditionColumnsValues45);
	
		DBApp.select("dw");
	
		String[] ConditionColumns47 = {"i"};
		String[] ConditionColumnsValues47 = {"i1"};
		DBApp.select("dw", ConditionColumns47, ConditionColumnsValues47);
	
		DBApp.select("dw");
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("dw", pageSelectPointer49,recordSelectPointer49);
	
		String[] ConditionColumns410 = {"h","f","k","i","l"};
		String[] ConditionColumnsValues410 = {"h7","f3","k2","i3","l3"};
		DBApp.select("dw", ConditionColumns410, ConditionColumnsValues410);
	
		DBApp.select("dw");
	
		String[] ConditionColumns412 = {"k"};
		String[] ConditionColumnsValues412 = {"k8"};
		DBApp.select("dw", ConditionColumns412, ConditionColumnsValues412);
	
		DBApp.select("dw");
	
		String[] ConditionColumns414 = {"c"};
		String[] ConditionColumnsValues414 = {"c1"};
		DBApp.select("dw", ConditionColumns414, ConditionColumnsValues414);
	
		String[] ConditionColumns415 = {"m"};
		String[] ConditionColumnsValues415 = {"m6"};
		DBApp.select("dw", ConditionColumns415, ConditionColumnsValues415);
	
		DBApp.select("dw");
	
		String selectTrace4 = DBApp.getFullTrace("dw");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 410);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 391"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("nxp", cols0);
		String [][] records_nxp = new String[216][cols0.length];
		for(int i=0;i<216;i++)
		{
			records_nxp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nxp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nxp", records_nxp[i]);
		}
	
		int pageCount0 = (int)Math.ceil(216.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		String[] ConditionColumns00 = {"g","h"};
		String[] ConditionColumnsValues00 = {"g6","h1"};
		DBApp.select("nxp", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"g","f","e"};
		String[] ConditionColumnsValues01 = {"g3","f1","e0"};
		DBApp.select("nxp", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"f"};
		String[] ConditionColumnsValues02 = {"f4"};
		DBApp.select("nxp", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a101"};
		DBApp.select("nxp", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"h"};
		String[] ConditionColumnsValues06 = {"h2"};
		DBApp.select("nxp", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"h"};
		String[] ConditionColumnsValues08 = {"h3"};
		DBApp.select("nxp", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b","d","e","a"};
		String[] ConditionColumnsValues010 = {"b0","d0","e3","a48"};
		DBApp.select("nxp", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("nxp");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a7"};
		DBApp.select("nxp", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("nxp");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((216%DBApp.dataPageSize))-1;
		DBApp.select("nxp", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("nxp");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 235);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 216"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("x22", cols1);
		String [][] records_x22 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			records_x22[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x22[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x22", records_x22[i]);
		}
	
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("x22", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("x22", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("x22");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("x22", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("x22", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("x22", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("x22");
	
		String selectTrace1 = DBApp.getFullTrace("x22");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 101);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 92"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qdfu", cols2);
		String [][] records_qdfu = new String[490][cols2.length];
		for(int i=0;i<490;i++)
		{
			records_qdfu[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qdfu[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qdfu", records_qdfu[i]);
		}
	
		int pageCount2 = (int)Math.ceil(490.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("qdfu", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("qdfu", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("qdfu", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"c","j"};
		String[] ConditionColumnsValues23 = {"c1","j1"};
		DBApp.select("qdfu", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("qdfu", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"c"};
		String[] ConditionColumnsValues25 = {"c1"};
		DBApp.select("qdfu", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"b","d","g","k","f"};
		String[] ConditionColumnsValues26 = {"b1","d3","g5","k9","f3"};
		DBApp.select("qdfu", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("qdfu", pageSelectPointer27,recordSelectPointer27);
	
		String selectTrace2 = DBApp.getFullTrace("qdfu");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 500);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 490"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ia", cols3);
		String [][] records_ia = new String[355][cols3.length];
		for(int i=0;i<355;i++)
		{
			records_ia[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ia[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ia", records_ia[i]);
		}
	
		int pageCount3 = (int)Math.ceil(355.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns30 = {"b"};
		String[] ConditionColumnsValues30 = {"b1"};
		DBApp.select("ia", ConditionColumns30, ConditionColumnsValues30);
	
		String[] ConditionColumns31 = {"h"};
		String[] ConditionColumnsValues31 = {"h6"};
		DBApp.select("ia", ConditionColumns31, ConditionColumnsValues31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((355%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer32,recordSelectPointer32);
	
		String[] ConditionColumns33 = {"g"};
		String[] ConditionColumnsValues33 = {"g6"};
		DBApp.select("ia", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((355%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"a","g","b","f"};
		String[] ConditionColumnsValues35 = {"a177","g2","b1","f3"};
		DBApp.select("ia", ConditionColumns35, ConditionColumnsValues35);
	
		String selectTrace3 = DBApp.getFullTrace("ia");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 363);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 355"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("zb6", cols0);
		String [][] records_zb6 = new String[418][cols0.length];
		for(int i=0;i<418;i++)
		{
			records_zb6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zb6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zb6", records_zb6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(418.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("zb6");
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("zb6", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"b"};
		String[] ConditionColumnsValues02 = {"b1"};
		DBApp.select("zb6", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("zb6");
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a370"};
		DBApp.select("zb6", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"b"};
		String[] ConditionColumnsValues05 = {"b1"};
		DBApp.select("zb6", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b0"};
		DBApp.select("zb6", ConditionColumns06, ConditionColumnsValues06);
	
		String selectTrace0 = DBApp.getFullTrace("zb6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 427);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 418"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u4pfe", cols1);
		String [][] records_u4pfe = new String[214][cols1.length];
		for(int i=0;i<214;i++)
		{
			records_u4pfe[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u4pfe[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u4pfe", records_u4pfe[i]);
		}
	
		int pageCount1 = (int)Math.ceil(214.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("u4pfe");
	
		String[] ConditionColumns11 = {"c","a"};
		String[] ConditionColumnsValues11 = {"c2","a200"};
		DBApp.select("u4pfe", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"e","c"};
		String[] ConditionColumnsValues13 = {"e1","c0"};
		DBApp.select("u4pfe", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"c"};
		String[] ConditionColumnsValues15 = {"c2"};
		DBApp.select("u4pfe", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("u4pfe");
	
		String[] ConditionColumns17 = {"d"};
		String[] ConditionColumnsValues17 = {"d2"};
		DBApp.select("u4pfe", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b0"};
		DBApp.select("u4pfe", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c0"};
		DBApp.select("u4pfe", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("u4pfe");
	
		String[] ConditionColumns113 = {"d"};
		String[] ConditionColumnsValues113 = {"d1"};
		DBApp.select("u4pfe", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("u4pfe");
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d1"};
		DBApp.select("u4pfe", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"g"};
		String[] ConditionColumnsValues116 = {"g2"};
		DBApp.select("u4pfe", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"e","g","f"};
		String[] ConditionColumnsValues117 = {"e2","g6","f0"};
		DBApp.select("u4pfe", ConditionColumns117, ConditionColumnsValues117);
	
		DBApp.select("u4pfe");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"c","e","b"};
		String[] ConditionColumnsValues120 = {"c1","e3","b1"};
		DBApp.select("u4pfe", ConditionColumns120, ConditionColumnsValues120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer121,recordSelectPointer121);
	
		DBApp.select("u4pfe");
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer123,recordSelectPointer123);
	
		int pageSelectPointer124 = genRandNum(pageCount1)-1;
		int recordSelectPointer124 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer124==pageCount1-1)
			recordSelectPointer124 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("u4pfe", pageSelectPointer124,recordSelectPointer124);
	
		String selectTrace1 = DBApp.getFullTrace("u4pfe");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 241);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 214"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h37um", cols0);
		String [][] records_h37um = new String[371][cols0.length];
		for(int i=0;i<371;i++)
		{
			records_h37um[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h37um[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h37um", records_h37um[i]);
		}
	
		int pageCount0 = (int)Math.ceil(371.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("h37um");
	
		DBApp.select("h37um");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((371%DBApp.dataPageSize))-1;
		DBApp.select("h37um", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"d","c","a"};
		String[] ConditionColumnsValues03 = {"d3","c0","a351"};
		DBApp.select("h37um", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("h37um");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((371%DBApp.dataPageSize))-1;
		DBApp.select("h37um", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("h37um");
	
		String[] ConditionColumns07 = {"d"};
		String[] ConditionColumnsValues07 = {"d1"};
		DBApp.select("h37um", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"g","b","c"};
		String[] ConditionColumnsValues08 = {"g0","b0","c1"};
		DBApp.select("h37um", ConditionColumns08, ConditionColumnsValues08);
	
		String selectTrace0 = DBApp.getFullTrace("h37um");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 382);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 371"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("b33", cols1);
		String [][] records_b33 = new String[369][cols1.length];
		for(int i=0;i<369;i++)
		{
			records_b33[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b33[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b33", records_b33[i]);
		}
	
		int pageCount1 = (int)Math.ceil(369.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("b33");
	
		DBApp.select("b33");
	
		String[] ConditionColumns12 = {"i","g","c","b","d"};
		String[] ConditionColumnsValues12 = {"i8","g6","c2","b1","d1"};
		DBApp.select("b33", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("b33");
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c1"};
		DBApp.select("b33", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("b33", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("b33", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"j"};
		String[] ConditionColumnsValues17 = {"j2"};
		DBApp.select("b33", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("b33");
	
		String[] ConditionColumns19 = {"e","j","l","c"};
		String[] ConditionColumnsValues19 = {"e0","j5","l1","c1"};
		DBApp.select("b33", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"j","f","h","l","m","i"};
		String[] ConditionColumnsValues110 = {"j4","f2","h6","l2","m7","i2"};
		DBApp.select("b33", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"m"};
		String[] ConditionColumnsValues111 = {"m9"};
		DBApp.select("b33", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("b33");
	
		String[] ConditionColumns113 = {"b","c","g","m","j","d"};
		String[] ConditionColumnsValues113 = {"b1","c2","g3","m10","j1","d1"};
		DBApp.select("b33", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"l"};
		String[] ConditionColumnsValues114 = {"l3"};
		DBApp.select("b33", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"m"};
		String[] ConditionColumnsValues115 = {"m0"};
		DBApp.select("b33", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("b33", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("b33");
	
		String[] ConditionColumns118 = {"m","k"};
		String[] ConditionColumnsValues118 = {"m12","k9"};
		DBApp.select("b33", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"e"};
		String[] ConditionColumnsValues119 = {"e0"};
		DBApp.select("b33", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("b33");
	
		DBApp.select("b33");
	
		String[] ConditionColumns122 = {"g","d","i"};
		String[] ConditionColumnsValues122 = {"g6","d2","i2"};
		DBApp.select("b33", ConditionColumns122, ConditionColumnsValues122);
	
		String selectTrace1 = DBApp.getFullTrace("b33");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 394);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 369"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("l6l97", cols2);
		String [][] records_l6l97 = new String[373][cols2.length];
		for(int i=0;i<373;i++)
		{
			records_l6l97[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l6l97[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l6l97", records_l6l97[i]);
		}
	
		int pageCount2 = (int)Math.ceil(373.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("l6l97");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("l6l97");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"b"};
		String[] ConditionColumnsValues25 = {"b1"};
		DBApp.select("l6l97", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"a"};
		String[] ConditionColumnsValues26 = {"a271"};
		DBApp.select("l6l97", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("l6l97");
	
		String[] ConditionColumns28 = {"b"};
		String[] ConditionColumnsValues28 = {"b1"};
		DBApp.select("l6l97", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("l6l97");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("l6l97");
	
		String[] ConditionColumns212 = {"b"};
		String[] ConditionColumnsValues212 = {"b0"};
		DBApp.select("l6l97", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("l6l97");
	
		String[] ConditionColumns214 = {"a"};
		String[] ConditionColumnsValues214 = {"a178"};
		DBApp.select("l6l97", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"b"};
		String[] ConditionColumnsValues215 = {"b1"};
		DBApp.select("l6l97", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"a"};
		String[] ConditionColumnsValues216 = {"a156"};
		DBApp.select("l6l97", ConditionColumns216, ConditionColumnsValues216);
	
		DBApp.select("l6l97");
	
		DBApp.select("l6l97");
	
		String[] ConditionColumns219 = {"a"};
		String[] ConditionColumnsValues219 = {"a205"};
		DBApp.select("l6l97", ConditionColumns219, ConditionColumnsValues219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer220,recordSelectPointer220);
	
		String[] ConditionColumns221 = {"b"};
		String[] ConditionColumnsValues221 = {"b0"};
		DBApp.select("l6l97", ConditionColumns221, ConditionColumnsValues221);
	
		String[] ConditionColumns222 = {"b"};
		String[] ConditionColumnsValues222 = {"b0"};
		DBApp.select("l6l97", ConditionColumns222, ConditionColumnsValues222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer223,recordSelectPointer223);
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer224,recordSelectPointer224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("l6l97", pageSelectPointer225,recordSelectPointer225);
	
		String selectTrace2 = DBApp.getFullTrace("l6l97");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 401);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 373"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("hp", cols0);
		String [][] records_hp = new String[375][cols0.length];
		for(int i=0;i<375;i++)
		{
			records_hp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hp", records_hp[i]);
		}
	
		int pageCount0 = (int)Math.ceil(375.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c2"};
		DBApp.select("hp", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("hp");
	
		DBApp.select("hp");
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d0"};
		DBApp.select("hp", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("hp");
	
		String[] ConditionColumns07 = {"d"};
		String[] ConditionColumnsValues07 = {"d1"};
		DBApp.select("hp", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"c"};
		String[] ConditionColumnsValues08 = {"c1"};
		DBApp.select("hp", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b0"};
		DBApp.select("hp", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"b","a"};
		String[] ConditionColumnsValues011 = {"b1","a355"};
		DBApp.select("hp", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("hp");
	
		String[] ConditionColumns016 = {"c"};
		String[] ConditionColumnsValues016 = {"c2"};
		DBApp.select("hp", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("hp");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((375%DBApp.dataPageSize))-1;
		DBApp.select("hp", pageSelectPointer018,recordSelectPointer018);
	
		String selectTrace0 = DBApp.getFullTrace("hp");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 396);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 375"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("sg2", cols1);
		String [][] records_sg2 = new String[343][cols1.length];
		for(int i=0;i<343;i++)
		{
			records_sg2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sg2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sg2", records_sg2[i]);
		}
	
		int pageCount1 = (int)Math.ceil(343.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"e","c","g"};
		String[] ConditionColumnsValues11 = {"e0","c2","g2"};
		DBApp.select("sg2", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("sg2");
	
		DBApp.select("sg2");
	
		String[] ConditionColumns14 = {"c","b","h"};
		String[] ConditionColumnsValues14 = {"c2","b0","h2"};
		DBApp.select("sg2", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("sg2");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"f"};
		String[] ConditionColumnsValues19 = {"f3"};
		DBApp.select("sg2", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("sg2");
	
		String[] ConditionColumns113 = {"i"};
		String[] ConditionColumnsValues113 = {"i8"};
		DBApp.select("sg2", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("sg2");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer117,recordSelectPointer117);
	
		String[] ConditionColumns118 = {"b"};
		String[] ConditionColumnsValues118 = {"b1"};
		DBApp.select("sg2", ConditionColumns118, ConditionColumnsValues118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer119,recordSelectPointer119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("sg2", pageSelectPointer121,recordSelectPointer121);
	
		String[] ConditionColumns122 = {"i","f","g"};
		String[] ConditionColumnsValues122 = {"i0","f0","g3"};
		DBApp.select("sg2", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"b"};
		String[] ConditionColumnsValues123 = {"b1"};
		DBApp.select("sg2", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("sg2");
	
		String selectTrace1 = DBApp.getFullTrace("sg2");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 370);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 343"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q3", cols2);
		String [][] records_q3 = new String[191][cols2.length];
		for(int i=0;i<191;i++)
		{
			records_q3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q3", records_q3[i]);
		}
	
		int pageCount2 = (int)Math.ceil(191.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("q3");
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b0"};
		DBApp.select("q3", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("q3");
	
		DBApp.select("q3");
	
		DBApp.select("q3");
	
		String[] ConditionColumns26 = {"e"};
		String[] ConditionColumnsValues26 = {"e0"};
		DBApp.select("q3", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("q3");
	
		String[] ConditionColumns28 = {"f"};
		String[] ConditionColumnsValues28 = {"f5"};
		DBApp.select("q3", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("q3");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("q3");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer215,recordSelectPointer215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer216,recordSelectPointer216);
	
		String[] ConditionColumns217 = {"f"};
		String[] ConditionColumnsValues217 = {"f4"};
		DBApp.select("q3", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("q3");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer219,recordSelectPointer219);
	
		DBApp.select("q3");
	
		DBApp.select("q3");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer222,recordSelectPointer222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer223,recordSelectPointer223);
	
		DBApp.select("q3");
	
		DBApp.select("q3");
	
		String[] ConditionColumns226 = {"f","a","c"};
		String[] ConditionColumnsValues226 = {"f1","a121","c1"};
		DBApp.select("q3", ConditionColumns226, ConditionColumnsValues226);
	
		DBApp.select("q3");
	
		int pageSelectPointer228 = genRandNum(pageCount2)-1;
		int recordSelectPointer228 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer228==pageCount2-1)
			recordSelectPointer228 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("q3", pageSelectPointer228,recordSelectPointer228);
	
		String selectTrace2 = DBApp.getFullTrace("q3");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 222);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 191"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j5", cols3);
		String [][] records_j5 = new String[412][cols3.length];
		for(int i=0;i<412;i++)
		{
			records_j5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j5", records_j5[i]);
		}
	
		int pageCount3 = (int)Math.ceil(412.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("j5", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"a"};
		String[] ConditionColumnsValues31 = {"a345"};
		DBApp.select("j5", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("j5");
	
		String[] ConditionColumns33 = {"f"};
		String[] ConditionColumnsValues33 = {"f4"};
		DBApp.select("j5", ConditionColumns33, ConditionColumnsValues33);
	
		String selectTrace3 = DBApp.getFullTrace("j5");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 418);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 412"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("np", cols0);
		String [][] records_np = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			records_np[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_np[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("np", records_np[i]);
		}
	
		int pageCount0 = (int)Math.ceil(54.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("np");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("np");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"e","f","h","c"};
		String[] ConditionColumnsValues09 = {"e1","f1","h7","c1"};
		DBApp.select("np", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("np");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("np");
	
		DBApp.select("np");
	
		DBApp.select("np");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("np");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer020,recordSelectPointer020);
	
		String[] ConditionColumns021 = {"j"};
		String[] ConditionColumnsValues021 = {"j4"};
		DBApp.select("np", ConditionColumns021, ConditionColumnsValues021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((54%DBApp.dataPageSize))-1;
		DBApp.select("np", pageSelectPointer023,recordSelectPointer023);
	
		String[] ConditionColumns024 = {"c","i"};
		String[] ConditionColumnsValues024 = {"c0","i6"};
		DBApp.select("np", ConditionColumns024, ConditionColumnsValues024);
	
		DBApp.select("np");
	
		DBApp.select("np");
	
		String selectTrace0 = DBApp.getFullTrace("np");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 83);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 54"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o87", cols1);
		String [][] records_o87 = new String[122][cols1.length];
		for(int i=0;i<122;i++)
		{
			records_o87[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o87[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o87", records_o87[i]);
		}
	
		int pageCount1 = (int)Math.ceil(122.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns10 = {"f"};
		String[] ConditionColumnsValues10 = {"f2"};
		DBApp.select("o87", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("o87");
	
		String[] ConditionColumns13 = {"d"};
		String[] ConditionColumnsValues13 = {"d1"};
		DBApp.select("o87", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c","e","a","g"};
		String[] ConditionColumnsValues14 = {"c1","e4","a109","g4"};
		DBApp.select("o87", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"g"};
		String[] ConditionColumnsValues15 = {"g4"};
		DBApp.select("o87", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"h","f","a"};
		String[] ConditionColumnsValues16 = {"h5","f1","a85"};
		DBApp.select("o87", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("o87");
	
		String[] ConditionColumns19 = {"h"};
		String[] ConditionColumnsValues19 = {"h7"};
		DBApp.select("o87", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("o87");
	
		DBApp.select("o87");
	
		DBApp.select("o87");
	
		String[] ConditionColumns113 = {"g"};
		String[] ConditionColumnsValues113 = {"g1"};
		DBApp.select("o87", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"f"};
		String[] ConditionColumnsValues114 = {"f1"};
		DBApp.select("o87", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer117,recordSelectPointer117);
	
		String[] ConditionColumns118 = {"g","c","h"};
		String[] ConditionColumnsValues118 = {"g0","c1","h7"};
		DBApp.select("o87", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"b"};
		String[] ConditionColumnsValues119 = {"b1"};
		DBApp.select("o87", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer120,recordSelectPointer120);
	
		DBApp.select("o87");
	
		String[] ConditionColumns122 = {"c"};
		String[] ConditionColumnsValues122 = {"c2"};
		DBApp.select("o87", ConditionColumns122, ConditionColumnsValues122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer123,recordSelectPointer123);
	
		int pageSelectPointer124 = genRandNum(pageCount1)-1;
		int recordSelectPointer124 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer124==pageCount1-1)
			recordSelectPointer124 = genRandNum((122%DBApp.dataPageSize))-1;
		DBApp.select("o87", pageSelectPointer124,recordSelectPointer124);
	
		String selectTrace1 = DBApp.getFullTrace("o87");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 149);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 122"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w1", cols2);
		String [][] records_w1 = new String[238][cols2.length];
		for(int i=0;i<238;i++)
		{
			records_w1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w1", records_w1[i]);
		}
	
		int pageCount2 = (int)Math.ceil(238.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns20 = {"i"};
		String[] ConditionColumnsValues20 = {"i0"};
		DBApp.select("w1", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("w1");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"h","k","n","a","g","c"};
		String[] ConditionColumnsValues23 = {"h7","k6","n1","a127","g1","c1"};
		DBApp.select("w1", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"o"};
		String[] ConditionColumnsValues26 = {"o7"};
		DBApp.select("w1", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"m"};
		String[] ConditionColumnsValues28 = {"m8"};
		DBApp.select("w1", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"j","g","o","m","i","k","n"};
		String[] ConditionColumnsValues29 = {"j7","g6","o2","m11","i5","k2","n13"};
		DBApp.select("w1", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"g"};
		String[] ConditionColumnsValues211 = {"g1"};
		DBApp.select("w1", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"f","d","m","i","l","b"};
		String[] ConditionColumnsValues212 = {"f3","d1","m3","i0","l9","b1"};
		DBApp.select("w1", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("w1");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("w1");
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer217,recordSelectPointer217);
	
		DBApp.select("w1");
	
		String[] ConditionColumns219 = {"f","j","n","i"};
		String[] ConditionColumnsValues219 = {"f5","j3","n3","i8"};
		DBApp.select("w1", ConditionColumns219, ConditionColumnsValues219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer220,recordSelectPointer220);
	
		DBApp.select("w1");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("w1");
	
		String[] ConditionColumns224 = {"m","h","j","d","i"};
		String[] ConditionColumnsValues224 = {"m6","h7","j1","d3","i8"};
		DBApp.select("w1", ConditionColumns224, ConditionColumnsValues224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("w1", pageSelectPointer225,recordSelectPointer225);
	
		String[] ConditionColumns226 = {"c"};
		String[] ConditionColumnsValues226 = {"c1"};
		DBApp.select("w1", ConditionColumns226, ConditionColumnsValues226);
	
		String[] ConditionColumns227 = {"e"};
		String[] ConditionColumnsValues227 = {"e2"};
		DBApp.select("w1", ConditionColumns227, ConditionColumnsValues227);
	
		String selectTrace2 = DBApp.getFullTrace("w1");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 268);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 238"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n8p", cols3);
		String [][] records_n8p = new String[456][cols3.length];
		for(int i=0;i<456;i++)
		{
			records_n8p[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n8p[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n8p", records_n8p[i]);
		}
	
		int pageCount3 = (int)Math.ceil(456.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		String[] ConditionColumns30 = {"k","g","f"};
		String[] ConditionColumnsValues30 = {"k9","g1","f1"};
		DBApp.select("n8p", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("n8p");
	
		String[] ConditionColumns32 = {"m"};
		String[] ConditionColumnsValues32 = {"m6"};
		DBApp.select("n8p", ConditionColumns32, ConditionColumnsValues32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("n8p", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("n8p", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"m"};
		String[] ConditionColumnsValues35 = {"m7"};
		DBApp.select("n8p", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"l"};
		String[] ConditionColumnsValues36 = {"l9"};
		DBApp.select("n8p", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("n8p");
	
		String[] ConditionColumns38 = {"a"};
		String[] ConditionColumnsValues38 = {"a55"};
		DBApp.select("n8p", ConditionColumns38, ConditionColumnsValues38);
	
		String[] ConditionColumns39 = {"a","k"};
		String[] ConditionColumnsValues39 = {"a144","k1"};
		DBApp.select("n8p", ConditionColumns39, ConditionColumnsValues39);
	
		DBApp.select("n8p");
	
		DBApp.select("n8p");
	
		String selectTrace3 = DBApp.getFullTrace("n8p");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 470);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 456"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("ba", cols0);
		String [][] records_ba = new String[282][cols0.length];
		for(int i=0;i<282;i++)
		{
			records_ba[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ba[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ba", records_ba[i]);
		}
	
		int pageCount0 = (int)Math.ceil(282.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		DBApp.select("ba");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((282%DBApp.dataPageSize))-1;
		DBApp.select("ba", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("ba");
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a124"};
		DBApp.select("ba", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("ba");
	
		DBApp.select("ba");
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a73"};
		DBApp.select("ba", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a94"};
		DBApp.select("ba", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a42"};
		DBApp.select("ba", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((282%DBApp.dataPageSize))-1;
		DBApp.select("ba", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("ba");
	
		String selectTrace0 = DBApp.getFullTrace("ba");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 295);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 282"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c11", cols1);
		String [][] records_c11 = new String[299][cols1.length];
		for(int i=0;i<299;i++)
		{
			records_c11[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c11[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c11", records_c11[i]);
		}
	
		int pageCount1 = (int)Math.ceil(299.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		String[] ConditionColumns10 = {"l"};
		String[] ConditionColumnsValues10 = {"l7"};
		DBApp.select("c11", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"g"};
		String[] ConditionColumnsValues11 = {"g3"};
		DBApp.select("c11", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("c11");
	
		String[] ConditionColumns13 = {"f","l","k","m"};
		String[] ConditionColumnsValues13 = {"f3","l9","k8","m12"};
		DBApp.select("c11", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((299%DBApp.dataPageSize))-1;
		DBApp.select("c11", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((299%DBApp.dataPageSize))-1;
		DBApp.select("c11", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"j"};
		String[] ConditionColumnsValues16 = {"j6"};
		DBApp.select("c11", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("c11");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((299%DBApp.dataPageSize))-1;
		DBApp.select("c11", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"k"};
		String[] ConditionColumnsValues19 = {"k1"};
		DBApp.select("c11", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"d","b","j","e","i"};
		String[] ConditionColumnsValues110 = {"d0","b0","j6","e1","i8"};
		DBApp.select("c11", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"i","g","l","f","b","h"};
		String[] ConditionColumnsValues111 = {"i5","g4","l8","f2","b0","h4"};
		DBApp.select("c11", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((299%DBApp.dataPageSize))-1;
		DBApp.select("c11", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("c11");
	
		DBApp.select("c11");
	
		String selectTrace1 = DBApp.getFullTrace("c11");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 316);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 299"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("ot733", cols2);
		String [][] records_ot733 = new String[447][cols2.length];
		for(int i=0;i<447;i++)
		{
			records_ot733[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ot733[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ot733", records_ot733[i]);
		}
	
		int pageCount2 = (int)Math.ceil(447.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("ot733");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("ot733");
	
		DBApp.select("ot733");
	
		String[] ConditionColumns24 = {"d"};
		String[] ConditionColumnsValues24 = {"d2"};
		DBApp.select("ot733", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("ot733");
	
		String[] ConditionColumns26 = {"c"};
		String[] ConditionColumnsValues26 = {"c0"};
		DBApp.select("ot733", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("ot733");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"c"};
		String[] ConditionColumnsValues29 = {"c0"};
		DBApp.select("ot733", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("ot733");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((447%DBApp.dataPageSize))-1;
		DBApp.select("ot733", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("ot733");
	
		DBApp.select("ot733");
	
		DBApp.select("ot733");
	
		String selectTrace2 = DBApp.getFullTrace("ot733");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 468);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 447"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("kfc", cols3);
		String [][] records_kfc = new String[413][cols3.length];
		for(int i=0;i<413;i++)
		{
			records_kfc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kfc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kfc", records_kfc[i]);
		}
	
		int pageCount3 = (int)Math.ceil(413.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		DBApp.select("kfc");
	
		String selectTrace3 = DBApp.getFullTrace("kfc");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 416);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 413"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("dnnpj", cols4);
		String [][] records_dnnpj = new String[260][cols4.length];
		for(int i=0;i<260;i++)
		{
			records_dnnpj[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dnnpj[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dnnpj", records_dnnpj[i]);
		}
	
		int pageCount4 = (int)Math.ceil(260.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		DBApp.select("dnnpj");
	
		DBApp.select("dnnpj");
	
		DBApp.select("dnnpj");
	
		DBApp.select("dnnpj");
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((260%DBApp.dataPageSize))-1;
		DBApp.select("dnnpj", pageSelectPointer44,recordSelectPointer44);
	
		DBApp.select("dnnpj");
	
		DBApp.select("dnnpj");
	
		DBApp.select("dnnpj");
	
		String selectTrace4 = DBApp.getFullTrace("dnnpj");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 270);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 260"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1717);
		return (int) (random.nextDouble()*max) + 1;
	}
	private static String genRandString() 
	{
		char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		char[] digits = "0123456789".toCharArray();
		int nameSize = genRandNum(5);
		if (nameSize < 2)
			nameSize += 2;
		String res = "";
		res += alphabet[(genRandNum(alphabet.length)) - 1];
		for (int i = 1; i < nameSize; i++)
			if (Math.random() >= 0.5)
				res += alphabet[(genRandNum(alphabet.length)) - 1];
			else
				res += digits[(genRandNum(digits.length)) - 1];
		return res;
	}
}