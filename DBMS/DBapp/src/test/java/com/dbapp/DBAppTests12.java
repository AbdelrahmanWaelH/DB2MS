package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests12
{

	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("x5u7", cols0);
		String [][] records_x5u7 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			records_x5u7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x5u7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x5u7", records_x5u7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x5u7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 33, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		int pageCount0 = (int)Math.ceil(99.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("x5u7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+33));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("lo89", cols1);
		String [][] records_lo89 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			records_lo89[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lo89[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lo89", records_lo89[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record75: [a75]
		//record76: [a76]
		//record77: [a77]
		//record78: [a78]
		//record79: [a79]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a27"};
		ArrayList<String[]> tableSelect1 = DBApp.select("lo89", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a27]
		int pageCount1 = (int)Math.ceil(80.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("lo89");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i63g", cols2);
		String [][] records_i63g = new String[428][cols2.length];
		for(int i=0;i<428;i++)
		{
			records_i63g[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i63g[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i63g", records_i63g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("i63g", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 107, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0]
		int pageCount2 = (int)Math.ceil(428.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("i63g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+107));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jblk7", cols3);
		String [][] records_jblk7 = new String[262][cols3.length];
		for(int i=0;i<262;i++)
		{
			records_jblk7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jblk7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jblk7", records_jblk7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		String[] ConditionColumns3 = {"b","f"};
		String[] ConditionColumnsValues3 = {"b0","f2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("jblk7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 44, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a20, b0, c2, d0, e0, f2, g6]
		//record4: [a26, b0, c2, d2, e1, f2, g5]
		int pageCount3 = (int)Math.ceil(262.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("jblk7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+44));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("v9e0", cols0);
		String [][] records_v9e0 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_v9e0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v9e0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v9e0", records_v9e0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("v9e0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3]
		int pageCount0 = (int)Math.ceil(22.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("v9e0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("yi", cols0);
		String [][] records_yi = new String[154][cols0.length];
		for(int i=0;i<154;i++)
		{
			records_yi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yi", records_yi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("yi", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 20, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		int pageCount0 = (int)Math.ceil(154.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("yi");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+20));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("q4n", cols1);
		String [][] records_q4n = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			records_q4n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q4n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q4n", records_q4n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record89: [a89, b1, c2]
		//record90: [a90, b0, c0]
		//record91: [a91, b1, c1]
		//record92: [a92, b0, c2]
		//record93: [a93, b1, c0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q4n", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 47, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount1 = (int)Math.ceil(94.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("q4n");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+47));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("x5k", cols2);
		String [][] records_x5k = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_x5k[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x5k[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x5k", records_x5k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x5k", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 13, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		int pageCount2 = (int)Math.ceil(40.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("x5k");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k7", cols0);
		String [][] records_k7 = new String[167][cols0.length];
		for(int i=0;i<167;i++)
		{
			records_k7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k7", records_k7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		int pageCount0 = (int)Math.ceil(167.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("k7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+21));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i7", cols1);
		String [][] records_i7 = new String[243][cols1.length];
		for(int i=0;i<243;i++)
		{
			records_i7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i7", records_i7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8]
		String[] ConditionColumns1 = {"l"};
		String[] ConditionColumnsValues1 = {"l3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("i7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record4: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12]
		int pageCount1 = (int)Math.ceil(243.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("i7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+20));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("bhx", cols2);
		String [][] records_bhx = new String[300][cols2.length];
		for(int i=0;i<300;i++)
		{
			records_bhx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bhx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bhx", records_bhx[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record295: [a295]
		//record296: [a296]
		//record297: [a297]
		//record298: [a298]
		//record299: [a299]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a218"};
		ArrayList<String[]> tableSelect2 = DBApp.select("bhx", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a218]
		int pageCount2 = (int)Math.ceil(300.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("bhx");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q26", cols3);
		String [][] records_q26 = new String[264][cols3.length];
		for(int i=0;i<264;i++)
		{
			records_q26[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q26[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q26", records_q26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9, m1]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10, m2]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11, m3]
		String[] ConditionColumns3 = {"l"};
		String[] ConditionColumnsValues3 = {"l5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q26", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 22, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record3: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		int pageCount3 = (int)Math.ceil(264.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("q26");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+22));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("lk", cols0);
		String [][] records_lk = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			records_lk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lk", records_lk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("lk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		int pageCount0 = (int)Math.ceil(90.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("lk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f3", cols1);
		String [][] records_f3 = new String[376][cols1.length];
		for(int i=0;i<376;i++)
		{
			records_f3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f3", records_f3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3, m11]
		String[] ConditionColumns1 = {"l","f","c","e","j","m"};
		String[] ConditionColumnsValues1 = {"l7","f1","c1","e0","j5","m11"};
		ArrayList<String[]> tableSelect1 = DBApp.select("f3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11]
		int pageCount1 = (int)Math.ceil(376.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("f3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("mv", cols2);
		String [][] records_mv = new String[198][cols2.length];
		for(int i=0;i<198;i++)
		{
			records_mv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mv", records_mv[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record193: [a193, b1]
		//record194: [a194, b0]
		//record195: [a195, b1]
		//record196: [a196, b0]
		//record197: [a197, b1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("mv", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 99, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
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
		String selectTrace2 = DBApp.getLastTrace("mv");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+99));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("rm", cols3);
		String [][] records_rm = new String[203][cols3.length];
		for(int i=0;i<203;i++)
		{
			records_rm[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_rm[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("rm", records_rm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record198: [a198, b0, c0, d2, e3, f0]
		//record199: [a199, b1, c1, d3, e4, f1]
		//record200: [a200, b0, c2, d0, e0, f2]
		//record201: [a201, b1, c0, d1, e1, f3]
		//record202: [a202, b0, c1, d2, e2, f4]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a150"};
		ArrayList<String[]> tableSelect3 = DBApp.select("rm", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a150, b0, c0, d2, e0, f0]
		int pageCount3 = (int)Math.ceil(203.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("rm");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nugr", cols4);
		String [][] records_nugr = new String[107][cols4.length];
		for(int i=0;i<107;i++)
		{
			records_nugr[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_nugr[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("nugr", records_nugr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6]
		String[] ConditionColumns4 = {"j","g"};
		String[] ConditionColumnsValues4 = {"j1","g4"};
		ArrayList<String[]> tableSelect4 = DBApp.select("nugr", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record1: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1]
		int pageCount4 = (int)Math.ceil(107.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("nugr");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ll", cols0);
		String [][] records_ll = new String[263][cols0.length];
		for(int i=0;i<263;i++)
		{
			records_ll[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ll[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ll", records_ll[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7, o4]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0, n8, o5]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9, m1, n9, o6]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10, m2, n10, o7]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ll", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 131, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		int pageCount0 = (int)Math.ceil(263.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ll");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+131));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ewb", cols1);
		String [][] records_ewb = new String[264][cols1.length];
		for(int i=0;i<264;i++)
		{
			records_ewb[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ewb[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ewb", records_ewb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ewb", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 44, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		int pageCount1 = (int)Math.ceil(264.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ewb");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+44));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ey", cols2);
		String [][] records_ey = new String[310][cols2.length];
		for(int i=0;i<310;i++)
		{
			records_ey[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ey[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ey", records_ey[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10, l7, m8]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8, m9]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ey", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 52, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		int pageCount2 = (int)Math.ceil(310.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ey");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+52));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nx2r", cols3);
		String [][] records_nx2r = new String[252][cols3.length];
		for(int i=0;i<252;i++)
		{
			records_nx2r[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_nx2r[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("nx2r", records_nx2r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record247: [a247, b1, c1, d3, e2, f1, g2]
		//record248: [a248, b0, c2, d0, e3, f2, g3]
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		//record251: [a251, b1, c2, d3, e1, f5, g6]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("nx2r", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 126, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		int pageCount3 = (int)Math.ceil(252.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("nx2r");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+126));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("u7c", cols4);
		String [][] records_u7c = new String[107][cols4.length];
		for(int i=0;i<107;i++)
		{
			records_u7c[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u7c[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u7c", records_u7c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2]
		//record103: [a103, b1, c1, d3, e3]
		//record104: [a104, b0, c2, d0, e4]
		//record105: [a105, b1, c0, d1, e0]
		//record106: [a106, b0, c1, d2, e1]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("u7c", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 27, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a9, b1, c0, d1, e4]
		//record3: [a13, b1, c1, d1, e3]
		//record4: [a17, b1, c2, d1, e2]
		int pageCount4 = (int)Math.ceil(107.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("u7c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("x0iv", cols0);
		String [][] records_x0iv = new String[407][cols0.length];
		for(int i=0;i<407;i++)
		{
			records_x0iv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x0iv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x0iv", records_x0iv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record402: [a402, b0, c0, d2]
		//record403: [a403, b1, c1, d3]
		//record404: [a404, b0, c2, d0]
		//record405: [a405, b1, c0, d1]
		//record406: [a406, b0, c1, d2]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x0iv", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 102, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a4, b0, c1, d0]
		//record2: [a8, b0, c2, d0]
		//record3: [a12, b0, c0, d0]
		//record4: [a16, b0, c1, d0]
		int pageCount0 = (int)Math.ceil(407.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("x0iv");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+102));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("nmw3", cols0);
		String [][] records_nmw3 = new String[188][cols0.length];
		for(int i=0;i<188;i++)
		{
			records_nmw3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nmw3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nmw3", records_nmw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1, o3]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2, n2, o4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5, o7]
		String[] ConditionColumns0 = {"b","e","h","g","m","c"};
		String[] ConditionColumnsValues0 = {"b1","e3","h5","g2","m2","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nmw3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9, o3]
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
		String selectTrace0 = DBApp.getLastTrace("nmw3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("b953", cols1);
		String [][] records_b953 = new String[108][cols1.length];
		for(int i=0;i<108;i++)
		{
			records_b953[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b953[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b953", records_b953[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record103: [a103, b1, c1, d3, e3, f1]
		//record104: [a104, b0, c2, d0, e4, f2]
		//record105: [a105, b1, c0, d1, e0, f3]
		//record106: [a106, b0, c1, d2, e1, f4]
		//record107: [a107, b1, c2, d3, e2, f5]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b953", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 18, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a15, b1, c0, d3, e0, f3]
		//record3: [a21, b1, c0, d1, e1, f3]
		//record4: [a27, b1, c0, d3, e2, f3]
		int pageCount1 = (int)Math.ceil(108.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("b953");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+18));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g929s", cols2);
		String [][] records_g929s = new String[143][cols2.length];
		for(int i=0;i<143;i++)
		{
			records_g929s[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g929s[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g929s", records_g929s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		//record140: [a140, b0, c2, d0, e0, f2, g0]
		//record141: [a141, b1, c0, d1, e1, f3, g1]
		//record142: [a142, b0, c1, d2, e2, f4, g2]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("g929s", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 36, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		int pageCount2 = (int)Math.ceil(143.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("g929s");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+36));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rzu", cols3);
		String [][] records_rzu = new String[253][cols3.length];
		for(int i=0;i<253;i++)
		{
			records_rzu[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_rzu[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("rzu", records_rzu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record248: [a248, b0, c2, d0, e3, f2, g3]
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		//record251: [a251, b1, c2, d3, e1, f5, g6]
		//record252: [a252, b0, c0, d0, e2, f0, g0]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("rzu", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 126, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		int pageCount3 = (int)Math.ceil(253.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("rzu");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+126));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("w99vi", cols4);
		String [][] records_w99vi = new String[236][cols4.length];
		for(int i=0;i<236;i++)
		{
			records_w99vi[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_w99vi[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("w99vi", records_w99vi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3]
		String[] ConditionColumns4 = {"h"};
		String[] ConditionColumnsValues4 = {"h0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("w99vi", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 30, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0]
		int pageCount4 = (int)Math.ceil(236.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("w99vi");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+30));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ru5", cols0);
		String [][] records_ru5 = new String[381][cols0.length];
		for(int i=0;i<381;i++)
		{
			records_ru5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ru5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ru5", records_ru5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		String[] ConditionColumns0 = {"d","a","c"};
		String[] ConditionColumnsValues0 = {"d1","a57","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ru5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a57, b1, c0, d1, e2, f3, g1]
		int pageCount0 = (int)Math.ceil(381.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ru5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("jw7", cols1);
		String [][] records_jw7 = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			records_jw7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jw7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jw7", records_jw7[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record25: [a25, b1]
		//record26: [a26, b0]
		//record27: [a27, b1]
		//record28: [a28, b0]
		//record29: [a29, b1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jw7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 15, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount1 = (int)Math.ceil(30.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("jw7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p35", cols0);
		String [][] records_p35 = new String[394][cols0.length];
		for(int i=0;i<394;i++)
		{
			records_p35[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p35[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p35", records_p35[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("p35", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 132, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4]
		int pageCount0 = (int)Math.ceil(394.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("p35");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+132));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("atr69", cols1);
		String [][] records_atr69 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			records_atr69[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_atr69[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("atr69", records_atr69[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1]
		String[] ConditionColumns1 = {"j","h","f","k"};
		String[] ConditionColumnsValues1 = {"j1","h3","f5","k0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("atr69", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		int pageCount1 = (int)Math.ceil(90.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("atr69");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("fsz", cols2);
		String [][] records_fsz = new String[476][cols2.length];
		for(int i=0;i<476;i++)
		{
			records_fsz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fsz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fsz", records_fsz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record471: [a471, b1, c0]
		//record472: [a472, b0, c1]
		//record473: [a473, b1, c2]
		//record474: [a474, b0, c0]
		//record475: [a475, b1, c1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("fsz", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 238, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		int pageCount2 = (int)Math.ceil(476.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("fsz");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+238));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("g62v8", cols3);
		String [][] records_g62v8 = new String[207][cols3.length];
		for(int i=0;i<207;i++)
		{
			records_g62v8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g62v8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g62v8", records_g62v8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record202: [a202, b0, c1, d2, e2]
		//record203: [a203, b1, c2, d3, e3]
		//record204: [a204, b0, c0, d0, e4]
		//record205: [a205, b1, c1, d1, e0]
		//record206: [a206, b0, c2, d2, e1]
		String[] ConditionColumns3 = {"b","c"};
		String[] ConditionColumnsValues3 = {"b0","c1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("g62v8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 34, records:
		//record0: [a4, b0, c1, d0, e4]
		//record1: [a10, b0, c1, d2, e0]
		//record2: [a16, b0, c1, d0, e1]
		//record3: [a22, b0, c1, d2, e2]
		//record4: [a28, b0, c1, d0, e3]
		int pageCount3 = (int)Math.ceil(207.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("g62v8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+34));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("bsb34", cols0);
		String [][] records_bsb34 = new String[313][cols0.length];
		for(int i=0;i<313;i++)
		{
			records_bsb34[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bsb34[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bsb34", records_bsb34[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bsb34", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 62, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		int pageCount0 = (int)Math.ceil(313.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("bsb34");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+62));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b9u", cols1);
		String [][] records_b9u = new String[416][cols1.length];
		for(int i=0;i<416;i++)
		{
			records_b9u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b9u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b9u", records_b9u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b9u", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 38, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		int pageCount1 = (int)Math.ceil(416.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("b9u");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+38));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("er8x4", cols0);
		String [][] records_er8x4 = new String[331][cols0.length];
		for(int i=0;i<331;i++)
		{
			records_er8x4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_er8x4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("er8x4", records_er8x4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record326: [a326, b0, c2, d2, e1, f2]
		//record327: [a327, b1, c0, d3, e2, f3]
		//record328: [a328, b0, c1, d0, e3, f4]
		//record329: [a329, b1, c2, d1, e4, f5]
		//record330: [a330, b0, c0, d2, e0, f0]
		String[] ConditionColumns0 = {"d","e"};
		String[] ConditionColumnsValues0 = {"d3","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("er8x4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 16, records:
		//record0: [a19, b1, c1, d3, e4, f1]
		//record1: [a39, b1, c0, d3, e4, f3]
		//record2: [a59, b1, c2, d3, e4, f5]
		//record3: [a79, b1, c1, d3, e4, f1]
		//record4: [a99, b1, c0, d3, e4, f3]
		int pageCount0 = (int)Math.ceil(331.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("er8x4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+16));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ves", cols1);
		String [][] records_ves = new String[266][cols1.length];
		for(int i=0;i<266;i++)
		{
			records_ves[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ves[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ves", records_ves[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4]
		String[] ConditionColumns1 = {"b","e"};
		String[] ConditionColumnsValues1 = {"b0","e4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ves", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8]
		int pageCount1 = (int)Math.ceil(266.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ves");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("fel", cols2);
		String [][] records_fel = new String[163][cols2.length];
		for(int i=0;i<163;i++)
		{
			records_fel[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fel[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fel", records_fel[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8]
		String[] ConditionColumns2 = {"l","a"};
		String[] ConditionColumnsValues2 = {"l9","a21"};
		ArrayList<String[]> tableSelect2 = DBApp.select("fel", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		int pageCount2 = (int)Math.ceil(163.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("fel");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("cj4h", cols3);
		String [][] records_cj4h = new String[432][cols3.length];
		for(int i=0;i<432;i++)
		{
			records_cj4h[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_cj4h[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("cj4h", records_cj4h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("cj4h", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 216, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		int pageCount3 = (int)Math.ceil(432.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("cj4h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+216));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dx59", cols4);
		String [][] records_dx59 = new String[122][cols4.length];
		for(int i=0;i<122;i++)
		{
			records_dx59[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dx59[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dx59", records_dx59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1]
		String[] ConditionColumns4 = {"c","d","i","b"};
		String[] ConditionColumnsValues4 = {"c1","d1","i4","b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("dx59", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 4, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record1: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1]
		//record2: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1]
		//record3: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1]
		int pageCount4 = (int)Math.ceil(122.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("dx59");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("rt8", cols0);
		String [][] records_rt8 = new String[162][cols0.length];
		for(int i=0;i<162;i++)
		{
			records_rt8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rt8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rt8", records_rt8[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record157: [a157, b1]
		//record158: [a158, b0]
		//record159: [a159, b1]
		//record160: [a160, b0]
		//record161: [a161, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rt8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 81, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(162.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("rt8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+81));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("u6", cols1);
		String [][] records_u6 = new String[318][cols1.length];
		for(int i=0;i<318;i++)
		{
			records_u6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u6", records_u6[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record313: [a313]
		//record314: [a314]
		//record315: [a315]
		//record316: [a316]
		//record317: [a317]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a98"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a98]
		int pageCount1 = (int)Math.ceil(318.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("u6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("dx", cols2);
		String [][] records_dx = new String[429][cols2.length];
		for(int i=0;i<429;i++)
		{
			records_dx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dx", records_dx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record424: [a424, b0, c1]
		//record425: [a425, b1, c2]
		//record426: [a426, b0, c0]
		//record427: [a427, b1, c1]
		//record428: [a428, b0, c2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("dx", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 143, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		int pageCount2 = (int)Math.ceil(429.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("dx");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+143));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("b1r15", cols3);
		String [][] records_b1r15 = new String[120][cols3.length];
		for(int i=0;i<120;i++)
		{
			records_b1r15[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b1r15[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b1r15", records_b1r15[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record115: [a115]
		//record116: [a116]
		//record117: [a117]
		//record118: [a118]
		//record119: [a119]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a19"};
		ArrayList<String[]> tableSelect3 = DBApp.select("b1r15", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a19]
		int pageCount3 = (int)Math.ceil(120.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("b1r15");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ge95x", cols4);
		String [][] records_ge95x = new String[249][cols4.length];
		for(int i=0;i<249;i++)
		{
			records_ge95x[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ge95x[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ge95x", records_ge95x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8]
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e4"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ge95x", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 49, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		int pageCount4 = (int)Math.ceil(249.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("ge95x");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+49));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y0", cols0);
		String [][] records_y0 = new String[395][cols0.length];
		for(int i=0;i<395;i++)
		{
			records_y0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y0", records_y0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4]
		String[] ConditionColumns0 = {"j","k","m","h"};
		String[] ConditionColumnsValues0 = {"j8","k0","m3","h6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3]
		int pageCount0 = (int)Math.ceil(395.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("y0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("pr9", cols1);
		String [][] records_pr9 = new String[153][cols1.length];
		for(int i=0;i<153;i++)
		{
			records_pr9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pr9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pr9", records_pr9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6, n9]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7, n10]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8, l7, m8, n11]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9, n12]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("pr9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 19, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		int pageCount1 = (int)Math.ceil(153.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("pr9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+19));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ic8", cols2);
		String [][] records_ic8 = new String[164][cols2.length];
		for(int i=0;i<164;i++)
		{
			records_ic8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ic8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ic8", records_ic8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ic8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 41, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0]
		int pageCount2 = (int)Math.ceil(164.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ic8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+41));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qk", cols3);
		String [][] records_qk = new String[383][cols3.length];
		for(int i=0;i<383;i++)
		{
			records_qk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qk", records_qk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("qk", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 191, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		int pageCount3 = (int)Math.ceil(383.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("qk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+191));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zh", cols0);
		String [][] records_zh = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_zh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zh", records_zh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		String[] ConditionColumns0 = {"j","l","g","f","m","b"};
		String[] ConditionColumnsValues0 = {"j0","l8","g6","f2","m7","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zh", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7]
		int pageCount0 = (int)Math.ceil(40.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("zh");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("qu", cols1);
		String [][] records_qu = new String[126][cols1.length];
		for(int i=0;i<126;i++)
		{
			records_qu[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qu[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qu", records_qu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record121: [a121, b1, c1, d1, e1, f1]
		//record122: [a122, b0, c2, d2, e2, f2]
		//record123: [a123, b1, c0, d3, e3, f3]
		//record124: [a124, b0, c1, d0, e4, f4]
		//record125: [a125, b1, c2, d1, e0, f5]
		String[] ConditionColumns1 = {"b","c"};
		String[] ConditionColumnsValues1 = {"b0","c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("qu", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 21, records:
		//record0: [a4, b0, c1, d0, e4, f4]
		//record1: [a10, b0, c1, d2, e0, f4]
		//record2: [a16, b0, c1, d0, e1, f4]
		//record3: [a22, b0, c1, d2, e2, f4]
		//record4: [a28, b0, c1, d0, e3, f4]
		int pageCount1 = (int)Math.ceil(126.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("qu");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+21));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k8", cols2);
		String [][] records_k8 = new String[395][cols2.length];
		for(int i=0;i<395;i++)
		{
			records_k8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k8", records_k8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4]
		String[] ConditionColumns2 = {"c","d","e"};
		String[] ConditionColumnsValues2 = {"c2","d3","e2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("k8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		//record1: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3]
		//record2: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11]
		//record3: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6]
		//record4: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1]
		int pageCount2 = (int)Math.ceil(395.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("k8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g13gk", cols0);
		String [][] records_g13gk = new String[479][cols0.length];
		for(int i=0;i<479;i++)
		{
			records_g13gk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g13gk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g13gk", records_g13gk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record474: [a474, b0, c0, d2, e4, f0, g5]
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g13gk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 96, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a20, b0, c2, d0, e0, f2, g6]
		int pageCount0 = (int)Math.ceil(479.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("g13gk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+96));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("s5oy", cols1);
		String [][] records_s5oy = new String[331][cols1.length];
		for(int i=0;i<331;i++)
		{
			records_s5oy[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s5oy[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s5oy", records_s5oy[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record326: [a326]
		//record327: [a327]
		//record328: [a328]
		//record329: [a329]
		//record330: [a330]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a330"};
		ArrayList<String[]> tableSelect1 = DBApp.select("s5oy", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a330]
		int pageCount1 = (int)Math.ceil(331.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("s5oy");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("g8", cols2);
		String [][] records_g8 = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			records_g8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g8", records_g8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1]
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("g8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7]
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("g8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("y9", cols0);
		String [][] records_y9 = new String[402][cols0.length];
		for(int i=0;i<402;i++)
		{
			records_y9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y9", records_y9[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record397: [a397, b1]
		//record398: [a398, b0]
		//record399: [a399, b1]
		//record400: [a400, b0]
		//record401: [a401, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 201, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(402.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("y9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+201));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("dh3", cols1);
		String [][] records_dh3 = new String[276][cols1.length];
		for(int i=0;i<276;i++)
		{
			records_dh3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dh3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dh3", records_dh3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0]
		String[] ConditionColumns1 = {"d","f"};
		String[] ConditionColumnsValues1 = {"d3","f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("dh3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 23, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record3: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4]
		int pageCount1 = (int)Math.ceil(276.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("dh3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+23));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qb57c", cols0);
		String [][] records_qb57c = new String[378][cols0.length];
		for(int i=0;i<378;i++)
		{
			records_qb57c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qb57c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qb57c", records_qb57c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2, l4]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("qb57c", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 42, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		int pageCount0 = (int)Math.ceil(378.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("qb57c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("h1", cols1);
		String [][] records_h1 = new String[307][cols1.length];
		for(int i=0;i<307;i++)
		{
			records_h1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h1", records_h1[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record302: [a302, b0]
		//record303: [a303, b1]
		//record304: [a304, b0]
		//record305: [a305, b1]
		//record306: [a306, b0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 153, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount1 = (int)Math.ceil(307.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("h1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+153));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("t06", cols2);
		String [][] records_t06 = new String[190][cols2.length];
		for(int i=0;i<190;i++)
		{
			records_t06[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t06[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t06", records_t06[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7]
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k10"};
		ArrayList<String[]> tableSelect2 = DBApp.select("t06", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 17, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2]
		int pageCount2 = (int)Math.ceil(190.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("t06");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("nl6", cols3);
		String [][] records_nl6 = new String[498][cols3.length];
		for(int i=0;i<498;i++)
		{
			records_nl6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_nl6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("nl6", records_nl6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3]
		String[] ConditionColumns3 = {"k"};
		String[] ConditionColumnsValues3 = {"k1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("nl6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 46, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		int pageCount3 = (int)Math.ceil(498.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("nl6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+46));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r42te", cols0);
		String [][] records_r42te = new String[421][cols0.length];
		for(int i=0;i<421;i++)
		{
			records_r42te[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r42te[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r42te", records_r42te[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9]
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r42te", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 140, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		int pageCount0 = (int)Math.ceil(421.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("r42te");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+140));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("q4", cols0);
		String [][] records_q4 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_q4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q4", records_q4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("q4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		int pageCount0 = (int)Math.ceil(26.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("q4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r7y", cols1);
		String [][] records_r7y = new String[198][cols1.length];
		for(int i=0;i<198;i++)
		{
			records_r7y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r7y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r7y", records_r7y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r7y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 50, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0]
		int pageCount1 = (int)Math.ceil(198.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("r7y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+50));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vvd", cols2);
		String [][] records_vvd = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_vvd[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vvd[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vvd", records_vvd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5]
		String[] ConditionColumns2 = {"i","h","g"};
		String[] ConditionColumnsValues2 = {"i4","h4","g4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("vvd", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		int pageCount2 = (int)Math.ceil(46.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("vvd");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("z644", cols3);
		String [][] records_z644 = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			records_z644[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z644[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z644", records_z644[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5]
		//record54: [a54, b0, c0, d2, e4, f0]
		//record55: [a55, b1, c1, d3, e0, f1]
		//record56: [a56, b0, c2, d0, e1, f2]
		//record57: [a57, b1, c0, d1, e2, f3]
		String[] ConditionColumns3 = {"a","e"};
		String[] ConditionColumnsValues3 = {"a13","e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z644", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a13, b1, c1, d1, e3, f1]
		int pageCount3 = (int)Math.ceil(58.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("z644");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("as5", cols0);
		String [][] records_as5 = new String[339][cols0.length];
		for(int i=0;i<339;i++)
		{
			records_as5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_as5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("as5", records_as5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record334: [a334, b0, c1, d2, e4, f4]
		//record335: [a335, b1, c2, d3, e0, f5]
		//record336: [a336, b0, c0, d0, e1, f0]
		//record337: [a337, b1, c1, d1, e2, f1]
		//record338: [a338, b0, c2, d2, e3, f2]
		String[] ConditionColumns0 = {"f","c","b"};
		String[] ConditionColumnsValues0 = {"f1","c1","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("as5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 57, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a19, b1, c1, d3, e4, f1]
		//record4: [a25, b1, c1, d1, e0, f1]
		int pageCount0 = (int)Math.ceil(339.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("as5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+57));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("jzj", cols1);
		String [][] records_jzj = new String[296][cols1.length];
		for(int i=0;i<296;i++)
		{
			records_jzj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jzj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jzj", records_jzj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record291: [a291, b1, c0, d3, e1, f3]
		//record292: [a292, b0, c1, d0, e2, f4]
		//record293: [a293, b1, c2, d1, e3, f5]
		//record294: [a294, b0, c0, d2, e4, f0]
		//record295: [a295, b1, c1, d3, e0, f1]
		String[] ConditionColumns1 = {"c","f","d"};
		String[] ConditionColumnsValues1 = {"c2","f2","d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jzj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a8, b0, c2, d0, e3, f2]
		//record1: [a20, b0, c2, d0, e0, f2]
		//record2: [a32, b0, c2, d0, e2, f2]
		//record3: [a44, b0, c2, d0, e4, f2]
		//record4: [a56, b0, c2, d0, e1, f2]
		int pageCount1 = (int)Math.ceil(296.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("jzj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("db", cols2);
		String [][] records_db = new String[269][cols2.length];
		for(int i=0;i<269;i++)
		{
			records_db[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_db[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("db", records_db[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8]
		String[] ConditionColumns2 = {"i","a"};
		String[] ConditionColumnsValues2 = {"i0","a18"};
		ArrayList<String[]> tableSelect2 = DBApp.select("db", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		int pageCount2 = (int)Math.ceil(269.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("db");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("kvb2q", cols3);
		String [][] records_kvb2q = new String[220][cols3.length];
		for(int i=0;i<220;i++)
		{
			records_kvb2q[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kvb2q[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kvb2q", records_kvb2q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9, o9]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("kvb2q", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 32, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
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
		String selectTrace3 = DBApp.getLastTrace("kvb2q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fe", cols0);
		String [][] records_fe = new String[392][cols0.length];
		for(int i=0;i<392;i++)
		{
			records_fe[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fe[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fe", records_fe[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7]
		String[] ConditionColumns0 = {"a","e","g"};
		String[] ConditionColumnsValues0 = {"a208","e3","g5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fe", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4]
		int pageCount0 = (int)Math.ceil(392.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fe");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a12", cols1);
		String [][] records_a12 = new String[183][cols1.length];
		for(int i=0;i<183;i++)
		{
			records_a12[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a12[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a12", records_a12[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9, n10]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11, n12]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i8"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a12", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		int pageCount1 = (int)Math.ceil(183.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a12");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+20));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("pk9", cols2);
		String [][] records_pk9 = new String[204][cols2.length];
		for(int i=0;i<204;i++)
		{
			records_pk9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pk9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pk9", records_pk9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1, l7, m4]
		//record200: [a200, b0, c2, d0, e0, f2, g4, h0, i2, j0, k2, l8, m5]
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3, l9, m6]
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2, i4, j2, k4, l10, m7]
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5, l11, m8]
		String[] ConditionColumns2 = {"k","h"};
		String[] ConditionColumnsValues2 = {"k2","h7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("pk9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1]
		//record1: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11]
		int pageCount2 = (int)Math.ceil(204.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("pk9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("q9s", cols3);
		String [][] records_q9s = new String[430][cols3.length];
		for(int i=0;i<430;i++)
		{
			records_q9s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q9s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q9s", records_q9s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record425: [a425, b1, c2, d1]
		//record426: [a426, b0, c0, d2]
		//record427: [a427, b1, c1, d3]
		//record428: [a428, b0, c2, d0]
		//record429: [a429, b1, c0, d1]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q9s", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 215, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		int pageCount3 = (int)Math.ceil(430.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("q9s");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+215));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("c3d", cols0);
		String [][] records_c3d = new String[351][cols0.length];
		for(int i=0;i<351;i++)
		{
			records_c3d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c3d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c3d", records_c3d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record346: [a346, b0, c1]
		//record347: [a347, b1, c2]
		//record348: [a348, b0, c0]
		//record349: [a349, b1, c1]
		//record350: [a350, b0, c2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c3d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 176, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount0 = (int)Math.ceil(351.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("c3d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+176));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("gso", cols1);
		String [][] records_gso = new String[247][cols1.length];
		for(int i=0;i<247;i++)
		{
			records_gso[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gso[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gso", records_gso[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record242: [a242, b0, c2, d2, e2]
		//record243: [a243, b1, c0, d3, e3]
		//record244: [a244, b0, c1, d0, e4]
		//record245: [a245, b1, c2, d1, e0]
		//record246: [a246, b0, c0, d2, e1]
		String[] ConditionColumns1 = {"c","a"};
		String[] ConditionColumnsValues1 = {"c1","a199"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gso", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a199, b1, c1, d3, e4]
		int pageCount1 = (int)Math.ceil(247.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("gso");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("oq2", cols2);
		String [][] records_oq2 = new String[129][cols2.length];
		for(int i=0;i<129;i++)
		{
			records_oq2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oq2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oq2", records_oq2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2, o8]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("oq2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 15, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		int pageCount2 = (int)Math.ceil(129.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("oq2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("n9rd2", cols0);
		String [][] records_n9rd2 = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_n9rd2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n9rd2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n9rd2", records_n9rd2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9]
		String[] ConditionColumns0 = {"f","e"};
		String[] ConditionColumnsValues0 = {"f2","e2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("n9rd2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2]
		//record2: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2]
		//record3: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2]
		//record4: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2]
		int pageCount0 = (int)Math.ceil(140.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("n9rd2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("nz", cols1);
		String [][] records_nz = new String[392][cols1.length];
		for(int i=0;i<392;i++)
		{
			records_nz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nz", records_nz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 79, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5]
		int pageCount1 = (int)Math.ceil(392.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("nz");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+79));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r6fd", cols2);
		String [][] records_r6fd = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			records_r6fd[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r6fd[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r6fd", records_r6fd[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("r6fd", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		int pageCount2 = (int)Math.ceil(2.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("r6fd");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b3o", cols0);
		String [][] records_b3o = new String[400][cols0.length];
		for(int i=0;i<400;i++)
		{
			records_b3o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b3o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b3o", records_b3o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3]
		String[] ConditionColumns0 = {"k","e","f","j"};
		String[] ConditionColumnsValues0 = {"k0","e4","f4","j4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b3o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0]
		int pageCount0 = (int)Math.ceil(400.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("b3o");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("js", cols0);
		String [][] records_js = new String[180][cols0.length];
		for(int i=0;i<180;i++)
		{
			records_js[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_js[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("js", records_js[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record175: [a175]
		//record176: [a176]
		//record177: [a177]
		//record178: [a178]
		//record179: [a179]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a130"};
		ArrayList<String[]> tableSelect0 = DBApp.select("js", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a130]
		int pageCount0 = (int)Math.ceil(180.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("js");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("mht", cols1);
		String [][] records_mht = new String[164][cols1.length];
		for(int i=0;i<164;i++)
		{
			records_mht[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mht[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mht", records_mht[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record159: [a159, b1, c0, d3, e4, f3]
		//record160: [a160, b0, c1, d0, e0, f4]
		//record161: [a161, b1, c2, d1, e1, f5]
		//record162: [a162, b0, c0, d2, e2, f0]
		//record163: [a163, b1, c1, d3, e3, f1]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("mht", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 28, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a6, b0, c0, d2, e1, f0]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a24, b0, c0, d0, e4, f0]
		int pageCount1 = (int)Math.ceil(164.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("mht");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ck64j", cols2);
		String [][] records_ck64j = new String[120][cols2.length];
		for(int i=0;i<120;i++)
		{
			records_ck64j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ck64j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ck64j", records_ck64j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9, m0]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11, m2]
		String[] ConditionColumns2 = {"m"};
		String[] ConditionColumnsValues2 = {"m10"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ck64j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record2: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record3: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10]
		//record4: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10]
		int pageCount2 = (int)Math.ceil(120.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ck64j");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("d5172", cols0);
		String [][] records_d5172 = new String[251][cols0.length];
		for(int i=0;i<251;i++)
		{
			records_d5172[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d5172[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d5172", records_d5172[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0, n9]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("d5172", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 23, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5]
		int pageCount0 = (int)Math.ceil(251.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("d5172");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+23));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("eu03", cols1);
		String [][] records_eu03 = new String[254][cols1.length];
		for(int i=0;i<254;i++)
		{
			records_eu03[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_eu03[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("eu03", records_eu03[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("eu03", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 127, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		int pageCount1 = (int)Math.ceil(254.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("eu03");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+127));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("wvn97", cols0);
		String [][] records_wvn97 = new String[298][cols0.length];
		for(int i=0;i<298;i++)
		{
			records_wvn97[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wvn97[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wvn97", records_wvn97[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3]
		//record294: [a294, b0, c0, d2, e4]
		//record295: [a295, b1, c1, d3, e0]
		//record296: [a296, b0, c2, d0, e1]
		//record297: [a297, b1, c0, d1, e2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("wvn97", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 149, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		int pageCount0 = (int)Math.ceil(298.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("wvn97");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+149));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a64", cols1);
		String [][] records_a64 = new String[109][cols1.length];
		for(int i=0;i<109;i++)
		{
			records_a64[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a64[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a64", records_a64[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a64", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 55, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		int pageCount1 = (int)Math.ceil(109.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a64");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+55));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p8vxk", cols2);
		String [][] records_p8vxk = new String[423][cols2.length];
		for(int i=0;i<423;i++)
		{
			records_p8vxk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p8vxk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p8vxk", records_p8vxk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2, n12, o13]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9, k1, l11, m3, n13, o14]
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0, m4, n0, o0]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1, m5, n1, o1]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2, o2]
		String[] ConditionColumns2 = {"c","h","d","n","o"};
		String[] ConditionColumnsValues2 = {"c2","h2","d2","n2","o2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("p8vxk", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		int pageCount2 = (int)Math.ceil(423.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("p8vxk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("z4", cols3);
		String [][] records_z4 = new String[309][cols3.length];
		for(int i=0;i<309;i++)
		{
			records_z4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z4", records_z4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2]
		String[] ConditionColumns3 = {"h","d","b","f"};
		String[] ConditionColumnsValues3 = {"h2","d2","b0","f0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 13, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record1: [a42, b0, c0, d2, e2, f0, g0, h2, i6]
		//record2: [a66, b0, c0, d2, e1, f0, g3, h2, i3]
		//record3: [a90, b0, c0, d2, e0, f0, g6, h2, i0]
		//record4: [a114, b0, c0, d2, e4, f0, g2, h2, i6]
		int pageCount3 = (int)Math.ceil(309.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("z4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xbq2", cols4);
		String [][] records_xbq2 = new String[29][cols4.length];
		for(int i=0;i<29;i++)
		{
			records_xbq2[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xbq2[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xbq2", records_xbq2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		String[] ConditionColumns4 = {"m"};
		String[] ConditionColumnsValues4 = {"m10"};
		ArrayList<String[]> tableSelect4 = DBApp.select("xbq2", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		int pageCount4 = (int)Math.ceil(29.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("xbq2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("o7l", cols0);
		String [][] records_o7l = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_o7l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o7l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o7l", records_o7l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0]
		//record46: [a46, b0, c1, d2, e1]
		//record47: [a47, b1, c2, d3, e2]
		//record48: [a48, b0, c0, d0, e3]
		//record49: [a49, b1, c1, d1, e4]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o7l", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		int pageCount0 = (int)Math.ceil(50.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("o7l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+25));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("aj", cols1);
		String [][] records_aj = new String[217][cols1.length];
		for(int i=0;i<217;i++)
		{
			records_aj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_aj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("aj", records_aj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("aj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 108, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1]
		int pageCount1 = (int)Math.ceil(217.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("aj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+108));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("oi8", cols2);
		String [][] records_oi8 = new String[175][cols2.length];
		for(int i=0;i<175;i++)
		{
			records_oi8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oi8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oi8", records_oi8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0, f2]
		//record171: [a171, b1, c0, d3, e1, f3]
		//record172: [a172, b0, c1, d0, e2, f4]
		//record173: [a173, b1, c2, d1, e3, f5]
		//record174: [a174, b0, c0, d2, e4, f0]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("oi8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 59, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a6, b0, c0, d2, e1, f0]
		//record3: [a9, b1, c0, d1, e4, f3]
		//record4: [a12, b0, c0, d0, e2, f0]
		int pageCount2 = (int)Math.ceil(175.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("oi8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+59));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g96", cols3);
		String [][] records_g96 = new String[232][cols3.length];
		for(int i=0;i<232;i++)
		{
			records_g96[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g96[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g96", records_g96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("g96", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 33, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0]
		int pageCount3 = (int)Math.ceil(232.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("g96");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+33));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("z5una", cols0);
		String [][] records_z5una = new String[263][cols0.length];
		for(int i=0;i<263;i++)
		{
			records_z5una[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z5una[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z5una", records_z5una[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("z5una", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 53, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		int pageCount0 = (int)Math.ceil(263.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("z5una");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+53));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("wx6", cols1);
		String [][] records_wx6 = new String[162][cols1.length];
		for(int i=0;i<162;i++)
		{
			records_wx6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wx6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wx6", records_wx6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record157: [a157, b1, c1, d1, e2, f1]
		//record158: [a158, b0, c2, d2, e3, f2]
		//record159: [a159, b1, c0, d3, e4, f3]
		//record160: [a160, b0, c1, d0, e0, f4]
		//record161: [a161, b1, c2, d1, e1, f5]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wx6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 32, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a23, b1, c2, d3, e3, f5]
		int pageCount1 = (int)Math.ceil(162.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("wx6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("g7m12", cols2);
		String [][] records_g7m12 = new String[432][cols2.length];
		for(int i=0;i<432;i++)
		{
			records_g7m12[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g7m12[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g7m12", records_g7m12[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7, o7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8, o8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11]
		String[] ConditionColumns2 = {"d","k","b"};
		String[] ConditionColumnsValues2 = {"d1","k5","b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("g7m12", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 10, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record1: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4]
		//record2: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9, o3]
		//record3: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2]
		//record4: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13, o1]
		int pageCount2 = (int)Math.ceil(432.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("g7m12");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c8y", cols3);
		String [][] records_c8y = new String[263][cols3.length];
		for(int i=0;i<263;i++)
		{
			records_c8y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c8y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c8y", records_c8y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("c8y", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 44, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a20, b0, c2, d0, e0, f2, g6]
		//record4: [a26, b0, c2, d2, e1, f2, g5]
		int pageCount3 = (int)Math.ceil(263.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("c8y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+44));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b5", cols4);
		String [][] records_b5 = new String[179][cols4.length];
		for(int i=0;i<179;i++)
		{
			records_b5[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_b5[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("b5", records_b5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1, l9]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10]
		String[] ConditionColumns4 = {"f"};
		String[] ConditionColumnsValues4 = {"f2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("b5", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 30, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		int pageCount4 = (int)Math.ceil(179.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("b5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+30));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("t7i", cols0);
		String [][] records_t7i = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			records_t7i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t7i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t7i", records_t7i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7]
		String[] ConditionColumns0 = {"f","m","d"};
		String[] ConditionColumnsValues0 = {"f3","m2","d3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("t7i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		int pageCount0 = (int)Math.ceil(73.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("t7i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xo37f", cols0);
		String [][] records_xo37f = new String[287][cols0.length];
		for(int i=0;i<287;i++)
		{
			records_xo37f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xo37f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xo37f", records_xo37f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9, n2, o12]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3, o13]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4, o14]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5, o0]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0, n6, o1]
		String[] ConditionColumns0 = {"n","j","e","c"};
		String[] ConditionColumnsValues0 = {"n2","j0","e0","c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xo37f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5]
		int pageCount0 = (int)Math.ceil(287.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("xo37f");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z28d8", cols1);
		String [][] records_z28d8 = new String[237][cols1.length];
		for(int i=0;i<237;i++)
		{
			records_z28d8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z28d8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z28d8", records_z28d8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8]
		String[] ConditionColumns1 = {"g","l","e","i","d"};
		String[] ConditionColumnsValues1 = {"g3","l0","e4","i6","d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z28d8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		int pageCount1 = (int)Math.ceil(237.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("z28d8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ucls3", cols2);
		String [][] records_ucls3 = new String[488][cols2.length];
		for(int i=0;i<488;i++)
		{
			records_ucls3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ucls3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ucls3", records_ucls3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ucls3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 97, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		int pageCount2 = (int)Math.ceil(488.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ucls3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+97));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ivgy7", cols0);
		String [][] records_ivgy7 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			records_ivgy7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ivgy7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ivgy7", records_ivgy7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ivgy7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		int pageCount0 = (int)Math.ceil(57.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ivgy7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+7));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h62", cols0);
		String [][] records_h62 = new String[440][cols0.length];
		for(int i=0;i<440;i++)
		{
			records_h62[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h62[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h62", records_h62[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h62", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 220, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		int pageCount0 = (int)Math.ceil(440.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("h62");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+220));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g92", cols1);
		String [][] records_g92 = new String[272][cols1.length];
		for(int i=0;i<272;i++)
		{
			records_g92[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g92[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g92", records_g92[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("g92", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		int pageCount1 = (int)Math.ceil(272.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("g92");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+39));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("b22", cols2);
		String [][] records_b22 = new String[378][cols2.length];
		for(int i=0;i<378;i++)
		{
			records_b22[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b22[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b22", records_b22[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record373: [a373]
		//record374: [a374]
		//record375: [a375]
		//record376: [a376]
		//record377: [a377]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a347"};
		ArrayList<String[]> tableSelect2 = DBApp.select("b22", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a347]
		int pageCount2 = (int)Math.ceil(378.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("b22");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("pbw8", cols0);
		String [][] records_pbw8 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			records_pbw8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pbw8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pbw8", records_pbw8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		String[] ConditionColumns0 = {"d","h","f","a"};
		String[] ConditionColumnsValues0 = {"d3","h3","f3","a3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pbw8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		int pageCount0 = (int)Math.ceil(34.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("pbw8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("pqd4e", cols1);
		String [][] records_pqd4e = new String[388][cols1.length];
		for(int i=0;i<388;i++)
		{
			records_pqd4e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pqd4e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pqd4e", records_pqd4e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("pqd4e", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 97, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		int pageCount1 = (int)Math.ceil(388.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("pqd4e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+97));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("flm6b", cols0);
		String [][] records_flm6b = new String[151][cols0.length];
		for(int i=0;i<151;i++)
		{
			records_flm6b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_flm6b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("flm6b", records_flm6b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("flm6b", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 50, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5]
		int pageCount0 = (int)Math.ceil(151.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("flm6b");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+50));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u0", cols1);
		String [][] records_u0 = new String[295][cols1.length];
		for(int i=0;i<295;i++)
		{
			records_u0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u0", records_u0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6]
		String[] ConditionColumns1 = {"h","b"};
		String[] ConditionColumnsValues1 = {"h0","b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0]
		int pageCount1 = (int)Math.ceil(295.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("u0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("u4744", cols2);
		String [][] records_u4744 = new String[482][cols2.length];
		for(int i=0;i<482;i++)
		{
			records_u4744[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u4744[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u4744", records_u4744[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record477: [a477, b1, c0]
		//record478: [a478, b0, c1]
		//record479: [a479, b1, c2]
		//record480: [a480, b0, c0]
		//record481: [a481, b1, c1]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("u4744", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 161, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		int pageCount2 = (int)Math.ceil(482.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("u4744");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+161));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("bo4", cols0);
		String [][] records_bo4 = new String[249][cols0.length];
		for(int i=0;i<249;i++)
		{
			records_bo4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bo4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bo4", records_bo4[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record244: [a244, b0]
		//record245: [a245, b1]
		//record246: [a246, b0]
		//record247: [a247, b1]
		//record248: [a248, b0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a21"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bo4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a21, b1]
		int pageCount0 = (int)Math.ceil(249.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("bo4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("l43", cols1);
		String [][] records_l43 = new String[217][cols1.length];
		for(int i=0;i<217;i++)
		{
			records_l43[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l43[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l43", records_l43[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0]
		String[] ConditionColumns1 = {"c","e","h","d"};
		String[] ConditionColumnsValues1 = {"c2","e3","h0","d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l43", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record1: [a128, b0, c2, d0, e3, f2, g2, h0]
		int pageCount1 = (int)Math.ceil(217.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("l43");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("w0sls", cols2);
		String [][] records_w0sls = new String[480][cols2.length];
		for(int i=0;i<480;i++)
		{
			records_w0sls[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w0sls[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w0sls", records_w0sls[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("w0sls", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 160, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		int pageCount2 = (int)Math.ceil(480.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("w0sls");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+160));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("ez01", cols3);
		String [][] records_ez01 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			records_ez01[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ez01[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ez01", records_ez01[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("ez01", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		int pageCount3 = (int)Math.ceil(1.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("ez01");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k8w4z", cols0);
		String [][] records_k8w4z = new String[298][cols0.length];
		for(int i=0;i<298;i++)
		{
			records_k8w4z[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k8w4z[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k8w4z", records_k8w4z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7]
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k8w4z", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 30, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		int pageCount0 = (int)Math.ceil(298.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("k8w4z");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+30));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("srf9", cols1);
		String [][] records_srf9 = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			records_srf9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_srf9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("srf9", records_srf9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("srf9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		int pageCount1 = (int)Math.ceil(7.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("srf9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("oj26", cols2);
		String [][] records_oj26 = new String[351][cols2.length];
		for(int i=0;i<351;i++)
		{
			records_oj26[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oj26[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oj26", records_oj26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record346: [a346, b0, c1, d2]
		//record347: [a347, b1, c2, d3]
		//record348: [a348, b0, c0, d0]
		//record349: [a349, b1, c1, d1]
		//record350: [a350, b0, c2, d2]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("oj26", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 176, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		int pageCount2 = (int)Math.ceil(351.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("oj26");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+176));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b87", cols3);
		String [][] records_b87 = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			records_b87[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b87[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b87", records_b87[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6]
		String[] ConditionColumns3 = {"e","g","b","d"};
		String[] ConditionColumnsValues3 = {"e3","g2","b1","d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("b87", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		int pageCount3 = (int)Math.ceil(47.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("b87");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yry", cols0);
		String [][] records_yry = new String[261][cols0.length];
		for(int i=0;i<261;i++)
		{
			records_yry[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yry[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yry", records_yry[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record256: [a256, b0, c1, d0, e1, f4, g4]
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("yry", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 65, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		int pageCount0 = (int)Math.ceil(261.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("yry");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+65));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("be22", cols1);
		String [][] records_be22 = new String[216][cols1.length];
		for(int i=0;i<216;i++)
		{
			records_be22[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_be22[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("be22", records_be22[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1]
		//record212: [a212, b0, c2, d0, e2, f2, g2]
		//record213: [a213, b1, c0, d1, e3, f3, g3]
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("be22", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 43, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a23, b1, c2, d3, e3, f5, g2]
		int pageCount1 = (int)Math.ceil(216.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("be22");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+43));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f5d", cols2);
		String [][] records_f5d = new String[277][cols2.length];
		for(int i=0;i<277;i++)
		{
			records_f5d[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f5d[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f5d", records_f5d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1, n8, o4]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2, n9, o5]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3, n10, o6]
		String[] ConditionColumns2 = {"e","j","n","d","o"};
		String[] ConditionColumnsValues2 = {"e2","j7","n7","d1","o2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("f5d", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7, o2]
		int pageCount2 = (int)Math.ceil(277.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("f5d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hr", cols3);
		String [][] records_hr = new String[481][cols3.length];
		for(int i=0;i<481;i++)
		{
			records_hr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hr", records_hr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0]
		String[] ConditionColumns3 = {"c","j","f"};
		String[] ConditionColumnsValues3 = {"c0","j0","f0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("hr", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record2: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0]
		//record3: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6]
		//record4: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0]
		int pageCount3 = (int)Math.ceil(481.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("hr");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hd9y8", cols0);
		String [][] records_hd9y8 = new String[110][cols0.length];
		for(int i=0;i<110;i++)
		{
			records_hd9y8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hd9y8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hd9y8", records_hd9y8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5]
		String[] ConditionColumns0 = {"h","d","a"};
		String[] ConditionColumnsValues0 = {"h6","d2","a62"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hd9y8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a62, b0, c2, d2, e2, f2, g6, h6]
		int pageCount0 = (int)Math.ceil(110.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("hd9y8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n2", cols1);
		String [][] records_n2 = new String[281][cols1.length];
		for(int i=0;i<281;i++)
		{
			records_n2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n2", records_n2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record276: [a276, b0, c0, d0, e1, f0, g3]
		//record277: [a277, b1, c1, d1, e2, f1, g4]
		//record278: [a278, b0, c2, d2, e3, f2, g5]
		//record279: [a279, b1, c0, d3, e4, f3, g6]
		//record280: [a280, b0, c1, d0, e0, f4, g0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 94, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		int pageCount1 = (int)Math.ceil(281.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("n2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+94));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sww0", cols2);
		String [][] records_sww0 = new String[105][cols2.length];
		for(int i=0;i<105;i++)
		{
			records_sww0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sww0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sww0", records_sww0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sww0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 17, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		int pageCount2 = (int)Math.ceil(105.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("sww0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("x8f", cols3);
		String [][] records_x8f = new String[96][cols3.length];
		for(int i=0;i<96;i++)
		{
			records_x8f[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x8f[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x8f", records_x8f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record91: [a91, b1, c1, d3, e1]
		//record92: [a92, b0, c2, d0, e2]
		//record93: [a93, b1, c0, d1, e3]
		//record94: [a94, b0, c1, d2, e4]
		//record95: [a95, b1, c2, d3, e0]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x8f", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 32, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		int pageCount3 = (int)Math.ceil(96.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("x8f");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("msv3", cols4);
		String [][] records_msv3 = new String[219][cols4.length];
		for(int i=0;i<219;i++)
		{
			records_msv3[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_msv3[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("msv3", records_msv3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4, o4]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8]
		String[] ConditionColumns4 = {"h","d"};
		String[] ConditionColumnsValues4 = {"h0","d0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("msv3", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 28, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		int pageCount4 = (int)Math.ceil(219.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("msv3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hkp", cols0);
		String [][] records_hkp = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_hkp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hkp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hkp", records_hkp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2]
		//record136: [a136, b0, c1, d0, e1, f4, g3]
		//record137: [a137, b1, c2, d1, e2, f5, g4]
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		String[] ConditionColumns0 = {"g","d"};
		String[] ConditionColumnsValues0 = {"g5","d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hkp", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a40, b0, c1, d0, e0, f4, g5]
		//record2: [a68, b0, c2, d0, e3, f2, g5]
		//record3: [a96, b0, c0, d0, e1, f0, g5]
		//record4: [a124, b0, c1, d0, e4, f4, g5]
		int pageCount0 = (int)Math.ceil(140.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("hkp");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("id", cols1);
		String [][] records_id = new String[107][cols1.length];
		for(int i=0;i<107;i++)
		{
			records_id[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_id[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("id", records_id[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6]
		String[] ConditionColumns1 = {"b","f"};
		String[] ConditionColumnsValues1 = {"b0","f0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("id", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 18, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		int pageCount1 = (int)Math.ceil(107.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("id");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+18));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("y6", cols0);
		String [][] records_y6 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_y6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y6", records_y6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record39: [a39, b1, c0, d3]
		//record40: [a40, b0, c1, d0]
		//record41: [a41, b1, c2, d1]
		//record42: [a42, b0, c0, d2]
		//record43: [a43, b1, c1, d3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		int pageCount0 = (int)Math.ceil(44.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("y6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+14));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cf", cols1);
		String [][] records_cf = new String[481][cols1.length];
		for(int i=0;i<481;i++)
		{
			records_cf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cf", records_cf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("cf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 61, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		int pageCount1 = (int)Math.ceil(481.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("cf");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+61));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o9", cols2);
		String [][] records_o9 = new String[226][cols2.length];
		for(int i=0;i<226;i++)
		{
			records_o9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o9", records_o9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1]
		String[] ConditionColumns2 = {"h","c","d"};
		String[] ConditionColumnsValues2 = {"h5","c1","d1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record1: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record2: [a61, b1, c1, d1, e1, f1, g5, h5]
		//record3: [a85, b1, c1, d1, e0, f1, g1, h5]
		//record4: [a109, b1, c1, d1, e4, f1, g4, h5]
		int pageCount2 = (int)Math.ceil(226.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("o9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("bu9", cols3);
		String [][] records_bu9 = new String[272][cols3.length];
		for(int i=0;i<272;i++)
		{
			records_bu9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bu9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bu9", records_bu9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("bu9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 136, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		int pageCount3 = (int)Math.ceil(272.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("bu9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+136));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("vz8", cols0);
		String [][] records_vz8 = new String[176][cols0.length];
		for(int i=0;i<176;i++)
		{
			records_vz8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vz8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vz8", records_vz8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record171: [a171, b1, c0, d3, e1, f3]
		//record172: [a172, b0, c1, d0, e2, f4]
		//record173: [a173, b1, c2, d1, e3, f5]
		//record174: [a174, b0, c0, d2, e4, f0]
		//record175: [a175, b1, c1, d3, e0, f1]
		String[] ConditionColumns0 = {"e","a","c"};
		String[] ConditionColumnsValues0 = {"e4","a69","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vz8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3]
		int pageCount0 = (int)Math.ceil(176.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vz8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("s5l", cols1);
		String [][] records_s5l = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_s5l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s5l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s5l", records_s5l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		String[] ConditionColumns1 = {"a","c"};
		String[] ConditionColumnsValues1 = {"a4","c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("s5l", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a4, b0, c1, d0]
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
		String selectTrace1 = DBApp.getLastTrace("s5l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s1", cols0);
		String [][] records_s1 = new String[425][cols0.length];
		for(int i=0;i<425;i++)
		{
			records_s1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s1", records_s1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 213, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		int pageCount0 = (int)Math.ceil(425.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("s1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+213));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eb", cols0);
		String [][] records_eb = new String[298][cols0.length];
		for(int i=0;i<298;i++)
		{
			records_eb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eb", records_eb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6]
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("eb", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 42, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a20, b0, c2, d0, e0, f2, g6]
		//record3: [a27, b1, c0, d3, e2, f3, g6]
		//record4: [a34, b0, c1, d2, e4, f4, g6]
		int pageCount0 = (int)Math.ceil(298.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("eb");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("u5ud", cols0);
		String [][] records_u5ud = new String[435][cols0.length];
		for(int i=0;i<435;i++)
		{
			records_u5ud[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u5ud[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u5ud", records_u5ud[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record430: [a430]
		//record431: [a431]
		//record432: [a432]
		//record433: [a433]
		//record434: [a434]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a33"};
		ArrayList<String[]> tableSelect0 = DBApp.select("u5ud", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a33]
		int pageCount0 = (int)Math.ceil(435.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("u5ud");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("lf20", cols1);
		String [][] records_lf20 = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			records_lf20[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lf20[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lf20", records_lf20[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("lf20", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 21, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1]
		int pageCount1 = (int)Math.ceil(83.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("lf20");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+21));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("l3va8", cols2);
		String [][] records_l3va8 = new String[449][cols2.length];
		for(int i=0;i<449;i++)
		{
			records_l3va8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l3va8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l3va8", records_l3va8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record444: [a444, b0, c0]
		//record445: [a445, b1, c1]
		//record446: [a446, b0, c2]
		//record447: [a447, b1, c0]
		//record448: [a448, b0, c1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a205"};
		ArrayList<String[]> tableSelect2 = DBApp.select("l3va8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a205, b1, c1]
		int pageCount2 = (int)Math.ceil(449.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("l3va8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("g4l2", cols3);
		String [][] records_g4l2 = new String[130][cols3.length];
		for(int i=0;i<130;i++)
		{
			records_g4l2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g4l2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g4l2", records_g4l2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0]
		//record126: [a126, b0, c0, d2, e1]
		//record127: [a127, b1, c1, d3, e2]
		//record128: [a128, b0, c2, d0, e3]
		//record129: [a129, b1, c0, d1, e4]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("g4l2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 26, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a20, b0, c2, d0, e0]
		int pageCount3 = (int)Math.ceil(130.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("g4l2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+26));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("noyuy", cols4);
		String [][] records_noyuy = new String[84][cols4.length];
		for(int i=0;i<84;i++)
		{
			records_noyuy[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_noyuy[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("noyuy", records_noyuy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		String[] ConditionColumns4 = {"g"};
		String[] ConditionColumnsValues4 = {"g1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("noyuy", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 12, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		int pageCount4 = (int)Math.ceil(84.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("noyuy");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gl", cols0);
		String [][] records_gl = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			records_gl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gl", records_gl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7]
		String[] ConditionColumns0 = {"g","a","h","d"};
		String[] ConditionColumnsValues0 = {"g1","a78","h6","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a78, b0, c0, d2, e3, f0, g1, h6, i6]
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("gl");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("y08c", cols1);
		String [][] records_y08c = new String[437][cols1.length];
		for(int i=0;i<437;i++)
		{
			records_y08c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y08c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y08c", records_y08c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4]
		String[] ConditionColumns1 = {"i","f"};
		String[] ConditionColumnsValues1 = {"i0","f3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y08c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record1: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record2: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record3: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record4: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9]
		int pageCount1 = (int)Math.ceil(437.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("y08c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("oj144", cols2);
		String [][] records_oj144 = new String[451][cols2.length];
		for(int i=0;i<451;i++)
		{
			records_oj144[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oj144[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oj144", records_oj144[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6]
		String[] ConditionColumns2 = {"l","j","k"};
		String[] ConditionColumnsValues2 = {"l9","j3","k4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("oj144", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9]
		int pageCount2 = (int)Math.ceil(451.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("oj144");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tf9h5", cols3);
		String [][] records_tf9h5 = new String[306][cols3.length];
		for(int i=0;i<306;i++)
		{
			records_tf9h5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tf9h5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tf9h5", records_tf9h5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("tf9h5", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 102, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		int pageCount3 = (int)Math.ceil(306.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("tf9h5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+102));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y0y", cols4);
		String [][] records_y0y = new String[225][cols4.length];
		for(int i=0;i<225;i++)
		{
			records_y0y[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_y0y[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("y0y", records_y0y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3]
		//record221: [a221, b1, c2, d1, e1, f5, g4]
		//record222: [a222, b0, c0, d2, e2, f0, g5]
		//record223: [a223, b1, c1, d3, e3, f1, g6]
		//record224: [a224, b0, c2, d0, e4, f2, g0]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("y0y", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 112, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		int pageCount4 = (int)Math.ceil(225.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("y0y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+112));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("vki", cols0);
		String [][] records_vki = new String[352][cols0.length];
		for(int i=0;i<352;i++)
		{
			records_vki[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vki[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vki", records_vki[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record347: [a347, b1, c2]
		//record348: [a348, b0, c0]
		//record349: [a349, b1, c1]
		//record350: [a350, b0, c2]
		//record351: [a351, b1, c0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vki", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 176, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount0 = (int)Math.ceil(352.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vki");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+176));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("n2m", cols1);
		String [][] records_n2m = new String[406][cols1.length];
		for(int i=0;i<406;i++)
		{
			records_n2m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n2m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n2m", records_n2m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record401: [a401, b1, c2, d1]
		//record402: [a402, b0, c0, d2]
		//record403: [a403, b1, c1, d3]
		//record404: [a404, b0, c2, d0]
		//record405: [a405, b1, c0, d1]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n2m", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 102, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a5, b1, c2, d1]
		//record2: [a9, b1, c0, d1]
		//record3: [a13, b1, c1, d1]
		//record4: [a17, b1, c2, d1]
		int pageCount1 = (int)Math.ceil(406.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("n2m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+102));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ceo", cols0);
		String [][] records_ceo = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_ceo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ceo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ceo", records_ceo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record34: [a34, b0, c1, d2, e4]
		//record35: [a35, b1, c2, d3, e0]
		//record36: [a36, b0, c0, d0, e1]
		//record37: [a37, b1, c1, d1, e2]
		//record38: [a38, b0, c2, d2, e3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ceo", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		int pageCount0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ceo");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a3hl4", cols1);
		String [][] records_a3hl4 = new String[224][cols1.length];
		for(int i=0;i<224;i++)
		{
			records_a3hl4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a3hl4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a3hl4", records_a3hl4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a3hl4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5]
		int pageCount1 = (int)Math.ceil(224.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a3hl4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("v1", cols2);
		String [][] records_v1 = new String[397][cols2.length];
		for(int i=0;i<397;i++)
		{
			records_v1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v1", records_v1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record392: [a392, b0, c2]
		//record393: [a393, b1, c0]
		//record394: [a394, b0, c1]
		//record395: [a395, b1, c2]
		//record396: [a396, b0, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("v1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 199, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount2 = (int)Math.ceil(397.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("v1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+199));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ow7", cols3);
		String [][] records_ow7 = new String[198][cols3.length];
		for(int i=0;i<198;i++)
		{
			records_ow7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ow7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ow7", records_ow7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2, n1]
		String[] ConditionColumns3 = {"i"};
		String[] ConditionColumnsValues3 = {"i2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("ow7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 22, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		int pageCount3 = (int)Math.ceil(198.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("ow7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+22));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vc1j", cols4);
		String [][] records_vc1j = new String[12][cols4.length];
		for(int i=0;i<12;i++)
		{
			records_vc1j[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_vc1j[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("vc1j", records_vc1j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("vc1j", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 6, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		int pageCount4 = (int)Math.ceil(12.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("vc1j");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vd7", cols0);
		String [][] records_vd7 = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			records_vd7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vd7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vd7", records_vd7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6]
		String[] ConditionColumns0 = {"e","a"};
		String[] ConditionColumnsValues0 = {"e3","a33"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vd7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		int pageCount0 = (int)Math.ceil(87.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vd7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("o0127", cols0);
		String [][] records_o0127 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_o0127[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0127[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0127", records_o0127[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		String[] ConditionColumns0 = {"m","i","e","j","h","f","c"};
		String[] ConditionColumnsValues0 = {"m0","i4","e3","j3","h5","f1","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o0127", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("o0127");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ty1y2", cols1);
		String [][] records_ty1y2 = new String[251][cols1.length];
		for(int i=0;i<251;i++)
		{
			records_ty1y2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ty1y2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ty1y2", records_ty1y2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0, n9]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ty1y2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 84, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		int pageCount1 = (int)Math.ceil(251.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ty1y2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+84));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("u167m", cols2);
		String [][] records_u167m = new String[145][cols2.length];
		for(int i=0;i<145;i++)
		{
			records_u167m[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u167m[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u167m", records_u167m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record140: [a140, b0, c2, d0, e0]
		//record141: [a141, b1, c0, d1, e1]
		//record142: [a142, b0, c1, d2, e2]
		//record143: [a143, b1, c2, d3, e3]
		//record144: [a144, b0, c0, d0, e4]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("u167m", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 29, records:
		//record0: [a4, b0, c1, d0, e4]
		//record1: [a9, b1, c0, d1, e4]
		//record2: [a14, b0, c2, d2, e4]
		//record3: [a19, b1, c1, d3, e4]
		//record4: [a24, b0, c0, d0, e4]
		int pageCount2 = (int)Math.ceil(145.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("u167m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+29));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("q473t", cols3);
		String [][] records_q473t = new String[171][cols3.length];
		for(int i=0;i<171;i++)
		{
			records_q473t[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q473t[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q473t", records_q473t[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record166: [a166, b0]
		//record167: [a167, b1]
		//record168: [a168, b0]
		//record169: [a169, b1]
		//record170: [a170, b0]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q473t", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 85, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount3 = (int)Math.ceil(171.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("q473t");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+85));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f5a", cols4);
		String [][] records_f5a = new String[327][cols4.length];
		for(int i=0;i<327;i++)
		{
			records_f5a[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_f5a[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("f5a", records_f5a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record322: [a322, b0, c1, d2, e2, f4, g0]
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		//record324: [a324, b0, c0, d0, e4, f0, g2]
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		String[] ConditionColumns4 = {"b","c","d"};
		String[] ConditionColumnsValues4 = {"b1","c2","d3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("f5a", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 27, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a23, b1, c2, d3, e3, f5, g2]
		//record2: [a35, b1, c2, d3, e0, f5, g0]
		//record3: [a47, b1, c2, d3, e2, f5, g5]
		//record4: [a59, b1, c2, d3, e4, f5, g3]
		int pageCount4 = (int)Math.ceil(327.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("f5a");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vc2", cols0);
		String [][] records_vc2 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_vc2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vc2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vc2", records_vc2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		String[] ConditionColumns0 = {"g","a","c"};
		String[] ConditionColumnsValues0 = {"g0","a21","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vc2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0]
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vc2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("a8h", cols1);
		String [][] records_a8h = new String[282][cols1.length];
		for(int i=0;i<282;i++)
		{
			records_a8h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a8h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a8h", records_a8h[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record277: [a277]
		//record278: [a278]
		//record279: [a279]
		//record280: [a280]
		//record281: [a281]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a107"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a8h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a107]
		int pageCount1 = (int)Math.ceil(282.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a8h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z906", cols2);
		String [][] records_z906 = new String[84][cols2.length];
		for(int i=0;i<84;i++)
		{
			records_z906[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z906[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z906", records_z906[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("z906", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 17, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		int pageCount2 = (int)Math.ceil(84.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("z906");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b79", cols3);
		String [][] records_b79 = new String[429][cols3.length];
		for(int i=0;i<429;i++)
		{
			records_b79[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b79[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b79", records_b79[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8]
		String[] ConditionColumns3 = {"f","l","k","h","i","j"};
		String[] ConditionColumnsValues3 = {"f3","l9","k3","h5","i6","j9"};
		ArrayList<String[]> tableSelect3 = DBApp.select("b79", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9]
		int pageCount3 = (int)Math.ceil(429.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("b79");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("oi3", cols4);
		String [][] records_oi3 = new String[478][cols4.length];
		for(int i=0;i<478;i++)
		{
			records_oi3[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_oi3[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("oi3", records_oi3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("oi3", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 239, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		int pageCount4 = (int)Math.ceil(478.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("oi3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+239));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("mk4s7", cols0);
		String [][] records_mk4s7 = new String[426][cols0.length];
		for(int i=0;i<426;i++)
		{
			records_mk4s7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mk4s7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mk4s7", records_mk4s7[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record421: [a421]
		//record422: [a422]
		//record423: [a423]
		//record424: [a424]
		//record425: [a425]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a146"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mk4s7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a146]
		int pageCount0 = (int)Math.ceil(426.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("mk4s7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v55", cols0);
		String [][] records_v55 = new String[379][cols0.length];
		for(int i=0;i<379;i++)
		{
			records_v55[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v55[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v55", records_v55[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10, n10]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3, m11, n11]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2, l4, m12, n12]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5, m0, n13]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8, k4, l6, m1, n0]
		String[] ConditionColumns0 = {"d","l","e","h","j"};
		String[] ConditionColumnsValues0 = {"d3","l11","e1","h3","j1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("v55", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record1: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1, n5]
		//record2: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13]
		//record3: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7, n7]
		int pageCount0 = (int)Math.ceil(379.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("v55");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("qqku5", cols1);
		String [][] records_qqku5 = new String[165][cols1.length];
		for(int i=0;i<165;i++)
		{
			records_qqku5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qqku5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qqku5", records_qqku5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0]
		//record161: [a161, b1, c2, d1, e1]
		//record162: [a162, b0, c0, d2, e2]
		//record163: [a163, b1, c1, d3, e3]
		//record164: [a164, b0, c2, d0, e4]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("qqku5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 41, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a9, b1, c0, d1, e4]
		//record3: [a13, b1, c1, d1, e3]
		//record4: [a17, b1, c2, d1, e2]
		int pageCount1 = (int)Math.ceil(165.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("qqku5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+41));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("tv10", cols2);
		String [][] records_tv10 = new String[313][cols2.length];
		for(int i=0;i<313;i++)
		{
			records_tv10[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tv10[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tv10", records_tv10[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record308: [a308, b0]
		//record309: [a309, b1]
		//record310: [a310, b0]
		//record311: [a311, b1]
		//record312: [a312, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("tv10", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 157, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount2 = (int)Math.ceil(313.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("tv10");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+157));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i62", cols3);
		String [][] records_i62 = new String[446][cols3.length];
		for(int i=0;i<446;i++)
		{
			records_i62[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_i62[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("i62", records_i62[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4, l0, m2]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5, l1, m3]
		String[] ConditionColumns3 = {"i"};
		String[] ConditionColumnsValues3 = {"i5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("i62", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 49, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		int pageCount3 = (int)Math.ceil(446.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("i62");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+49));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("m8", cols4);
		String [][] records_m8 = new String[58][cols4.length];
		for(int i=0;i<58;i++)
		{
			records_m8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_m8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("m8", records_m8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1]
		String[] ConditionColumns4 = {"h","f"};
		String[] ConditionColumnsValues4 = {"h1","f5"};
		ArrayList<String[]> tableSelect4 = DBApp.select("m8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record1: [a41, b1, c2, d1, e1, f5, g6, h1]
		int pageCount4 = (int)Math.ceil(58.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("m8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("b2d", cols0);
		String [][] records_b2d = new String[385][cols0.length];
		for(int i=0;i<385;i++)
		{
			records_b2d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b2d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b2d", records_b2d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record380: [a380, b0, c2, d0, e0]
		//record381: [a381, b1, c0, d1, e1]
		//record382: [a382, b0, c1, d2, e2]
		//record383: [a383, b1, c2, d3, e3]
		//record384: [a384, b0, c0, d0, e4]
		String[] ConditionColumns0 = {"c","a"};
		String[] ConditionColumnsValues0 = {"c0","a354"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b2d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a354, b0, c0, d2, e4]
		int pageCount0 = (int)Math.ceil(385.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("b2d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e2gg", cols1);
		String [][] records_e2gg = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			records_e2gg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e2gg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e2gg", records_e2gg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6]
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a40"};
		ArrayList<String[]> tableSelect1 = DBApp.select("e2gg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		int pageCount1 = (int)Math.ceil(77.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("e2gg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("t72", cols0);
		String [][] records_t72 = new String[457][cols0.length];
		for(int i=0;i<457;i++)
		{
			records_t72[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t72[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t72", records_t72[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1]
		String[] ConditionColumns0 = {"j","m","e","c"};
		String[] ConditionColumnsValues0 = {"j8","m7","e3","c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("t72", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7]
		int pageCount0 = (int)Math.ceil(457.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("t72");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lyf", cols0);
		String [][] records_lyf = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_lyf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lyf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lyf", records_lyf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		String[] ConditionColumns0 = {"f","a","e","b"};
		String[] ConditionColumnsValues0 = {"f0","a6","e1","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("lyf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		int pageCount0 = (int)Math.ceil(8.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("lyf");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("w187h", cols1);
		String [][] records_w187h = new String[430][cols1.length];
		for(int i=0;i<430;i++)
		{
			records_w187h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w187h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w187h", records_w187h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record425: [a425, b1, c2]
		//record426: [a426, b0, c0]
		//record427: [a427, b1, c1]
		//record428: [a428, b0, c2]
		//record429: [a429, b1, c0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w187h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 144, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		int pageCount1 = (int)Math.ceil(430.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w187h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+144));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("a0", cols0);
		String [][] records_a0 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			records_a0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a0", records_a0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2]
		//record87: [a87, b1, c0, d3, e2, f3]
		//record88: [a88, b0, c1, d0, e3, f4]
		//record89: [a89, b1, c2, d1, e4, f5]
		//record90: [a90, b0, c0, d2, e0, f0]
		String[] ConditionColumns0 = {"b","f"};
		String[] ConditionColumnsValues0 = {"b1","f5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a5, b1, c2, d1, e0, f5]
		//record1: [a11, b1, c2, d3, e1, f5]
		//record2: [a17, b1, c2, d1, e2, f5]
		//record3: [a23, b1, c2, d3, e3, f5]
		//record4: [a29, b1, c2, d1, e4, f5]
		int pageCount0 = (int)Math.ceil(91.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h2i", cols1);
		String [][] records_h2i = new String[329][cols1.length];
		for(int i=0;i<329;i++)
		{
			records_h2i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h2i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h2i", records_h2i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j9"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h2i", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 32, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		int pageCount1 = (int)Math.ceil(329.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("h2i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a2d0", cols2);
		String [][] records_a2d0 = new String[253][cols2.length];
		for(int i=0;i<253;i++)
		{
			records_a2d0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a2d0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a2d0", records_a2d0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a2d0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 36, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6]
		int pageCount2 = (int)Math.ceil(253.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("a2d0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+36));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x9", cols3);
		String [][] records_x9 = new String[261][cols3.length];
		for(int i=0;i<261;i++)
		{
			records_x9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x9", records_x9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0]
		String[] ConditionColumns3 = {"a","d","g","b"};
		String[] ConditionColumnsValues3 = {"a86","d2","g2","b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6]
		int pageCount3 = (int)Math.ceil(261.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("x9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("fwp", cols4);
		String [][] records_fwp = new String[349][cols4.length];
		for(int i=0;i<349;i++)
		{
			records_fwp[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_fwp[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("fwp", records_fwp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record344: [a344, b0, c2, d0, e4, f2]
		//record345: [a345, b1, c0, d1, e0, f3]
		//record346: [a346, b0, c1, d2, e1, f4]
		//record347: [a347, b1, c2, d3, e2, f5]
		//record348: [a348, b0, c0, d0, e3, f0]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("fwp", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 116, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a5, b1, c2, d1, e0, f5]
		//record2: [a8, b0, c2, d0, e3, f2]
		//record3: [a11, b1, c2, d3, e1, f5]
		//record4: [a14, b0, c2, d2, e4, f2]
		int pageCount4 = (int)Math.ceil(349.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("fwp");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+116));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(132);
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