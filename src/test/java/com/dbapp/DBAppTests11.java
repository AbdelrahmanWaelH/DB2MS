package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests11
{

	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("srm7", cols0);
		String [][] records_srm7 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			records_srm7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_srm7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("srm7", records_srm7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record75: [a75, b1, c0, d3]
		//record76: [a76, b0, c1, d0]
		//record77: [a77, b1, c2, d1]
		//record78: [a78, b0, c0, d2]
		//record79: [a79, b1, c1, d3]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a9"};
		ArrayList<String[]> tableSelect0 = DBApp.select("srm7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a9, b1, c0, d1]
		int pageCount0 = (int)Math.ceil(80.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("srm7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("y5fxw", cols1);
		String [][] records_y5fxw = new String[317][cols1.length];
		for(int i=0;i<317;i++)
		{
			records_y5fxw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y5fxw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y5fxw", records_y5fxw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j8"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y5fxw", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 31, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record3: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		int pageCount1 = (int)Math.ceil(317.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("y5fxw");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j3", cols2);
		String [][] records_j3 = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			records_j3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j3", records_j3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("j3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3]
		int pageCount2 = (int)Math.ceil(30.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("j3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p2vt2", cols3);
		String [][] records_p2vt2 = new String[415][cols3.length];
		for(int i=0;i<415;i++)
		{
			records_p2vt2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p2vt2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p2vt2", records_p2vt2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2, m7, n4]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3, m8, n5]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9, n6]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10, n7]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8]
		String[] ConditionColumns3 = {"k","c","i","g","b","e","l"};
		String[] ConditionColumnsValues3 = {"k7","c1","i7","g2","b1","e0","l1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p2vt2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10, n9]
		int pageCount3 = (int)Math.ceil(415.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("p2vt2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ci", cols4);
		String [][] records_ci = new String[102][cols4.length];
		for(int i=0;i<102;i++)
		{
			records_ci[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ci[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ci", records_ci[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ci", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 51, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1]
		int pageCount4 = (int)Math.ceil(102.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("ci");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+51));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m6", cols0);
		String [][] records_m6 = new String[129][cols0.length];
		for(int i=0;i<129;i++)
		{
			records_m6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m6", records_m6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		String[] ConditionColumns0 = {"d","g"};
		String[] ConditionColumnsValues0 = {"d1","g1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("m6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a29, b1, c2, d1, e4, f5, g1]
		//record2: [a57, b1, c0, d1, e2, f3, g1]
		//record3: [a85, b1, c1, d1, e0, f1, g1]
		//record4: [a113, b1, c2, d1, e3, f5, g1]
		int pageCount0 = (int)Math.ceil(129.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("m6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("y28", cols1);
		String [][] records_y28 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_y28[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y28[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y28", records_y28[i]);
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
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y28", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("y28");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+8));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("m6v6l", cols2);
		String [][] records_m6v6l = new String[339][cols2.length];
		for(int i=0;i<339;i++)
		{
			records_m6v6l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m6v6l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m6v6l", records_m6v6l[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record334: [a334]
		//record335: [a335]
		//record336: [a336]
		//record337: [a337]
		//record338: [a338]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a18"};
		ArrayList<String[]> tableSelect2 = DBApp.select("m6v6l", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a18]
		int pageCount2 = (int)Math.ceil(339.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("m6v6l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("k43", cols0);
		String [][] records_k43 = new String[124][cols0.length];
		for(int i=0;i<124;i++)
		{
			records_k43[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k43[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k43", records_k43[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record119: [a119, b1, c2]
		//record120: [a120, b0, c0]
		//record121: [a121, b1, c1]
		//record122: [a122, b0, c2]
		//record123: [a123, b1, c0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k43", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 41, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		int pageCount0 = (int)Math.ceil(124.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("k43");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+41));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k00", cols1);
		String [][] records_k00 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			records_k00[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k00[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k00", records_k00[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8]
		String[] ConditionColumns1 = {"b","h"};
		String[] ConditionColumnsValues1 = {"b0","h4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k00", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		int pageCount1 = (int)Math.ceil(69.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("k00");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n85", cols2);
		String [][] records_n85 = new String[410][cols2.length];
		for(int i=0;i<410;i++)
		{
			records_n85[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n85[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n85", records_n85[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6]
		//record406: [a406, b0, c1, d2, e1, f4, g0]
		//record407: [a407, b1, c2, d3, e2, f5, g1]
		//record408: [a408, b0, c0, d0, e3, f0, g2]
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("n85", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 68, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		int pageCount2 = (int)Math.ceil(410.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("n85");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+68));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("er", cols3);
		String [][] records_er = new String[281][cols3.length];
		for(int i=0;i<281;i++)
		{
			records_er[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_er[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("er", records_er[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3, n10, o6]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7, k2, l1, m4, n11, o7]
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2, m5, n12, o8]
		//record279: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9, k4, l3, m6, n13, o9]
		//record280: [a280, b0, c1, d0, e0, f4, g0, h0, i1, j0, k5, l4, m7, n0, o10]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("er", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 93, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		int pageCount3 = (int)Math.ceil(281.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("er");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+93));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mx", cols0);
		String [][] records_mx = new String[401][cols0.length];
		for(int i=0;i<401;i++)
		{
			records_mx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mx", records_mx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record396: [a396, b0, c0, d0, e1, f0, g4]
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		//record400: [a400, b0, c1, d0, e0, f4, g1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mx", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 200, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		int pageCount0 = (int)Math.ceil(401.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("mx");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+200));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("w9i", cols0);
		String [][] records_w9i = new String[274][cols0.length];
		for(int i=0;i<274;i++)
		{
			records_w9i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w9i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w9i", records_w9i[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record269: [a269, b1]
		//record270: [a270, b0]
		//record271: [a271, b1]
		//record272: [a272, b0]
		//record273: [a273, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w9i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 137, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount0 = (int)Math.ceil(274.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("w9i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+137));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wp4a", cols1);
		String [][] records_wp4a = new String[431][cols1.length];
		for(int i=0;i<431;i++)
		{
			records_wp4a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wp4a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wp4a", records_wp4a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wp4a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 216, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		int pageCount1 = (int)Math.ceil(431.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("wp4a");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+216));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("tj9", cols2);
		String [][] records_tj9 = new String[259][cols2.length];
		for(int i=0;i<259;i++)
		{
			records_tj9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tj9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tj9", records_tj9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("tj9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 32, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		int pageCount2 = (int)Math.ceil(259.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("tj9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vqg", cols3);
		String [][] records_vqg = new String[14][cols3.length];
		for(int i=0;i<14;i++)
		{
			records_vqg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vqg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vqg", records_vqg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		String[] ConditionColumns3 = {"i","g"};
		String[] ConditionColumnsValues3 = {"i3","g3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("vqg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		int pageCount3 = (int)Math.ceil(14.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("vqg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("m044", cols0);
		String [][] records_m044 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			records_m044[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m044[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m044", records_m044[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record75: [a75, b1, c0, d3, e0, f3]
		//record76: [a76, b0, c1, d0, e1, f4]
		//record77: [a77, b1, c2, d1, e2, f5]
		//record78: [a78, b0, c0, d2, e3, f0]
		//record79: [a79, b1, c1, d3, e4, f1]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a79"};
		ArrayList<String[]> tableSelect0 = DBApp.select("m044", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a79, b1, c1, d3, e4, f1]
		int pageCount0 = (int)Math.ceil(80.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("m044");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("g3ua", cols1);
		String [][] records_g3ua = new String[356][cols1.length];
		for(int i=0;i<356;i++)
		{
			records_g3ua[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g3ua[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g3ua", records_g3ua[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record351: [a351, b1, c0]
		//record352: [a352, b0, c1]
		//record353: [a353, b1, c2]
		//record354: [a354, b0, c0]
		//record355: [a355, b1, c1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("g3ua", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 118, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		int pageCount1 = (int)Math.ceil(356.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("g3ua");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+118));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("o6", cols2);
		String [][] records_o6 = new String[187][cols2.length];
		for(int i=0;i<187;i++)
		{
			records_o6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o6", records_o6[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record182: [a182, b0]
		//record183: [a183, b1]
		//record184: [a184, b0]
		//record185: [a185, b1]
		//record186: [a186, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 94, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount2 = (int)Math.ceil(187.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("o6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+94));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("nm", cols3);
		String [][] records_nm = new String[60][cols3.length];
		for(int i=0;i<60;i++)
		{
			records_nm[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_nm[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("nm", records_nm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2]
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3]
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("nm", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 30, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		int pageCount3 = (int)Math.ceil(60.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("nm");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+30));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("u0k6", cols4);
		String [][] records_u0k6 = new String[21][cols4.length];
		for(int i=0;i<21;i++)
		{
			records_u0k6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u0k6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u0k6", records_u0k6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		String[] ConditionColumns4 = {"h","d","g","e"};
		String[] ConditionColumnsValues4 = {"h4","d0","g5","e2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("u0k6", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		int pageCount4 = (int)Math.ceil(21.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("u0k6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e7d", cols0);
		String [][] records_e7d = new String[104][cols0.length];
		for(int i=0;i<104;i++)
		{
			records_e7d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e7d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e7d", records_e7d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5]
		String[] ConditionColumns0 = {"n"};
		String[] ConditionColumnsValues0 = {"n13"};
		ArrayList<String[]> tableSelect0 = DBApp.select("e7d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record1: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record2: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13]
		//record3: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13]
		//record4: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13]
		int pageCount0 = (int)Math.ceil(104.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("e7d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+7));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("su2", cols1);
		String [][] records_su2 = new String[396][cols1.length];
		for(int i=0;i<396;i++)
		{
			records_su2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_su2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("su2", records_su2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13, o1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1, o3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2, o4]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10, l11, m5, n3, o5]
		String[] ConditionColumns1 = {"o"};
		String[] ConditionColumnsValues1 = {"o1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("su2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record3: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1]
		//record4: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1]
		int pageCount1 = (int)Math.ceil(396.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("su2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hc6g", cols2);
		String [][] records_hc6g = new String[121][cols2.length];
		for(int i=0;i<121;i++)
		{
			records_hc6g[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hc6g[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hc6g", records_hc6g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0]
		String[] ConditionColumns2 = {"j"};
		String[] ConditionColumnsValues2 = {"j8"};
		ArrayList<String[]> tableSelect2 = DBApp.select("hc6g", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 12, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record3: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8]
		int pageCount2 = (int)Math.ceil(121.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("hc6g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("m39", cols3);
		String [][] records_m39 = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			records_m39[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m39[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m39", records_m39[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record46: [a46]
		//record47: [a47]
		//record48: [a48]
		//record49: [a49]
		//record50: [a50]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a37"};
		ArrayList<String[]> tableSelect3 = DBApp.select("m39", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a37]
		int pageCount3 = (int)Math.ceil(51.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("m39");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("a526", cols0);
		String [][] records_a526 = new String[422][cols0.length];
		for(int i=0;i<422;i++)
		{
			records_a526[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a526[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a526", records_a526[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record417: [a417, b1, c0, d1, e2]
		//record418: [a418, b0, c1, d2, e3]
		//record419: [a419, b1, c2, d3, e4]
		//record420: [a420, b0, c0, d0, e0]
		//record421: [a421, b1, c1, d1, e1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a526", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 211, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a2, b0, c2, d2, e2]
		//record2: [a4, b0, c1, d0, e4]
		//record3: [a6, b0, c0, d2, e1]
		//record4: [a8, b0, c2, d0, e3]
		int pageCount0 = (int)Math.ceil(422.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a526");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+211));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("v17yz", cols0);
		String [][] records_v17yz = new String[178][cols0.length];
		for(int i=0;i<178;i++)
		{
			records_v17yz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v17yz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v17yz", records_v17yz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1, l9]
		String[] ConditionColumns0 = {"i","e","c","h","b"};
		String[] ConditionColumnsValues0 = {"i1","e3","c1","h1","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("v17yz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1]
		int pageCount0 = (int)Math.ceil(178.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("v17yz");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("u5", cols1);
		String [][] records_u5 = new String[347][cols1.length];
		for(int i=0;i<347;i++)
		{
			records_u5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u5", records_u5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 174, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		int pageCount1 = (int)Math.ceil(347.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("u5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+174));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("o4x2", cols0);
		String [][] records_o4x2 = new String[486][cols0.length];
		for(int i=0;i<486;i++)
		{
			records_o4x2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o4x2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o4x2", records_o4x2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record481: [a481, b1, c1, d1, e1, f1]
		//record482: [a482, b0, c2, d2, e2, f2]
		//record483: [a483, b1, c0, d3, e3, f3]
		//record484: [a484, b0, c1, d0, e4, f4]
		//record485: [a485, b1, c2, d1, e0, f5]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o4x2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 162, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a7, b1, c1, d3, e2, f1]
		//record3: [a10, b0, c1, d2, e0, f4]
		//record4: [a13, b1, c1, d1, e3, f1]
		int pageCount0 = (int)Math.ceil(486.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("o4x2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+162));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l60", cols1);
		String [][] records_l60 = new String[295][cols1.length];
		for(int i=0;i<295;i++)
		{
			records_l60[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l60[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l60", records_l60[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8]
		String[] ConditionColumns1 = {"e","g"};
		String[] ConditionColumnsValues1 = {"e1","g5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l60", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record1: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9]
		//record2: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5]
		//record3: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record4: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10]
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
		String selectTrace1 = DBApp.getLastTrace("l60");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+8));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("b4f", cols2);
		String [][] records_b4f = new String[93][cols2.length];
		for(int i=0;i<93;i++)
		{
			records_b4f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b4f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b4f", records_b4f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record88: [a88, b0, c1, d0]
		//record89: [a89, b1, c2, d1]
		//record90: [a90, b0, c0, d2]
		//record91: [a91, b1, c1, d3]
		//record92: [a92, b0, c2, d0]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("b4f", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 31, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		int pageCount2 = (int)Math.ceil(93.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("b4f");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+31));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("h8k1k", cols3);
		String [][] records_h8k1k = new String[286][cols3.length];
		for(int i=0;i<286;i++)
		{
			records_h8k1k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h8k1k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h8k1k", records_h8k1k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5]
		String[] ConditionColumns3 = {"d","c","b"};
		String[] ConditionColumnsValues3 = {"d1","c1","b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("h8k1k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 24, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1]
		int pageCount3 = (int)Math.ceil(286.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("h8k1k");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("fs", cols0);
		String [][] records_fs = new String[179][cols0.length];
		for(int i=0;i<179;i++)
		{
			records_fs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fs", records_fs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0]
		//record175: [a175, b1, c1, d3, e0, f1]
		//record176: [a176, b0, c2, d0, e1, f2]
		//record177: [a177, b1, c0, d1, e2, f3]
		//record178: [a178, b0, c1, d2, e3, f4]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fs", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 89, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a5, b1, c2, d1, e0, f5]
		//record3: [a7, b1, c1, d3, e2, f1]
		//record4: [a9, b1, c0, d1, e4, f3]
		int pageCount0 = (int)Math.ceil(179.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fs");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+89));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("i84", cols1);
		String [][] records_i84 = new String[266][cols1.length];
		for(int i=0;i<266;i++)
		{
			records_i84[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i84[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i84", records_i84[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record261: [a261, b1, c0, d1]
		//record262: [a262, b0, c1, d2]
		//record263: [a263, b1, c2, d3]
		//record264: [a264, b0, c0, d0]
		//record265: [a265, b1, c1, d1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("i84", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 89, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a4, b0, c1, d0]
		//record2: [a7, b1, c1, d3]
		//record3: [a10, b0, c1, d2]
		//record4: [a13, b1, c1, d1]
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
		String selectTrace1 = DBApp.getLastTrace("i84");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+89));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("cw6x", cols0);
		String [][] records_cw6x = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			records_cw6x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cw6x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cw6x", records_cw6x[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record91: [a91]
		//record92: [a92]
		//record93: [a93]
		//record94: [a94]
		//record95: [a95]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a56"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cw6x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a56]
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("cw6x");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q53j7", cols1);
		String [][] records_q53j7 = new String[367][cols1.length];
		for(int i=0;i<367;i++)
		{
			records_q53j7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q53j7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q53j7", records_q53j7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q53j7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3]
		int pageCount1 = (int)Math.ceil(367.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("q53j7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yb", cols2);
		String [][] records_yb = new String[415][cols2.length];
		for(int i=0;i<415;i++)
		{
			records_yb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yb", records_yb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("yb", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 69, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		int pageCount2 = (int)Math.ceil(415.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("yb");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+69));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k7t", cols3);
		String [][] records_k7t = new String[89][cols3.length];
		for(int i=0;i<89;i++)
		{
			records_k7t[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k7t[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k7t", records_k7t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("k7t", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 44, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		int pageCount3 = (int)Math.ceil(89.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("k7t");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+44));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zo3", cols0);
		String [][] records_zo3 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			records_zo3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zo3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zo3", records_zo3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		String[] ConditionColumns0 = {"e","c","h","k","b"};
		String[] ConditionColumnsValues0 = {"e0","c1","h2","k10","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zo3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		int pageCount0 = (int)Math.ceil(11.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("zo3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("f8854", cols1);
		String [][] records_f8854 = new String[292][cols1.length];
		for(int i=0;i<292;i++)
		{
			records_f8854[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f8854[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f8854", records_f8854[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record287: [a287]
		//record288: [a288]
		//record289: [a289]
		//record290: [a290]
		//record291: [a291]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a240"};
		ArrayList<String[]> tableSelect1 = DBApp.select("f8854", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a240]
		int pageCount1 = (int)Math.ceil(292.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("f8854");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bq5", cols2);
		String [][] records_bq5 = new String[415][cols2.length];
		for(int i=0;i<415;i++)
		{
			records_bq5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bq5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bq5", records_bq5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6]
		String[] ConditionColumns2 = {"g","a","f"};
		String[] ConditionColumnsValues2 = {"g5","a299","f5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("bq5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a299, b1, c2, d3, e4, f5, g5, h3]
		int pageCount2 = (int)Math.ceil(415.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("bq5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w717", cols3);
		String [][] records_w717 = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			records_w717[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w717[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w717", records_w717[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("w717", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 28, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		int pageCount3 = (int)Math.ceil(56.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("w717");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("rc8ip", cols0);
		String [][] records_rc8ip = new String[427][cols0.length];
		for(int i=0;i<427;i++)
		{
			records_rc8ip[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rc8ip[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rc8ip", records_rc8ip[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record422: [a422, b0, c2, d2, e2]
		//record423: [a423, b1, c0, d3, e3]
		//record424: [a424, b0, c1, d0, e4]
		//record425: [a425, b1, c2, d1, e0]
		//record426: [a426, b0, c0, d2, e1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rc8ip", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 214, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a2, b0, c2, d2, e2]
		//record2: [a4, b0, c1, d0, e4]
		//record3: [a6, b0, c0, d2, e1]
		//record4: [a8, b0, c2, d0, e3]
		int pageCount0 = (int)Math.ceil(427.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("rc8ip");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+214));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kpt5j", cols1);
		String [][] records_kpt5j = new String[224][cols1.length];
		for(int i=0;i<224;i++)
		{
			records_kpt5j[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kpt5j[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kpt5j", records_kpt5j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7]
		String[] ConditionColumns1 = {"h","f","i","b","k"};
		String[] ConditionColumnsValues1 = {"h2","f2","i8","b0","k4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("kpt5j", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
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
		String selectTrace1 = DBApp.getLastTrace("kpt5j");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("orc", cols2);
		String [][] records_orc = new String[439][cols2.length];
		for(int i=0;i<439;i++)
		{
			records_orc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_orc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("orc", records_orc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("orc", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 55, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		int pageCount2 = (int)Math.ceil(439.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("orc");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+55));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("w2oq", cols0);
		String [][] records_w2oq = new String[300][cols0.length];
		for(int i=0;i<300;i++)
		{
			records_w2oq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w2oq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w2oq", records_w2oq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record295: [a295, b1, c1]
		//record296: [a296, b0, c2]
		//record297: [a297, b1, c0]
		//record298: [a298, b0, c1]
		//record299: [a299, b1, c2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w2oq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 100, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		int pageCount0 = (int)Math.ceil(300.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("w2oq");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+100));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("w55", cols1);
		String [][] records_w55 = new String[200][cols1.length];
		for(int i=0;i<200;i++)
		{
			records_w55[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w55[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w55", records_w55[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record195: [a195, b1]
		//record196: [a196, b0]
		//record197: [a197, b1]
		//record198: [a198, b0]
		//record199: [a199, b1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w55", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 100, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount1 = (int)Math.ceil(200.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w55");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+100));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("mro69", cols0);
		String [][] records_mro69 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_mro69[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mro69[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mro69", records_mro69[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mro69", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		int pageCount0 = (int)Math.ceil(2.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("mro69");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("hs76l", cols1);
		String [][] records_hs76l = new String[353][cols1.length];
		for(int i=0;i<353;i++)
		{
			records_hs76l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hs76l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hs76l", records_hs76l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0, n1]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1, n2]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("hs76l", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		int pageCount1 = (int)Math.ceil(353.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("hs76l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+39));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yw62", cols2);
		String [][] records_yw62 = new String[136][cols2.length];
		for(int i=0;i<136;i++)
		{
			records_yw62[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yw62[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yw62", records_yw62[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("yw62", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 20, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4]
		int pageCount2 = (int)Math.ceil(136.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("yw62");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+20));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("z3426", cols3);
		String [][] records_z3426 = new String[158][cols3.length];
		for(int i=0;i<158;i++)
		{
			records_z3426[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z3426[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z3426", records_z3426[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z3426", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 39, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		int pageCount3 = (int)Math.ceil(158.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("z3426");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+39));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("h2", cols0);
		String [][] records_h2 = new String[370][cols0.length];
		for(int i=0;i<370;i++)
		{
			records_h2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h2", records_h2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5]
		//record366: [a366, b0, c0, d2, e1, f0]
		//record367: [a367, b1, c1, d3, e2, f1]
		//record368: [a368, b0, c2, d0, e3, f2]
		//record369: [a369, b1, c0, d1, e4, f3]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 61, records:
		//record0: [a4, b0, c1, d0, e4, f4]
		//record1: [a10, b0, c1, d2, e0, f4]
		//record2: [a16, b0, c1, d0, e1, f4]
		//record3: [a22, b0, c1, d2, e2, f4]
		//record4: [a28, b0, c1, d0, e3, f4]
		int pageCount0 = (int)Math.ceil(370.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("h2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+61));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("frx0", cols1);
		String [][] records_frx0 = new String[456][cols1.length];
		for(int i=0;i<456;i++)
		{
			records_frx0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_frx0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("frx0", records_frx0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record451: [a451, b1, c1]
		//record452: [a452, b0, c2]
		//record453: [a453, b1, c0]
		//record454: [a454, b0, c1]
		//record455: [a455, b1, c2]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a346"};
		ArrayList<String[]> tableSelect1 = DBApp.select("frx0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a346, b0, c1]
		int pageCount1 = (int)Math.ceil(456.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("frx0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ok", cols2);
		String [][] records_ok = new String[455][cols2.length];
		for(int i=0;i<455;i++)
		{
			records_ok[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ok[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ok", records_ok[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ok", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 75, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		int pageCount2 = (int)Math.ceil(455.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ok");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+75));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ory", cols0);
		String [][] records_ory = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_ory[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ory[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ory", records_ory[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ory", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		int pageCount0 = (int)Math.ceil(47.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ory");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ugv17", cols1);
		String [][] records_ugv17 = new String[189][cols1.length];
		for(int i=0;i<189;i++)
		{
			records_ugv17[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ugv17[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ugv17", records_ugv17[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ugv17", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		int pageCount1 = (int)Math.ceil(189.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ugv17");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qo5i", cols2);
		String [][] records_qo5i = new String[474][cols2.length];
		for(int i=0;i<474;i++)
		{
			records_qo5i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qo5i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qo5i", records_qo5i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record469: [a469, b1, c1, d1, e4, f1, g0]
		//record470: [a470, b0, c2, d2, e0, f2, g1]
		//record471: [a471, b1, c0, d3, e1, f3, g2]
		//record472: [a472, b0, c1, d0, e2, f4, g3]
		//record473: [a473, b1, c2, d1, e3, f5, g4]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qo5i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 118, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		int pageCount2 = (int)Math.ceil(474.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("qo5i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+118));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("xw2", cols0);
		String [][] records_xw2 = new String[278][cols0.length];
		for(int i=0;i<278;i++)
		{
			records_xw2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xw2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xw2", records_xw2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record273: [a273, b1, c0]
		//record274: [a274, b0, c1]
		//record275: [a275, b1, c2]
		//record276: [a276, b0, c0]
		//record277: [a277, b1, c1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xw2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 139, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		int pageCount0 = (int)Math.ceil(278.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("xw2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+139));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("vdiq", cols0);
		String [][] records_vdiq = new String[132][cols0.length];
		for(int i=0;i<132;i++)
		{
			records_vdiq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vdiq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vdiq", records_vdiq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record127: [a127, b1, c1, d3]
		//record128: [a128, b0, c2, d0]
		//record129: [a129, b1, c0, d1]
		//record130: [a130, b0, c1, d2]
		//record131: [a131, b1, c2, d3]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vdiq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 66, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		int pageCount0 = (int)Math.ceil(132.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vdiq");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+66));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j2", cols0);
		String [][] records_j2 = new String[489][cols0.length];
		for(int i=0;i<489;i++)
		{
			records_j2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j2", records_j2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 81, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		int pageCount0 = (int)Math.ceil(489.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("j2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+81));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f256", cols1);
		String [][] records_f256 = new String[337][cols1.length];
		for(int i=0;i<337;i++)
		{
			records_f256[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f256[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f256", records_f256[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10, o2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12, o4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13, o5]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("f256", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 67, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		int pageCount1 = (int)Math.ceil(337.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("f256");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+67));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("uj", cols2);
		String [][] records_uj = new String[374][cols2.length];
		for(int i=0;i<374;i++)
		{
			records_uj[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_uj[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("uj", records_uj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record369: [a369, b1, c0, d1, e4]
		//record370: [a370, b0, c1, d2, e0]
		//record371: [a371, b1, c2, d3, e1]
		//record372: [a372, b0, c0, d0, e2]
		//record373: [a373, b1, c1, d1, e3]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("uj", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 75, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a7, b1, c1, d3, e2]
		//record2: [a12, b0, c0, d0, e2]
		//record3: [a17, b1, c2, d1, e2]
		//record4: [a22, b0, c1, d2, e2]
		int pageCount2 = (int)Math.ceil(374.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("uj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+75));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c94", cols0);
		String [][] records_c94 = new String[401][cols0.length];
		for(int i=0;i<401;i++)
		{
			records_c94[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c94[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c94", records_c94[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record396: [a396, b0, c0, d0, e1, f0, g4]
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		//record400: [a400, b0, c1, d0, e0, f4, g1]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c94", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 100, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		int pageCount0 = (int)Math.ceil(401.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("c94");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+100));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("ng3", cols1);
		String [][] records_ng3 = new String[370][cols1.length];
		for(int i=0;i<370;i++)
		{
			records_ng3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ng3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ng3", records_ng3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5]
		//record366: [a366, b0, c0, d2, e1, f0]
		//record367: [a367, b1, c1, d3, e2, f1]
		//record368: [a368, b0, c2, d0, e3, f2]
		//record369: [a369, b1, c0, d1, e4, f3]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ng3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 93, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a5, b1, c2, d1, e0, f5]
		//record2: [a9, b1, c0, d1, e4, f3]
		//record3: [a13, b1, c1, d1, e3, f1]
		//record4: [a17, b1, c2, d1, e2, f5]
		int pageCount1 = (int)Math.ceil(370.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ng3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+93));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("m5701", cols2);
		String [][] records_m5701 = new String[273][cols2.length];
		for(int i=0;i<273;i++)
		{
			records_m5701[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m5701[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m5701", records_m5701[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record268: [a268, b0, c1, d0, e3]
		//record269: [a269, b1, c2, d1, e4]
		//record270: [a270, b0, c0, d2, e0]
		//record271: [a271, b1, c1, d3, e1]
		//record272: [a272, b0, c2, d0, e2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("m5701", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 91, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		int pageCount2 = (int)Math.ceil(273.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("m5701");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+91));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tm6", cols3);
		String [][] records_tm6 = new String[145][cols3.length];
		for(int i=0;i<145;i++)
		{
			records_tm6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tm6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tm6", records_tm6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4]
		String[] ConditionColumns3 = {"c","g"};
		String[] ConditionColumnsValues3 = {"c0","g4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("tm6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record1: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		//record2: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0]
		//record3: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1]
		//record4: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2]
		int pageCount3 = (int)Math.ceil(145.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("tm6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+7));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("ty", cols4);
		String [][] records_ty = new String[251][cols4.length];
		for(int i=0;i<251;i++)
		{
			records_ty[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ty[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ty", records_ty[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record246: [a246, b0]
		//record247: [a247, b1]
		//record248: [a248, b0]
		//record249: [a249, b1]
		//record250: [a250, b0]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a26"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ty", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a26, b0]
		int pageCount4 = (int)Math.ceil(251.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("ty");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a3e", cols0);
		String [][] records_a3e = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			records_a3e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a3e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a3e", records_a3e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a3e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1]
		int pageCount0 = (int)Math.ceil(52.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a3e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ka", cols0);
		String [][] records_ka = new String[370][cols0.length];
		for(int i=0;i<370;i++)
		{
			records_ka[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ka[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ka", records_ka[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record365: [a365, b1, c2, d1]
		//record366: [a366, b0, c0, d2]
		//record367: [a367, b1, c1, d3]
		//record368: [a368, b0, c2, d0]
		//record369: [a369, b1, c0, d1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ka", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 185, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		int pageCount0 = (int)Math.ceil(370.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ka");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+185));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ihx", cols1);
		String [][] records_ihx = new String[173][cols1.length];
		for(int i=0;i<173;i++)
		{
			records_ihx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ihx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ihx", records_ihx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ihx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 18, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		int pageCount1 = (int)Math.ceil(173.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ihx");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+18));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qo3y", cols2);
		String [][] records_qo3y = new String[112][cols2.length];
		for(int i=0;i<112;i++)
		{
			records_qo3y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qo3y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qo3y", records_qo3y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qo3y", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 28, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		int pageCount2 = (int)Math.ceil(112.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("qo3y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("t93", cols0);
		String [][] records_t93 = new String[431][cols0.length];
		for(int i=0;i<431;i++)
		{
			records_t93[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t93[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t93", records_t93[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("t93", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 36, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9]
		int pageCount0 = (int)Math.ceil(431.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("t93");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+36));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a1s", cols1);
		String [][] records_a1s = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_a1s[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a1s[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a1s", records_a1s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		String[] ConditionColumns1 = {"k","g","i"};
		String[] ConditionColumnsValues1 = {"k4","g2","i1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a1s", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		int pageCount1 = (int)Math.ceil(47.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a1s");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lle", cols2);
		String [][] records_lle = new String[319][cols2.length];
		for(int i=0;i<319;i++)
		{
			records_lle[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lle[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lle", records_lle[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("lle", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 106, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		int pageCount2 = (int)Math.ceil(319.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("lle");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+106));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("gj", cols0);
		String [][] records_gj = new String[335][cols0.length];
		for(int i=0;i<335;i++)
		{
			records_gj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gj", records_gj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12]
		String[] ConditionColumns0 = {"h","g","l","a","m","c","e"};
		String[] ConditionColumnsValues0 = {"h0","g6","l0","a48","m9","c0","e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6]
		int pageCount0 = (int)Math.ceil(335.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("gj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p0", cols1);
		String [][] records_p0 = new String[262][cols1.length];
		for(int i=0;i<262;i++)
		{
			records_p0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p0", records_p0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9, m1]
		String[] ConditionColumns1 = {"b","j","h"};
		String[] ConditionColumnsValues1 = {"b1","j5","h7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("p0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record1: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3]
		//record2: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4]
		//record3: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5]
		//record4: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6]
		int pageCount1 = (int)Math.ceil(262.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("p0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+7));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y3g", cols2);
		String [][] records_y3g = new String[440][cols2.length];
		for(int i=0;i<440;i++)
		{
			records_y3g[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y3g[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y3g", records_y3g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9]
		String[] ConditionColumns2 = {"j"};
		String[] ConditionColumnsValues2 = {"j3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y3g", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 44, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3]
		int pageCount2 = (int)Math.ceil(440.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("y3g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+44));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("quu", cols3);
		String [][] records_quu = new String[425][cols3.length];
		for(int i=0;i<425;i++)
		{
			records_quu[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_quu[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("quu", records_quu[i]);
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
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("quu", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 61, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		int pageCount3 = (int)Math.ceil(425.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("quu");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+61));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xwp", cols4);
		String [][] records_xwp = new String[464][cols4.length];
		for(int i=0;i<464;i++)
		{
			records_xwp[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xwp[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xwp", records_xwp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0, j9, k8, l3, m4, n11]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1, j0, k9, l4, m5, n12]
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2, j1, k10, l5, m6, n13]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0, l6, m7, n0]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7, m8, n1]
		String[] ConditionColumns4 = {"l"};
		String[] ConditionColumnsValues4 = {"l6"};
		ArrayList<String[]> tableSelect4 = DBApp.select("xwp", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 39, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		int pageCount4 = (int)Math.ceil(464.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("xwp");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+39));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ow73q", cols0);
		String [][] records_ow73q = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_ow73q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ow73q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ow73q", records_ow73q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ow73q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		int pageCount0 = (int)Math.ceil(42.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ow73q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+7));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k3d", cols0);
		String [][] records_k3d = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_k3d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k3d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k3d", records_k3d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k3d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		int pageCount0 = (int)Math.ceil(46.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("k3d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f4", cols1);
		String [][] records_f4 = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			records_f4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f4", records_f4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1]
		String[] ConditionColumns1 = {"n"};
		String[] ConditionColumnsValues1 = {"n9"};
		ArrayList<String[]> tableSelect1 = DBApp.select("f4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 4, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9]
		int pageCount1 = (int)Math.ceil(58.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("f4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e05en", cols0);
		String [][] records_e05en = new String[158][cols0.length];
		for(int i=0;i<158;i++)
		{
			records_e05en[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e05en[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e05en", records_e05en[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6]
		//record154: [a154, b0, c1, d2, e4, f4, g0]
		//record155: [a155, b1, c2, d3, e0, f5, g1]
		//record156: [a156, b0, c0, d0, e1, f0, g2]
		//record157: [a157, b1, c1, d1, e2, f1, g3]
		String[] ConditionColumns0 = {"f","a"};
		String[] ConditionColumnsValues0 = {"f5","a95"};
		ArrayList<String[]> tableSelect0 = DBApp.select("e05en", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a95, b1, c2, d3, e0, f5, g4]
		int pageCount0 = (int)Math.ceil(158.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("e05en");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sn1", cols1);
		String [][] records_sn1 = new String[86][cols1.length];
		for(int i=0;i<86;i++)
		{
			records_sn1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sn1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sn1", records_sn1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("sn1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 29, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		int pageCount1 = (int)Math.ceil(86.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("sn1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+29));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("n4t", cols2);
		String [][] records_n4t = new String[127][cols2.length];
		for(int i=0;i<127;i++)
		{
			records_n4t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n4t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n4t", records_n4t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record122: [a122, b0, c2]
		//record123: [a123, b1, c0]
		//record124: [a124, b0, c1]
		//record125: [a125, b1, c2]
		//record126: [a126, b0, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("n4t", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 63, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		int pageCount2 = (int)Math.ceil(127.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("n4t");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+63));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mw46", cols3);
		String [][] records_mw46 = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			records_mw46[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_mw46[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("mw46", records_mw46[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("mw46", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		int pageCount3 = (int)Math.ceil(2.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("mw46");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w17q0", cols4);
		String [][] records_w17q0 = new String[472][cols4.length];
		for(int i=0;i<472;i++)
		{
			records_w17q0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_w17q0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("w17q0", records_w17q0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12, n5]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9]
		String[] ConditionColumns4 = {"j"};
		String[] ConditionColumnsValues4 = {"j3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("w17q0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 47, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1]
		int pageCount4 = (int)Math.ceil(472.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("w17q0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+47));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("mtg", cols0);
		String [][] records_mtg = new String[462][cols0.length];
		for(int i=0;i<462;i++)
		{
			records_mtg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mtg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mtg", records_mtg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record457: [a457, b1, c1, d1]
		//record458: [a458, b0, c2, d2]
		//record459: [a459, b1, c0, d3]
		//record460: [a460, b0, c1, d0]
		//record461: [a461, b1, c2, d1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mtg", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 154, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a4, b0, c1, d0]
		//record2: [a7, b1, c1, d3]
		//record3: [a10, b0, c1, d2]
		//record4: [a13, b1, c1, d1]
		int pageCount0 = (int)Math.ceil(462.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("mtg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+154));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u3m", cols1);
		String [][] records_u3m = new String[420][cols1.length];
		for(int i=0;i<420;i++)
		{
			records_u3m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u3m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u3m", records_u3m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9, k1, l11]
		String[] ConditionColumns1 = {"j","h","c","i","d"};
		String[] ConditionColumnsValues1 = {"j9","h1","c1","i7","d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u3m", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1]
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
		String selectTrace1 = DBApp.getLastTrace("u3m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("y8vr", cols2);
		String [][] records_y8vr = new String[470][cols2.length];
		for(int i=0;i<470;i++)
		{
			records_y8vr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y8vr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y8vr", records_y8vr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record465: [a465, b1, c0, d1, e0, f3]
		//record466: [a466, b0, c1, d2, e1, f4]
		//record467: [a467, b1, c2, d3, e2, f5]
		//record468: [a468, b0, c0, d0, e3, f0]
		//record469: [a469, b1, c1, d1, e4, f1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a292"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y8vr", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a292, b0, c1, d0, e2, f4]
		int pageCount2 = (int)Math.ceil(470.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("y8vr");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("fj405", cols0);
		String [][] records_fj405 = new String[221][cols0.length];
		for(int i=0;i<221;i++)
		{
			records_fj405[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fj405[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fj405", records_fj405[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record216: [a216]
		//record217: [a217]
		//record218: [a218]
		//record219: [a219]
		//record220: [a220]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a104"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fj405", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a104]
		int pageCount0 = (int)Math.ceil(221.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fj405");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wgwc", cols1);
		String [][] records_wgwc = new String[409][cols1.length];
		for(int i=0;i<409;i++)
		{
			records_wgwc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wgwc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wgwc", records_wgwc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9, m2, n13]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10, m3, n0]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11, m4, n1]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0, m5, n2]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wgwc", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 68, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		int pageCount1 = (int)Math.ceil(409.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("wgwc");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+68));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s68", cols2);
		String [][] records_s68 = new String[299][cols2.length];
		for(int i=0;i<299;i++)
		{
			records_s68[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s68[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s68", records_s68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		//record298: [a298, b0, c1, d2, e3, f4, g4]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s68", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 75, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		int pageCount2 = (int)Math.ceil(299.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("s68");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+75));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("d43y7", cols3);
		String [][] records_d43y7 = new String[384][cols3.length];
		for(int i=0;i<384;i++)
		{
			records_d43y7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_d43y7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("d43y7", records_d43y7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4]
		//record380: [a380, b0, c2, d0, e0]
		//record381: [a381, b1, c0, d1, e1]
		//record382: [a382, b0, c1, d2, e2]
		//record383: [a383, b1, c2, d3, e3]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("d43y7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 192, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		int pageCount3 = (int)Math.ceil(384.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("d43y7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+192));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("a5o", cols4);
		String [][] records_a5o = new String[235][cols4.length];
		for(int i=0;i<235;i++)
		{
			records_a5o[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_a5o[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("a5o", records_a5o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record230: [a230, b0, c2, d2]
		//record231: [a231, b1, c0, d3]
		//record232: [a232, b0, c1, d0]
		//record233: [a233, b1, c2, d1]
		//record234: [a234, b0, c0, d2]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("a5o", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 117, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		int pageCount4 = (int)Math.ceil(235.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("a5o");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+117));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("g2w", cols0);
		String [][] records_g2w = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			records_g2w[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g2w[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g2w", records_g2w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record92: [a92, b0, c2]
		//record93: [a93, b1, c0]
		//record94: [a94, b0, c1]
		//record95: [a95, b1, c2]
		//record96: [a96, b0, c0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g2w", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 32, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		int pageCount0 = (int)Math.ceil(97.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("g2w");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+32));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("aeyk", cols1);
		String [][] records_aeyk = new String[453][cols1.length];
		for(int i=0;i<453;i++)
		{
			records_aeyk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_aeyk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("aeyk", records_aeyk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("aeyk", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 50, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13]
		int pageCount1 = (int)Math.ceil(453.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("aeyk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+50));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("ve", cols2);
		String [][] records_ve = new String[482][cols2.length];
		for(int i=0;i<482;i++)
		{
			records_ve[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ve[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ve", records_ve[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record477: [a477, b1]
		//record478: [a478, b0]
		//record479: [a479, b1]
		//record480: [a480, b0]
		//record481: [a481, b1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ve", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 241, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
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
		String selectTrace2 = DBApp.getLastTrace("ve");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+241));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("x5o", cols0);
		String [][] records_x5o = new String[198][cols0.length];
		for(int i=0;i<198;i++)
		{
			records_x5o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x5o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x5o", records_x5o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record193: [a193, b1, c1, d1]
		//record194: [a194, b0, c2, d2]
		//record195: [a195, b1, c0, d3]
		//record196: [a196, b0, c1, d0]
		//record197: [a197, b1, c2, d1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x5o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 99, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		int pageCount0 = (int)Math.ceil(198.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("x5o");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+99));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s6", cols1);
		String [][] records_s6 = new String[307][cols1.length];
		for(int i=0;i<307;i++)
		{
			records_s6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s6", records_s6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7]
		String[] ConditionColumns1 = {"a","h","f","d","g"};
		String[] ConditionColumnsValues1 = {"a222","h6","f0","d2","g5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("s6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1]
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
		String selectTrace1 = DBApp.getLastTrace("s6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("bxgo5", cols2);
		String [][] records_bxgo5 = new String[139][cols2.length];
		for(int i=0;i<139;i++)
		{
			records_bxgo5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bxgo5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bxgo5", records_bxgo5[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record134: [a134, b0]
		//record135: [a135, b1]
		//record136: [a136, b0]
		//record137: [a137, b1]
		//record138: [a138, b0]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a61"};
		ArrayList<String[]> tableSelect2 = DBApp.select("bxgo5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a61, b1]
		int pageCount2 = (int)Math.ceil(139.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("bxgo5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("a0s", cols0);
		String [][] records_a0s = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			records_a0s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a0s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a0s", records_a0s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record93: [a93, b1, c0, d1, e3, f3]
		//record94: [a94, b0, c1, d2, e4, f4]
		//record95: [a95, b1, c2, d3, e0, f5]
		//record96: [a96, b0, c0, d0, e1, f0]
		//record97: [a97, b1, c1, d1, e2, f1]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a52"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a0s", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a52, b0, c1, d0, e2, f4]
		int pageCount0 = (int)Math.ceil(98.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a0s");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bcft", cols1);
		String [][] records_bcft = new String[438][cols1.length];
		for(int i=0;i<438;i++)
		{
			records_bcft[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bcft[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bcft", records_bcft[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		String[] ConditionColumns1 = {"d","g","e"};
		String[] ConditionColumnsValues1 = {"d2","g6","e1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("bcft", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 4, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a146, b0, c2, d2, e1, f2, g6]
		//record2: [a286, b0, c1, d2, e1, f4, g6]
		//record3: [a426, b0, c0, d2, e1, f0, g6]
		int pageCount1 = (int)Math.ceil(438.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("bcft");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("x08", cols2);
		String [][] records_x08 = new String[295][cols2.length];
		for(int i=0;i<295;i++)
		{
			records_x08[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x08[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x08", records_x08[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record290: [a290, b0]
		//record291: [a291, b1]
		//record292: [a292, b0]
		//record293: [a293, b1]
		//record294: [a294, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x08", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 148, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount2 = (int)Math.ceil(295.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("x08");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+148));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s2", cols3);
		String [][] records_s2 = new String[124][cols3.length];
		for(int i=0;i<124;i++)
		{
			records_s2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s2", records_s2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11, m2]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1, m4]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6]
		String[] ConditionColumns3 = {"j"};
		String[] ConditionColumnsValues3 = {"j4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("s2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 12, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		int pageCount3 = (int)Math.ceil(124.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("s2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("av8rk", cols4);
		String [][] records_av8rk = new String[263][cols4.length];
		for(int i=0;i<263;i++)
		{
			records_av8rk[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_av8rk[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("av8rk", records_av8rk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record258: [a258, b0, c0]
		//record259: [a259, b1, c1]
		//record260: [a260, b0, c2]
		//record261: [a261, b1, c0]
		//record262: [a262, b0, c1]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("av8rk", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 87, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		int pageCount4 = (int)Math.ceil(263.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("av8rk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+87));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("in4n8", cols0);
		String [][] records_in4n8 = new String[480][cols0.length];
		for(int i=0;i<480;i++)
		{
			records_in4n8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_in4n8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("in4n8", records_in4n8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13, o10]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0, o11]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1, o12]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10, n2, o13]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11, m11, n3, o14]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("in4n8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 160, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		int pageCount0 = (int)Math.ceil(480.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("in4n8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+160));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("p205e", cols0);
		String [][] records_p205e = new String[121][cols0.length];
		for(int i=0;i<121;i++)
		{
			records_p205e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p205e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p205e", records_p205e[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record116: [a116, b0]
		//record117: [a117, b1]
		//record118: [a118, b0]
		//record119: [a119, b1]
		//record120: [a120, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("p205e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 61, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(121.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("p205e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+61));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rjjv", cols1);
		String [][] records_rjjv = new String[151][cols1.length];
		for(int i=0;i<151;i++)
		{
			records_rjjv[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rjjv[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rjjv", records_rjjv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7]
		String[] ConditionColumns1 = {"i","f"};
		String[] ConditionColumnsValues1 = {"i1","f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rjjv", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record3: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3]
		//record4: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1, m8]
		int pageCount1 = (int)Math.ceil(151.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("rjjv");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("d40p", cols2);
		String [][] records_d40p = new String[413][cols2.length];
		for(int i=0;i<413;i++)
		{
			records_d40p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d40p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d40p", records_d40p[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record408: [a408]
		//record409: [a409]
		//record410: [a410]
		//record411: [a411]
		//record412: [a412]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a320"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d40p", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a320]
		int pageCount2 = (int)Math.ceil(413.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("d40p");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("x164", cols3);
		String [][] records_x164 = new String[266][cols3.length];
		for(int i=0;i<266;i++)
		{
			records_x164[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x164[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x164", records_x164[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x164", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 88, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6]
		int pageCount3 = (int)Math.ceil(266.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("x164");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+88));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("o08", cols0);
		String [][] records_o08 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			records_o08[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o08[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o08", records_o08[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record40: [a40]
		//record41: [a41]
		//record42: [a42]
		//record43: [a43]
		//record44: [a44]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a12"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o08", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a12]
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("o08");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p1ud", cols1);
		String [][] records_p1ud = new String[147][cols1.length];
		for(int i=0;i<147;i++)
		{
			records_p1ud[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p1ud[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p1ud", records_p1ud[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("p1ud", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3]
		int pageCount1 = (int)Math.ceil(147.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("p1ud");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+14));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t4r", cols2);
		String [][] records_t4r = new String[92][cols2.length];
		for(int i=0;i<92;i++)
		{
			records_t4r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t4r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t4r", records_t4r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		//record89: [a89, b1, c2, d1, e4, f5, g5]
		//record90: [a90, b0, c0, d2, e0, f0, g6]
		//record91: [a91, b1, c1, d3, e1, f1, g0]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("t4r", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 23, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		int pageCount2 = (int)Math.ceil(92.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("t4r");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+23));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fd2", cols0);
		String [][] records_fd2 = new String[115][cols0.length];
		for(int i=0;i<115;i++)
		{
			records_fd2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fd2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fd2", records_fd2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9, n1, o8]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10, n2, o9]
		String[] ConditionColumns0 = {"o","j","l","m","a","d","b"};
		String[] ConditionColumnsValues0 = {"o13","j3","l7","m12","a103","d3","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fd2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13]
		int pageCount0 = (int)Math.ceil(115.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fd2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("yj8", cols1);
		String [][] records_yj8 = new String[482][cols1.length];
		for(int i=0;i<482;i++)
		{
			records_yj8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yj8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yj8", records_yj8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10, n2]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11, m11, n3]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12, n4]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0, n5]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("yj8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 241, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		int pageCount1 = (int)Math.ceil(482.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("yj8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+241));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("fml", cols0);
		String [][] records_fml = new String[286][cols0.length];
		for(int i=0;i<286;i++)
		{
			records_fml[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fml[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fml", records_fml[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5]
		//record282: [a282, b0, c0, d2, e2, f0]
		//record283: [a283, b1, c1, d3, e3, f1]
		//record284: [a284, b0, c2, d0, e4, f2]
		//record285: [a285, b1, c0, d1, e0, f3]
		String[] ConditionColumns0 = {"f","c"};
		String[] ConditionColumnsValues0 = {"f4","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fml", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 47, records:
		//record0: [a4, b0, c1, d0, e4, f4]
		//record1: [a10, b0, c1, d2, e0, f4]
		//record2: [a16, b0, c1, d0, e1, f4]
		//record3: [a22, b0, c1, d2, e2, f4]
		//record4: [a28, b0, c1, d0, e3, f4]
		int pageCount0 = (int)Math.ceil(286.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fml");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+47));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("la6q", cols1);
		String [][] records_la6q = new String[448][cols1.length];
		for(int i=0;i<448;i++)
		{
			records_la6q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_la6q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("la6q", records_la6q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record443: [a443, b1, c2, d3, e3, f5]
		//record444: [a444, b0, c0, d0, e4, f0]
		//record445: [a445, b1, c1, d1, e0, f1]
		//record446: [a446, b0, c2, d2, e1, f2]
		//record447: [a447, b1, c0, d3, e2, f3]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a239"};
		ArrayList<String[]> tableSelect1 = DBApp.select("la6q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a239, b1, c2, d3, e4, f5]
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
		String selectTrace1 = DBApp.getLastTrace("la6q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("pc2u3", cols2);
		String [][] records_pc2u3 = new String[328][cols2.length];
		for(int i=0;i<328;i++)
		{
			records_pc2u3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pc2u3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pc2u3", records_pc2u3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("pc2u3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 37, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		int pageCount2 = (int)Math.ceil(328.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("pc2u3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("fni", cols0);
		String [][] records_fni = new String[390][cols0.length];
		for(int i=0;i<390;i++)
		{
			records_fni[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fni[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fni", records_fni[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record385: [a385]
		//record386: [a386]
		//record387: [a387]
		//record388: [a388]
		//record389: [a389]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fni", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1]
		int pageCount0 = (int)Math.ceil(390.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("fni");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("gw3", cols1);
		String [][] records_gw3 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			records_gw3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gw3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gw3", records_gw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record46: [a46, b0]
		//record47: [a47, b1]
		//record48: [a48, b0]
		//record49: [a49, b1]
		//record50: [a50, b0]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a47"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gw3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a47, b1]
		int pageCount1 = (int)Math.ceil(51.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("gw3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("oqv2", cols2);
		String [][] records_oqv2 = new String[469][cols2.length];
		for(int i=0;i<469;i++)
		{
			records_oqv2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oqv2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oqv2", records_oqv2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8]
		String[] ConditionColumns2 = {"e","f"};
		String[] ConditionColumnsValues2 = {"e3","f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("oqv2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 15, records:
		//record0: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record1: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8]
		//record2: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8]
		//record3: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8]
		//record4: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8]
		int pageCount2 = (int)Math.ceil(469.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("oqv2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("auj", cols3);
		String [][] records_auj = new String[116][cols3.length];
		for(int i=0;i<116;i++)
		{
			records_auj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_auj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("auj", records_auj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record111: [a111, b1, c0]
		//record112: [a112, b0, c1]
		//record113: [a113, b1, c2]
		//record114: [a114, b0, c0]
		//record115: [a115, b1, c1]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("auj", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 58, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		int pageCount3 = (int)Math.ceil(116.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("auj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+58));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("c9w", cols0);
		String [][] records_c9w = new String[298][cols0.length];
		for(int i=0;i<298;i++)
		{
			records_c9w[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c9w[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c9w", records_c9w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record293: [a293, b1, c2, d1]
		//record294: [a294, b0, c0, d2]
		//record295: [a295, b1, c1, d3]
		//record296: [a296, b0, c2, d0]
		//record297: [a297, b1, c0, d1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c9w", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 99, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
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
		String selectTrace0 = DBApp.getLastTrace("c9w");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+99));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("pkg4", cols1);
		String [][] records_pkg4 = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			records_pkg4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pkg4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pkg4", records_pkg4[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//last 4 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("pkg4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		int pageCount1 = (int)Math.ceil(4.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("pkg4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c7", cols2);
		String [][] records_c7 = new String[311][cols2.length];
		for(int i=0;i<311;i++)
		{
			records_c7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c7", records_c7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record306: [a306, b0, c0, d2, e1, f0, g5]
		//record307: [a307, b1, c1, d3, e2, f1, g6]
		//record308: [a308, b0, c2, d0, e3, f2, g0]
		//record309: [a309, b1, c0, d1, e4, f3, g1]
		//record310: [a310, b0, c1, d2, e0, f4, g2]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("c7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 52, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		int pageCount2 = (int)Math.ceil(311.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("c7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+52));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("if", cols3);
		String [][] records_if = new String[381][cols3.length];
		for(int i=0;i<381;i++)
		{
			records_if[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_if[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("if", records_if[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("if", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 64, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2]
		int pageCount3 = (int)Math.ceil(381.0/DBApp.dataPageSize);
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
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+64));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cg5ba", cols4);
		String [][] records_cg5ba = new String[347][cols4.length];
		for(int i=0;i<347;i++)
		{
			records_cg5ba[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_cg5ba[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("cg5ba", records_cg5ba[i]);
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
		String[] ConditionColumns4 = {"f"};
		String[] ConditionColumnsValues4 = {"f2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("cg5ba", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 58, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		int pageCount4 = (int)Math.ceil(347.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("cg5ba");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+58));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("i6654", cols0);
		String [][] records_i6654 = new String[454][cols0.length];
		for(int i=0;i<454;i++)
		{
			records_i6654[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i6654[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i6654", records_i6654[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record449: [a449, b1, c2]
		//record450: [a450, b0, c0]
		//record451: [a451, b1, c1]
		//record452: [a452, b0, c2]
		//record453: [a453, b1, c0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i6654", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 227, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount0 = (int)Math.ceil(454.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("i6654");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+227));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k231k", cols1);
		String [][] records_k231k = new String[410][cols1.length];
		for(int i=0;i<410;i++)
		{
			records_k231k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k231k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k231k", records_k231k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k231k", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 103, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1]
		int pageCount1 = (int)Math.ceil(410.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("k231k");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+103));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("o86", cols2);
		String [][] records_o86 = new String[305][cols2.length];
		for(int i=0;i<305;i++)
		{
			records_o86[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o86[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o86", records_o86[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record300: [a300, b0]
		//record301: [a301, b1]
		//record302: [a302, b0]
		//record303: [a303, b1]
		//record304: [a304, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o86", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 152, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount2 = (int)Math.ceil(305.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("o86");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+152));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("cop", cols0);
		String [][] records_cop = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			records_cop[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cop[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cop", records_cop[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record87: [a87, b1, c0, d3]
		//record88: [a88, b0, c1, d0]
		//record89: [a89, b1, c2, d1]
		//record90: [a90, b0, c0, d2]
		//record91: [a91, b1, c1, d3]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cop", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 46, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		int pageCount0 = (int)Math.ceil(92.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("cop");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+46));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k13", cols1);
		String [][] records_k13 = new String[337][cols1.length];
		for(int i=0;i<337;i++)
		{
			records_k13[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k13[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k13", records_k13[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k8"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k13", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 30, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record3: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4]
		int pageCount1 = (int)Math.ceil(337.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("k13");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+30));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("zavov", cols0);
		String [][] records_zavov = new String[129][cols0.length];
		for(int i=0;i<129;i++)
		{
			records_zavov[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zavov[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zavov", records_zavov[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record124: [a124, b0]
		//record125: [a125, b1]
		//record126: [a126, b0]
		//record127: [a127, b1]
		//record128: [a128, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zavov", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 65, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount0 = (int)Math.ceil(129.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("zavov");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+65));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("u57", cols1);
		String [][] records_u57 = new String[207][cols1.length];
		for(int i=0;i<207;i++)
		{
			records_u57[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u57[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u57", records_u57[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2, i4, j2, k4, l10, m7]
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5, l11, m8]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4, k6, l0, m9]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11]
		String[] ConditionColumns1 = {"j","f","i","e"};
		String[] ConditionColumnsValues1 = {"j1","f5","i8","e1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u57", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6]
		//record1: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5]
		int pageCount1 = (int)Math.ceil(207.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("u57");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("j0", cols2);
		String [][] records_j0 = new String[184][cols2.length];
		for(int i=0;i<184;i++)
		{
			records_j0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j0", records_j0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record179: [a179, b1, c2, d3, e4]
		//record180: [a180, b0, c0, d0, e0]
		//record181: [a181, b1, c1, d1, e1]
		//record182: [a182, b0, c2, d2, e2]
		//record183: [a183, b1, c0, d3, e3]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("j0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 61, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a8, b0, c2, d0, e3]
		//record3: [a11, b1, c2, d3, e1]
		//record4: [a14, b0, c2, d2, e4]
		int pageCount2 = (int)Math.ceil(184.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("j0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+61));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("pjdd9", cols0);
		String [][] records_pjdd9 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_pjdd9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pjdd9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pjdd9", records_pjdd9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pjdd9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		int pageCount0 = (int)Math.ceil(33.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("pjdd9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+11));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("cnw3", cols0);
		String [][] records_cnw3 = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_cnw3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cnw3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cnw3", records_cnw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record207: [a207, b1, c0]
		//record208: [a208, b0, c1]
		//record209: [a209, b1, c2]
		//record210: [a210, b0, c0]
		//record211: [a211, b1, c1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cnw3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 71, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
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
		String selectTrace0 = DBApp.getLastTrace("cnw3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+71));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("h36y", cols1);
		String [][] records_h36y = new String[224][cols1.length];
		for(int i=0;i<224;i++)
		{
			records_h36y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h36y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h36y", records_h36y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record219: [a219, b1, c0, d3]
		//record220: [a220, b0, c1, d0]
		//record221: [a221, b1, c2, d1]
		//record222: [a222, b0, c0, d2]
		//record223: [a223, b1, c1, d3]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h36y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 56, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a5, b1, c2, d1]
		//record2: [a9, b1, c0, d1]
		//record3: [a13, b1, c1, d1]
		//record4: [a17, b1, c2, d1]
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
		String selectTrace1 = DBApp.getLastTrace("h36y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+56));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("r41", cols0);
		String [][] records_r41 = new String[430][cols0.length];
		for(int i=0;i<430;i++)
		{
			records_r41[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r41[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r41", records_r41[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9, n5]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9]
		String[] ConditionColumns0 = {"m","g"};
		String[] ConditionColumnsValues0 = {"m1","g3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r41", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10]
		//record1: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3]
		//record2: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10]
		//record3: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3]
		int pageCount0 = (int)Math.ceil(430.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("r41");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("js", cols0);
		String [][] records_js = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			records_js[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_js[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("js", records_js[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("js", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7]
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
		String selectTrace0 = DBApp.getLastTrace("js");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+6));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("gi6", cols1);
		String [][] records_gi6 = new String[215][cols1.length];
		for(int i=0;i<215;i++)
		{
			records_gi6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gi6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gi6", records_gi6[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record210: [a210, b0]
		//record211: [a211, b1]
		//record212: [a212, b0]
		//record213: [a213, b1]
		//record214: [a214, b0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gi6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 108, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount1 = (int)Math.ceil(215.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("gi6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+108));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("pc95c", cols2);
		String [][] records_pc95c = new String[372][cols2.length];
		for(int i=0;i<372;i++)
		{
			records_pc95c[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pc95c[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pc95c", records_pc95c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11]
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("pc95c", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 34, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		int pageCount2 = (int)Math.ceil(372.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("pc95c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+34));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u8w04", cols0);
		String [][] records_u8w04 = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_u8w04[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u8w04[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u8w04", records_u8w04[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("u8w04", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 43, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
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
		String selectTrace0 = DBApp.getLastTrace("u8w04");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+43));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("kr", cols1);
		String [][] records_kr = new String[330][cols1.length];
		for(int i=0;i<330;i++)
		{
			records_kr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kr", records_kr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record325: [a325, b1, c1, d1, e0]
		//record326: [a326, b0, c2, d2, e1]
		//record327: [a327, b1, c0, d3, e2]
		//record328: [a328, b0, c1, d0, e3]
		//record329: [a329, b1, c2, d1, e4]
		String[] ConditionColumns1 = {"e","b"};
		String[] ConditionColumnsValues1 = {"e4","b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("kr", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 33, records:
		//record0: [a9, b1, c0, d1, e4]
		//record1: [a19, b1, c1, d3, e4]
		//record2: [a29, b1, c2, d1, e4]
		//record3: [a39, b1, c0, d3, e4]
		//record4: [a49, b1, c1, d1, e4]
		int pageCount1 = (int)Math.ceil(330.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("kr");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+33));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("he", cols0);
		String [][] records_he = new String[439][cols0.length];
		for(int i=0;i<439;i++)
		{
			records_he[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_he[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("he", records_he[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("he", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 55, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3, i8]
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
		String selectTrace0 = DBApp.getLastTrace("he");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+55));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b1qy", cols1);
		String [][] records_b1qy = new String[192][cols1.length];
		for(int i=0;i<192;i++)
		{
			records_b1qy[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1qy[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1qy", records_b1qy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b1qy", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 48, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		int pageCount1 = (int)Math.ceil(192.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("b1qy");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+48));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ha8", cols0);
		String [][] records_ha8 = new String[246][cols0.length];
		for(int i=0;i<246;i++)
		{
			records_ha8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ha8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ha8", records_ha8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3]
		String[] ConditionColumns0 = {"j","k"};
		String[] ConditionColumnsValues0 = {"j8","k4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ha8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		//record1: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4]
		int pageCount0 = (int)Math.ceil(246.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ha8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("etqp", cols1);
		String [][] records_etqp = new String[415][cols1.length];
		for(int i=0;i<415;i++)
		{
			records_etqp[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_etqp[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("etqp", records_etqp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("etqp", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 59, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a12, b0, c0, d0, e2, f0, g5]
		//record2: [a19, b1, c1, d3, e4, f1, g5]
		//record3: [a26, b0, c2, d2, e1, f2, g5]
		//record4: [a33, b1, c0, d1, e3, f3, g5]
		int pageCount1 = (int)Math.ceil(415.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("etqp");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+59));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("hd0", cols2);
		String [][] records_hd0 = new String[307][cols2.length];
		for(int i=0;i<307;i++)
		{
			records_hd0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hd0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hd0", records_hd0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record302: [a302, b0, c2]
		//record303: [a303, b1, c0]
		//record304: [a304, b0, c1]
		//record305: [a305, b1, c2]
		//record306: [a306, b0, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("hd0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 154, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount2 = (int)Math.ceil(307.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("hd0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+154));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("s8n", cols0);
		String [][] records_s8n = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_s8n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s8n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s8n", records_s8n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1]
		//record380: [a380, b0, c2, d0, e0, f2]
		//record381: [a381, b1, c0, d1, e1, f3]
		//record382: [a382, b0, c1, d2, e2, f4]
		//record383: [a383, b1, c2, d3, e3, f5]
		String[] ConditionColumns0 = {"c","e","a"};
		String[] ConditionColumnsValues0 = {"c0","e4","a114"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s8n", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a114, b0, c0, d2, e4, f0]
		int pageCount0 = (int)Math.ceil(384.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("s8n");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yjmk8", cols1);
		String [][] records_yjmk8 = new String[104][cols1.length];
		for(int i=0;i<104;i++)
		{
			records_yjmk8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yjmk8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yjmk8", records_yjmk8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("yjmk8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 15, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6]
		int pageCount1 = (int)Math.ceil(104.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("yjmk8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r66v", cols2);
		String [][] records_r66v = new String[292][cols2.length];
		for(int i=0;i<292;i++)
		{
			records_r66v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r66v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r66v", records_r66v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0, m2]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5]
		String[] ConditionColumns2 = {"j","m","k","i","a","e"};
		String[] ConditionColumnsValues2 = {"j5","m11","k5","i7","a115","e0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("r66v", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11]
		int pageCount2 = (int)Math.ceil(292.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("r66v");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("bx0y", cols3);
		String [][] records_bx0y = new String[104][cols3.length];
		for(int i=0;i<104;i++)
		{
			records_bx0y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bx0y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bx0y", records_bx0y[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record99: [a99]
		//record100: [a100]
		//record101: [a101]
		//record102: [a102]
		//record103: [a103]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a28"};
		ArrayList<String[]> tableSelect3 = DBApp.select("bx0y", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a28]
		int pageCount3 = (int)Math.ceil(104.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("bx0y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ak40", cols4);
		String [][] records_ak40 = new String[30][cols4.length];
		for(int i=0;i<30;i++)
		{
			records_ak40[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ak40[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ak40", records_ak40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ak40", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 8, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		int pageCount4 = (int)Math.ceil(30.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("ak40");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+8));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("yq0", cols0);
		String [][] records_yq0 = new String[111][cols0.length];
		for(int i=0;i<111;i++)
		{
			records_yq0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yq0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yq0", records_yq0[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record106: [a106, b0]
		//record107: [a107, b1]
		//record108: [a108, b0]
		//record109: [a109, b1]
		//record110: [a110, b0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a66"};
		ArrayList<String[]> tableSelect0 = DBApp.select("yq0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a66, b0]
		int pageCount0 = (int)Math.ceil(111.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("yq0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("v04", cols1);
		String [][] records_v04 = new String[126][cols1.length];
		for(int i=0;i<126;i++)
		{
			records_v04[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v04[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v04", records_v04[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record121: [a121, b1, c1]
		//record122: [a122, b0, c2]
		//record123: [a123, b1, c0]
		//record124: [a124, b0, c1]
		//record125: [a125, b1, c2]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v04", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 42, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
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
		String selectTrace1 = DBApp.getLastTrace("v04");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("gt", cols2);
		String [][] records_gt = new String[390][cols2.length];
		for(int i=0;i<390;i++)
		{
			records_gt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gt", records_gt[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record385: [a385, b1]
		//record386: [a386, b0]
		//record387: [a387, b1]
		//record388: [a388, b0]
		//record389: [a389, b1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("gt", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 195, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount2 = (int)Math.ceil(390.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("gt");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+195));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s3mc", cols3);
		String [][] records_s3mc = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			records_s3mc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s3mc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s3mc", records_s3mc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("s3mc", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		int pageCount3 = (int)Math.ceil(20.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("s3mc");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s7", cols4);
		String [][] records_s7 = new String[92][cols4.length];
		for(int i=0;i<92;i++)
		{
			records_s7[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_s7[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("s7", records_s7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3, l7]
		String[] ConditionColumns4 = {"i"};
		String[] ConditionColumnsValues4 = {"i2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("s7", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 10, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2]
		int pageCount4 = (int)Math.ceil(92.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("s7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rt1i", cols0);
		String [][] records_rt1i = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			records_rt1i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rt1i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rt1i", records_rt1i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		String[] ConditionColumns0 = {"n"};
		String[] ConditionColumnsValues0 = {"n2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rt1i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		int pageCount0 = (int)Math.ceil(36.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("rt1i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("w1j6y", cols1);
		String [][] records_w1j6y = new String[228][cols1.length];
		for(int i=0;i<228;i++)
		{
			records_w1j6y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w1j6y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w1j6y", records_w1j6y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record223: [a223, b1, c1]
		//record224: [a224, b0, c2]
		//record225: [a225, b1, c0]
		//record226: [a226, b0, c1]
		//record227: [a227, b1, c2]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w1j6y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 76, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		int pageCount1 = (int)Math.ceil(228.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w1j6y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+76));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("s4sm1", cols2);
		String [][] records_s4sm1 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			records_s4sm1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s4sm1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s4sm1", records_s4sm1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record45: [a45, b1, c0, d1]
		//record46: [a46, b0, c1, d2]
		//record47: [a47, b1, c2, d3]
		//record48: [a48, b0, c0, d0]
		//record49: [a49, b1, c1, d1]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s4sm1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 13, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a4, b0, c1, d0]
		//record2: [a8, b0, c2, d0]
		//record3: [a12, b0, c0, d0]
		//record4: [a16, b0, c1, d0]
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
		String selectTrace2 = DBApp.getLastTrace("s4sm1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("afi0", cols3);
		String [][] records_afi0 = new String[93][cols3.length];
		for(int i=0;i<93;i++)
		{
			records_afi0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_afi0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("afi0", records_afi0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3, l7]
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4, l8]
		String[] ConditionColumns3 = {"b","c"};
		String[] ConditionColumnsValues3 = {"b0","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("afi0", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 16, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		int pageCount3 = (int)Math.ceil(93.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("afi0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+16));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s4l", cols0);
		String [][] records_s4l = new String[380][cols0.length];
		for(int i=0;i<380;i++)
		{
			records_s4l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s4l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s4l", records_s4l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record375: [a375, b1, c0, d3, e0, f3, g4]
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s4l", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 76, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a17, b1, c2, d1, e2, f5, g3]
		//record4: [a22, b0, c1, d2, e2, f4, g1]
		int pageCount0 = (int)Math.ceil(380.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("s4l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+76));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ov17r", cols1);
		String [][] records_ov17r = new String[352][cols1.length];
		for(int i=0;i<352;i++)
		{
			records_ov17r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ov17r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ov17r", records_ov17r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record347: [a347, b1, c2, d3, e2, f5, g4]
		//record348: [a348, b0, c0, d0, e3, f0, g5]
		//record349: [a349, b1, c1, d1, e4, f1, g6]
		//record350: [a350, b0, c2, d2, e0, f2, g0]
		//record351: [a351, b1, c0, d3, e1, f3, g1]
		String[] ConditionColumns1 = {"b","a"};
		String[] ConditionColumnsValues1 = {"b0","a344"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ov17r", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a344, b0, c2, d0, e4, f2, g1]
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
		String selectTrace1 = DBApp.getLastTrace("ov17r");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ba8", cols2);
		String [][] records_ba8 = new String[80][cols2.length];
		for(int i=0;i<80;i++)
		{
			records_ba8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ba8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ba8", records_ba8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ba8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 10, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		int pageCount2 = (int)Math.ceil(80.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ba8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("g5", cols3);
		String [][] records_g5 = new String[59][cols3.length];
		for(int i=0;i<59;i++)
		{
			records_g5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g5", records_g5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record54: [a54, b0, c0]
		//record55: [a55, b1, c1]
		//record56: [a56, b0, c2]
		//record57: [a57, b1, c0]
		//record58: [a58, b0, c1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("g5", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 20, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		int pageCount3 = (int)Math.ceil(59.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("g5");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+20));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("nk2", cols4);
		String [][] records_nk2 = new String[326][cols4.length];
		for(int i=0;i<326;i++)
		{
			records_nk2[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_nk2[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("nk2", records_nk2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record321: [a321, b1, c0]
		//record322: [a322, b0, c1]
		//record323: [a323, b1, c2]
		//record324: [a324, b0, c0]
		//record325: [a325, b1, c1]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("nk2", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 163, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		int pageCount4 = (int)Math.ceil(326.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("nk2");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+163));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ke", cols0);
		String [][] records_ke = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_ke[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ke[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ke", records_ke[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6, n9, o14]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7, n10, o0]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8, l7, m8, n11, o1]
		String[] ConditionColumns0 = {"n"};
		String[] ConditionColumnsValues0 = {"n7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ke", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record3: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4]
		//record4: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		int pageCount0 = (int)Math.ceil(152.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ke");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+11));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vm19", cols1);
		String [][] records_vm19 = new String[447][cols1.length];
		for(int i=0;i<447;i++)
		{
			records_vm19[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vm19[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vm19", records_vm19[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6]
		String[] ConditionColumns1 = {"h","e","g"};
		String[] ConditionColumnsValues1 = {"h1","e3","g6"};
		ArrayList<String[]> tableSelect1 = DBApp.select("vm19", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10]
		//record1: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4]
		int pageCount1 = (int)Math.ceil(447.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("vm19");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("o3", cols2);
		String [][] records_o3 = new String[150][cols2.length];
		for(int i=0;i<150;i++)
		{
			records_o3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o3", records_o3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6, n9, o14]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 37, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record3: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record4: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		int pageCount2 = (int)Math.ceil(150.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("o3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+37));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vz3x4", cols3);
		String [][] records_vz3x4 = new String[352][cols3.length];
		for(int i=0;i<352;i++)
		{
			records_vz3x4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vz3x4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vz3x4", records_vz3x4[i]);
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
		String[] ConditionColumns3 = {"f","j","c","k"};
		String[] ConditionColumnsValues3 = {"f0","j8","c0","k7"};
		ArrayList<String[]> tableSelect3 = DBApp.select("vz3x4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record1: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7]
		int pageCount3 = (int)Math.ceil(352.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("vz3x4");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("v9dl", cols0);
		String [][] records_v9dl = new String[136][cols0.length];
		for(int i=0;i<136;i++)
		{
			records_v9dl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v9dl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v9dl", records_v9dl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("v9dl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 22, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		int pageCount0 = (int)Math.ceil(136.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("v9dl");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+22));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l68h", cols1);
		String [][] records_l68h = new String[129][cols1.length];
		for(int i=0;i<129;i++)
		{
			records_l68h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l68h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l68h", records_l68h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2]
		String[] ConditionColumns1 = {"g","i","h","d"};
		String[] ConditionColumnsValues1 = {"g5","i8","h2","d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l68h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		int pageCount1 = (int)Math.ceil(129.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("l68h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("g7", cols2);
		String [][] records_g7 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			records_g7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g7", records_g7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("g7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 12, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		int pageCount2 = (int)Math.ceil(88.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("g7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("y03n", cols3);
		String [][] records_y03n = new String[391][cols3.length];
		for(int i=0;i<391;i++)
		{
			records_y03n[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y03n[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y03n", records_y03n[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record386: [a386]
		//record387: [a387]
		//record388: [a388]
		//record389: [a389]
		//record390: [a390]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a297"};
		ArrayList<String[]> tableSelect3 = DBApp.select("y03n", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a297]
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
		String selectTrace3 = DBApp.getLastTrace("y03n");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c2x", cols0);
		String [][] records_c2x = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_c2x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c2x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c2x", records_c2x[i]);
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
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c2x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
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
		String selectTrace0 = DBApp.getLastTrace("c2x");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v8", cols1);
		String [][] records_v8 = new String[144][cols1.length];
		for(int i=0;i<144;i++)
		{
			records_v8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v8", records_v8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		int pageCount1 = (int)Math.ceil(144.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("v8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("uxj", cols0);
		String [][] records_uxj = new String[414][cols0.length];
		for(int i=0;i<414;i++)
		{
			records_uxj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uxj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uxj", records_uxj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("uxj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 59, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6]
		int pageCount0 = (int)Math.ceil(414.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("uxj");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+59));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s3e", cols1);
		String [][] records_s3e = new String[399][cols1.length];
		for(int i=0;i<399;i++)
		{
			records_s3e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s3e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s3e", records_s3e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2]
		String[] ConditionColumns1 = {"h","i"};
		String[] ConditionColumnsValues1 = {"h5","i0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("s3e", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		//record1: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7]
		//record2: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2]
		//record3: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8]
		//record4: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3]
		int pageCount1 = (int)Math.ceil(399.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("s3e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("t6i", cols2);
		String [][] records_t6i = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			records_t6i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t6i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t6i", records_t6i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		String[] ConditionColumns2 = {"j","b","h","k","g"};
		String[] ConditionColumnsValues2 = {"j0","b0","h2","k10","g3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("t6i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		int pageCount2 = (int)Math.ceil(23.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("t6i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z7s", cols3);
		String [][] records_z7s = new String[78][cols3.length];
		for(int i=0;i<78;i++)
		{
			records_z7s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z7s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z7s", records_z7s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5]
		String[] ConditionColumns3 = {"b","i","f","l","g"};
		String[] ConditionColumnsValues3 = {"b0","i5","f2","l8","g5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z7s", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8]
		int pageCount3 = (int)Math.ceil(78.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("z7s");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eh34q", cols0);
		String [][] records_eh34q = new String[127][cols0.length];
		for(int i=0;i<127;i++)
		{
			records_eh34q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eh34q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eh34q", records_eh34q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2, g3]
		//record123: [a123, b1, c0, d3, e3, f3, g4]
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		String[] ConditionColumns0 = {"d","c"};
		String[] ConditionColumnsValues0 = {"d0","c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("eh34q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1]
		//record1: [a20, b0, c2, d0, e0, f2, g6]
		//record2: [a32, b0, c2, d0, e2, f2, g4]
		//record3: [a44, b0, c2, d0, e4, f2, g2]
		//record4: [a56, b0, c2, d0, e1, f2, g0]
		int pageCount0 = (int)Math.ceil(127.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("eh34q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+10));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l422", cols1);
		String [][] records_l422 = new String[422][cols1.length];
		for(int i=0;i<422;i++)
		{
			records_l422[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l422[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l422", records_l422[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2, n12, o13]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9, k1, l11, m3, n13, o14]
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0, m4, n0, o0]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1, m5, n1, o1]
		String[] ConditionColumns1 = {"c","b","j"};
		String[] ConditionColumnsValues1 = {"c0","b1","j3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l422", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record2: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		//record3: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9, o3]
		//record4: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3]
		int pageCount1 = (int)Math.ceil(422.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("l422");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+14));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(29);
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