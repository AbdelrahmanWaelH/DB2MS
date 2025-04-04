package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests08
{

	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("pep", cols0);
		String [][] records_pep = new String[357][cols0.length];
		for(int i=0;i<357;i++)
		{
			records_pep[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pep[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pep", records_pep[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record352: [a352, b0, c1, d0, e2]
		//record353: [a353, b1, c2, d1, e3]
		//record354: [a354, b0, c0, d2, e4]
		//record355: [a355, b1, c1, d3, e0]
		//record356: [a356, b0, c2, d0, e1]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pep", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 90, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a8, b0, c2, d0, e3]
		//record3: [a12, b0, c0, d0, e2]
		//record4: [a16, b0, c1, d0, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 90);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("qz", cols1);
		String [][] records_qz = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			records_qz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qz", records_qz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record32: [a32, b0, c2, d0, e2]
		//record33: [a33, b1, c0, d1, e3]
		//record34: [a34, b0, c1, d2, e4]
		//record35: [a35, b1, c2, d3, e0]
		//record36: [a36, b0, c0, d0, e1]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("qz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a6, b0, c0, d2, e1]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a14, b0, c2, d2, e4]
		//record4: [a18, b0, c0, d2, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 9);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q1", cols2);
		String [][] records_q1 = new String[491][cols2.length];
		for(int i=0;i<491;i++)
		{
			records_q1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q1", records_q1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("q1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 123, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 123);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("di", cols3);
		String [][] records_di = new String[200][cols3.length];
		for(int i=0;i<200;i++)
		{
			records_di[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_di[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("di", records_di[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1]
		String[] ConditionColumns3 = {"d","b","h"};
		String[] ConditionColumnsValues3 = {"d1","b1","h1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("di", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 25, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 25);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xc18", cols4);
		String [][] records_xc18 = new String[324][cols4.length];
		for(int i=0;i<324;i++)
		{
			records_xc18[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xc18[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xc18", records_xc18[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1]
		String[] ConditionColumns4 = {"g","h"};
		String[] ConditionColumnsValues4 = {"g3","h6"};
		ArrayList<String[]> tableSelect4 = DBApp.select("xc18", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 6, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		//record1: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10]
		//record2: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7, n10]
		//record3: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10]
		//record4: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10, m2, n10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 6);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a94","b0","c1","d2","e4","f4","g3","h6","i4","j4","k6","l10","m3","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a150","b0","c0","d2","e0","f0","g3","h6","i6","j0","k7","l6","m7","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a206","b0","c2","d2","e1","f2","g3","h6","i8","j6","k8","l2","m11","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a262","b0","c1","d2","e2","f4","g3","h6","i1","j2","k9","l10","m2","n10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("iy", cols0);
		String [][] records_iy = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_iy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_iy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("iy", records_iy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12, n3, o13]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4, o14]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5, o0]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2, n6, o1]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3, n7, o2]
		String[] ConditionColumns0 = {"g","l","b","m","d"};
		String[] ConditionColumnsValues0 = {"g5","l2","b0","m12","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("iy", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12, o14]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a194","b0","c2","d2","e4","f2","g5","h2","i5","j4","k7","l2","m12","n12","o14"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("xx9", cols1);
		String [][] records_xx9 = new String[324][cols1.length];
		for(int i=0;i<324;i++)
		{
			records_xx9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xx9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xx9", records_xx9[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record319: [a319]
		//record320: [a320]
		//record321: [a321]
		//record322: [a322]
		//record323: [a323]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a24"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xx9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a24]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("i6", cols2);
		String [][] records_i6 = new String[3][cols2.length];
		for(int i=0;i<3;i++)
		{
			records_i6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i6", records_i6[i]);
		}
		//first 3 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//last 3 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("i6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qns0", cols0);
		String [][] records_qns0 = new String[425][cols0.length];
		for(int i=0;i<425;i++)
		{
			records_qns0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qns0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qns0", records_qns0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0, m4, n0, o0]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1, m5, n1, o1]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2, o2]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3, m7, n3, o3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8, n4, o4]
		String[] ConditionColumns0 = {"i","e","h","j"};
		String[] ConditionColumnsValues0 = {"i8","e3","h1","j3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("qns0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a233","b1","c2","d1","e3","f5","g2","h1","i8","j3","k2","l5","m12","n9","o8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("o23", cols1);
		String [][] records_o23 = new String[197][cols1.length];
		for(int i=0;i<197;i++)
		{
			records_o23[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o23[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o23", records_o23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record192: [a192, b0, c0, d0, e2, f0]
		//record193: [a193, b1, c1, d1, e3, f1]
		//record194: [a194, b0, c2, d2, e4, f2]
		//record195: [a195, b1, c0, d3, e0, f3]
		//record196: [a196, b0, c1, d0, e1, f4]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("o23", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 66, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a6, b0, c0, d2, e1, f0]
		//record3: [a9, b1, c0, d1, e4, f3]
		//record4: [a12, b0, c0, d0, e2, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 66);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("rb1", cols2);
		String [][] records_rb1 = new String[442][cols2.length];
		for(int i=0;i<442;i++)
		{
			records_rb1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rb1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rb1", records_rb1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record437: [a437, b1, c2, d1]
		//record438: [a438, b0, c0, d2]
		//record439: [a439, b1, c1, d3]
		//record440: [a440, b0, c2, d0]
		//record441: [a441, b1, c0, d1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a440"};
		ArrayList<String[]> tableSelect2 = DBApp.select("rb1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a440, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a440","b0","c2","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c7fcn", cols0);
		String [][] records_c7fcn = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			records_c7fcn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c7fcn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c7fcn", records_c7fcn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c7fcn", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 7);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("x30qq", cols1);
		String [][] records_x30qq = new String[178][cols1.length];
		for(int i=0;i<178;i++)
		{
			records_x30qq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x30qq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x30qq", records_x30qq[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record173: [a173]
		//record174: [a174]
		//record175: [a175]
		//record176: [a176]
		//record177: [a177]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a108"};
		ArrayList<String[]> tableSelect1 = DBApp.select("x30qq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a108]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a108"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s4", cols2);
		String [][] records_s4 = new String[239][cols2.length];
		for(int i=0;i<239;i++)
		{
			records_s4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4]
		String[] ConditionColumns2 = {"i","j","m","g","e"};
		String[] ConditionColumnsValues2 = {"i7","j8","m9","g3","e3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a178","b0","c1","d2","e3","f4","g3","h2","i7","j8","k2","l10","m9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cg", cols3);
		String [][] records_cg = new String[264][cols3.length];
		for(int i=0;i<264;i++)
		{
			records_cg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_cg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("cg", records_cg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2]
		String[] ConditionColumns3 = {"d","b","i"};
		String[] ConditionColumnsValues3 = {"d1","b1","i1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("cg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 8, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		//record2: [a73, b1, c1, d1, e3, f1, g3, h1, i1]
		//record3: [a109, b1, c1, d1, e4, f1, g4, h5, i1]
		//record4: [a145, b1, c1, d1, e0, f1, g5, h1, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 8);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a73","b1","c1","d1","e3","f1","g3","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a109","b1","c1","d1","e4","f1","g4","h5","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a145","b1","c1","d1","e0","f1","g5","h1","i1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("un", cols0);
		String [][] records_un = new String[282][cols0.length];
		for(int i=0;i<282;i++)
		{
			records_un[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_un[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("un", records_un[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record277: [a277, b1, c1, d1, e2, f1, g4]
		//record278: [a278, b0, c2, d2, e3, f2, g5]
		//record279: [a279, b1, c0, d3, e4, f3, g6]
		//record280: [a280, b0, c1, d0, e0, f4, g0]
		//record281: [a281, b1, c2, d1, e1, f5, g1]
		String[] ConditionColumns0 = {"d","e","f"};
		String[] ConditionColumnsValues0 = {"d2","e2","f0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("un", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0]
		//record1: [a102, b0, c0, d2, e2, f0, g4]
		//record2: [a162, b0, c0, d2, e2, f0, g1]
		//record3: [a222, b0, c0, d2, e2, f0, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a102","b0","c0","d2","e2","f0","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a162","b0","c0","d2","e2","f0","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a222","b0","c0","d2","e2","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("sp0id", cols1);
		String [][] records_sp0id = new String[225][cols1.length];
		for(int i=0;i<225;i++)
		{
			records_sp0id[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sp0id[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sp0id", records_sp0id[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4]
		//record221: [a221, b1, c2, d1, e1, f5]
		//record222: [a222, b0, c0, d2, e2, f0]
		//record223: [a223, b1, c1, d3, e3, f1]
		//record224: [a224, b0, c2, d0, e4, f2]
		String[] ConditionColumns1 = {"d","b"};
		String[] ConditionColumnsValues1 = {"d1","b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("sp0id", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 56, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a5, b1, c2, d1, e0, f5]
		//record2: [a9, b1, c0, d1, e4, f3]
		//record3: [a13, b1, c1, d1, e3, f1]
		//record4: [a17, b1, c2, d1, e2, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 56);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d62r", cols2);
		String [][] records_d62r = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			records_d62r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d62r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d62r", records_d62r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3]
		String[] ConditionColumns2 = {"c","h","l"};
		String[] ConditionColumnsValues2 = {"c0","h6","l6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d62r", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record2: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6]
		//record3: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 4);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a78","b0","c0","d2","e3","f0","g1","h6","i6","j8","k1","l6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h3ort", cols3);
		String [][] records_h3ort = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_h3ort[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h3ort[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h3ort", records_h3ort[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("h3ort", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 5, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 5);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("v591", cols4);
		String [][] records_v591 = new String[248][cols4.length];
		for(int i=0;i<248;i++)
		{
			records_v591[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_v591[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("v591", records_v591[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record243: [a243, b1, c0]
		//record244: [a244, b0, c1]
		//record245: [a245, b1, c2]
		//record246: [a246, b0, c0]
		//record247: [a247, b1, c1]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("v591", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 124, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 124);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ugo", cols0);
		String [][] records_ugo = new String[219][cols0.length];
		for(int i=0;i<219;i++)
		{
			records_ugo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ugo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ugo", records_ugo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		//record216: [a216, b0, c0, d0, e1, f0, g6]
		//record217: [a217, b1, c1, d1, e2, f1, g0]
		//record218: [a218, b0, c2, d2, e3, f2, g1]
		String[] ConditionColumns0 = {"g","a","d"};
		String[] ConditionColumnsValues0 = {"g6","a83","d3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ugo", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a83, b1, c2, d3, e3, f5, g6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("rw739", cols1);
		String [][] records_rw739 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			records_rw739[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rw739[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rw739", records_rw739[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6]
		String[] ConditionColumns1 = {"c","f","b"};
		String[] ConditionColumnsValues1 = {"c1","f4","b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rw739", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 13, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 13);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("se2", cols0);
		String [][] records_se2 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			records_se2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_se2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("se2", records_se2[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("se2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 18, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a23, b1, c2, d3, e3, f5, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 18);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("xso", cols1);
		String [][] records_xso = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			records_xso[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xso[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xso", records_xso[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record19: [a19, b1, c1]
		//record20: [a20, b0, c2]
		//record21: [a21, b1, c0]
		//record22: [a22, b0, c1]
		//record23: [a23, b1, c2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xso", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 12, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 12);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("aws1", cols2);
		String [][] records_aws1 = new String[188][cols2.length];
		for(int i=0;i<188;i++)
		{
			records_aws1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_aws1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("aws1", records_aws1[i]);
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
		String[] ConditionColumns2 = {"n"};
		String[] ConditionColumnsValues2 = {"n11"};
		ArrayList<String[]> tableSelect2 = DBApp.select("aws1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 13, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record2: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		//record3: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
		//record4: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 13);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0","n11","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1","n11","o8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7","k1","l7","m2","n11","o7"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k75", cols3);
		String [][] records_k75 = new String[192][cols3.length];
		for(int i=0;i<192;i++)
		{
			records_k75[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k75[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k75", records_k75[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4]
		String[] ConditionColumns3 = {"j"};
		String[] ConditionColumnsValues3 = {"j3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("k75", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 19, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 19);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ea", cols4);
		String [][] records_ea = new String[259][cols4.length];
		for(int i=0;i<259;i++)
		{
			records_ea[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ea[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ea", records_ea[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6]
		String[] ConditionColumns4 = {"j"};
		String[] ConditionColumnsValues4 = {"j7"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ea", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 26, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 26);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8","n5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bkdgw", cols0);
		String [][] records_bkdgw = new String[444][cols0.length];
		for(int i=0;i<444;i++)
		{
			records_bkdgw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bkdgw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bkdgw", records_bkdgw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bkdgw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 89, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 89);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jtq", cols1);
		String [][] records_jtq = new String[121][cols1.length];
		for(int i=0;i<121;i++)
		{
			records_jtq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jtq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jtq", records_jtq[i]);
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
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jtq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 30, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 30);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("x9il", cols2);
		String [][] records_x9il = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			records_x9il[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x9il[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x9il", records_x9il[i]);
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
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x9il", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 16, records:
		//record0: [a5, b1, c2, d1, e0, f5]
		//record1: [a11, b1, c2, d3, e1, f5]
		//record2: [a17, b1, c2, d1, e2, f5]
		//record3: [a23, b1, c2, d3, e3, f5]
		//record4: [a29, b1, c2, d1, e4, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 16);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q3h4", cols3);
		String [][] records_q3h4 = new String[36][cols3.length];
		for(int i=0;i<36;i++)
		{
			records_q3h4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q3h4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q3h4", records_q3h4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q3h4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 7);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("o0q0", cols4);
		String [][] records_o0q0 = new String[217][cols4.length];
		for(int i=0;i<217;i++)
		{
			records_o0q0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o0q0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o0q0", records_o0q0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record212: [a212, b0, c2, d0, e2, f2]
		//record213: [a213, b1, c0, d1, e3, f3]
		//record214: [a214, b0, c1, d2, e4, f4]
		//record215: [a215, b1, c2, d3, e0, f5]
		//record216: [a216, b0, c0, d0, e1, f0]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("o0q0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 54, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a11, b1, c2, d3, e1, f5]
		//record3: [a15, b1, c0, d3, e0, f3]
		//record4: [a19, b1, c1, d3, e4, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 54);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s5", cols0);
		String [][] records_s5 = new String[209][cols0.length];
		for(int i=0;i<209;i++)
		{
			records_s5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s5", records_s5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 30, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 30);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("eq", cols1);
		String [][] records_eq = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			records_eq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_eq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("eq", records_eq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record10: [a10, b0, c1]
		//record11: [a11, b1, c2]
		//record12: [a12, b0, c0]
		//record13: [a13, b1, c1]
		//record14: [a14, b0, c2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("eq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 8);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("sk1", cols2);
		String [][] records_sk1 = new String[342][cols2.length];
		for(int i=0;i<342;i++)
		{
			records_sk1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sk1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sk1", records_sk1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sk1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 86, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 86);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c65h", cols0);
		String [][] records_c65h = new String[414][cols0.length];
		for(int i=0;i<414;i++)
		{
			records_c65h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c65h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c65h", records_c65h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		String[] ConditionColumns0 = {"b","e"};
		String[] ConditionColumnsValues0 = {"b0","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c65h", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 41, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a14, b0, c2, d2, e4, f2, g0]
		//record2: [a24, b0, c0, d0, e4, f0, g3]
		//record3: [a34, b0, c1, d2, e4, f4, g6]
		//record4: [a44, b0, c2, d0, e4, f2, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 41);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("uyor1", cols1);
		String [][] records_uyor1 = new String[329][cols1.length];
		for(int i=0;i<329;i++)
		{
			records_uyor1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uyor1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uyor1", records_uyor1[i]);
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
		String[] ConditionColumns1 = {"e","k","n","l","b","c","m"};
		String[] ConditionColumnsValues1 = {"e0","k2","n6","l6","b0","c0","m12"};
		ArrayList<String[]> tableSelect1 = DBApp.select("uyor1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6, m12, n6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a90","b0","c0","d2","e0","f0","g6","h2","i0","j0","k2","l6","m12","n6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("vbm", cols2);
		String [][] records_vbm = new String[148][cols2.length];
		for(int i=0;i<148;i++)
		{
			records_vbm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vbm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vbm", records_vbm[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record143: [a143, b1]
		//record144: [a144, b0]
		//record145: [a145, b1]
		//record146: [a146, b0]
		//record147: [a147, b1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("vbm", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 74, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 74);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("z2f", cols3);
		String [][] records_z2f = new String[355][cols3.length];
		for(int i=0;i<355;i++)
		{
			records_z2f[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z2f[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z2f", records_z2f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record350: [a350, b0, c2, d2, e0]
		//record351: [a351, b1, c0, d3, e1]
		//record352: [a352, b0, c1, d0, e2]
		//record353: [a353, b1, c2, d1, e3]
		//record354: [a354, b0, c0, d2, e4]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z2f", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 119, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a6, b0, c0, d2, e1]
		//record3: [a9, b1, c0, d1, e4]
		//record4: [a12, b0, c0, d0, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 119);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ct1u", cols0);
		String [][] records_ct1u = new String[259][cols0.length];
		for(int i=0;i<259;i++)
		{
			records_ct1u[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ct1u[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ct1u", records_ct1u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ct1u", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 65, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 65);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("ww", cols1);
		String [][] records_ww = new String[121][cols1.length];
		for(int i=0;i<121;i++)
		{
			records_ww[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ww[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ww", records_ww[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record116: [a116, b0, c2, d0]
		//record117: [a117, b1, c0, d1]
		//record118: [a118, b0, c1, d2]
		//record119: [a119, b1, c2, d3]
		//record120: [a120, b0, c0, d0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ww", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 40, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 40);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("o4ki", cols0);
		String [][] records_o4ki = new String[266][cols0.length];
		for(int i=0;i<266;i++)
		{
			records_o4ki[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o4ki[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o4ki", records_o4ki[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record261: [a261]
		//record262: [a262]
		//record263: [a263]
		//record264: [a264]
		//record265: [a265]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a225"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o4ki", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a225]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a225"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ab", cols1);
		String [][] records_ab = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			records_ab[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ab[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ab", records_ab[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0, o9]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4, o13]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m11"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ab", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record3: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5]
		//record4: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2","m11","n8","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3","k8","l3","m11","n7","o3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("le3yu", cols0);
		String [][] records_le3yu = new String[118][cols0.length];
		for(int i=0;i<118;i++)
		{
			records_le3yu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_le3yu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("le3yu", records_le3yu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("le3yu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("f83", cols0);
		String [][] records_f83 = new String[276][cols0.length];
		for(int i=0;i<276;i++)
		{
			records_f83[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f83[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f83", records_f83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record271: [a271, b1, c1]
		//record272: [a272, b0, c2]
		//record273: [a273, b1, c0]
		//record274: [a274, b0, c1]
		//record275: [a275, b1, c2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f83", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 138, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 138);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yqv5x", cols1);
		String [][] records_yqv5x = new String[283][cols1.length];
		for(int i=0;i<283;i++)
		{
			records_yqv5x[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yqv5x[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yqv5x", records_yqv5x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2]
		//record279: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9, k4, l3]
		//record280: [a280, b0, c1, d0, e0, f4, g0, h0, i1, j0, k5, l4]
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6]
		String[] ConditionColumns1 = {"k","f","h","e"};
		String[] ConditionColumnsValues1 = {"k8","f5","h3","e2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("yqv5x", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a107","b1","c2","d3","e2","f5","g2","h3","i8","j7","k8","l11"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("is2", cols2);
		String [][] records_is2 = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			records_is2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_is2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("is2", records_is2[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("is2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("n9b", cols0);
		String [][] records_n9b = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_n9b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n9b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n9b", records_n9b[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record37: [a37]
		//record38: [a38]
		//record39: [a39]
		//record40: [a40]
		//record41: [a41]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a11"};
		ArrayList<String[]> tableSelect0 = DBApp.select("n9b", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("ov", cols1);
		String [][] records_ov = new String[163][cols1.length];
		for(int i=0;i<163;i++)
		{
			records_ov[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ov[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ov", records_ov[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record158: [a158, b0, c2, d2, e3, f2]
		//record159: [a159, b1, c0, d3, e4, f3]
		//record160: [a160, b0, c1, d0, e0, f4]
		//record161: [a161, b1, c2, d1, e1, f5]
		//record162: [a162, b0, c0, d2, e2, f0]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ov", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a19, b1, c1, d3, e4, f1]
		//record4: [a25, b1, c1, d1, e0, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 27);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e0", cols2);
		String [][] records_e0 = new String[471][cols2.length];
		for(int i=0;i<471;i++)
		{
			records_e0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e0", records_e0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8]
		String[] ConditionColumns2 = {"b","d","j","h"};
		String[] ConditionColumnsValues2 = {"b0","d0","j6","h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("e0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 11, records:
		//record0: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		//record1: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10]
		//record2: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6]
		//record3: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2]
		//record4: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 11);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a76","b0","c1","d0","e1","f4","g6","h4","i4","j6","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a116","b0","c2","d0","e1","f2","g4","h4","i8","j6","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a156","b0","c0","d0","e1","f0","g2","h4","i3","j6","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a196","b0","c1","d0","e1","f4","g0","h4","i7","j6","k9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("q550", cols3);
		String [][] records_q550 = new String[366][cols3.length];
		for(int i=0;i<366;i++)
		{
			records_q550[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q550[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q550", records_q550[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record361: [a361, b1, c1, d1, e1, f1]
		//record362: [a362, b0, c2, d2, e2, f2]
		//record363: [a363, b1, c0, d3, e3, f3]
		//record364: [a364, b0, c1, d0, e4, f4]
		//record365: [a365, b1, c2, d1, e0, f5]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q550", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 73, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a23, b1, c2, d3, e3, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 73);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yf", cols4);
		String [][] records_yf = new String[358][cols4.length];
		for(int i=0;i<358;i++)
		{
			records_yf[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_yf[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("yf", records_yf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6]
		String[] ConditionColumns4 = {"f","a","b","c"};
		String[] ConditionColumnsValues4 = {"f5","a65","b1","c2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("yf", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2, h1, i2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2","h1","i2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("o0s", cols0);
		String [][] records_o0s = new String[309][cols0.length];
		for(int i=0;i<309;i++)
		{
			records_o0s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0s", records_o0s[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o0s", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 62, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 62);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n36z8", cols1);
		String [][] records_n36z8 = new String[213][cols1.length];
		for(int i=0;i<213;i++)
		{
			records_n36z8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n36z8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n36z8", records_n36z8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n36z8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 43, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 43);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("iph", cols2);
		String [][] records_iph = new String[244][cols2.length];
		for(int i=0;i<244;i++)
		{
			records_iph[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_iph[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("iph", records_iph[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4]
		//record240: [a240, b0, c0, d0, e0]
		//record241: [a241, b1, c1, d1, e1]
		//record242: [a242, b0, c2, d2, e2]
		//record243: [a243, b1, c0, d3, e3]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a81"};
		ArrayList<String[]> tableSelect2 = DBApp.select("iph", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a81, b1, c0, d1, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y2", cols0);
		String [][] records_y2 = new String[471][cols0.length];
		for(int i=0;i<471;i++)
		{
			records_y2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y2", records_y2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 79, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 79);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("erz9", cols1);
		String [][] records_erz9 = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			records_erz9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_erz9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("erz9", records_erz9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1]
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2]
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("erz9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 30, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 30);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("j7j", cols2);
		String [][] records_j7j = new String[471][cols2.length];
		for(int i=0;i<471;i++)
		{
			records_j7j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j7j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j7j", records_j7j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record466: [a466, b0, c1, d2, e1, f4]
		//record467: [a467, b1, c2, d3, e2, f5]
		//record468: [a468, b0, c0, d0, e3, f0]
		//record469: [a469, b1, c1, d1, e4, f1]
		//record470: [a470, b0, c2, d2, e0, f2]
		String[] ConditionColumns2 = {"c","f"};
		String[] ConditionColumnsValues2 = {"c2","f2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("j7j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 79, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a14, b0, c2, d2, e4, f2]
		//record3: [a20, b0, c2, d0, e0, f2]
		//record4: [a26, b0, c2, d2, e1, f2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 79);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("izv", cols3);
		String [][] records_izv = new String[453][cols3.length];
		for(int i=0;i<453;i++)
		{
			records_izv[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_izv[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("izv", records_izv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record448: [a448, b0, c1, d0, e3, f4, g0]
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		//record450: [a450, b0, c0, d2, e0, f0, g2]
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		String[] ConditionColumns3 = {"a","g","f"};
		String[] ConditionColumnsValues3 = {"a65","g2","f5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("izv", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e3554", cols0);
		String [][] records_e3554 = new String[324][cols0.length];
		for(int i=0;i<324;i++)
		{
			records_e3554[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e3554[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e3554", records_e3554[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1, g4]
		//record320: [a320, b0, c2, d0, e0, f2, g5]
		//record321: [a321, b1, c0, d1, e1, f3, g6]
		//record322: [a322, b0, c1, d2, e2, f4, g0]
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("e3554", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 47, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a15, b1, c0, d3, e0, f3, g1]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 47);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("g0a", cols1);
		String [][] records_g0a = new String[449][cols1.length];
		for(int i=0;i<449;i++)
		{
			records_g0a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g0a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g0a", records_g0a[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record444: [a444, b0]
		//record445: [a445, b1]
		//record446: [a446, b0]
		//record447: [a447, b1]
		//record448: [a448, b0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("g0a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 224, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 224);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ogs8", cols2);
		String [][] records_ogs8 = new String[250][cols2.length];
		for(int i=0;i<250;i++)
		{
			records_ogs8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ogs8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ogs8", records_ogs8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11, n7]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0, n9]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11]
		String[] ConditionColumns2 = {"i","a","k"};
		String[] ConditionColumnsValues2 = {"i0","a54","k10"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ogs8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2","n12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("f64q", cols0);
		String [][] records_f64q = new String[171][cols0.length];
		for(int i=0;i<171;i++)
		{
			records_f64q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f64q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f64q", records_f64q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a148"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f64q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a148, b0, c1, d0, e3, f4, g1, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a148","b0","c1","d0","e3","f4","g1","h4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l8", cols1);
		String [][] records_l8 = new String[278][cols1.length];
		for(int i=0;i<278;i++)
		{
			records_l8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l8", records_l8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7, k2, l1, m4]
		String[] ConditionColumns1 = {"b","e"};
		String[] ConditionColumnsValues1 = {"b1","e2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 28, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 28);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z76", cols0);
		String [][] records_z76 = new String[304][cols0.length];
		for(int i=0;i<304;i++)
		{
			records_z76[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z76[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z76", records_z76[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2, l11, m0, n5, o14]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0, m1, n6, o0]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1, m2, n7, o1]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3, n8, o2]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4, n9, o3]
		String[] ConditionColumns0 = {"f","e","l","h","i"};
		String[] ConditionColumnsValues0 = {"f4","e4","l10","h2","i7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("z76", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8","n6","o4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("h4gn", cols1);
		String [][] records_h4gn = new String[268][cols1.length];
		for(int i=0;i<268;i++)
		{
			records_h4gn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h4gn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h4gn", records_h4gn[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record263: [a263, b1]
		//record264: [a264, b0]
		//record265: [a265, b1]
		//record266: [a266, b0]
		//record267: [a267, b1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h4gn", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 134, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 134);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("eb4rh", cols2);
		String [][] records_eb4rh = new String[165][cols2.length];
		for(int i=0;i<165;i++)
		{
			records_eb4rh[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eb4rh[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eb4rh", records_eb4rh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0, f4]
		//record161: [a161, b1, c2, d1, e1, f5]
		//record162: [a162, b0, c0, d2, e2, f0]
		//record163: [a163, b1, c1, d3, e3, f1]
		//record164: [a164, b0, c2, d0, e4, f2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("eb4rh", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 55, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a7, b1, c1, d3, e2, f1]
		//record3: [a10, b0, c1, d2, e0, f4]
		//record4: [a13, b1, c1, d1, e3, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 55);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j6p", cols3);
		String [][] records_j6p = new String[303][cols3.length];
		for(int i=0;i<303;i++)
		{
			records_j6p[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j6p[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j6p", records_j6p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1]
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5]
		String[] ConditionColumns3 = {"b","d","h"};
		String[] ConditionColumnsValues3 = {"b1","d1","h5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("j6p", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 38, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 38);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p551", cols4);
		String [][] records_p551 = new String[479][cols4.length];
		for(int i=0;i<479;i++)
		{
			records_p551[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_p551[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("p551", records_p551[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10]
		String[] ConditionColumns4 = {"b","i","m","j","d","k"};
		String[] ConditionColumnsValues4 = {"b0","i8","m5","j8","d2","k3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("p551", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2, m5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a278","b0","c2","d2","e3","f2","g5","h6","i8","j8","k3","l2","m5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a8h", cols0);
		String [][] records_a8h = new String[224][cols0.length];
		for(int i=0;i<224;i++)
		{
			records_a8h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a8h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a8h", records_a8h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0, n11]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2, n13]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a8h", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 45, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 45);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u5", cols1);
		String [][] records_u5 = new String[125][cols1.length];
		for(int i=0;i<125;i++)
		{
			records_u5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u5", records_u5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4]
		String[] ConditionColumns1 = {"h","j","g"};
		String[] ConditionColumnsValues1 = {"h1","j3","g5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("u5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("i4c63", cols0);
		String [][] records_i4c63 = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_i4c63[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i4c63[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i4c63", records_i4c63[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record172: [a172]
		//record173: [a173]
		//record174: [a174]
		//record175: [a175]
		//record176: [a176]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a76"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i4c63", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a76]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a76"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("cj5r", cols0);
		String [][] records_cj5r = new String[349][cols0.length];
		for(int i=0;i<349;i++)
		{
			records_cj5r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cj5r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cj5r", records_cj5r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3]
		String[] ConditionColumns0 = {"j","b","h","f","m","k"};
		String[] ConditionColumnsValues0 = {"j0","b0","h2","f0","m12","k2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cj5r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6, m12, n6, o0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a90","b0","c0","d2","e0","f0","g6","h2","i0","j0","k2","l6","m12","n6","o0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("acz", cols1);
		String [][] records_acz = new String[198][cols1.length];
		for(int i=0;i<198;i++)
		{
			records_acz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_acz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("acz", records_acz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8]
		String[] ConditionColumns1 = {"c","e","g"};
		String[] ConditionColumnsValues1 = {"c2","e0","g6"};
		ArrayList<String[]> tableSelect1 = DBApp.select("acz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record1: [a125, b1, c2, d1, e0, f5, g6, h5, i8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a125","b1","c2","d1","e0","f5","g6","h5","i8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zg407", cols2);
		String [][] records_zg407 = new String[41][cols2.length];
		for(int i=0;i<41;i++)
		{
			records_zg407[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zg407[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zg407", records_zg407[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0]
		String[] ConditionColumns2 = {"c","h"};
		String[] ConditionColumnsValues2 = {"c1","h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("zg407", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 2);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("j5r", cols3);
		String [][] records_j5r = new String[179][cols3.length];
		for(int i=0;i<179;i++)
		{
			records_j5r[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j5r[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j5r", records_j5r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6, n7, o10]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8, o11]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1, l9, m8, n9, o12]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9, n10, o13]
		String[] ConditionColumns3 = {"l","n"};
		String[] ConditionColumnsValues3 = {"l8","n6"};
		ArrayList<String[]> tableSelect3 = DBApp.select("j5r", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		//record1: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 2);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6","h0","i5","j4","k5","l8","m0","n6","o14"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l9ie4", cols0);
		String [][] records_l9ie4 = new String[269][cols0.length];
		for(int i=0;i<269;i++)
		{
			records_l9ie4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l9ie4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l9ie4", records_l9ie4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a229"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l9ie4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a229","b1","c1","d1","e4","f1","g5","h5","i4","j9","k9","l1","m8","n5","o4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("x5", cols1);
		String [][] records_x5 = new String[335][cols1.length];
		for(int i=0;i<335;i++)
		{
			records_x5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x5", records_x5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record330: [a330, b0, c0]
		//record331: [a331, b1, c1]
		//record332: [a332, b0, c2]
		//record333: [a333, b1, c0]
		//record334: [a334, b0, c1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("x5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 167, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 167);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m10nz", cols0);
		String [][] records_m10nz = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_m10nz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m10nz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m10nz", records_m10nz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		String[] ConditionColumns0 = {"e","c"};
		String[] ConditionColumnsValues0 = {"e1","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("m10nz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("k07", cols1);
		String [][] records_k07 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			records_k07[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k07[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k07", records_k07[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record64: [a64, b0, c1]
		//record65: [a65, b1, c2]
		//record66: [a66, b0, c0]
		//record67: [a67, b1, c1]
		//record68: [a68, b0, c2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k07", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 34, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 34);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("xuf", cols2);
		String [][] records_xuf = new String[411][cols2.length];
		for(int i=0;i<411;i++)
		{
			records_xuf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xuf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xuf", records_xuf[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record406: [a406]
		//record407: [a407]
		//record408: [a408]
		//record409: [a409]
		//record410: [a410]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a56"};
		ArrayList<String[]> tableSelect2 = DBApp.select("xuf", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a56]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a56"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("zf", cols0);
		String [][] records_zf = new String[409][cols0.length];
		for(int i=0;i<409;i++)
		{
			records_zf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zf", records_zf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record404: [a404, b0, c2, d0]
		//record405: [a405, b1, c0, d1]
		//record406: [a406, b0, c1, d2]
		//record407: [a407, b1, c2, d3]
		//record408: [a408, b0, c0, d0]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 102, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 102);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("g82", cols1);
		String [][] records_g82 = new String[499][cols1.length];
		for(int i=0;i<499;i++)
		{
			records_g82[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g82[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g82", records_g82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3]
		//record498: [a498, b0, c0, d2, e3, f0, g1, h2, i3, j8, k3, l6, m4]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("g82", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 45, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record4: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 45);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3","m12"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a8", cols2);
		String [][] records_a8 = new String[212][cols2.length];
		for(int i=0;i<212;i++)
		{
			records_a8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a8", records_a8[i]);
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
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 71, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 71);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k9", cols3);
		String [][] records_k9 = new String[153][cols3.length];
		for(int i=0;i<153;i++)
		{
			records_k9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k9", records_k9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9]
		String[] ConditionColumns3 = {"f","j","c","g"};
		String[] ConditionColumnsValues3 = {"f4","j6","c1","g1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("k9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a106","b0","c1","d2","e1","f4","g1","h2","i7","j6","k7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("odj4", cols0);
		String [][] records_odj4 = new String[459][cols0.length];
		for(int i=0;i<459;i++)
		{
			records_odj4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_odj4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("odj4", records_odj4[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record454: [a454, b0]
		//record455: [a455, b1]
		//record456: [a456, b0]
		//record457: [a457, b1]
		//record458: [a458, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("odj4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 229, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 229);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("t0t", cols1);
		String [][] records_t0t = new String[320][cols1.length];
		for(int i=0;i<320;i++)
		{
			records_t0t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t0t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t0t", records_t0t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4]
		String[] ConditionColumns1 = {"b","h"};
		String[] ConditionColumnsValues1 = {"b0","h0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("t0t", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 40, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 40);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("lo77", cols2);
		String [][] records_lo77 = new String[395][cols2.length];
		for(int i=0;i<395;i++)
		{
			records_lo77[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lo77[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lo77", records_lo77[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record390: [a390, b0, c0]
		//record391: [a391, b1, c1]
		//record392: [a392, b0, c2]
		//record393: [a393, b1, c0]
		//record394: [a394, b0, c1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("lo77", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 198, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 198);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("td", cols3);
		String [][] records_td = new String[487][cols3.length];
		for(int i=0;i<487;i++)
		{
			records_td[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_td[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("td", records_td[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record482: [a482, b0, c2, d2, e2, f2, g6]
		//record483: [a483, b1, c0, d3, e3, f3, g0]
		//record484: [a484, b0, c1, d0, e4, f4, g1]
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		//record486: [a486, b0, c0, d2, e1, f0, g3]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a280"};
		ArrayList<String[]> tableSelect3 = DBApp.select("td", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a280, b0, c1, d0, e0, f4, g0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a280","b0","c1","d0","e0","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("qhger", cols4);
		String [][] records_qhger = new String[55][cols4.length];
		for(int i=0;i<55;i++)
		{
			records_qhger[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_qhger[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("qhger", records_qhger[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record50: [a50, b0, c2, d2, e0]
		//record51: [a51, b1, c0, d3, e1]
		//record52: [a52, b0, c1, d0, e2]
		//record53: [a53, b1, c2, d1, e3]
		//record54: [a54, b0, c0, d2, e4]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("qhger", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 18, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 18);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qo", cols0);
		String [][] records_qo = new String[130][cols0.length];
		for(int i=0;i<130;i++)
		{
			records_qo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qo", records_qo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2, o8]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("qo", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 15);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1","n13","o12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8","o6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("su", cols0);
		String [][] records_su = new String[366][cols0.length];
		for(int i=0;i<366;i++)
		{
			records_su[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_su[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("su", records_su[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record361: [a361, b1, c1, d1, e1]
		//record362: [a362, b0, c2, d2, e2]
		//record363: [a363, b1, c0, d3, e3]
		//record364: [a364, b0, c1, d0, e4]
		//record365: [a365, b1, c2, d1, e0]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("su", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 73, records:
		//record0: [a4, b0, c1, d0, e4]
		//record1: [a9, b1, c0, d1, e4]
		//record2: [a14, b0, c2, d2, e4]
		//record3: [a19, b1, c1, d3, e4]
		//record4: [a24, b0, c0, d0, e4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 73);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gzt", cols1);
		String [][] records_gzt = new String[269][cols1.length];
		for(int i=0;i<269;i++)
		{
			records_gzt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gzt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gzt", records_gzt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4]
		String[] ConditionColumns1 = {"d","g","h","c","j"};
		String[] ConditionColumnsValues1 = {"d3","g5","h7","c0","j9"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gzt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a159","b1","c0","d3","e4","f3","g5","h7","i6","j9","k5","l3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q1b71", cols2);
		String [][] records_q1b71 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			records_q1b71[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q1b71[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q1b71", records_q1b71[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1]
		String[] ConditionColumns2 = {"e","f","d"};
		String[] ConditionColumnsValues2 = {"e1","f1","d1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("q1b71", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s54", cols0);
		String [][] records_s54 = new String[255][cols0.length];
		for(int i=0;i<255;i++)
		{
			records_s54[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s54[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s54", records_s54[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s54", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 32, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 32);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("gt7", cols1);
		String [][] records_gt7 = new String[232][cols1.length];
		for(int i=0;i<232;i++)
		{
			records_gt7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gt7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gt7", records_gt7[i]);
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
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gt7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 47, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a20, b0, c2, d0, e0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 47);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v22", cols2);
		String [][] records_v22 = new String[436][cols2.length];
		for(int i=0;i<436;i++)
		{
			records_v22[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v22[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v22", records_v22[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("v22", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 109, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 109);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f68", cols0);
		String [][] records_f68 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			records_f68[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f68[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f68", records_f68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f68", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 5);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xtj", cols1);
		String [][] records_xtj = new String[148][cols1.length];
		for(int i=0;i<148;i++)
		{
			records_xtj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xtj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xtj", records_xtj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xtj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 17, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 17);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a734b", cols2);
		String [][] records_a734b = new String[296][cols2.length];
		for(int i=0;i<296;i++)
		{
			records_a734b[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a734b[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a734b", records_a734b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a734b", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 60, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 60);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bo", cols0);
		String [][] records_bo = new String[386][cols0.length];
		for(int i=0;i<386;i++)
		{
			records_bo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bo", records_bo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		//record384: [a384, b0, c0, d0, e4, f0, g6]
		//record385: [a385, b1, c1, d1, e0, f1, g0]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bo", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 64, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a20, b0, c2, d0, e0, f2, g6]
		//record4: [a26, b0, c2, d2, e1, f2, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 64);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n66", cols0);
		String [][] records_n66 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			records_n66[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n66[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n66", records_n66[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		String[] ConditionColumns0 = {"j","b","f","m","l","d"};
		String[] ConditionColumnsValues0 = {"j5","b1","f1","m12","l1","d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("n66", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ak", cols1);
		String [][] records_ak = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			records_ak[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ak[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ak", records_ak[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		String[] ConditionColumns1 = {"b","g"};
		String[] ConditionColumnsValues1 = {"b1","g0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ak", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0]
		//record1: [a21, b1, c0, d1, e1, f3, g0]
		//record2: [a35, b1, c2, d3, e0, f5, g0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ov6", cols0);
		String [][] records_ov6 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			records_ov6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ov6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ov6", records_ov6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0]
		String[] ConditionColumns0 = {"g","i"};
		String[] ConditionColumnsValues0 = {"g4","i3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ov6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("b5", cols1);
		String [][] records_b5 = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			records_b5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b5", records_b5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record33: [a33, b1, c0, d1, e3, f3]
		//record34: [a34, b0, c1, d2, e4, f4]
		//record35: [a35, b1, c2, d3, e0, f5]
		//record36: [a36, b0, c0, d0, e1, f0]
		//record37: [a37, b1, c1, d1, e2, f1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 13, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a7, b1, c1, d3, e2, f1]
		//record3: [a10, b0, c1, d2, e0, f4]
		//record4: [a13, b1, c1, d1, e3, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 13);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r12p", cols2);
		String [][] records_r12p = new String[404][cols2.length];
		for(int i=0;i<404;i++)
		{
			records_r12p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r12p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r12p", records_r12p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3, l3, m9]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0]
		String[] ConditionColumns2 = {"j"};
		String[] ConditionColumnsValues2 = {"j8"};
		ArrayList<String[]> tableSelect2 = DBApp.select("r12p", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 40, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record3: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 40);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0","i3","j8","k4","l0","m9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("zn8g", cols3);
		String [][] records_zn8g = new String[173][cols3.length];
		for(int i=0;i<173;i++)
		{
			records_zn8g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zn8g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zn8g", records_zn8g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0]
		//record169: [a169, b1, c1, d1, e4, f1]
		//record170: [a170, b0, c2, d2, e0, f2]
		//record171: [a171, b1, c0, d3, e1, f3]
		//record172: [a172, b0, c1, d0, e2, f4]
		String[] ConditionColumns3 = {"f","c"};
		String[] ConditionColumnsValues3 = {"f2","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("zn8g", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 29, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a14, b0, c2, d2, e4, f2]
		//record3: [a20, b0, c2, d0, e0, f2]
		//record4: [a26, b0, c2, d2, e1, f2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 29);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h68", cols4);
		String [][] records_h68 = new String[460][cols4.length];
		for(int i=0;i<460;i++)
		{
			records_h68[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_h68[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("h68", records_h68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1, n8]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1, m2, n9]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2, m3, n10]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0, j9, k8, l3, m4, n11]
		String[] ConditionColumns4 = {"k","l","a","n","f"};
		String[] ConditionColumnsValues4 = {"k3","l10","a322","n0","f4"};
		ArrayList<String[]> tableSelect4 = DBApp.select("h68", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a322","b0","c1","d2","e2","f4","g0","h2","i7","j2","k3","l10","m10","n0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("tf97", cols0);
		String [][] records_tf97 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			records_tf97[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tf97[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tf97", records_tf97[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("tf97", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 12);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("eqy4", cols1);
		String [][] records_eqy4 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			records_eqy4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_eqy4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("eqy4", records_eqy4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11, n5]
		String[] ConditionColumns1 = {"l","c","k"};
		String[] ConditionColumnsValues1 = {"l9","c0","k2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("eqy4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a57","b1","c0","d1","e2","f3","g1","h1","i3","j7","k2","l9","m5","n1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("es5", cols0);
		String [][] records_es5 = new String[288][cols0.length];
		for(int i=0;i<288;i++)
		{
			records_es5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_es5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("es5", records_es5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1]
		String[] ConditionColumns0 = {"h","i","k"};
		String[] ConditionColumnsValues0 = {"h7","i6","k0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("es5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a231","b1","c0","d3","e1","f3","g0","h7","i6","j1","k0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jg7", cols0);
		String [][] records_jg7 = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			records_jg7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jg7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jg7", records_jg7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1]
		//record93: [a93, b1, c0, d1, e3, f3, g2]
		//record94: [a94, b0, c1, d2, e4, f4, g3]
		//record95: [a95, b1, c2, d3, e0, f5, g4]
		//record96: [a96, b0, c0, d0, e1, f0, g5]
		String[] ConditionColumns0 = {"e","d","b"};
		String[] ConditionColumnsValues0 = {"e2","d0","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("jg7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a32, b0, c2, d0, e2, f2, g4]
		//record2: [a52, b0, c1, d0, e2, f4, g3]
		//record3: [a72, b0, c0, d0, e2, f0, g2]
		//record4: [a92, b0, c2, d0, e2, f2, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 5);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a92","b0","c2","d0","e2","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hadk5", cols1);
		String [][] records_hadk5 = new String[295][cols1.length];
		for(int i=0;i<295;i++)
		{
			records_hadk5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hadk5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hadk5", records_hadk5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6]
		String[] ConditionColumns1 = {"l"};
		String[] ConditionColumnsValues1 = {"l9"};
		ArrayList<String[]> tableSelect1 = DBApp.select("hadk5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record2: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record4: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 24);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a57","b1","c0","d1","e2","f3","g1","h1","i3","j7","k2","l9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("olf", cols2);
		String [][] records_olf = new String[308][cols2.length];
		for(int i=0;i<308;i++)
		{
			records_olf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_olf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("olf", records_olf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record303: [a303, b1, c0, d3, e3, f3]
		//record304: [a304, b0, c1, d0, e4, f4]
		//record305: [a305, b1, c2, d1, e0, f5]
		//record306: [a306, b0, c0, d2, e1, f0]
		//record307: [a307, b1, c1, d3, e2, f1]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("olf", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 62, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a5, b1, c2, d1, e0, f5]
		//record2: [a10, b0, c1, d2, e0, f4]
		//record3: [a15, b1, c0, d3, e0, f3]
		//record4: [a20, b0, c2, d0, e0, f2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 62);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("tea", cols3);
		String [][] records_tea = new String[329][cols3.length];
		for(int i=0;i<329;i++)
		{
			records_tea[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tea[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tea", records_tea[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("tea", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 54, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 54);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("r7h", cols4);
		String [][] records_r7h = new String[474][cols4.length];
		for(int i=0;i<474;i++)
		{
			records_r7h[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r7h[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r7h", records_r7h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record469: [a469, b1, c1, d1, e4, f1]
		//record470: [a470, b0, c2, d2, e0, f2]
		//record471: [a471, b1, c0, d3, e1, f3]
		//record472: [a472, b0, c1, d0, e2, f4]
		//record473: [a473, b1, c2, d1, e3, f5]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("r7h", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 118, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a11, b1, c2, d3, e1, f5]
		//record3: [a15, b1, c0, d3, e0, f3]
		//record4: [a19, b1, c1, d3, e4, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 118);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y5g", cols0);
		String [][] records_y5g = new String[215][cols0.length];
		for(int i=0;i<215;i++)
		{
			records_y5g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y5g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y5g", records_y5g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8, m4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y5g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 17);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("buwg", cols1);
		String [][] records_buwg = new String[265][cols1.length];
		for(int i=0;i<265;i++)
		{
			records_buwg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_buwg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("buwg", records_buwg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record260: [a260, b0, c2, d0, e0]
		//record261: [a261, b1, c0, d1, e1]
		//record262: [a262, b0, c1, d2, e2]
		//record263: [a263, b1, c2, d3, e3]
		//record264: [a264, b0, c0, d0, e4]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("buwg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 53, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a20, b0, c2, d0, e0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 53);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i2", cols2);
		String [][] records_i2 = new String[371][cols2.length];
		for(int i=0;i<371;i++)
		{
			records_i2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i2", records_i2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6, m2]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7, m3]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8, m4]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6]
		String[] ConditionColumns2 = {"g","d","e","b","l"};
		String[] ConditionColumnsValues2 = {"g4","d3","e1","b1","l3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("i2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a291","b1","c0","d3","e1","f3","g4","h3","i3","j1","k5","l3","m5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p1306", cols3);
		String [][] records_p1306 = new String[54][cols3.length];
		for(int i=0;i<54;i++)
		{
			records_p1306[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p1306[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p1306", records_p1306[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p1306", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 14, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 14);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("x60bw", cols4);
		String [][] records_x60bw = new String[131][cols4.length];
		for(int i=0;i<131;i++)
		{
			records_x60bw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x60bw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x60bw", records_x60bw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record126: [a126, b0, c0, d2, e1]
		//record127: [a127, b1, c1, d3, e2]
		//record128: [a128, b0, c2, d0, e3]
		//record129: [a129, b1, c0, d1, e4]
		//record130: [a130, b0, c1, d2, e0]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("x60bw", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 65, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 65);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p2n3x", cols0);
		String [][] records_p2n3x = new String[421][cols0.length];
		for(int i=0;i<421;i++)
		{
			records_p2n3x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p2n3x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p2n3x", records_p2n3x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0, n10, o11]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2, n12, o13]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9, k1, l11, m3, n13, o14]
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0, m4, n0, o0]
		String[] ConditionColumns0 = {"k","o","i","h","n"};
		String[] ConditionColumnsValues0 = {"k4","o11","i5","h4","n6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("p2n3x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a356","b0","c2","d0","e1","f2","g6","h4","i5","j6","k4","l8","m5","n6","o11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q2", cols1);
		String [][] records_q2 = new String[251][cols1.length];
		for(int i=0;i<251;i++)
		{
			records_q2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q2", records_q2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1]
		//record247: [a247, b1, c1, d3, e2, f1, g2]
		//record248: [a248, b0, c2, d0, e3, f2, g3]
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("q2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 126, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 126);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("rs1j", cols2);
		String [][] records_rs1j = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			records_rs1j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rs1j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rs1j", records_rs1j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record7: [a7, b1, c1, d3, e2]
		//record8: [a8, b0, c2, d0, e3]
		//record9: [a9, b1, c0, d1, e4]
		//record10: [a10, b0, c1, d2, e0]
		//record11: [a11, b1, c2, d3, e1]
		String[] ConditionColumns2 = {"e","a"};
		String[] ConditionColumnsValues2 = {"e0","a10"};
		ArrayList<String[]> tableSelect2 = DBApp.select("rs1j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("z0x", cols3);
		String [][] records_z0x = new String[301][cols3.length];
		for(int i=0;i<301;i++)
		{
			records_z0x[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z0x[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z0x", records_z0x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record296: [a296, b0, c2, d0]
		//record297: [a297, b1, c0, d1]
		//record298: [a298, b0, c1, d2]
		//record299: [a299, b1, c2, d3]
		//record300: [a300, b0, c0, d0]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("z0x", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 75, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 75);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ma", cols0);
		String [][] records_ma = new String[491][cols0.length];
		for(int i=0;i<491;i++)
		{
			records_ma[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ma[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ma", records_ma[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ma", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 246, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 246);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xj", cols1);
		String [][] records_xj = new String[200][cols1.length];
		for(int i=0;i<200;i++)
		{
			records_xj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xj", records_xj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1, l7, m4]
		String[] ConditionColumns1 = {"l"};
		String[] ConditionColumnsValues1 = {"l1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 17, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 17);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("y4", cols0);
		String [][] records_y4 = new String[300][cols0.length];
		for(int i=0;i<300;i++)
		{
			records_y4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y4", records_y4[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record295: [a295, b1]
		//record296: [a296, b0]
		//record297: [a297, b1]
		//record298: [a298, b0]
		//record299: [a299, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 150, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 150);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("h4", cols1);
		String [][] records_h4 = new String[369][cols1.length];
		for(int i=0;i<369;i++)
		{
			records_h4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h4", records_h4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4]
		//record365: [a365, b1, c2, d1, e0]
		//record366: [a366, b0, c0, d2, e1]
		//record367: [a367, b1, c1, d3, e2]
		//record368: [a368, b0, c2, d0, e3]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 74, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a7, b1, c1, d3, e2]
		//record2: [a12, b0, c0, d0, e2]
		//record3: [a17, b1, c2, d1, e2]
		//record4: [a22, b0, c1, d2, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 74);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("tqe", cols2);
		String [][] records_tqe = new String[297][cols2.length];
		for(int i=0;i<297;i++)
		{
			records_tqe[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tqe[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tqe", records_tqe[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record292: [a292, b0, c1, d0, e2]
		//record293: [a293, b1, c2, d1, e3]
		//record294: [a294, b0, c0, d2, e4]
		//record295: [a295, b1, c1, d3, e0]
		//record296: [a296, b0, c2, d0, e1]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("tqe", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 99, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 99);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g0", cols0);
		String [][] records_g0 = new String[242][cols0.length];
		for(int i=0;i<242;i++)
		{
			records_g0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g0", records_g0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10]
		String[] ConditionColumns0 = {"e","k"};
		String[] ConditionColumnsValues0 = {"e4","k0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0]
		//record1: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0]
		//record2: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0]
		//record3: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a154","b0","c1","d2","e4","f4","g0","h2","i1","j4","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a209","b1","c2","d1","e4","f5","g6","h1","i2","j9","k0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("uur", cols1);
		String [][] records_uur = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			records_uur[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uur[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uur", records_uur[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		String[] ConditionColumns1 = {"m","l","d"};
		String[] ConditionColumnsValues1 = {"m5","l6","d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("uur", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a205t", cols2);
		String [][] records_a205t = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			records_a205t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a205t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a205t", records_a205t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a205t", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 16, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 16);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("fbv02", cols0);
		String [][] records_fbv02 = new String[103][cols0.length];
		for(int i=0;i<103;i++)
		{
			records_fbv02[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fbv02[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fbv02", records_fbv02[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("fbv02", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 21);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d21", cols1);
		String [][] records_d21 = new String[375][cols1.length];
		for(int i=0;i<375;i++)
		{
			records_d21[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d21[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d21", records_d21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("d21", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 188, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 188);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ix4e", cols2);
		String [][] records_ix4e = new String[124][cols2.length];
		for(int i=0;i<124;i++)
		{
			records_ix4e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ix4e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ix4e", records_ix4e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3]
		String[] ConditionColumns2 = {"l"};
		String[] ConditionColumnsValues2 = {"l2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ix4e", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 11, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record3: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2]
		//record4: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 11);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q41e1", cols0);
		String [][] records_q41e1 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			records_q41e1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q41e1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q41e1", records_q41e1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("q41e1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record4: [a20, b0, c2, d0, e0, f2, g6, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 5);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("wfcd", cols1);
		String [][] records_wfcd = new String[121][cols1.length];
		for(int i=0;i<121;i++)
		{
			records_wfcd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wfcd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wfcd", records_wfcd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record116: [a116, b0, c2]
		//record117: [a117, b1, c0]
		//record118: [a118, b0, c1]
		//record119: [a119, b1, c2]
		//record120: [a120, b0, c0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wfcd", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 61, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 61);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("t4f", cols0);
		String [][] records_t4f = new String[173][cols0.length];
		for(int i=0;i<173;i++)
		{
			records_t4f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t4f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t4f", records_t4f[i]);
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
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("t4f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4","n1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("v7y", cols1);
		String [][] records_v7y = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_v7y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v7y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v7y", records_v7y[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record42: [a42, b0]
		//record43: [a43, b1]
		//record44: [a44, b0]
		//record45: [a45, b1]
		//record46: [a46, b0]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a27"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v7y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a27, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a27","b1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("qn6", cols2);
		String [][] records_qn6 = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			records_qn6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qn6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qn6", records_qn6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record92: [a92, b0, c2, d0]
		//record93: [a93, b1, c0, d1]
		//record94: [a94, b0, c1, d2]
		//record95: [a95, b1, c2, d3]
		//record96: [a96, b0, c0, d0]
		String[] ConditionColumns2 = {"d","b"};
		String[] ConditionColumnsValues2 = {"d3","b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qn6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 24, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 24);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j21", cols3);
		String [][] records_j21 = new String[114][cols3.length];
		for(int i=0;i<114;i++)
		{
			records_j21[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j21[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j21", records_j21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("j21", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 16, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 16);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("h3e", cols0);
		String [][] records_h3e = new String[196][cols0.length];
		for(int i=0;i<196;i++)
		{
			records_h3e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h3e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h3e", records_h3e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h3e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("nf", cols0);
		String [][] records_nf = new String[453][cols0.length];
		for(int i=0;i<453;i++)
		{
			records_nf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nf", records_nf[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record448: [a448, b0]
		//record449: [a449, b1]
		//record450: [a450, b0]
		//record451: [a451, b1]
		//record452: [a452, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 227, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 227);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("rd7", cols0);
		String [][] records_rd7 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_rd7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rd7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rd7", records_rd7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		String[] ConditionColumns0 = {"h","e","c","i","f"};
		String[] ConditionColumnsValues0 = {"h3","e4","c1","i1","f1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rd7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l554t", cols1);
		String [][] records_l554t = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			records_l554t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l554t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l554t", records_l554t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record82: [a82, b0, c1, d2, e2, f4, g5]
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		String[] ConditionColumns1 = {"c","g","d"};
		String[] ConditionColumnsValues1 = {"c1","g4","d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l554t", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a46, b0, c1, d2, e1, f4, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b67u2", cols0);
		String [][] records_b67u2 = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			records_b67u2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b67u2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b67u2", records_b67u2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		String[] ConditionColumns0 = {"f","g","h"};
		String[] ConditionColumnsValues0 = {"f5","g5","h5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b67u2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("r2i", cols1);
		String [][] records_r2i = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			records_r2i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r2i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r2i", records_r2i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11]
		String[] ConditionColumns1 = {"g","d","e","f"};
		String[] ConditionColumnsValues1 = {"g6","d2","e2","f2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r2i", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a62","b0","c2","d2","e2","f2","g6","h6","i8","j2","k7","l2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("d88", cols2);
		String [][] records_d88 = new String[193][cols2.length];
		for(int i=0;i<193;i++)
		{
			records_d88[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d88[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d88", records_d88[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record188: [a188, b0, c2]
		//record189: [a189, b1, c0]
		//record190: [a190, b0, c1]
		//record191: [a191, b1, c2]
		//record192: [a192, b0, c0]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a157"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d88", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a157, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a157","b1","c1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("hc7", cols3);
		String [][] records_hc7 = new String[403][cols3.length];
		for(int i=0;i<403;i++)
		{
			records_hc7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hc7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hc7", records_hc7[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record398: [a398, b0]
		//record399: [a399, b1]
		//record400: [a400, b0]
		//record401: [a401, b1]
		//record402: [a402, b0]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("hc7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 201, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 201);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("j8i", cols4);
		String [][] records_j8i = new String[233][cols4.length];
		for(int i=0;i<233;i++)
		{
			records_j8i[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j8i[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j8i", records_j8i[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record228: [a228, b0]
		//record229: [a229, b1]
		//record230: [a230, b0]
		//record231: [a231, b1]
		//record232: [a232, b0]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("j8i", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 117, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 117);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j9x", cols0);
		String [][] records_j9x = new String[240][cols0.length];
		for(int i=0;i<240;i++)
		{
			records_j9x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j9x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j9x", records_j9x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11]
		String[] ConditionColumns0 = {"l","e","k","b","f","g"};
		String[] ConditionColumnsValues0 = {"l0","e4","k2","b0","f0","g3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j9x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zn44n", cols1);
		String [][] records_zn44n = new String[235][cols1.length];
		for(int i=0;i<235;i++)
		{
			records_zn44n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zn44n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zn44n", records_zn44n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6]
		//record231: [a231, b1, c0, d3, e1, f3, g0]
		//record232: [a232, b0, c1, d0, e2, f4, g1]
		//record233: [a233, b1, c2, d1, e3, f5, g2]
		//record234: [a234, b0, c0, d2, e4, f0, g3]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a103"};
		ArrayList<String[]> tableSelect1 = DBApp.select("zn44n", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a103, b1, c1, d3, e3, f1, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a103","b1","c1","d3","e3","f1","g5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("lxkyu", cols2);
		String [][] records_lxkyu = new String[186][cols2.length];
		for(int i=0;i<186;i++)
		{
			records_lxkyu[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lxkyu[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lxkyu", records_lxkyu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record181: [a181, b1, c1, d1, e1, f1]
		//record182: [a182, b0, c2, d2, e2, f2]
		//record183: [a183, b1, c0, d3, e3, f3]
		//record184: [a184, b0, c1, d0, e4, f4]
		//record185: [a185, b1, c2, d1, e0, f5]
		String[] ConditionColumns2 = {"a","d","f"};
		String[] ConditionColumnsValues2 = {"a88","d0","f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("lxkyu", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a88, b0, c1, d0, e3, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a88","b0","c1","d0","e3","f4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lv3qv", cols3);
		String [][] records_lv3qv = new String[386][cols3.length];
		for(int i=0;i<386;i++)
		{
			records_lv3qv[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lv3qv[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lv3qv", records_lv3qv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8]
		String[] ConditionColumns3 = {"h"};
		String[] ConditionColumnsValues3 = {"h1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("lv3qv", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 49, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 49);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("amh7", cols0);
		String [][] records_amh7 = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			records_amh7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_amh7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("amh7", records_amh7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7]
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8]
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9]
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("amh7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 21);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b7geo", cols0);
		String [][] records_b7geo = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			records_b7geo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b7geo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b7geo", records_b7geo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2]
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b7geo", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r668g", cols1);
		String [][] records_r668g = new String[148][cols1.length];
		for(int i=0;i<148;i++)
		{
			records_r668g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r668g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r668g", records_r668g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3]
		//record144: [a144, b0, c0, d0, e4, f0, g4]
		//record145: [a145, b1, c1, d1, e0, f1, g5]
		//record146: [a146, b0, c2, d2, e1, f2, g6]
		//record147: [a147, b1, c0, d3, e2, f3, g0]
		String[] ConditionColumns1 = {"d","f","g"};
		String[] ConditionColumnsValues1 = {"d1","f1","g0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r668g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a49, b1, c1, d1, e4, f1, g0]
		//record1: [a133, b1, c1, d1, e3, f1, g0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a133","b1","c1","d1","e3","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("rc4", cols2);
		String [][] records_rc4 = new String[324][cols2.length];
		for(int i=0;i<324;i++)
		{
			records_rc4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rc4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rc4", records_rc4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1]
		//record320: [a320, b0, c2, d0, e0, f2]
		//record321: [a321, b1, c0, d1, e1, f3]
		//record322: [a322, b0, c1, d2, e2, f4]
		//record323: [a323, b1, c2, d3, e3, f5]
		String[] ConditionColumns2 = {"e","d","a"};
		String[] ConditionColumnsValues2 = {"e4","d1","a49"};
		ArrayList<String[]> tableSelect2 = DBApp.select("rc4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a49, b1, c1, d1, e4, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t48", cols3);
		String [][] records_t48 = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			records_t48[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t48[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t48", records_t48[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		String[] ConditionColumns3 = {"j"};
		String[] ConditionColumnsValues3 = {"j9"};
		ArrayList<String[]> tableSelect3 = DBApp.select("t48", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lf", cols4);
		String [][] records_lf = new String[312][cols4.length];
		for(int i=0;i<312;i++)
		{
			records_lf[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_lf[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("lf", records_lf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record307: [a307, b1, c1, d3, e2, f1, g6]
		//record308: [a308, b0, c2, d0, e3, f2, g0]
		//record309: [a309, b1, c0, d1, e4, f3, g1]
		//record310: [a310, b0, c1, d2, e0, f4, g2]
		//record311: [a311, b1, c2, d3, e1, f5, g3]
		String[] ConditionColumns4 = {"g","d"};
		String[] ConditionColumnsValues4 = {"g5","d2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("lf", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 11, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5]
		//record1: [a54, b0, c0, d2, e4, f0, g5]
		//record2: [a82, b0, c1, d2, e2, f4, g5]
		//record3: [a110, b0, c2, d2, e0, f2, g5]
		//record4: [a138, b0, c0, d2, e3, f0, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 11);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a82","b0","c1","d2","e2","f4","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a110","b0","c2","d2","e0","f2","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a138","b0","c0","d2","e3","f0","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ck1", cols0);
		String [][] records_ck1 = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_ck1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ck1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ck1", records_ck1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record493: [a493, b1, c1, d1]
		//record494: [a494, b0, c2, d2]
		//record495: [a495, b1, c0, d3]
		//record496: [a496, b0, c1, d0]
		//record497: [a497, b1, c2, d1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ck1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 166, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 166);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ikw", cols0);
		String [][] records_ikw = new String[305][cols0.length];
		for(int i=0;i<305;i++)
		{
			records_ikw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ikw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ikw", records_ikw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record300: [a300, b0, c0, d0, e0]
		//record301: [a301, b1, c1, d1, e1]
		//record302: [a302, b0, c2, d2, e2]
		//record303: [a303, b1, c0, d3, e3]
		//record304: [a304, b0, c1, d0, e4]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ikw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 61, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a7, b1, c1, d3, e2]
		//record2: [a12, b0, c0, d0, e2]
		//record3: [a17, b1, c2, d1, e2]
		//record4: [a22, b0, c1, d2, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 61);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("kn9", cols0);
		String [][] records_kn9 = new String[493][cols0.length];
		for(int i=0;i<493;i++)
		{
			records_kn9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kn9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kn9", records_kn9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record488: [a488, b0, c2]
		//record489: [a489, b1, c0]
		//record490: [a490, b0, c1]
		//record491: [a491, b1, c2]
		//record492: [a492, b0, c0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a31"};
		ArrayList<String[]> tableSelect0 = DBApp.select("kn9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a31, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i2", cols1);
		String [][] records_i2 = new String[329][cols1.length];
		for(int i=0;i<329;i++)
		{
			records_i2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i2", records_i2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8]
		String[] ConditionColumns1 = {"a","b"};
		String[] ConditionColumnsValues1 = {"a276","b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("i2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a276","b0","c0","d0","e1","f0","g3","h4","i6","j6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ri0", cols2);
		String [][] records_ri0 = new String[346][cols2.length];
		for(int i=0;i<346;i++)
		{
			records_ri0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ri0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ri0", records_ri0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ri0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 43, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 43);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("e15n", cols3);
		String [][] records_e15n = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			records_e15n[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e15n[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e15n", records_e15n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record61: [a61, b1, c1, d1, e1]
		//record62: [a62, b0, c2, d2, e2]
		//record63: [a63, b1, c0, d3, e3]
		//record64: [a64, b0, c1, d0, e4]
		//record65: [a65, b1, c2, d1, e0]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("e15n", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a8, b0, c2, d0, e3]
		//record3: [a12, b0, c0, d0, e2]
		//record4: [a16, b0, c1, d0, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 17);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("c58w1", cols0);
		String [][] records_c58w1 = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_c58w1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c58w1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c58w1", records_c58w1[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record412: [a412, b0]
		//record413: [a413, b1]
		//record414: [a414, b0]
		//record415: [a415, b1]
		//record416: [a416, b0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a350"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c58w1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a350, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a350","b0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xlt82", cols1);
		String [][] records_xlt82 = new String[356][cols1.length];
		for(int i=0;i<356;i++)
		{
			records_xlt82[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xlt82[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xlt82", records_xlt82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3]
		String[] ConditionColumns1 = {"b","j","h","d","c"};
		String[] ConditionColumnsValues1 = {"b0","j0","h0","d0","c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xlt82", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10]
		//record2: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a120","b0","c0","d0","e0","f0","g1","h0","i3","j0","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a240","b0","c0","d0","e0","f0","g2","h0","i6","j0","k9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("y69", cols2);
		String [][] records_y69 = new String[406][cols2.length];
		for(int i=0;i<406;i++)
		{
			records_y69[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y69[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y69", records_y69[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y69", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 203, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 203);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("t3", cols3);
		String [][] records_t3 = new String[403][cols3.length];
		for(int i=0;i<403;i++)
		{
			records_t3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t3", records_t3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record398: [a398, b0, c2, d2, e3, f2]
		//record399: [a399, b1, c0, d3, e4, f3]
		//record400: [a400, b0, c1, d0, e0, f4]
		//record401: [a401, b1, c2, d1, e1, f5]
		//record402: [a402, b0, c0, d2, e2, f0]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("t3", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 67, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a14, b0, c2, d2, e4, f2]
		//record3: [a20, b0, c2, d0, e0, f2]
		//record4: [a26, b0, c2, d2, e1, f2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 67);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ag", cols4);
		String [][] records_ag = new String[433][cols4.length];
		for(int i=0;i<433;i++)
		{
			records_ag[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ag[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ag", records_ag[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12]
		String[] ConditionColumns4 = {"g","c"};
		String[] ConditionColumnsValues4 = {"g6","c1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ag", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 20, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record1: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		//record2: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13]
		//record3: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11, n6]
		//record4: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 20);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a76","b0","c1","d0","e1","f4","g6","h4","i4","j6","k10","l4","m11","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a97","b1","c1","d1","e2","f1","g6","h1","i7","j7","k9","l1","m6","n13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z8e", cols0);
		String [][] records_z8e = new String[492][cols0.length];
		for(int i=0;i<492;i++)
		{
			records_z8e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z8e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z8e", records_z8e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record487: [a487, b1, c1, d3, e2, f1, g4]
		//record488: [a488, b0, c2, d0, e3, f2, g5]
		//record489: [a489, b1, c0, d1, e4, f3, g6]
		//record490: [a490, b0, c1, d2, e0, f4, g0]
		//record491: [a491, b1, c2, d3, e1, f5, g1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("z8e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 246, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 246);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e3g", cols0);
		String [][] records_e3g = new String[255][cols0.length];
		for(int i=0;i<255;i++)
		{
			records_e3g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e3g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e3g", records_e3g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6, n1]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("e3g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 21);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("du5", cols1);
		String [][] records_du5 = new String[413][cols1.length];
		for(int i=0;i<413;i++)
		{
			records_du5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_du5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("du5", records_du5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2, l1]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k10"};
		ArrayList<String[]> tableSelect1 = DBApp.select("du5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 37);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("f8gq1", cols0);
		String [][] records_f8gq1 = new String[109][cols0.length];
		for(int i=0;i<109;i++)
		{
			records_f8gq1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f8gq1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f8gq1", records_f8gq1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2]
		//record105: [a105, b1, c0, d1, e0, f3]
		//record106: [a106, b0, c1, d2, e1, f4]
		//record107: [a107, b1, c2, d3, e2, f5]
		//record108: [a108, b0, c0, d0, e3, f0]
		String[] ConditionColumns0 = {"f","d"};
		String[] ConditionColumnsValues0 = {"f5","d3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f8gq1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a11, b1, c2, d3, e1, f5]
		//record1: [a23, b1, c2, d3, e3, f5]
		//record2: [a35, b1, c2, d3, e0, f5]
		//record3: [a47, b1, c2, d3, e2, f5]
		//record4: [a59, b1, c2, d3, e4, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a59","b1","c2","d3","e4","f5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("gh199", cols1);
		String [][] records_gh199 = new String[102][cols1.length];
		for(int i=0;i<102;i++)
		{
			records_gh199[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gh199[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gh199", records_gh199[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2]
		String[] ConditionColumns1 = {"c","j"};
		String[] ConditionColumnsValues1 = {"c1","j3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gh199", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record1: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record2: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a73","b1","c1","d1","e3","f1","g3","h1","i1","j3","k7"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("sz", cols2);
		String [][] records_sz = new String[143][cols2.length];
		for(int i=0;i<143;i++)
		{
			records_sz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sz", records_sz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sz", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 48, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 48);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bn23", cols0);
		String [][] records_bn23 = new String[294][cols0.length];
		for(int i=0;i<294;i++)
		{
			records_bn23[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bn23[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bn23", records_bn23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bn23", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 49, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 49);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("sj2", cols1);
		String [][] records_sj2 = new String[419][cols1.length];
		for(int i=0;i<419;i++)
		{
			records_sj2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sj2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sj2", records_sj2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0]
		String[] ConditionColumns1 = {"c","j","e","i","d"};
		String[] ConditionColumnsValues1 = {"c1","j3","e3","i1","d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("sj2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9]
		//record1: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a163","b1","c1","d3","e3","f1","g2","h3","i1","j3","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a343","b1","c1","d3","e3","f1","g0","h7","i1","j3","k2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("t1o", cols2);
		String [][] records_t1o = new String[131][cols2.length];
		for(int i=0;i<131;i++)
		{
			records_t1o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t1o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t1o", records_t1o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record126: [a126, b0, c0]
		//record127: [a127, b1, c1]
		//record128: [a128, b0, c2]
		//record129: [a129, b1, c0]
		//record130: [a130, b0, c1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a47"};
		ArrayList<String[]> tableSelect2 = DBApp.select("t1o", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a47, b1, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a47","b1","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("f96", cols0);
		String [][] records_f96 = new String[264][cols0.length];
		for(int i=0;i<264;i++)
		{
			records_f96[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f96[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f96", records_f96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record259: [a259, b1, c1, d3, e4, f1]
		//record260: [a260, b0, c2, d0, e0, f2]
		//record261: [a261, b1, c0, d1, e1, f3]
		//record262: [a262, b0, c1, d2, e2, f4]
		//record263: [a263, b1, c2, d3, e3, f5]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f96", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 44, records:
		//record0: [a4, b0, c1, d0, e4, f4]
		//record1: [a10, b0, c1, d2, e0, f4]
		//record2: [a16, b0, c1, d0, e1, f4]
		//record3: [a22, b0, c1, d2, e2, f4]
		//record4: [a28, b0, c1, d0, e3, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 44);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("m49", cols1);
		String [][] records_m49 = new String[211][cols1.length];
		for(int i=0;i<211;i++)
		{
			records_m49[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m49[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m49", records_m49[i]);
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
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a133"};
		ArrayList<String[]> tableSelect1 = DBApp.select("m49", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a133]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a133"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("g32", cols0);
		String [][] records_g32 = new String[162][cols0.length];
		for(int i=0;i<162;i++)
		{
			records_g32[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g32[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g32", records_g32[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4, o8]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g32", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 15);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9","n7","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2","l10","m7","n4","o1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w42", cols1);
		String [][] records_w42 = new String[395][cols1.length];
		for(int i=0;i<395;i++)
		{
			records_w42[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w42[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w42", records_w42[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w42", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 198, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 198);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lk", cols0);
		String [][] records_lk = new String[430][cols0.length];
		for(int i=0;i<430;i++)
		{
			records_lk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lk", records_lk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("lk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 54, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 54);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("svan1", cols1);
		String [][] records_svan1 = new String[455][cols1.length];
		for(int i=0;i<455;i++)
		{
			records_svan1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_svan1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("svan1", records_svan1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record450: [a450, b0, c0, d2, e0, f0]
		//record451: [a451, b1, c1, d3, e1, f1]
		//record452: [a452, b0, c2, d0, e2, f2]
		//record453: [a453, b1, c0, d1, e3, f3]
		//record454: [a454, b0, c1, d2, e4, f4]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("svan1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 114, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a8, b0, c2, d0, e3, f2]
		//record3: [a12, b0, c0, d0, e2, f0]
		//record4: [a16, b0, c1, d0, e1, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 114);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("sq74", cols2);
		String [][] records_sq74 = new String[144][cols2.length];
		for(int i=0;i<144;i++)
		{
			records_sq74[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sq74[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sq74", records_sq74[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record139: [a139, b1, c1, d3]
		//record140: [a140, b0, c2, d0]
		//record141: [a141, b1, c0, d1]
		//record142: [a142, b0, c1, d2]
		//record143: [a143, b1, c2, d3]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sq74", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 48, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 48);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("c59", cols3);
		String [][] records_c59 = new String[208][cols3.length];
		for(int i=0;i<208;i++)
		{
			records_c59[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c59[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c59", records_c59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record203: [a203, b1, c2, d3, e3, f5]
		//record204: [a204, b0, c0, d0, e4, f0]
		//record205: [a205, b1, c1, d1, e0, f1]
		//record206: [a206, b0, c2, d2, e1, f2]
		//record207: [a207, b1, c0, d3, e2, f3]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("c59", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 35, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a8, b0, c2, d0, e3, f2]
		//record2: [a14, b0, c2, d2, e4, f2]
		//record3: [a20, b0, c2, d0, e0, f2]
		//record4: [a26, b0, c2, d2, e1, f2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 35);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("k15", cols0);
		String [][] records_k15 = new String[419][cols0.length];
		for(int i=0;i<419;i++)
		{
			records_k15[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k15[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k15", records_k15[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record414: [a414, b0, c0, d2, e4]
		//record415: [a415, b1, c1, d3, e0]
		//record416: [a416, b0, c2, d0, e1]
		//record417: [a417, b1, c0, d1, e2]
		//record418: [a418, b0, c1, d2, e3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k15", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 140, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 140);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("uny", cols1);
		String [][] records_uny = new String[352][cols1.length];
		for(int i=0;i<352;i++)
		{
			records_uny[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uny[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uny", records_uny[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0, n1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("uny", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 118, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 118);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vt7", cols2);
		String [][] records_vt7 = new String[311][cols2.length];
		for(int i=0;i<311;i++)
		{
			records_vt7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vt7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vt7", records_vt7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6]
		String[] ConditionColumns2 = {"e","f","d","h"};
		String[] ConditionColumnsValues2 = {"e3","f2","d0","h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("vt7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a68, b0, c2, d0, e3, f2, g5, h4]
		//record1: [a188, b0, c2, d0, e3, f2, g6, h4]
		//record2: [a308, b0, c2, d0, e3, f2, g0, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 3);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a68","b0","c2","d0","e3","f2","g5","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a188","b0","c2","d0","e3","f2","g6","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a308","b0","c2","d0","e3","f2","g0","h4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("n6yi4", cols3);
		String [][] records_n6yi4 = new String[136][cols3.length];
		for(int i=0;i<136;i++)
		{
			records_n6yi4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n6yi4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n6yi4", records_n6yi4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record131: [a131, b1, c2, d3, e1, f5]
		//record132: [a132, b0, c0, d0, e2, f0]
		//record133: [a133, b1, c1, d1, e3, f1]
		//record134: [a134, b0, c2, d2, e4, f2]
		//record135: [a135, b1, c0, d3, e0, f3]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("n6yi4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 68, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a5, b1, c2, d1, e0, f5]
		//record3: [a7, b1, c1, d3, e2, f1]
		//record4: [a9, b1, c0, d1, e4, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 68);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("k7r", cols0);
		String [][] records_k7r = new String[267][cols0.length];
		for(int i=0;i<267;i++)
		{
			records_k7r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k7r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k7r", records_k7r[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record262: [a262]
		//record263: [a263]
		//record264: [a264]
		//record265: [a265]
		//record266: [a266]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a156"};
		ArrayList<String[]> tableSelect0 = DBApp.select("k7r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a156]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a156"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("bkk6", cols0);
		String [][] records_bkk6 = new String[164][cols0.length];
		for(int i=0;i<164;i++)
		{
			records_bkk6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bkk6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bkk6", records_bkk6[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record159: [a159]
		//record160: [a160]
		//record161: [a161]
		//record162: [a162]
		//record163: [a163]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a158"};
		ArrayList<String[]> tableSelect0 = DBApp.select("bkk6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a158]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a158"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("x1m20", cols1);
		String [][] records_x1m20 = new String[116][cols1.length];
		for(int i=0;i<116;i++)
		{
			records_x1m20[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x1m20[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x1m20", records_x1m20[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record111: [a111, b1, c0, d3, e1]
		//record112: [a112, b0, c1, d0, e2]
		//record113: [a113, b1, c2, d1, e3]
		//record114: [a114, b0, c0, d2, e4]
		//record115: [a115, b1, c1, d3, e0]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("x1m20", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 29, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a8, b0, c2, d0, e3]
		//record3: [a12, b0, c0, d0, e2]
		//record4: [a16, b0, c1, d0, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 29);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("c6", cols0);
		String [][] records_c6 = new String[398][cols0.length];
		for(int i=0;i<398;i++)
		{
			records_c6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c6", records_c6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1, o3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2, o4]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10, l11, m5, n3, o5]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0, l0, m6, n4, o6]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1, l1, m7, n5, o7]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("c6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 57, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 57);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5","n5","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7","n5","o3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("be", cols1);
		String [][] records_be = new String[492][cols1.length];
		for(int i=0;i<492;i++)
		{
			records_be[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_be[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("be", records_be[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record487: [a487, b1, c1, d3]
		//record488: [a488, b0, c2, d0]
		//record489: [a489, b1, c0, d1]
		//record490: [a490, b0, c1, d2]
		//record491: [a491, b1, c2, d3]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("be", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 164, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 164);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f66", cols2);
		String [][] records_f66 = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			records_f66[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f66[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f66", records_f66[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("f66", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 9);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a1u", cols3);
		String [][] records_a1u = new String[426][cols3.length];
		for(int i=0;i<426;i++)
		{
			records_a1u[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_a1u[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("a1u", records_a1u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1]
		String[] ConditionColumns3 = {"h","g","d"};
		String[] ConditionColumnsValues3 = {"h7","g0","d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("a1u", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 8, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record1: [a63, b1, c0, d3, e3, f3, g0, h7]
		//record2: [a119, b1, c2, d3, e4, f5, g0, h7]
		//record3: [a175, b1, c1, d3, e0, f1, g0, h7]
		//record4: [a231, b1, c0, d3, e1, f3, g0, h7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 8);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a119","b1","c2","d3","e4","f5","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a175","b1","c1","d3","e0","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a231","b1","c0","d3","e1","f3","g0","h7"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("uzdo", cols4);
		String [][] records_uzdo = new String[313][cols4.length];
		for(int i=0;i<313;i++)
		{
			records_uzdo[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_uzdo[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("uzdo", records_uzdo[i]);
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
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("uzdo", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 156, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 156);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mtj", cols0);
		String [][] records_mtj = new String[296][cols0.length];
		for(int i=0;i<296;i++)
		{
			records_mtj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mtj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mtj", records_mtj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7]
		String[] ConditionColumns0 = {"b","g"};
		String[] ConditionColumnsValues0 = {"b1","g3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mtj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 21);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a59","b1","c2","d3","e4","f5","g3","h3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hdy1", cols1);
		String [][] records_hdy1 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_hdy1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hdy1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hdy1", records_hdy1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("hdy1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record3: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record4: [a18, b0, c0, d2, e3, f0, g4, h2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 11);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jn4i", cols2);
		String [][] records_jn4i = new String[170][cols2.length];
		for(int i=0;i<170;i++)
		{
			records_jn4i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jn4i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jn4i", records_jn4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record165: [a165, b1]
		//record166: [a166, b0]
		//record167: [a167, b1]
		//record168: [a168, b0]
		//record169: [a169, b1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a154"};
		ArrayList<String[]> tableSelect2 = DBApp.select("jn4i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a154, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a154","b0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("zxo4b", cols3);
		String [][] records_zxo4b = new String[318][cols3.length];
		for(int i=0;i<318;i++)
		{
			records_zxo4b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zxo4b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zxo4b", records_zxo4b[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record313: [a313, b1]
		//record314: [a314, b0]
		//record315: [a315, b1]
		//record316: [a316, b0]
		//record317: [a317, b1]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("zxo4b", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 159, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 159);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j5j", cols4);
		String [][] records_j5j = new String[108][cols4.length];
		for(int i=0;i<108;i++)
		{
			records_j5j[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j5j[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j5j", records_j5j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3]
		String[] ConditionColumns4 = {"g"};
		String[] ConditionColumnsValues4 = {"g1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("j5j", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 16, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 16);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("tyv", cols0);
		String [][] records_tyv = new String[401][cols0.length];
		for(int i=0;i<401;i++)
		{
			records_tyv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tyv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tyv", records_tyv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record396: [a396, b0, c0]
		//record397: [a397, b1, c1]
		//record398: [a398, b0, c2]
		//record399: [a399, b1, c0]
		//record400: [a400, b0, c1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("tyv", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 201, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 201);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ey", cols1);
		String [][] records_ey = new String[279][cols1.length];
		for(int i=0;i<279;i++)
		{
			records_ey[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ey[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ey", records_ey[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1, n8, o4]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2, n9, o5]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3, n10, o6]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7, k2, l1, m4, n11, o7]
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2, m5, n12, o8]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ey", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 22, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12]
		//record4: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 22);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3","n1","o14"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3","n0","o12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3","n13","o10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("hbl2k", cols2);
		String [][] records_hbl2k = new String[212][cols2.length];
		for(int i=0;i<212;i++)
		{
			records_hbl2k[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hbl2k[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hbl2k", records_hbl2k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3, n1]
		String[] ConditionColumns2 = {"e","j","f","a","b","h","g"};
		String[] ConditionColumnsValues2 = {"e2","j2","f4","a82","b0","h2","g5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("hbl2k", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a82","b0","c1","d2","e2","f4","g5","h2","i1","j2","k5","l10","m4","n12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(528);
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