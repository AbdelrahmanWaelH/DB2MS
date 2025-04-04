package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests09
{

	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h6be", cols0);
		String [][] records_h6be = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_h6be[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h6be[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h6be", records_h6be[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		//record497: [a497, b1, c2, d1, e2, f5, g0]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h6be", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 71, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a20, b0, c2, d0, e0, f2, g6]
		//record3: [a27, b1, c0, d3, e2, f3, g6]
		//record4: [a34, b0, c1, d2, e4, f4, g6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 71);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("yf0", cols1);
		String [][] records_yf0 = new String[67][cols1.length];
		for(int i=0;i<67;i++)
		{
			records_yf0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yf0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yf0", records_yf0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("yf0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 34, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 34);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v407o", cols2);
		String [][] records_v407o = new String[402][cols2.length];
		for(int i=0;i<402;i++)
		{
			records_v407o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v407o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v407o", records_v407o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1, l1, m7, n5, o7]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2, l2, m8, n6, o8]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3, l3, m9, n7, o9]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8, o10]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9, o11]
		String[] ConditionColumns2 = {"n"};
		String[] ConditionColumnsValues2 = {"n2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("v407o", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 29, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14]
		//record4: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3, l10, m6, n2, o13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 29);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4","n2","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5","n2","o14"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a58","b0","c1","d2","e3","f4","g2","h2","i4","j8","k3","l10","m6","n2","o13"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("l36m", cols3);
		String [][] records_l36m = new String[414][cols3.length];
		for(int i=0;i<414;i++)
		{
			records_l36m[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l36m[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l36m", records_l36m[i]);
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
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f5"};
		ArrayList<String[]> tableSelect3 = DBApp.select("l36m", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 69, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 69);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d6", cols4);
		String [][] records_d6 = new String[485][cols4.length];
		for(int i=0;i<485;i++)
		{
			records_d6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_d6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("d6", records_d6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0]
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9, l2, m1]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3, m2]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3]
		String[] ConditionColumns4 = {"j","m","c","h"};
		String[] ConditionColumnsValues4 = {"j7","m1","c0","h3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("d6", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("hu", cols0);
		String [][] records_hu = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			records_hu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hu", records_hu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0, o11]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h3o", cols1);
		String [][] records_h3o = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			records_h3o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h3o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h3o", records_h3o[i]);
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
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h3o", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 20);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("q3x", cols2);
		String [][] records_q3x = new String[491][cols2.length];
		for(int i=0;i<491;i++)
		{
			records_q3x[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q3x[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q3x", records_q3x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5, n10, o6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("q3x", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 55, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record4: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 55);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5","n3","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("j1", cols3);
		String [][] records_j1 = new String[319][cols3.length];
		for(int i=0;i<319;i++)
		{
			records_j1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j1", records_j1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2]
		//record315: [a315, b1, c0, d3, e0, f3]
		//record316: [a316, b0, c1, d0, e1, f4]
		//record317: [a317, b1, c2, d1, e2, f5]
		//record318: [a318, b0, c0, d2, e3, f0]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("j1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 63, records:
		//record0: [a4, b0, c1, d0, e4, f4]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a14, b0, c2, d2, e4, f2]
		//record3: [a19, b1, c1, d3, e4, f1]
		//record4: [a24, b0, c0, d0, e4, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 63);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("fx9", cols4);
		String [][] records_fx9 = new String[87][cols4.length];
		for(int i=0;i<87;i++)
		{
			records_fx9[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_fx9[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("fx9", records_fx9[i]);
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
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("fx9", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 17, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 17);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ne9r", cols0);
		String [][] records_ne9r = new String[411][cols0.length];
		for(int i=0;i<411;i++)
		{
			records_ne9r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ne9r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ne9r", records_ne9r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record406: [a406, b0, c1]
		//record407: [a407, b1, c2]
		//record408: [a408, b0, c0]
		//record409: [a409, b1, c1]
		//record410: [a410, b0, c2]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a252"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ne9r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a252, b0, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a252","b0","c0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z0", cols1);
		String [][] records_z0 = new String[220][cols1.length];
		for(int i=0;i<220;i++)
		{
			records_z0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z0", records_z0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 22, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 22);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p17", cols2);
		String [][] records_p17 = new String[81][cols2.length];
		for(int i=0;i<81;i++)
		{
			records_p17[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p17[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p17", records_p17[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("p17", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 27, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 27);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("zkjb5", cols0);
		String [][] records_zkjb5 = new String[309][cols0.length];
		for(int i=0;i<309;i++)
		{
			records_zkjb5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zkjb5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zkjb5", records_zkjb5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record304: [a304, b0, c1, d0]
		//record305: [a305, b1, c2, d1]
		//record306: [a306, b0, c0, d2]
		//record307: [a307, b1, c1, d3]
		//record308: [a308, b0, c2, d0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zkjb5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 103, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 103);
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
	public void test76TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("be47", cols0);
		String [][] records_be47 = new String[155][cols0.length];
		for(int i=0;i<155;i++)
		{
			records_be47[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_be47[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("be47", records_be47[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2]
		String[] ConditionColumns0 = {"b","h"};
		String[] ConditionColumnsValues0 = {"b1","h7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("be47", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("aw7", cols1);
		String [][] records_aw7 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			records_aw7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_aw7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("aw7", records_aw7[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record88: [a88]
		//record89: [a89]
		//record90: [a90]
		//record91: [a91]
		//record92: [a92]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a92"};
		ArrayList<String[]> tableSelect1 = DBApp.select("aw7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a92]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a92"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p4mo1", cols2);
		String [][] records_p4mo1 = new String[393][cols2.length];
		for(int i=0;i<393;i++)
		{
			records_p4mo1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p4mo1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p4mo1", records_p4mo1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("p4mo1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 131, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 131);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("v6w", cols3);
		String [][] records_v6w = new String[276][cols3.length];
		for(int i=0;i<276;i++)
		{
			records_v6w[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v6w[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v6w", records_v6w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record271: [a271, b1, c1, d3, e1]
		//record272: [a272, b0, c2, d0, e2]
		//record273: [a273, b1, c0, d1, e3]
		//record274: [a274, b0, c1, d2, e4]
		//record275: [a275, b1, c2, d3, e0]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("v6w", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 69, records:
		//record0: [a2, b0, c2, d2, e2]
		//record1: [a6, b0, c0, d2, e1]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a14, b0, c2, d2, e4]
		//record4: [a18, b0, c0, d2, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 69);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("x08", cols4);
		String [][] records_x08 = new String[496][cols4.length];
		for(int i=0;i<496;i++)
		{
			records_x08[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x08[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x08", records_x08[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record491: [a491, b1, c2, d3, e1, f5]
		//record492: [a492, b0, c0, d0, e2, f0]
		//record493: [a493, b1, c1, d1, e3, f1]
		//record494: [a494, b0, c2, d2, e4, f2]
		//record495: [a495, b1, c0, d3, e0, f3]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("x08", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 248, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a5, b1, c2, d1, e0, f5]
		//record3: [a7, b1, c1, d3, e2, f1]
		//record4: [a9, b1, c0, d1, e4, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 248);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("hfi", cols0);
		String [][] records_hfi = new String[225][cols0.length];
		for(int i=0;i<225;i++)
		{
			records_hfi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hfi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hfi", records_hfi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10, o10]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0, n11, o11]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2, n13, o13]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3, n0, o14]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m11"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hfi", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record3: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5]
		//record4: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 17);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2","m11","n8","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3","k8","l3","m11","n7","o3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ch", cols1);
		String [][] records_ch = new String[343][cols1.length];
		for(int i=0;i<343;i++)
		{
			records_ch[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ch[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ch", records_ch[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ch", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 49, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 49);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p0g", cols2);
		String [][] records_p0g = new String[195][cols2.length];
		for(int i=0;i<195;i++)
		{
			records_p0g[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p0g[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p0g", records_p0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("p0g", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 65, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 65);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("x64", cols3);
		String [][] records_x64 = new String[410][cols3.length];
		for(int i=0;i<410;i++)
		{
			records_x64[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x64[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x64", records_x64[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0]
		//record406: [a406, b0, c1, d2, e1]
		//record407: [a407, b1, c2, d3, e2]
		//record408: [a408, b0, c0, d0, e3]
		//record409: [a409, b1, c1, d1, e4]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x64", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 82, records:
		//record0: [a3, b1, c0, d3, e3]
		//record1: [a8, b0, c2, d0, e3]
		//record2: [a13, b1, c1, d1, e3]
		//record3: [a18, b0, c0, d2, e3]
		//record4: [a23, b1, c2, d3, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 82);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("nss", cols0);
		String [][] records_nss = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			records_nss[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nss[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nss", records_nss[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record69: [a69, b1, c0, d1, e4, f3]
		//record70: [a70, b0, c1, d2, e0, f4]
		//record71: [a71, b1, c2, d3, e1, f5]
		//record72: [a72, b0, c0, d0, e2, f0]
		//record73: [a73, b1, c1, d1, e3, f1]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nss", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a5, b1, c2, d1, e0, f5]
		//record1: [a11, b1, c2, d3, e1, f5]
		//record2: [a17, b1, c2, d1, e2, f5]
		//record3: [a23, b1, c2, d3, e3, f5]
		//record4: [a29, b1, c2, d1, e4, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 12);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b6qf", cols1);
		String [][] records_b6qf = new String[446][cols1.length];
		for(int i=0;i<446;i++)
		{
			records_b6qf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b6qf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b6qf", records_b6qf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5]
		String[] ConditionColumns1 = {"d","j","b","h"};
		String[] ConditionColumnsValues1 = {"d3","j3","b1","h7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b6qf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record1: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3]
		//record2: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3]
		//record3: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3]
		//record4: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 11);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a103","b1","c1","d3","e3","f1","g5","h7","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a143","b1","c2","d3","e3","f5","g3","h7","i8","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a183","b1","c0","d3","e3","f3","g1","h7","i3","j3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gr", cols2);
		String [][] records_gr = new String[385][cols2.length];
		for(int i=0;i<385;i++)
		{
			records_gr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gr", records_gr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		//record384: [a384, b0, c0, d0, e4, f0, g6]
		String[] ConditionColumns2 = {"d","f"};
		String[] ConditionColumnsValues2 = {"d3","f1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("gr", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 32, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0]
		//record1: [a19, b1, c1, d3, e4, f1, g5]
		//record2: [a31, b1, c1, d3, e1, f1, g3]
		//record3: [a43, b1, c1, d3, e3, f1, g1]
		//record4: [a55, b1, c1, d3, e0, f1, g6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 32);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j6f9", cols3);
		String [][] records_j6f9 = new String[320][cols3.length];
		for(int i=0;i<320;i++)
		{
			records_j6f9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j6f9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j6f9", records_j6f9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7]
		String[] ConditionColumns3 = {"j"};
		String[] ConditionColumnsValues3 = {"j3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("j6f9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 32, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 32);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("pn2", cols0);
		String [][] records_pn2 = new String[156][cols0.length];
		for(int i=0;i<156;i++)
		{
			records_pn2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pn2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pn2", records_pn2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record151: [a151, b1, c1, d3]
		//record152: [a152, b0, c2, d0]
		//record153: [a153, b1, c0, d1]
		//record154: [a154, b0, c1, d2]
		//record155: [a155, b1, c2, d3]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pn2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 78, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 78);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("rh", cols0);
		String [][] records_rh = new String[334][cols0.length];
		for(int i=0;i<334;i++)
		{
			records_rh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rh", records_rh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record329: [a329, b1, c2]
		//record330: [a330, b0, c0]
		//record331: [a331, b1, c1]
		//record332: [a332, b0, c2]
		//record333: [a333, b1, c0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rh", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 167, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 167);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("cb9", cols0);
		String [][] records_cb9 = new String[416][cols0.length];
		for(int i=0;i<416;i++)
		{
			records_cb9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cb9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cb9", records_cb9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record411: [a411, b1, c0]
		//record412: [a412, b0, c1]
		//record413: [a413, b1, c2]
		//record414: [a414, b0, c0]
		//record415: [a415, b1, c1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cb9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 208, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 208);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vh5", cols1);
		String [][] records_vh5 = new String[350][cols1.length];
		for(int i=0;i<350;i++)
		{
			records_vh5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vh5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vh5", records_vh5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("vh5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 39);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("dot", cols0);
		String [][] records_dot = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_dot[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dot[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dot", records_dot[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("dot", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 15);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("mrl", cols1);
		String [][] records_mrl = new String[179][cols1.length];
		for(int i=0;i<179;i++)
		{
			records_mrl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mrl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mrl", records_mrl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2]
		String[] ConditionColumns1 = {"k","c","d","i","e"};
		String[] ConditionColumnsValues1 = {"k5","c2","d2","i8","e0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("mrl", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a170","b0","c2","d2","e0","f2","g2","h2","i8","j0","k5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("y63w", cols0);
		String [][] records_y63w = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			records_y63w[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y63w[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y63w", records_y63w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record72: [a72, b0, c0]
		//record73: [a73, b1, c1]
		//record74: [a74, b0, c2]
		//record75: [a75, b1, c0]
		//record76: [a76, b0, c1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y63w", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 25);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nb", cols1);
		String [][] records_nb = new String[449][cols1.length];
		for(int i=0;i<449;i++)
		{
			records_nb[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nb[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nb", records_nb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nb", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 75, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 75);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("o8j", cols2);
		String [][] records_o8j = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			records_o8j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o8j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o8j", records_o8j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record71: [a71, b1, c2, d3, e1, f5]
		//record72: [a72, b0, c0, d0, e2, f0]
		//record73: [a73, b1, c1, d1, e3, f1]
		//record74: [a74, b0, c2, d2, e4, f2]
		//record75: [a75, b1, c0, d3, e0, f3]
		String[] ConditionColumns2 = {"f","e"};
		String[] ConditionColumnsValues2 = {"f1","e1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o8j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a31, b1, c1, d3, e1, f1]
		//record2: [a61, b1, c1, d1, e1, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 3);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x6k", cols3);
		String [][] records_x6k = new String[147][cols3.length];
		for(int i=0;i<147;i++)
		{
			records_x6k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x6k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x6k", records_x6k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record142: [a142, b0, c1, d2, e2, f4, g2]
		//record143: [a143, b1, c2, d3, e3, f5, g3]
		//record144: [a144, b0, c0, d0, e4, f0, g4]
		//record145: [a145, b1, c1, d1, e0, f1, g5]
		//record146: [a146, b0, c2, d2, e1, f2, g6]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x6k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 37, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 37);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("rg", cols4);
		String [][] records_rg = new String[292][cols4.length];
		for(int i=0;i<292;i++)
		{
			records_rg[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_rg[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("rg", records_rg[i]);
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
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a283"};
		ArrayList<String[]> tableSelect4 = DBApp.select("rg", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a283]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a283"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l8", cols0);
		String [][] records_l8 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			records_l8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l8", records_l8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record69: [a69, b1, c0, d1, e4, f3, g6]
		//record70: [a70, b0, c1, d2, e0, f4, g0]
		//record71: [a71, b1, c2, d3, e1, f5, g1]
		//record72: [a72, b0, c0, d0, e2, f0, g2]
		//record73: [a73, b1, c1, d1, e3, f1, g3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 25);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("jilr", cols1);
		String [][] records_jilr = new String[466][cols1.length];
		for(int i=0;i<466;i++)
		{
			records_jilr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jilr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jilr", records_jilr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2, j1, k10]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3]
		String[] ConditionColumns1 = {"e","a"};
		String[] ConditionColumnsValues1 = {"e1","a131"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jilr", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a131","b1","c2","d3","e1","f5","g5","h3","i5","j1","k10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("rzn", cols0);
		String [][] records_rzn = new String[292][cols0.length];
		for(int i=0;i<292;i++)
		{
			records_rzn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rzn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rzn", records_rzn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record287: [a287, b1, c2, d3]
		//record288: [a288, b0, c0, d0]
		//record289: [a289, b1, c1, d1]
		//record290: [a290, b0, c2, d2]
		//record291: [a291, b1, c0, d3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rzn", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 98, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 98);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xs1r5", cols1);
		String [][] records_xs1r5 = new String[393][cols1.length];
		for(int i=0;i<393;i++)
		{
			records_xs1r5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xs1r5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xs1r5", records_xs1r5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11, n10, o13]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12, n11, o14]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12, o0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13, o1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2]
		String[] ConditionColumns1 = {"l","f","m","g","k","n"};
		String[] ConditionColumnsValues1 = {"l4","f4","m7","g5","k3","n12"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xs1r5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a124","b0","c1","d0","e4","f4","g5","h4","i7","j4","k3","l4","m7","n12","o4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("xm42", cols2);
		String [][] records_xm42 = new String[152][cols2.length];
		for(int i=0;i<152;i++)
		{
			records_xm42[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xm42[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xm42", records_xm42[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record147: [a147, b1, c0]
		//record148: [a148, b0, c1]
		//record149: [a149, b1, c2]
		//record150: [a150, b0, c0]
		//record151: [a151, b1, c1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("xm42", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 76, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 76);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y059", cols3);
		String [][] records_y059 = new String[267][cols3.length];
		for(int i=0;i<267;i++)
		{
			records_y059[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y059[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y059", records_y059[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10, m2]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11, m3]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("y059", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 66, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 66);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ov", cols0);
		String [][] records_ov = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			records_ov[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ov[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ov", records_ov[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record77: [a77, b1, c2, d1, e2]
		//record78: [a78, b0, c0, d2, e3]
		//record79: [a79, b1, c1, d3, e4]
		//record80: [a80, b0, c2, d0, e0]
		//record81: [a81, b1, c0, d1, e1]
		String[] ConditionColumns0 = {"e","b"};
		String[] ConditionColumnsValues0 = {"e1","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ov", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a6, b0, c0, d2, e1]
		//record1: [a16, b0, c1, d0, e1]
		//record2: [a26, b0, c2, d2, e1]
		//record3: [a36, b0, c0, d0, e1]
		//record4: [a46, b0, c1, d2, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x0150", cols0);
		String [][] records_x0150 = new String[189][cols0.length];
		for(int i=0;i<189;i++)
		{
			records_x0150[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x0150[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x0150", records_x0150[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x0150", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 24, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 24);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("ud7b", cols1);
		String [][] records_ud7b = new String[303][cols1.length];
		for(int i=0;i<303;i++)
		{
			records_ud7b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ud7b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ud7b", records_ud7b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record298: [a298, b0, c1]
		//record299: [a299, b1, c2]
		//record300: [a300, b0, c0]
		//record301: [a301, b1, c1]
		//record302: [a302, b0, c2]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a291"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ud7b", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a291, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a291","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("np0", cols2);
		String [][] records_np0 = new String[270][cols2.length];
		for(int i=0;i<270;i++)
		{
			records_np0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_np0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("np0", records_np0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9]
		String[] ConditionColumns2 = {"m","d","i","c"};
		String[] ConditionColumnsValues2 = {"m5","d3","i0","c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("np0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a135","b1","c0","d3","e0","f3","g2","h7","i0","j5","k3","l3","m5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("p5nt", cols0);
		String [][] records_p5nt = new String[101][cols0.length];
		for(int i=0;i<101;i++)
		{
			records_p5nt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p5nt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p5nt", records_p5nt[i]);
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
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a17"};
		ArrayList<String[]> tableSelect0 = DBApp.select("p5nt", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a17, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j0", cols1);
		String [][] records_j0 = new String[413][cols1.length];
		for(int i=0;i<413;i++)
		{
			records_j0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j0", records_j0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record408: [a408, b0, c0, d0, e3, f0, g2]
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("j0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 69, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 69);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w2r", cols2);
		String [][] records_w2r = new String[107][cols2.length];
		for(int i=0;i<107;i++)
		{
			records_w2r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w2r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w2r", records_w2r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("w2r", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 18, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 18);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("q59", cols3);
		String [][] records_q59 = new String[419][cols3.length];
		for(int i=0;i<419;i++)
		{
			records_q59[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q59[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q59", records_q59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record414: [a414, b0, c0, d2]
		//record415: [a415, b1, c1, d3]
		//record416: [a416, b0, c2, d0]
		//record417: [a417, b1, c0, d1]
		//record418: [a418, b0, c1, d2]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q59", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 105, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a5, b1, c2, d1]
		//record2: [a9, b1, c0, d1]
		//record3: [a13, b1, c1, d1]
		//record4: [a17, b1, c2, d1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 105);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("k9m0", cols4);
		String [][] records_k9m0 = new String[117][cols4.length];
		for(int i=0;i<117;i++)
		{
			records_k9m0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_k9m0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("k9m0", records_k9m0[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record112: [a112]
		//record113: [a113]
		//record114: [a114]
		//record115: [a115]
		//record116: [a116]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("k9m0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i31s4", cols0);
		String [][] records_i31s4 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_i31s4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i31s4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i31s4", records_i31s4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i31s4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("ns9", cols1);
		String [][] records_ns9 = new String[415][cols1.length];
		for(int i=0;i<415;i++)
		{
			records_ns9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ns9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ns9", records_ns9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0]
		//record411: [a411, b1, c0, d3, e1]
		//record412: [a412, b0, c1, d0, e2]
		//record413: [a413, b1, c2, d1, e3]
		//record414: [a414, b0, c0, d2, e4]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a357"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ns9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a357, b1, c0, d1, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a357","b1","c0","d1","e2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("ndm", cols2);
		String [][] records_ndm = new String[213][cols2.length];
		for(int i=0;i<213;i++)
		{
			records_ndm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ndm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ndm", records_ndm[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record208: [a208]
		//record209: [a209]
		//record210: [a210]
		//record211: [a211]
		//record212: [a212]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a110"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ndm", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a110]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a110"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o9", cols3);
		String [][] records_o9 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			records_o9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o9", records_o9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4]
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8]
		String[] ConditionColumns3 = {"k","a"};
		String[] ConditionColumnsValues3 = {"k3","a3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("o9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("nu", cols0);
		String [][] records_nu = new String[210][cols0.length];
		for(int i=0;i<210;i++)
		{
			records_nu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nu", records_nu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0]
		//record206: [a206, b0, c2, d2, e1]
		//record207: [a207, b1, c0, d3, e2]
		//record208: [a208, b0, c1, d0, e3]
		//record209: [a209, b1, c2, d1, e4]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 70, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a6, b0, c0, d2, e1]
		//record3: [a9, b1, c0, d1, e4]
		//record4: [a12, b0, c0, d0, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 70);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gt5", cols0);
		String [][] records_gt5 = new String[385][cols0.length];
		for(int i=0;i<385;i++)
		{
			records_gt5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gt5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gt5", records_gt5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gt5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 48, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 48);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("dx22", cols1);
		String [][] records_dx22 = new String[200][cols1.length];
		for(int i=0;i<200;i++)
		{
			records_dx22[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dx22[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dx22", records_dx22[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1]
		String[] ConditionColumns1 = {"f","i","k","c"};
		String[] ConditionColumnsValues1 = {"f3","i3","k9","c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("dx22", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a75","b1","c0","d3","e0","f3","g5","h3","i3","j5","k9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("yw858", cols2);
		String [][] records_yw858 = new String[129][cols2.length];
		for(int i=0;i<129;i++)
		{
			records_yw858[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yw858[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yw858", records_yw858[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("yw858", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 43, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 43);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("s009", cols3);
		String [][] records_s009 = new String[405][cols3.length];
		for(int i=0;i<405;i++)
		{
			records_s009[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s009[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s009", records_s009[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12, n10]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12]
		String[] ConditionColumns3 = {"k"};
		String[] ConditionColumnsValues3 = {"k4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("s009", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 37, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 37);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0","i3","j8","k4","l0","m9","n6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("w59k", cols0);
		String [][] records_w59k = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			records_w59k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w59k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w59k", records_w59k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w59k", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 17);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("a3", cols0);
		String [][] records_a3 = new String[346][cols0.length];
		for(int i=0;i<346;i++)
		{
			records_a3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a3", records_a3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record341: [a341, b1, c2]
		//record342: [a342, b0, c0]
		//record343: [a343, b1, c1]
		//record344: [a344, b0, c2]
		//record345: [a345, b1, c0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 116, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 116);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("rn71", cols1);
		String [][] records_rn71 = new String[499][cols1.length];
		for(int i=0;i<499;i++)
		{
			records_rn71[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rn71[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rn71", records_rn71[i]);
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
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rn71", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 39);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j40", cols2);
		String [][] records_j40 = new String[374][cols2.length];
		for(int i=0;i<374;i++)
		{
			records_j40[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j40[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j40", records_j40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("j40", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 53, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 53);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("ff743", cols0);
		String [][] records_ff743 = new String[439][cols0.length];
		for(int i=0;i<439;i++)
		{
			records_ff743[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ff743[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ff743", records_ff743[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record434: [a434, b0]
		//record435: [a435, b1]
		//record436: [a436, b0]
		//record437: [a437, b1]
		//record438: [a438, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ff743", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 220, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 220);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("k34", cols1);
		String [][] records_k34 = new String[125][cols1.length];
		for(int i=0;i<125;i++)
		{
			records_k34[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k34[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k34", records_k34[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record120: [a120]
		//record121: [a121]
		//record122: [a122]
		//record123: [a123]
		//record124: [a124]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a72"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k34", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a72]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a72"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("qyg", cols2);
		String [][] records_qyg = new String[355][cols2.length];
		for(int i=0;i<355;i++)
		{
			records_qyg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qyg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qyg", records_qyg[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record350: [a350]
		//record351: [a351]
		//record352: [a352]
		//record353: [a353]
		//record354: [a354]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a42"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qyg", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a42]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s13", cols0);
		String [][] records_s13 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			records_s13[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s13[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s13", records_s13[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s13", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("uf5", cols1);
		String [][] records_uf5 = new String[187][cols1.length];
		for(int i=0;i<187;i++)
		{
			records_uf5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uf5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uf5", records_uf5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record182: [a182, b0, c2, d2]
		//record183: [a183, b1, c0, d3]
		//record184: [a184, b0, c1, d0]
		//record185: [a185, b1, c2, d1]
		//record186: [a186, b0, c0, d2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("uf5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 94, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 94);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ke1", cols2);
		String [][] records_ke1 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_ke1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ke1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ke1", records_ke1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		String[] ConditionColumns2 = {"b","f","i","g","m","l"};
		String[] ConditionColumnsValues2 = {"b1","f5","i2","g1","m3","l5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ke1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("cr", cols3);
		String [][] records_cr = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			records_cr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_cr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("cr", records_cr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("cr", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 10, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 10);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dc", cols4);
		String [][] records_dc = new String[462][cols4.length];
		for(int i=0;i<462;i++)
		{
			records_dc[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dc[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dc", records_dc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		//record458: [a458, b0, c2, d2, e3, f2, g3]
		//record459: [a459, b1, c0, d3, e4, f3, g4]
		//record460: [a460, b0, c1, d0, e0, f4, g5]
		//record461: [a461, b1, c2, d1, e1, f5, g6]
		String[] ConditionColumns4 = {"g","a","f"};
		String[] ConditionColumnsValues4 = {"g5","a278","f2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("dc", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a278, b0, c2, d2, e3, f2, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a278","b0","c2","d2","e3","f2","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("x8e", cols0);
		String [][] records_x8e = new String[429][cols0.length];
		for(int i=0;i<429;i++)
		{
			records_x8e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x8e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x8e", records_x8e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record424: [a424, b0, c1, d0, e4]
		//record425: [a425, b1, c2, d1, e0]
		//record426: [a426, b0, c0, d2, e1]
		//record427: [a427, b1, c1, d3, e2]
		//record428: [a428, b0, c2, d0, e3]
		String[] ConditionColumns0 = {"e","d"};
		String[] ConditionColumnsValues0 = {"e1","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x8e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 22, records:
		//record0: [a6, b0, c0, d2, e1]
		//record1: [a26, b0, c2, d2, e1]
		//record2: [a46, b0, c1, d2, e1]
		//record3: [a66, b0, c0, d2, e1]
		//record4: [a86, b0, c2, d2, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 22);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y9i8", cols1);
		String [][] records_y9i8 = new String[195][cols1.length];
		for(int i=0;i<195;i++)
		{
			records_y9i8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y9i8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y9i8", records_y9i8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4, l11, m9, n9]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10, n10]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12]
		String[] ConditionColumns1 = {"l","c","e","m","h"};
		String[] ConditionColumnsValues1 = {"l11","c2","e0","m9","h3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y9i8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9","n7"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dak9", cols2);
		String [][] records_dak9 = new String[387][cols2.length];
		for(int i=0;i<387;i++)
		{
			records_dak9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dak9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dak9", records_dak9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("dak9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 48, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 48);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("lgw69", cols3);
		String [][] records_lgw69 = new String[9][cols3.length];
		for(int i=0;i<9;i++)
		{
			records_lgw69[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lgw69[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lgw69", records_lgw69[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		String[] ConditionColumns3 = {"i"};
		String[] ConditionColumnsValues3 = {"i6"};
		ArrayList<String[]> tableSelect3 = DBApp.select("lgw69", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("q8537", cols4);
		String [][] records_q8537 = new String[438][cols4.length];
		for(int i=0;i<438;i++)
		{
			records_q8537[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q8537[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q8537", records_q8537[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2, o1]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2]
		String[] ConditionColumns4 = {"h","m","o","k","a","n"};
		String[] ConditionColumnsValues4 = {"h5","m8","o2","k8","a437","n3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("q8537", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a437","b1","c2","d1","e2","f5","g3","h5","i5","j7","k8","l5","m8","n3","o2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("j0rp4", cols0);
		String [][] records_j0rp4 = new String[123][cols0.length];
		for(int i=0;i<123;i++)
		{
			records_j0rp4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j0rp4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j0rp4", records_j0rp4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1, n6, o13]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11, m2, n7, o14]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8, o0]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1, m4, n9, o1]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10, o2]
		String[] ConditionColumns0 = {"a","f","i","b","m","h"};
		String[] ConditionColumnsValues0 = {"a38","f2","i2","b0","m12","h6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j0rp4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("nzw3", cols1);
		String [][] records_nzw3 = new String[168][cols1.length];
		for(int i=0;i<168;i++)
		{
			records_nzw3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nzw3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nzw3", records_nzw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7]
		String[] ConditionColumns1 = {"g","h"};
		String[] ConditionColumnsValues1 = {"g3","h4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nzw3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4]
		//record1: [a108, b0, c0, d0, e3, f0, g3, h4]
		//record2: [a164, b0, c2, d0, e4, f2, g3, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a108","b0","c0","d0","e3","f0","g3","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a164","b0","c2","d0","e4","f2","g3","h4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dbx", cols2);
		String [][] records_dbx = new String[455][cols2.length];
		for(int i=0;i<455;i++)
		{
			records_dbx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dbx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dbx", records_dbx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10]
		String[] ConditionColumns2 = {"j"};
		String[] ConditionColumnsValues2 = {"j5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("dbx", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 45, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 45);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1","l9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m712", cols0);
		String [][] records_m712 = new String[194][cols0.length];
		for(int i=0;i<194;i++)
		{
			records_m712[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m712[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m712", records_m712[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("m712", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 49, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 49);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y91", cols1);
		String [][] records_y91 = new String[160][cols1.length];
		for(int i=0;i<160;i++)
		{
			records_y91[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y91[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y91", records_y91[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a91"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y91", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3, l7, m0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a91","b1","c1","d3","e1","f1","g0","h3","i1","j1","k3","l7","m0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("h8", cols2);
		String [][] records_h8 = new String[274][cols2.length];
		for(int i=0;i<274;i++)
		{
			records_h8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h8", records_h8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3, o14]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4, o0]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5, o1]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3]
		String[] ConditionColumns2 = {"f","c"};
		String[] ConditionColumnsValues2 = {"f4","c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("h8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 45, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 45);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10","o10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i32", cols0);
		String [][] records_i32 = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_i32[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i32[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i32", records_i32[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4]
		String[] ConditionColumns0 = {"g","d","a","h"};
		String[] ConditionColumnsValues0 = {"g5","d3","a131","h3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i32", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a131, b1, c2, d3, e1, f5, g5, h3, i5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a131","b1","c2","d3","e1","f5","g5","h3","i5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("am31z", cols1);
		String [][] records_am31z = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			records_am31z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_am31z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("am31z", records_am31z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3]
		//record64: [a64, b0, c1, d0, e4]
		//record65: [a65, b1, c2, d1, e0]
		//record66: [a66, b0, c0, d2, e1]
		//record67: [a67, b1, c1, d3, e2]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("am31z", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 23, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a6, b0, c0, d2, e1]
		//record3: [a9, b1, c0, d1, e4]
		//record4: [a12, b0, c0, d0, e2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 23);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("d88", cols2);
		String [][] records_d88 = new String[259][cols2.length];
		for(int i=0;i<259;i++)
		{
			records_d88[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d88[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d88", records_d88[i]);
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
		String[] ConditionColumns2 = {"b","h","c","g","k","e"};
		String[] ConditionColumnsValues2 = {"b0","h6","c2","g2","k1","e4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d88", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a254","b0","c2","d2","e4","f2","g2","h6","i2","j4","k1","l2","m7","n2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wm866", cols3);
		String [][] records_wm866 = new String[407][cols3.length];
		for(int i=0;i<407;i++)
		{
			records_wm866[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wm866[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wm866", records_wm866[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10]
		String[] ConditionColumns3 = {"d","e","b"};
		String[] ConditionColumnsValues3 = {"d2","e1","b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("wm866", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 21, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record1: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record2: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		//record3: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0]
		//record4: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 21);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3","h2","i3","j6","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2","h6","i5","j6","k9"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("enq8r", cols4);
		String [][] records_enq8r = new String[1][cols4.length];
		for(int i=0;i<1;i++)
		{
			records_enq8r[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_enq8r[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("enq8r", records_enq8r[i]);
		}
		//first 1 records:
		//record0: [a0]
		//last 1 records:
		//record0: [a0]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("enq8r", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r7", cols0);
		String [][] records_r7 = new String[161][cols0.length];
		for(int i=0;i<161;i++)
		{
			records_r7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r7", records_r7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7]
		String[] ConditionColumns0 = {"e","f","i"};
		String[] ConditionColumnsValues0 = {"e0","f4","i7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a70, b0, c1, d2, e0, f4, g0, h6, i7]
		//record1: [a160, b0, c1, d0, e0, f4, g6, h0, i7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a160","b0","c1","d0","e0","f4","g6","h0","i7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z918", cols1);
		String [][] records_z918 = new String[118][cols1.length];
		for(int i=0;i<118;i++)
		{
			records_z918[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z918[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z918", records_z918[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9, m0]
		String[] ConditionColumns1 = {"c","i","d","h","m"};
		String[] ConditionColumnsValues1 = {"c1","i1","d1","h1","m1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z918", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("uetlb", cols2);
		String [][] records_uetlb = new String[307][cols2.length];
		for(int i=0;i<307;i++)
		{
			records_uetlb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_uetlb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("uetlb", records_uetlb[i]);
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
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("uetlb", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 153, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 153);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("y3n", cols0);
		String [][] records_y3n = new String[441][cols0.length];
		for(int i=0;i<441;i++)
		{
			records_y3n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y3n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y3n", records_y3n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record436: [a436, b0, c1, d0]
		//record437: [a437, b1, c2, d1]
		//record438: [a438, b0, c0, d2]
		//record439: [a439, b1, c1, d3]
		//record440: [a440, b0, c2, d0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y3n", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 220, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 220);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("kj18", cols1);
		String [][] records_kj18 = new String[232][cols1.length];
		for(int i=0;i<232;i++)
		{
			records_kj18[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kj18[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kj18", records_kj18[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7]
		String[] ConditionColumns1 = {"n"};
		String[] ConditionColumnsValues1 = {"n11"};
		ArrayList<String[]> tableSelect1 = DBApp.select("kj18", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 16, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record2: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		//record3: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11]
		//record4: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 16);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7","k1","l7","m2","n11"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jy47v", cols2);
		String [][] records_jy47v = new String[490][cols2.length];
		for(int i=0;i<490;i++)
		{
			records_jy47v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jy47v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jy47v", records_jy47v[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record485: [a485, b1]
		//record486: [a486, b0]
		//record487: [a487, b1]
		//record488: [a488, b0]
		//record489: [a489, b1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a379"};
		ArrayList<String[]> tableSelect2 = DBApp.select("jy47v", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a379, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a379","b1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l7j", cols3);
		String [][] records_l7j = new String[442][cols3.length];
		for(int i=0;i<442;i++)
		{
			records_l7j[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l7j[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l7j", records_l7j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		//record438: [a438, b0, c0, d2, e3, f0, g4]
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		//record441: [a441, b1, c0, d1, e1, f3, g0]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("l7j", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 63, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a15, b1, c0, d3, e0, f3, g1]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 63);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q0g", cols0);
		String [][] records_q0g = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_q0g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q0g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q0g", records_q0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8, l7, m8]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("q0g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 26, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 26);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("mw2sj", cols1);
		String [][] records_mw2sj = new String[439][cols1.length];
		for(int i=0;i<439;i++)
		{
			records_mw2sj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mw2sj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mw2sj", records_mw2sj[i]);
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
		String[] ConditionColumns1 = {"b","j","g","d"};
		String[] ConditionColumnsValues1 = {"b1","j9","g1","d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("mw2sj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3]
		//record1: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11]
		//record2: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9","k0","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a239","b1","c2","d3","e4","f5","g1","h7","i5","j9","k8","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a379","b1","c1","d3","e4","f1","g1","h3","i1","j9","k5","l7"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d365", cols2);
		String [][] records_d365 = new String[314][cols2.length];
		for(int i=0;i<314;i++)
		{
			records_d365[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d365[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d365", records_d365[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5, l1, m1]
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("d365", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 29, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 29);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("o3c", cols3);
		String [][] records_o3c = new String[153][cols3.length];
		for(int i=0;i<153;i++)
		{
			records_o3c[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o3c[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o3c", records_o3c[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record148: [a148]
		//record149: [a149]
		//record150: [a150]
		//record151: [a151]
		//record152: [a152]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a36"};
		ArrayList<String[]> tableSelect3 = DBApp.select("o3c", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a36]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a36"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("ibq", cols4);
		String [][] records_ibq = new String[302][cols4.length];
		for(int i=0;i<302;i++)
		{
			records_ibq[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ibq[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ibq", records_ibq[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record297: [a297]
		//record298: [a298]
		//record299: [a299]
		//record300: [a300]
		//record301: [a301]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a155"};
		ArrayList<String[]> tableSelect4 = DBApp.select("ibq", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a155]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a155"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("eqe", cols0);
		String [][] records_eqe = new String[424][cols0.length];
		for(int i=0;i<424;i++)
		{
			records_eqe[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eqe[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eqe", records_eqe[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record419: [a419]
		//record420: [a420]
		//record421: [a421]
		//record422: [a422]
		//record423: [a423]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a404"};
		ArrayList<String[]> tableSelect0 = DBApp.select("eqe", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a404]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a404"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b7y", cols1);
		String [][] records_b7y = new String[147][cols1.length];
		for(int i=0;i<147;i++)
		{
			records_b7y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b7y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b7y", records_b7y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b7y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 14);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("gzh", cols0);
		String [][] records_gzh = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_gzh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gzh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gzh", records_gzh[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record147: [a147, b1]
		//record148: [a148, b0]
		//record149: [a149, b1]
		//record150: [a150, b0]
		//record151: [a151, b1]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a91"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gzh", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a91, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a91","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lp3uz", cols1);
		String [][] records_lp3uz = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			records_lp3uz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lp3uz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lp3uz", records_lp3uz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a25"};
		ArrayList<String[]> tableSelect1 = DBApp.select("lp3uz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r4ild", cols2);
		String [][] records_r4ild = new String[490][cols2.length];
		for(int i=0;i<490;i++)
		{
			records_r4ild[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r4ild[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r4ild", records_r4ild[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		//record486: [a486, b0, c0, d2, e1, f0, g3]
		//record487: [a487, b1, c1, d3, e2, f1, g4]
		//record488: [a488, b0, c2, d0, e3, f2, g5]
		//record489: [a489, b1, c0, d1, e4, f3, g6]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("r4ild", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 98, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a23, b1, c2, d3, e3, f5, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 98);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("p6vh7", cols3);
		String [][] records_p6vh7 = new String[461][cols3.length];
		for(int i=0;i<461;i++)
		{
			records_p6vh7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p6vh7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p6vh7", records_p6vh7[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record456: [a456]
		//record457: [a457]
		//record458: [a458]
		//record459: [a459]
		//record460: [a460]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a199"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p6vh7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a199]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a199"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l2843", cols4);
		String [][] records_l2843 = new String[99][cols4.length];
		for(int i=0;i<99;i++)
		{
			records_l2843[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_l2843[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("l2843", records_l2843[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10, o4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8]
		String[] ConditionColumns4 = {"o","l","e","j","i"};
		String[] ConditionColumnsValues4 = {"o12","l0","e2","j2","i0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("l2843", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7, n2, o12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2","h0","i0","j2","k6","l0","m7","n2","o12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j6gvs", cols0);
		String [][] records_j6gvs = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_j6gvs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j6gvs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j6gvs", records_j6gvs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j6gvs", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 25);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("f1", cols0);
		String [][] records_f1 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_f1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f1", records_f1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record10: [a10, b0, c1, d2, e0]
		//record11: [a11, b1, c2, d3, e1]
		//record12: [a12, b0, c0, d0, e2]
		//record13: [a13, b1, c1, d1, e3]
		//record14: [a14, b0, c2, d2, e4]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("f1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a9, b1, c0, d1, e4]
		//record3: [a13, b1, c1, d1, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ba", cols1);
		String [][] records_ba = new String[327][cols1.length];
		for(int i=0;i<327;i++)
		{
			records_ba[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ba[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ba", records_ba[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6]
		String[] ConditionColumns1 = {"b","e","d","i","g"};
		String[] ConditionColumnsValues1 = {"b1","e1","d1","i7","g3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ba", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a241","b1","c1","d1","e1","f1","g3","h1","i7","j1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y0c", cols2);
		String [][] records_y0c = new String[160][cols2.length];
		for(int i=0;i<160;i++)
		{
			records_y0c[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y0c[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y0c", records_y0c[i]);
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
		String[] ConditionColumns2 = {"g","f","i","l","b"};
		String[] ConditionColumnsValues2 = {"g2","f5","i5","l11","b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y0c", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("cl6", cols0);
		String [][] records_cl6 = new String[406][cols0.length];
		for(int i=0;i<406;i++)
		{
			records_cl6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cl6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cl6", records_cl6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record401: [a401, b1, c2, d1, e1, f5]
		//record402: [a402, b0, c0, d2, e2, f0]
		//record403: [a403, b1, c1, d3, e3, f1]
		//record404: [a404, b0, c2, d0, e4, f2]
		//record405: [a405, b1, c0, d1, e0, f3]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cl6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 68, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a15, b1, c0, d3, e0, f3]
		//record3: [a21, b1, c0, d1, e1, f3]
		//record4: [a27, b1, c0, d3, e2, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 68);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ztex", cols1);
		String [][] records_ztex = new String[336][cols1.length];
		for(int i=0;i<336;i++)
		{
			records_ztex[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ztex[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ztex", records_ztex[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record331: [a331, b1, c1, d3, e1, f1, g2]
		//record332: [a332, b0, c2, d0, e2, f2, g3]
		//record333: [a333, b1, c0, d1, e3, f3, g4]
		//record334: [a334, b0, c1, d2, e4, f4, g5]
		//record335: [a335, b1, c2, d3, e0, f5, g6]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ztex", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 48, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a18, b0, c0, d2, e3, f0, g4]
		//record3: [a25, b1, c1, d1, e0, f1, g4]
		//record4: [a32, b0, c2, d0, e2, f2, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 48);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("o2x", cols2);
		String [][] records_o2x = new String[378][cols2.length];
		for(int i=0;i<378;i++)
		{
			records_o2x[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o2x[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o2x", records_o2x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3, m11]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2, l4, m12]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5, m0]
		String[] ConditionColumns2 = {"l"};
		String[] ConditionColumnsValues2 = {"l11"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o2x", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 31, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record3: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 31);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a59","b1","c2","d3","e4","f5","g3","h3","i5","j9","k4","l11","m7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("d9mgk", cols0);
		String [][] records_d9mgk = new String[387][cols0.length];
		for(int i=0;i<387;i++)
		{
			records_d9mgk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d9mgk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d9mgk", records_d9mgk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record382: [a382, b0, c1, d2, e2, f4]
		//record383: [a383, b1, c2, d3, e3, f5]
		//record384: [a384, b0, c0, d0, e4, f0]
		//record385: [a385, b1, c1, d1, e0, f1]
		//record386: [a386, b0, c2, d2, e1, f2]
		String[] ConditionColumns0 = {"b","c"};
		String[] ConditionColumnsValues0 = {"b0","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("d9mgk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 65, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a6, b0, c0, d2, e1, f0]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a24, b0, c0, d0, e4, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 65);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v1897", cols1);
		String [][] records_v1897 = new String[286][cols1.length];
		for(int i=0;i<286;i++)
		{
			records_v1897[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v1897[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v1897", records_v1897[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v1897", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 143, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 143);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("z2", cols2);
		String [][] records_z2 = new String[272][cols2.length];
		for(int i=0;i<272;i++)
		{
			records_z2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z2", records_z2[i]);
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
		String[] ConditionColumns2 = {"g","d","a","b","c"};
		String[] ConditionColumnsValues2 = {"g0","d2","a126","b0","c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("z2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a126","b0","c0","d2","e1","f0","g0","h6","i0","j6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wi", cols3);
		String [][] records_wi = new String[86][cols3.length];
		for(int i=0;i<86;i++)
		{
			records_wi[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wi[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wi", records_wi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		String[] ConditionColumns3 = {"a","k","e","m","i"};
		String[] ConditionColumnsValues3 = {"a15","k4","e0","m2","i6"};
		ArrayList<String[]> tableSelect3 = DBApp.select("wi", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wx", cols4);
		String [][] records_wx = new String[489][cols4.length];
		for(int i=0;i<489;i++)
		{
			records_wx[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wx[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wx", records_wx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("wx", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 122, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 122);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n64p9", cols0);
		String [][] records_n64p9 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			records_n64p9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n64p9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n64p9", records_n64p9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4]
		//record54: [a54, b0, c0, d2, e4, f0, g5]
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		//record56: [a56, b0, c2, d0, e1, f2, g0]
		//record57: [a57, b1, c0, d1, e2, f3, g1]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("n64p9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a24, b0, c0, d0, e4, f0, g3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 10);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w83", cols1);
		String [][] records_w83 = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			records_w83[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w83[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w83", records_w83[i]);
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
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k6"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w83", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record4: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 5);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2","m11"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("aa35", cols0);
		String [][] records_aa35 = new String[129][cols0.length];
		for(int i=0;i<129;i++)
		{
			records_aa35[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_aa35[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("aa35", records_aa35[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record124: [a124, b0, c1]
		//record125: [a125, b1, c2]
		//record126: [a126, b0, c0]
		//record127: [a127, b1, c1]
		//record128: [a128, b0, c2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("aa35", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 43, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 43);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("jyc", cols1);
		String [][] records_jyc = new String[246][cols1.length];
		for(int i=0;i<246;i++)
		{
			records_jyc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jyc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jyc", records_jyc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record241: [a241, b1, c1, d1]
		//record242: [a242, b0, c2, d2]
		//record243: [a243, b1, c0, d3]
		//record244: [a244, b0, c1, d0]
		//record245: [a245, b1, c2, d1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jyc", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 123, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 123);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("dgxu", cols2);
		String [][] records_dgxu = new String[416][cols2.length];
		for(int i=0;i<416;i++)
		{
			records_dgxu[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dgxu[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dgxu", records_dgxu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record411: [a411, b1, c0]
		//record412: [a412, b0, c1]
		//record413: [a413, b1, c2]
		//record414: [a414, b0, c0]
		//record415: [a415, b1, c1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("dgxu", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 208, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 208);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("d5", cols3);
		String [][] records_d5 = new String[195][cols3.length];
		for(int i=0;i<195;i++)
		{
			records_d5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_d5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("d5", records_d5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4]
		//record191: [a191, b1, c2, d3, e1, f5]
		//record192: [a192, b0, c0, d0, e2, f0]
		//record193: [a193, b1, c1, d1, e3, f1]
		//record194: [a194, b0, c2, d2, e4, f2]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a141"};
		ArrayList<String[]> tableSelect3 = DBApp.select("d5", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a141, b1, c0, d1, e1, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a141","b1","c0","d1","e1","f3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("r2qa", cols4);
		String [][] records_r2qa = new String[225][cols4.length];
		for(int i=0;i<225;i++)
		{
			records_r2qa[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r2qa[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r2qa", records_r2qa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record220: [a220, b0, c1, d0]
		//record221: [a221, b1, c2, d1]
		//record222: [a222, b0, c0, d2]
		//record223: [a223, b1, c1, d3]
		//record224: [a224, b0, c2, d0]
		String[] ConditionColumns4 = {"c"};
		String[] ConditionColumnsValues4 = {"c0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("r2qa", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 75, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 75);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xyfiz", cols0);
		String [][] records_xyfiz = new String[351][cols0.length];
		for(int i=0;i<351;i++)
		{
			records_xyfiz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xyfiz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xyfiz", records_xyfiz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xyfiz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 29);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l9h", cols0);
		String [][] records_l9h = new String[143][cols0.length];
		for(int i=0;i<143;i++)
		{
			records_l9h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l9h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l9h", records_l9h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l9h", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 18, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 18);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9","n7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tphj", cols1);
		String [][] records_tphj = new String[125][cols1.length];
		for(int i=0;i<125;i++)
		{
			records_tphj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tphj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tphj", records_tphj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1, m4, n9]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12]
		String[] ConditionColumns1 = {"l"};
		String[] ConditionColumnsValues1 = {"l6"};
		ArrayList<String[]> tableSelect1 = DBApp.select("tphj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 10, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 10);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2","n12"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("am53p", cols2);
		String [][] records_am53p = new String[378][cols2.length];
		for(int i=0;i<378;i++)
		{
			records_am53p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_am53p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("am53p", records_am53p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("am53p", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 54, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 54);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("u4r2", cols3);
		String [][] records_u4r2 = new String[312][cols3.length];
		for(int i=0;i<312;i++)
		{
			records_u4r2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u4r2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u4r2", records_u4r2[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record307: [a307, b1]
		//record308: [a308, b0]
		//record309: [a309, b1]
		//record310: [a310, b0]
		//record311: [a311, b1]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a147"};
		ArrayList<String[]> tableSelect3 = DBApp.select("u4r2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a147, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a147","b1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("g46", cols0);
		String [][] records_g46 = new String[496][cols0.length];
		for(int i=0;i<496;i++)
		{
			records_g46[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g46[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g46", records_g46[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1, o11]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11, n2, o12]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12, n3, o13]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4, o14]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5, o0]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g46", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 45, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 45);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0","i3","j8","k4","l0","m9","n6","o3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("rf8", cols1);
		String [][] records_rf8 = new String[232][cols1.length];
		for(int i=0;i<232;i++)
		{
			records_rf8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rf8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rf8", records_rf8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record227: [a227, b1, c2]
		//record228: [a228, b0, c0]
		//record229: [a229, b1, c1]
		//record230: [a230, b0, c2]
		//record231: [a231, b1, c0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rf8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 77, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 77);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y3e", cols0);
		String [][] records_y3e = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			records_y3e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y3e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y3e", records_y3e[i]);
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
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y3e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 16, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 16);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j3245", cols1);
		String [][] records_j3245 = new String[435][cols1.length];
		for(int i=0;i<435;i++)
		{
			records_j3245[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j3245[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j3245", records_j3245[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("j3245", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 218, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 218);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a510", cols2);
		String [][] records_a510 = new String[113][cols2.length];
		for(int i=0;i<113;i++)
		{
			records_a510[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a510[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a510", records_a510[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0]
		String[] ConditionColumns2 = {"n"};
		String[] ConditionColumnsValues2 = {"n0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a510", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 9);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cdy", cols0);
		String [][] records_cdy = new String[286][cols0.length];
		for(int i=0;i<286;i++)
		{
			records_cdy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cdy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cdy", records_cdy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6]
		String[] ConditionColumns0 = {"f","b","c","e"};
		String[] ConditionColumnsValues0 = {"f0","b0","c0","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cdy", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record1: [a54, b0, c0, d2, e4, f0, g5, h6, i0]
		//record2: [a84, b0, c0, d0, e4, f0, g0, h4, i3]
		//record3: [a114, b0, c0, d2, e4, f0, g2, h2, i6]
		//record4: [a144, b0, c0, d0, e4, f0, g4, h0, i0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a84","b0","c0","d0","e4","f0","g0","h4","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a114","b0","c0","d2","e4","f0","g2","h2","i6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a144","b0","c0","d0","e4","f0","g4","h0","i0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("nz0", cols1);
		String [][] records_nz0 = new String[240][cols1.length];
		for(int i=0;i<240;i++)
		{
			records_nz0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nz0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nz0", records_nz0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13, o12]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nz0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 22, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10]
		//record4: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 22);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3","n1","o14"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3","m12","n9","o6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("x68", cols2);
		String [][] records_x68 = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			records_x68[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x68[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x68", records_x68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record13: [a13, b1, c1, d1]
		//record14: [a14, b0, c2, d2]
		//record15: [a15, b1, c0, d3]
		//record16: [a16, b0, c1, d0]
		//record17: [a17, b1, c2, d1]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x68", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a4, b0, c1, d0]
		//record2: [a8, b0, c2, d0]
		//record3: [a12, b0, c0, d0]
		//record4: [a16, b0, c1, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 5);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l1j", cols0);
		String [][] records_l1j = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			records_l1j[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l1j[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l1j", records_l1j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l1j", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10","n7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nr6ga", cols1);
		String [][] records_nr6ga = new String[140][cols1.length];
		for(int i=0;i<140;i++)
		{
			records_nr6ga[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nr6ga[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nr6ga", records_nr6ga[i]);
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
		String[] ConditionColumns1 = {"e","d"};
		String[] ConditionColumnsValues1 = {"e0","d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nr6ga", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1]
		//record1: [a35, b1, c2, d3, e0, f5, g0]
		//record2: [a55, b1, c1, d3, e0, f1, g6]
		//record3: [a75, b1, c0, d3, e0, f3, g5]
		//record4: [a95, b1, c2, d3, e0, f5, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 7);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a75","b1","c0","d3","e0","f3","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a95","b1","c2","d3","e0","f5","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qj2", cols2);
		String [][] records_qj2 = new String[291][cols2.length];
		for(int i=0;i<291;i++)
		{
			records_qj2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qj2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qj2", records_qj2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2]
		String[] ConditionColumns2 = {"c","l","d","f","i"};
		String[] ConditionColumnsValues2 = {"c1","l7","d3","f1","i4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qj2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 8, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record1: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7]
		//record2: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7]
		//record3: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7]
		//record4: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 8);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7","k1","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a103","b1","c1","d3","e3","f1","g5","h7","i4","j3","k4","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a139","b1","c1","d3","e4","f1","g6","h3","i4","j9","k7","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a175","b1","c1","d3","e0","f1","g0","h7","i4","j5","k10","l7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r0l", cols0);
		String [][] records_r0l = new String[251][cols0.length];
		for(int i=0;i<251;i++)
		{
			records_r0l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r0l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r0l", records_r0l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2]
		String[] ConditionColumns0 = {"a","c","d","b"};
		String[] ConditionColumnsValues0 = {"a210","c0","d2","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r0l", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a210, b0, c0, d2, e0, f0, g0, h2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a210","b0","c0","d2","e0","f0","g0","h2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("n48", cols1);
		String [][] records_n48 = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			records_n48[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n48[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n48", records_n48[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record70: [a70]
		//record71: [a71]
		//record72: [a72]
		//record73: [a73]
		//record74: [a74]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a12"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n48", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l03", cols2);
		String [][] records_l03 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_l03[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l03[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l03", records_l03[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("l03", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 7);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5","n5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("t08", cols3);
		String [][] records_t08 = new String[428][cols3.length];
		for(int i=0;i<428;i++)
		{
			records_t08[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t08[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t08", records_t08[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4]
		String[] ConditionColumns3 = {"c","f"};
		String[] ConditionColumnsValues3 = {"c1","f1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("t08", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 72, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 72);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("moa4o", cols0);
		String [][] records_moa4o = new String[282][cols0.length];
		for(int i=0;i<282;i++)
		{
			records_moa4o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_moa4o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("moa4o", records_moa4o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record277: [a277, b1, c1, d1, e2]
		//record278: [a278, b0, c2, d2, e3]
		//record279: [a279, b1, c0, d3, e4]
		//record280: [a280, b0, c1, d0, e0]
		//record281: [a281, b1, c2, d1, e1]
		String[] ConditionColumns0 = {"e","d"};
		String[] ConditionColumnsValues0 = {"e0","d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("moa4o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a10, b0, c1, d2, e0]
		//record1: [a30, b0, c0, d2, e0]
		//record2: [a50, b0, c2, d2, e0]
		//record3: [a70, b0, c1, d2, e0]
		//record4: [a90, b0, c0, d2, e0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 14);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a90","b0","c0","d2","e0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("z42", cols1);
		String [][] records_z42 = new String[305][cols1.length];
		for(int i=0;i<305;i++)
		{
			records_z42[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z42[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z42", records_z42[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record300: [a300, b0, c0, d0, e0, f0]
		//record301: [a301, b1, c1, d1, e1, f1]
		//record302: [a302, b0, c2, d2, e2, f2]
		//record303: [a303, b1, c0, d3, e3, f3]
		//record304: [a304, b0, c1, d0, e4, f4]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a168"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z42", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a168, b0, c0, d0, e3, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a168","b0","c0","d0","e3","f0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("m3p0", cols2);
		String [][] records_m3p0 = new String[392][cols2.length];
		for(int i=0;i<392;i++)
		{
			records_m3p0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m3p0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m3p0", records_m3p0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3, m10, n9, o12]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11, n10, o13]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12, n11, o14]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12, o0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13, o1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("m3p0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 196, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 196);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6","o6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w64", cols3);
		String [][] records_w64 = new String[416][cols3.length];
		for(int i=0;i<416;i++)
		{
			records_w64[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w64[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w64", records_w64[i]);
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
		String[] ConditionColumns3 = {"c","a","d","f","b"};
		String[] ConditionColumnsValues3 = {"c0","a105","d1","f3","b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("w64", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a105","b1","c0","d1","e0","f3","g0","h1","i6","j5","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("l6w2", cols0);
		String [][] records_l6w2 = new String[370][cols0.length];
		for(int i=0;i<370;i++)
		{
			records_l6w2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l6w2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l6w2", records_l6w2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a248"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l6w2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a248","b0","c2","d0","e3","f2","g3","h0","i5","j8","k6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("t1b", cols1);
		String [][] records_t1b = new String[257][cols1.length];
		for(int i=0;i<257;i++)
		{
			records_t1b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t1b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t1b", records_t1b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("t1b", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 32, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 32);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ue", cols0);
		String [][] records_ue = new String[115][cols0.length];
		for(int i=0;i<115;i++)
		{
			records_ue[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ue[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ue", records_ue[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record110: [a110, b0, c2]
		//record111: [a111, b1, c0]
		//record112: [a112, b0, c1]
		//record113: [a113, b1, c2]
		//record114: [a114, b0, c0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ue", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 58, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 58);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("r5jm0", cols1);
		String [][] records_r5jm0 = new String[111][cols1.length];
		for(int i=0;i<111;i++)
		{
			records_r5jm0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r5jm0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r5jm0", records_r5jm0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0]
		String[] ConditionColumns1 = {"h","d","c","b","a"};
		String[] ConditionColumnsValues1 = {"h1","d1","c0","b1","a105"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r5jm0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a105","b1","c0","d1","e0","f3","g0","h1","i6","j5","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("txo8", cols0);
		String [][] records_txo8 = new String[219][cols0.length];
		for(int i=0;i<219;i++)
		{
			records_txo8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_txo8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("txo8", records_txo8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record214: [a214, b0, c1]
		//record215: [a215, b1, c2]
		//record216: [a216, b0, c0]
		//record217: [a217, b1, c1]
		//record218: [a218, b0, c2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("txo8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 109, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 109);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z814a", cols1);
		String [][] records_z814a = new String[210][cols1.length];
		for(int i=0;i<210;i++)
		{
			records_z814a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z814a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z814a", records_z814a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1]
		String[] ConditionColumns1 = {"a","e","h","g","m","c"};
		String[] ConditionColumnsValues1 = {"a39","e4","h7","g4","m0","c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z814a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n0s8", cols2);
		String [][] records_n0s8 = new String[218][cols2.length];
		for(int i=0;i<218;i++)
		{
			records_n0s8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n0s8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n0s8", records_n0s8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("n0s8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 37, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 37);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("l7k", cols3);
		String [][] records_l7k = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			records_l7k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l7k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l7k", records_l7k[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record15: [a15]
		//record16: [a16]
		//record17: [a17]
		//record18: [a18]
		//record19: [a19]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a10"};
		ArrayList<String[]> tableSelect3 = DBApp.select("l7k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r26", cols4);
		String [][] records_r26 = new String[246][cols4.length];
		for(int i=0;i<246;i++)
		{
			records_r26[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r26[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r26", records_r26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2, l4, m10]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("r26", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 61, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record3: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record4: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 61);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("x8", cols0);
		String [][] records_x8 = new String[175][cols0.length];
		for(int i=0;i<175;i++)
		{
			records_x8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x8", records_x8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 29);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w9", cols0);
		String [][] records_w9 = new String[236][cols0.length];
		for(int i=0;i<236;i++)
		{
			records_w9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w9", records_w9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record231: [a231, b1, c0, d3, e1, f3, g0]
		//record232: [a232, b0, c1, d0, e2, f4, g1]
		//record233: [a233, b1, c2, d1, e3, f5, g2]
		//record234: [a234, b0, c0, d2, e4, f0, g3]
		//record235: [a235, b1, c1, d3, e0, f1, g4]
		String[] ConditionColumns0 = {"d","g","b"};
		String[] ConditionColumnsValues0 = {"d2","g1","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a22, b0, c1, d2, e2, f4, g1]
		//record1: [a50, b0, c2, d2, e0, f2, g1]
		//record2: [a78, b0, c0, d2, e3, f0, g1]
		//record3: [a106, b0, c1, d2, e1, f4, g1]
		//record4: [a134, b0, c2, d2, e4, f2, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a78","b0","c0","d2","e3","f0","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a106","b0","c1","d2","e1","f4","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a134","b0","c2","d2","e4","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("sml", cols1);
		String [][] records_sml = new String[105][cols1.length];
		for(int i=0;i<105;i++)
		{
			records_sml[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sml[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sml", records_sml[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record100: [a100, b0, c1, d0]
		//record101: [a101, b1, c2, d1]
		//record102: [a102, b0, c0, d2]
		//record103: [a103, b1, c1, d3]
		//record104: [a104, b0, c2, d0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("sml", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 53, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 53);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("y35lx", cols2);
		String [][] records_y35lx = new String[481][cols2.length];
		for(int i=0;i<481;i++)
		{
			records_y35lx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y35lx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y35lx", records_y35lx[i]);
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
		String[] ConditionColumns2 = {"d","j"};
		String[] ConditionColumnsValues2 = {"d3","j3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y35lx", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 24, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record2: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		//record3: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record4: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 24);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3","k8","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6","h3","i2","j3","k6","l11"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("yps8b", cols0);
		String [][] records_yps8b = new String[342][cols0.length];
		for(int i=0;i<342;i++)
		{
			records_yps8b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yps8b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yps8b", records_yps8b[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record337: [a337, b1]
		//record338: [a338, b0]
		//record339: [a339, b1]
		//record340: [a340, b0]
		//record341: [a341, b1]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a117"};
		ArrayList<String[]> tableSelect0 = DBApp.select("yps8b", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a117, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a117","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("p03", cols1);
		String [][] records_p03 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			records_p03[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p03[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p03", records_p03[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record46: [a46, b0, c1, d2, e1, f4]
		//record47: [a47, b1, c2, d3, e2, f5]
		//record48: [a48, b0, c0, d0, e3, f0]
		//record49: [a49, b1, c1, d1, e4, f1]
		//record50: [a50, b0, c2, d2, e0, f2]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("p03", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 13, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a5, b1, c2, d1, e0, f5]
		//record2: [a9, b1, c0, d1, e4, f3]
		//record3: [a13, b1, c1, d1, e3, f1]
		//record4: [a17, b1, c2, d1, e2, f5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 13);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i9kwf", cols2);
		String [][] records_i9kwf = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			records_i9kwf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i9kwf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i9kwf", records_i9kwf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1]
		String[] ConditionColumns2 = {"g","h","e"};
		String[] ConditionColumnsValues2 = {"g6","h4","e1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("i9kwf", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a76, b0, c1, d0, e1, f4, g6, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a76","b0","c1","d0","e1","f4","g6","h4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ubk", cols3);
		String [][] records_ubk = new String[402][cols3.length];
		for(int i=0;i<402;i++)
		{
			records_ubk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ubk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ubk", records_ubk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1, l1, m7]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2, l2, m8]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3, l3, m9]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11]
		String[] ConditionColumns3 = {"d","e","m"};
		String[] ConditionColumnsValues3 = {"d1","e1","m3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("ubk", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3]
		//record1: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 2);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1","f3","g4","h1","i0","j1","k4","l9","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a341","b1","c2","d1","e1","f5","g5","h5","i8","j1","k0","l5","m3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cva", cols4);
		String [][] records_cva = new String[290][cols4.length];
		for(int i=0;i<290;i++)
		{
			records_cva[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_cva[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("cva", records_cva[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9]
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e3"};
		ArrayList<String[]> tableSelect4 = DBApp.select("cva", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 58, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 58);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("b2324", cols0);
		String [][] records_b2324 = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_b2324[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b2324[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b2324", records_b2324[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record412: [a412, b0, c1, d0, e2]
		//record413: [a413, b1, c2, d1, e3]
		//record414: [a414, b0, c0, d2, e4]
		//record415: [a415, b1, c1, d3, e0]
		//record416: [a416, b0, c2, d0, e1]
		String[] ConditionColumns0 = {"c","d"};
		String[] ConditionColumnsValues0 = {"c2","d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b2324", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 35, records:
		//record0: [a8, b0, c2, d0, e3]
		//record1: [a20, b0, c2, d0, e0]
		//record2: [a32, b0, c2, d0, e2]
		//record3: [a44, b0, c2, d0, e4]
		//record4: [a56, b0, c2, d0, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 35);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b710", cols1);
		String [][] records_b710 = new String[212][cols1.length];
		for(int i=0;i<212;i++)
		{
			records_b710[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b710[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b710", records_b710[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4]
		String[] ConditionColumns1 = {"g","d","i","f"};
		String[] ConditionColumnsValues1 = {"g6","d3","i1","f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b710", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("hkr", cols2);
		String [][] records_hkr = new String[160][cols2.length];
		for(int i=0;i<160;i++)
		{
			records_hkr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hkr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hkr", records_hkr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record155: [a155, b1, c2]
		//record156: [a156, b0, c0]
		//record157: [a157, b1, c1]
		//record158: [a158, b0, c2]
		//record159: [a159, b1, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("hkr", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 80, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 80);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v32", cols3);
		String [][] records_v32 = new String[261][cols3.length];
		for(int i=0;i<261;i++)
		{
			records_v32[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v32[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v32", records_v32[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7]
		String[] ConditionColumns3 = {"h"};
		String[] ConditionColumnsValues3 = {"h4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("v32", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 33, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 33);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("j45", cols0);
		String [][] records_j45 = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			records_j45[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j45[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j45", records_j45[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record64: [a64, b0]
		//record65: [a65, b1]
		//record66: [a66, b0]
		//record67: [a67, b1]
		//record68: [a68, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j45", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 35, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 35);
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
	public void test127TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jt372", cols0);
		String [][] records_jt372 = new String[356][cols0.length];
		for(int i=0;i<356;i++)
		{
			records_jt372[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jt372[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jt372", records_jt372[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record351: [a351, b1, c0, d3, e1, f3, g1]
		//record352: [a352, b0, c1, d0, e2, f4, g2]
		//record353: [a353, b1, c2, d1, e3, f5, g3]
		//record354: [a354, b0, c0, d2, e4, f0, g4]
		//record355: [a355, b1, c1, d3, e0, f1, g5]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("jt372", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 119, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 119);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("id", cols1);
		String [][] records_id = new String[464][cols1.length];
		for(int i=0;i<464;i++)
		{
			records_id[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_id[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("id", records_id[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record459: [a459]
		//record460: [a460]
		//record461: [a461]
		//record462: [a462]
		//record463: [a463]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a43"};
		ArrayList<String[]> tableSelect1 = DBApp.select("id", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a43]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("cc", cols2);
		String [][] records_cc = new String[467][cols2.length];
		for(int i=0;i<467;i++)
		{
			records_cc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cc", records_cc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0, l6]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("cc", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 156, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 156);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("o56", cols0);
		String [][] records_o56 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			records_o56[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o56[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o56", records_o56[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record72: [a72, b0, c0]
		//record73: [a73, b1, c1]
		//record74: [a74, b0, c2]
		//record75: [a75, b1, c0]
		//record76: [a76, b0, c1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o56", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 39, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 39);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("osx", cols0);
		String [][] records_osx = new String[170][cols0.length];
		for(int i=0;i<170;i++)
		{
			records_osx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_osx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("osx", records_osx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("osx", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 28);
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
		DBApp.createTable("aocn", cols1);
		String [][] records_aocn = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_aocn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_aocn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("aocn", records_aocn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("aocn", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 20);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("g9r", cols0);
		String [][] records_g9r = new String[184][cols0.length];
		for(int i=0;i<184;i++)
		{
			records_g9r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g9r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g9r", records_g9r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11, n12]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0]
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1]
		String[] ConditionColumns0 = {"a","d"};
		String[] ConditionColumnsValues0 = {"a104","d0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g9r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6","h0","i5","j4","k5","l8","m0","n6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x9l", cols1);
		String [][] records_x9l = new String[454][cols1.length];
		for(int i=0;i<454;i++)
		{
			records_x9l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x9l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x9l", records_x9l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		//record450: [a450, b0, c0, d2, e0, f0, g2]
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		//record453: [a453, b1, c0, d1, e3, f3, g5]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("x9l", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 113, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 113);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("w85", cols0);
		String [][] records_w85 = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_w85[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w85[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w85", records_w85[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1]
		//record494: [a494, b0, c2, d2, e4, f2]
		//record495: [a495, b1, c0, d3, e0, f3]
		//record496: [a496, b0, c1, d0, e1, f4]
		//record497: [a497, b1, c2, d1, e2, f5]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w85", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 83, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a6, b0, c0, d2, e1, f0]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a24, b0, c0, d0, e4, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 83);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("b9", cols1);
		String [][] records_b9 = new String[300][cols1.length];
		for(int i=0;i<300;i++)
		{
			records_b9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record295: [a295, b1, c1, d3, e0, f1]
		//record296: [a296, b0, c2, d0, e1, f2]
		//record297: [a297, b1, c0, d1, e2, f3]
		//record298: [a298, b0, c1, d2, e3, f4]
		//record299: [a299, b1, c2, d3, e4, f5]
		String[] ConditionColumns1 = {"b","f"};
		String[] ConditionColumnsValues1 = {"b1","f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 50, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a19, b1, c1, d3, e4, f1]
		//record4: [a25, b1, c1, d1, e0, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 50);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("l2", cols2);
		String [][] records_l2 = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			records_l2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l2", records_l2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record30: [a30, b0, c0, d2, e0, f0]
		//record31: [a31, b1, c1, d3, e1, f1]
		//record32: [a32, b0, c2, d0, e2, f2]
		//record33: [a33, b1, c0, d1, e3, f3]
		//record34: [a34, b0, c1, d2, e4, f4]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("l2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a17, b1, c2, d1, e2, f5]
		//record4: [a22, b0, c1, d2, e2, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 7);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("p40", cols3);
		String [][] records_p40 = new String[352][cols3.length];
		for(int i=0;i<352;i++)
		{
			records_p40[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p40[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p40", records_p40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record347: [a347, b1, c2, d3]
		//record348: [a348, b0, c0, d0]
		//record349: [a349, b1, c1, d1]
		//record350: [a350, b0, c2, d2]
		//record351: [a351, b1, c0, d3]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p40", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 176, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a2, b0, c2, d2]
		//record2: [a4, b0, c1, d0]
		//record3: [a6, b0, c0, d2]
		//record4: [a8, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 176);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m5v69", cols4);
		String [][] records_m5v69 = new String[122][cols4.length];
		for(int i=0;i<122;i++)
		{
			records_m5v69[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_m5v69[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("m5v69", records_m5v69[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record117: [a117, b1, c0, d1, e2, f3, g5]
		//record118: [a118, b0, c1, d2, e3, f4, g6]
		//record119: [a119, b1, c2, d3, e4, f5, g0]
		//record120: [a120, b0, c0, d0, e0, f0, g1]
		//record121: [a121, b1, c1, d1, e1, f1, g2]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("m5v69", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 61, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 61);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e54g9", cols0);
		String [][] records_e54g9 = new String[450][cols0.length];
		for(int i=0;i<450;i++)
		{
			records_e54g9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e54g9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e54g9", records_e54g9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5, l1, m3, n11]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2, m4, n12]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j6"};
		ArrayList<String[]> tableSelect0 = DBApp.select("e54g9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 45, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 45);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2","l10","m7","n4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a76", cols1);
		String [][] records_a76 = new String[452][cols1.length];
		for(int i=0;i<452;i++)
		{
			records_a76[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a76[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a76", records_a76[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a76", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 151, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 151);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("yyad6", cols0);
		String [][] records_yyad6 = new String[346][cols0.length];
		for(int i=0;i<346;i++)
		{
			records_yyad6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yyad6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yyad6", records_yyad6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3, n5]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4, n6]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5, n7]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("yyad6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 39, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 39);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j7grc", cols0);
		String [][] records_j7grc = new String[215][cols0.length];
		for(int i=0;i<215;i++)
		{
			records_j7grc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j7grc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j7grc", records_j7grc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j7grc", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 107, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 107);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("j3", cols1);
		String [][] records_j3 = new String[453][cols1.length];
		for(int i=0;i<453;i++)
		{
			records_j3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j3", records_j3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("j3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 76, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 76);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("e694f", cols2);
		String [][] records_e694f = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			records_e694f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e694f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e694f", records_e694f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record70: [a70, b0, c1, d2, e0, f4]
		//record71: [a71, b1, c2, d3, e1, f5]
		//record72: [a72, b0, c0, d0, e2, f0]
		//record73: [a73, b1, c1, d1, e3, f1]
		//record74: [a74, b0, c2, d2, e4, f2]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("e694f", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 12, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a15, b1, c0, d3, e0, f3]
		//record3: [a21, b1, c0, d1, e1, f3]
		//record4: [a27, b1, c0, d3, e2, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 12);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("jf41i", cols3);
		String [][] records_jf41i = new String[329][cols3.length];
		for(int i=0;i<329;i++)
		{
			records_jf41i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jf41i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jf41i", records_jf41i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9]
		String[] ConditionColumns3 = {"k","g"};
		String[] ConditionColumnsValues3 = {"k5","g3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("jf41i", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record1: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5]
		//record2: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5]
		//record3: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 4);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a115","b1","c1","d3","e0","f1","g3","h3","i7","j5","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a192","b0","c0","d0","e2","f0","g3","h0","i3","j2","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a269","b1","c2","d1","e4","f5","g3","h5","i8","j9","k5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("nm991", cols0);
		String [][] records_nm991 = new String[303][cols0.length];
		for(int i=0;i<303;i++)
		{
			records_nm991[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nm991[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nm991", records_nm991[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record298: [a298, b0, c1, d2, e3, f4]
		//record299: [a299, b1, c2, d3, e4, f5]
		//record300: [a300, b0, c0, d0, e0, f0]
		//record301: [a301, b1, c1, d1, e1, f1]
		//record302: [a302, b0, c2, d2, e2, f2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("nm991", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 101, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a7, b1, c1, d3, e2, f1]
		//record3: [a10, b0, c1, d2, e0, f4]
		//record4: [a13, b1, c1, d1, e3, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 101);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("bl", cols1);
		String [][] records_bl = new String[358][cols1.length];
		for(int i=0;i<358;i++)
		{
			records_bl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bl", records_bl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record353: [a353, b1, c2, d1, e3]
		//record354: [a354, b0, c0, d2, e4]
		//record355: [a355, b1, c1, d3, e0]
		//record356: [a356, b0, c2, d0, e1]
		//record357: [a357, b1, c0, d1, e2]
		String[] ConditionColumns1 = {"c","e"};
		String[] ConditionColumnsValues1 = {"c2","e1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("bl", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a11, b1, c2, d3, e1]
		//record1: [a26, b0, c2, d2, e1]
		//record2: [a41, b1, c2, d1, e1]
		//record3: [a56, b0, c2, d0, e1]
		//record4: [a71, b1, c2, d3, e1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 24);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a71","b1","c2","d3","e1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("n2b59", cols2);
		String [][] records_n2b59 = new String[368][cols2.length];
		for(int i=0;i<368;i++)
		{
			records_n2b59[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n2b59[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n2b59", records_n2b59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("n2b59", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 41, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 41);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1","k8","l5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("bb", cols3);
		String [][] records_bb = new String[454][cols3.length];
		for(int i=0;i<454;i++)
		{
			records_bb[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bb[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bb", records_bb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record449: [a449, b1, c2, d1]
		//record450: [a450, b0, c0, d2]
		//record451: [a451, b1, c1, d3]
		//record452: [a452, b0, c2, d0]
		//record453: [a453, b1, c0, d1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("bb", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 152, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 152);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wpd", cols4);
		String [][] records_wpd = new String[150][cols4.length];
		for(int i=0;i<150;i++)
		{
			records_wpd[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wpd[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wpd", records_wpd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5]
		String[] ConditionColumns4 = {"g"};
		String[] ConditionColumnsValues4 = {"g0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("wpd", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 22, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 22);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("gx11", cols0);
		String [][] records_gx11 = new String[105][cols0.length];
		for(int i=0;i<105;i++)
		{
			records_gx11[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gx11[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gx11", records_gx11[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6]
		String[] ConditionColumns0 = {"n","c","e"};
		String[] ConditionColumnsValues0 = {"n5","c0","e3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gx11", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7","n5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("n21", cols1);
		String [][] records_n21 = new String[180][cols1.length];
		for(int i=0;i<180;i++)
		{
			records_n21[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n21[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n21", records_n21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record175: [a175, b1, c1]
		//record176: [a176, b0, c2]
		//record177: [a177, b1, c0]
		//record178: [a178, b0, c1]
		//record179: [a179, b1, c2]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("n21", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 60, records:
		//record0: [a0, b0, c0]
		//record1: [a3, b1, c0]
		//record2: [a6, b0, c0]
		//record3: [a9, b1, c0]
		//record4: [a12, b0, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 60);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("kmx5r", cols2);
		String [][] records_kmx5r = new String[168][cols2.length];
		for(int i=0;i<168;i++)
		{
			records_kmx5r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kmx5r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kmx5r", records_kmx5r[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record163: [a163, b1]
		//record164: [a164, b0]
		//record165: [a165, b1]
		//record166: [a166, b0]
		//record167: [a167, b1]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a113"};
		ArrayList<String[]> tableSelect2 = DBApp.select("kmx5r", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a113, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a113","b1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("bu3v3", cols3);
		String [][] records_bu3v3 = new String[109][cols3.length];
		for(int i=0;i<109;i++)
		{
			records_bu3v3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bu3v3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bu3v3", records_bu3v3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4]
		//record105: [a105, b1, c0, d1, e0]
		//record106: [a106, b0, c1, d2, e1]
		//record107: [a107, b1, c2, d3, e2]
		//record108: [a108, b0, c0, d0, e3]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a98"};
		ArrayList<String[]> tableSelect3 = DBApp.select("bu3v3", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a98, b0, c2, d2, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a98","b0","c2","d2","e3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g58", cols0);
		String [][] records_g58 = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_g58[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g58[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g58", records_g58[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		//record416: [a416, b0, c2, d0, e1, f2, g3]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g58", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 104, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 104);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y6z7", cols1);
		String [][] records_y6z7 = new String[386][cols1.length];
		for(int i=0;i<386;i++)
		{
			records_y6z7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y6z7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y6z7", records_y6z7[i]);
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
		String[] ConditionColumns1 = {"i","a","l","e","g","h"};
		String[] ConditionColumnsValues1 = {"i5","a113","l5","e3","g1","h1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y6z7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a113","b1","c2","d1","e3","f5","g1","h1","i5","j3","k3","l5","m9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w4pq", cols2);
		String [][] records_w4pq = new String[183][cols2.length];
		for(int i=0;i<183;i++)
		{
			records_w4pq[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w4pq[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w4pq", records_w4pq[i]);
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
		String[] ConditionColumns2 = {"l"};
		String[] ConditionColumnsValues2 = {"l1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("w4pq", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 16, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 16);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9","o7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10","n7","o4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("l8s", cols3);
		String [][] records_l8s = new String[492][cols3.length];
		for(int i=0;i<492;i++)
		{
			records_l8s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l8s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l8s", records_l8s[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record487: [a487, b1]
		//record488: [a488, b0]
		//record489: [a489, b1]
		//record490: [a490, b0]
		//record491: [a491, b1]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("l8s", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 246, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 246);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h6po", cols4);
		String [][] records_h6po = new String[95][cols4.length];
		for(int i=0;i<95;i++)
		{
			records_h6po[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_h6po[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("h6po", records_h6po[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1]
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4]
		String[] ConditionColumns4 = {"h"};
		String[] ConditionColumnsValues4 = {"h2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("h6po", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 12, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 12);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1894);
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