package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests10
{

	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("h87", cols0);
		String [][] records_h87 = new String[169][cols0.length];
		for(int i=0;i<169;i++)
		{
			records_h87[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h87[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h87", records_h87[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record164: [a164, b0]
		//record165: [a165, b1]
		//record166: [a166, b0]
		//record167: [a167, b1]
		//record168: [a168, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h87", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 85, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 85);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("wf1", cols1);
		String [][] records_wf1 = new String[410][cols1.length];
		for(int i=0;i<410;i++)
		{
			records_wf1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wf1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wf1", records_wf1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record405: [a405, b1, c0, d1]
		//record406: [a406, b0, c1, d2]
		//record407: [a407, b1, c2, d3]
		//record408: [a408, b0, c0, d0]
		//record409: [a409, b1, c1, d1]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a43"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wf1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a43, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hx3", cols2);
		String [][] records_hx3 = new String[351][cols2.length];
		for(int i=0;i<351;i++)
		{
			records_hx3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hx3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hx3", records_hx3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2]
		String[] ConditionColumns2 = {"i","j"};
		String[] ConditionColumnsValues2 = {"i2","j5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("hx3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5]
		//record1: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11]
		//record2: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5]
		//record3: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 4);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2","h1","i2","j5","k10","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a155","b1","c2","d3","e0","f5","g1","h3","i2","j5","k1","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a245","b1","c2","d1","e0","f5","g0","h5","i2","j5","k3","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a335","b1","c2","d3","e0","f5","g6","h7","i2","j5","k5","l11"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("u8", cols3);
		String [][] records_u8 = new String[296][cols3.length];
		for(int i=0;i<296;i++)
		{
			records_u8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u8", records_u8[i]);
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
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("u8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 59, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a17, b1, c2, d1, e2, f5]
		//record4: [a22, b0, c1, d2, e2, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 59);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("na9", cols4);
		String [][] records_na9 = new String[330][cols4.length];
		for(int i=0;i<330;i++)
		{
			records_na9[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_na9[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("na9", records_na9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("na9", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 165, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 165);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pc", cols0);
		String [][] records_pc = new String[396][cols0.length];
		for(int i=0;i<396;i++)
		{
			records_pc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pc", records_pc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3]
		String[] ConditionColumns0 = {"c","h","d"};
		String[] ConditionColumnsValues0 = {"c0","h5","d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pc", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 16, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record1: [a45, b1, c0, d1, e0, f3, g3, h5]
		//record2: [a69, b1, c0, d1, e4, f3, g6, h5]
		//record3: [a93, b1, c0, d1, e3, f3, g2, h5]
		//record4: [a117, b1, c0, d1, e2, f3, g5, h5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 16);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a93","b1","c0","d1","e3","f3","g2","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a117","b1","c0","d1","e2","f3","g5","h5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e92q", cols1);
		String [][] records_e92q = new String[120][cols1.length];
		for(int i=0;i<120;i++)
		{
			records_e92q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e92q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e92q", records_e92q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11]
		String[] ConditionColumns1 = {"c","d","g","f","l"};
		String[] ConditionColumnsValues1 = {"c1","d3","g6","f1","l7"};
		ArrayList<String[]> tableSelect1 = DBApp.select("e92q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k031", cols2);
		String [][] records_k031 = new String[135][cols2.length];
		for(int i=0;i<135;i++)
		{
			records_k031[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k031[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k031", records_k031[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4]
		String[] ConditionColumns2 = {"m"};
		String[] ConditionColumnsValues2 = {"m6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("k031", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 10, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		//record4: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3, l10, m6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 10);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1","l9","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a58","b0","c1","d2","e3","f4","g2","h2","i4","j8","k3","l10","m6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("f9l", cols3);
		String [][] records_f9l = new String[78][cols3.length];
		for(int i=0;i<78;i++)
		{
			records_f9l[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f9l[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f9l", records_f9l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record73: [a73, b1, c1, d1, e3]
		//record74: [a74, b0, c2, d2, e4]
		//record75: [a75, b1, c0, d3, e0]
		//record76: [a76, b0, c1, d0, e1]
		//record77: [a77, b1, c2, d1, e2]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a70"};
		ArrayList<String[]> tableSelect3 = DBApp.select("f9l", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a70, b0, c1, d2, e0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("oy0", cols0);
		String [][] records_oy0 = new String[76][cols0.length];
		for(int i=0;i<76;i++)
		{
			records_oy0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oy0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oy0", records_oy0[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("oy0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a17, b1, c2, d1, e2, f5]
		//record4: [a22, b0, c1, d2, e2, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 15);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("bbq", cols1);
		String [][] records_bbq = new String[119][cols1.length];
		for(int i=0;i<119;i++)
		{
			records_bbq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bbq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bbq", records_bbq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("bbq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 11);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2","l10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x8m6z", cols2);
		String [][] records_x8m6z = new String[161][cols2.length];
		for(int i=0;i<161;i++)
		{
			records_x8m6z[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x8m6z[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x8m6z", records_x8m6z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4]
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k9"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x8m6z", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 14, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 14);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("bs392", cols3);
		String [][] records_bs392 = new String[224][cols3.length];
		for(int i=0;i<224;i++)
		{
			records_bs392[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bs392[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bs392", records_bs392[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record219: [a219, b1, c0]
		//record220: [a220, b0, c1]
		//record221: [a221, b1, c2]
		//record222: [a222, b0, c0]
		//record223: [a223, b1, c1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("bs392", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 74, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 74);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("di3b", cols4);
		String [][] records_di3b = new String[374][cols4.length];
		for(int i=0;i<374;i++)
		{
			records_di3b[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_di3b[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("di3b", records_di3b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10]
		String[] ConditionColumns4 = {"j"};
		String[] ConditionColumnsValues4 = {"j8"};
		ArrayList<String[]> tableSelect4 = DBApp.select("di3b", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 37, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record3: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 37);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0","i3","j8","k4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ana", cols0);
		String [][] records_ana = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			records_ana[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ana[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ana", records_ana[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3, m10]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a12"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ana", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z77", cols1);
		String [][] records_z77 = new String[214][cols1.length];
		for(int i=0;i<214;i++)
		{
			records_z77[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z77[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z77", records_z77[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3, n1]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8, m4, n2]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5, n3]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z77", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 43, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 43);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("sr68", cols2);
		String [][] records_sr68 = new String[256][cols2.length];
		for(int i=0;i<256;i++)
		{
			records_sr68[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sr68[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sr68", records_sr68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0, o12]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6, n1, o13]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0]
		String[] ConditionColumns2 = {"b","k","h","o","j","d"};
		String[] ConditionColumnsValues2 = {"b1","k9","h3","o13","j3","d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sr68", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a163","b1","c1","d3","e3","f1","g2","h3","i1","j3","k9","l7","m7","n9","o13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("vx653", cols0);
		String [][] records_vx653 = new String[490][cols0.length];
		for(int i=0;i<490;i++)
		{
			records_vx653[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vx653[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vx653", records_vx653[i]);
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
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vx653", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 245, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 245);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l7w", cols1);
		String [][] records_l7w = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			records_l7w[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l7w[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l7w", records_l7w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		//record56: [a56, b0, c2, d0, e1, f2, g0]
		//record57: [a57, b1, c0, d1, e2, f3, g1]
		//record58: [a58, b0, c1, d2, e3, f4, g2]
		//record59: [a59, b1, c2, d3, e4, f5, g3]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a44"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l7w", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a44, b0, c2, d0, e4, f2, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("cqu9z", cols0);
		String [][] records_cqu9z = new String[164][cols0.length];
		for(int i=0;i<164;i++)
		{
			records_cqu9z[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cqu9z[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cqu9z", records_cqu9z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record159: [a159, b1, c0, d3]
		//record160: [a160, b0, c1, d0]
		//record161: [a161, b1, c2, d1]
		//record162: [a162, b0, c0, d2]
		//record163: [a163, b1, c1, d3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cqu9z", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 55, records:
		//record0: [a0, b0, c0, d0]
		//record1: [a3, b1, c0, d3]
		//record2: [a6, b0, c0, d2]
		//record3: [a9, b1, c0, d1]
		//record4: [a12, b0, c0, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 55);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y62m9", cols1);
		String [][] records_y62m9 = new String[354][cols1.length];
		for(int i=0;i<354;i++)
		{
			records_y62m9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y62m9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y62m9", records_y62m9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("y62m9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 36, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 36);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1","k8","l5","m2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("sz", cols2);
		String [][] records_sz = new String[310][cols2.length];
		for(int i=0;i<310;i++)
		{
			records_sz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sz", records_sz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record305: [a305, b1, c2]
		//record306: [a306, b0, c0]
		//record307: [a307, b1, c1]
		//record308: [a308, b0, c2]
		//record309: [a309, b1, c0]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("sz", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 103, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 103);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hy", cols0);
		String [][] records_hy = new String[409][cols0.length];
		for(int i=0;i<409;i++)
		{
			records_hy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hy", records_hy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hy", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 204, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 204);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("wo7k2", cols1);
		String [][] records_wo7k2 = new String[477][cols1.length];
		for(int i=0;i<477;i++)
		{
			records_wo7k2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wo7k2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wo7k2", records_wo7k2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("wo7k2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 60, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 60);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lph", cols2);
		String [][] records_lph = new String[157][cols2.length];
		for(int i=0;i<157;i++)
		{
			records_lph[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lph[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lph", records_lph[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9, m10]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10, m11]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0]
		String[] ConditionColumns2 = {"l"};
		String[] ConditionColumnsValues2 = {"l7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("lph", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 13, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		//record4: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 13);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("t7d", cols3);
		String [][] records_t7d = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			records_t7d[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t7d[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t7d", records_t7d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1, f3]
		//record52: [a52, b0, c1, d0, e2, f4]
		//record53: [a53, b1, c2, d1, e3, f5]
		//record54: [a54, b0, c0, d2, e4, f0]
		//record55: [a55, b1, c1, d3, e0, f1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("t7d", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 19, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a6, b0, c0, d2, e1, f0]
		//record3: [a9, b1, c0, d1, e4, f3]
		//record4: [a12, b0, c0, d0, e2, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 19);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("mt", cols4);
		String [][] records_mt = new String[188][cols4.length];
		for(int i=0;i<188;i++)
		{
			records_mt[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mt[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mt", records_mt[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record183: [a183]
		//record184: [a184]
		//record185: [a185]
		//record186: [a186]
		//record187: [a187]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a134"};
		ArrayList<String[]> tableSelect4 = DBApp.select("mt", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a134]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a134"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vs98e", cols0);
		String [][] records_vs98e = new String[314][cols0.length];
		for(int i=0;i<314;i++)
		{
			records_vs98e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vs98e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vs98e", records_vs98e[i]);
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
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k8"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vs98e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		//record3: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 28);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1","k8","l5","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v16", cols1);
		String [][] records_v16 = new String[386][cols1.length];
		for(int i=0;i<386;i++)
		{
			records_v16[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v16[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v16", records_v16[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m12"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v16", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 29, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9]
		//record4: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 29);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3","m12","n9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a64","b0","c1","d0","e4","f4","g1","h0","i1","j4","k9","l4","m12","n8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("wlh2", cols2);
		String [][] records_wlh2 = new String[231][cols2.length];
		for(int i=0;i<231;i++)
		{
			records_wlh2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wlh2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wlh2", records_wlh2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("wlh2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 115, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 115);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x52", cols3);
		String [][] records_x52 = new String[409][cols3.length];
		for(int i=0;i<409;i++)
		{
			records_x52[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x52[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x52", records_x52[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9, m2]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10, m3]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11, m4]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0, m5]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x52", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 58, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 58);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("xhb", cols4);
		String [][] records_xhb = new String[446][cols4.length];
		for(int i=0;i<446;i++)
		{
			records_xhb[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xhb[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xhb", records_xhb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4, l0]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5, l1]
		String[] ConditionColumns4 = {"l"};
		String[] ConditionColumnsValues4 = {"l4"};
		ArrayList<String[]> tableSelect4 = DBApp.select("xhb", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 37, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 37);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ux5", cols0);
		String [][] records_ux5 = new String[494][cols0.length];
		for(int i=0;i<494;i++)
		{
			records_ux5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ux5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ux5", records_ux5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record489: [a489, b1, c0, d1, e4]
		//record490: [a490, b0, c1, d2, e0]
		//record491: [a491, b1, c2, d3, e1]
		//record492: [a492, b0, c0, d0, e2]
		//record493: [a493, b1, c1, d1, e3]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ux5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 247, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a2, b0, c2, d2, e2]
		//record2: [a4, b0, c1, d0, e4]
		//record3: [a6, b0, c0, d2, e1]
		//record4: [a8, b0, c2, d0, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 247);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vm", cols1);
		String [][] records_vm = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_vm[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vm[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vm", records_vm[i]);
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
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("vm", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 7);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s4f", cols2);
		String [][] records_s4f = new String[213][cols2.length];
		for(int i=0;i<213;i++)
		{
			records_s4f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s4f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s4f", records_s4f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("s4f", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 26, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 26);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("aq1b", cols3);
		String [][] records_aq1b = new String[159][cols3.length];
		for(int i=0;i<159;i++)
		{
			records_aq1b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_aq1b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("aq1b", records_aq1b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record154: [a154, b0, c1]
		//record155: [a155, b1, c2]
		//record156: [a156, b0, c0]
		//record157: [a157, b1, c1]
		//record158: [a158, b0, c2]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("aq1b", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 53, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 53);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("za07f", cols0);
		String [][] records_za07f = new String[380][cols0.length];
		for(int i=0;i<380;i++)
		{
			records_za07f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_za07f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("za07f", records_za07f[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record375: [a375, b1]
		//record376: [a376, b0]
		//record377: [a377, b1]
		//record378: [a378, b0]
		//record379: [a379, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("za07f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 190, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 190);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("pt", cols1);
		String [][] records_pt = new String[334][cols1.length];
		for(int i=0;i<334;i++)
		{
			records_pt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pt", records_pt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("pt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 42, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 42);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("x3t", cols2);
		String [][] records_x3t = new String[260][cols2.length];
		for(int i=0;i<260;i++)
		{
			records_x3t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x3t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x3t", records_x3t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x3t", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 87, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 87);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("t6rr8", cols3);
		String [][] records_t6rr8 = new String[268][cols3.length];
		for(int i=0;i<268;i++)
		{
			records_t6rr8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t6rr8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t6rr8", records_t6rr8[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record263: [a263]
		//record264: [a264]
		//record265: [a265]
		//record266: [a266]
		//record267: [a267]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a210"};
		ArrayList<String[]> tableSelect3 = DBApp.select("t6rr8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a210]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a210"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("y8", cols4);
		String [][] records_y8 = new String[379][cols4.length];
		for(int i=0;i<379;i++)
		{
			records_y8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_y8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("y8", records_y8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record374: [a374, b0, c2, d2]
		//record375: [a375, b1, c0, d3]
		//record376: [a376, b0, c1, d0]
		//record377: [a377, b1, c2, d1]
		//record378: [a378, b0, c0, d2]
		String[] ConditionColumns4 = {"d","a"};
		String[] ConditionColumnsValues4 = {"d0","a164"};
		ArrayList<String[]> tableSelect4 = DBApp.select("y8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a164, b0, c2, d0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a164","b0","c2","d0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("xc", cols0);
		String [][] records_xc = new String[270][cols0.length];
		for(int i=0;i<270;i++)
		{
			records_xc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xc", records_xc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record265: [a265, b1, c1]
		//record266: [a266, b0, c2]
		//record267: [a267, b1, c0]
		//record268: [a268, b0, c1]
		//record269: [a269, b1, c2]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xc", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 135, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 135);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("jq", cols1);
		String [][] records_jq = new String[280][cols1.length];
		for(int i=0;i<280;i++)
		{
			records_jq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jq", records_jq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record275: [a275, b1, c2, d3]
		//record276: [a276, b0, c0, d0]
		//record277: [a277, b1, c1, d1]
		//record278: [a278, b0, c2, d2]
		//record279: [a279, b1, c0, d3]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 70, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 70);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("ygx", cols2);
		String [][] records_ygx = new String[145][cols2.length];
		for(int i=0;i<145;i++)
		{
			records_ygx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ygx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ygx", records_ygx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record140: [a140, b0, c2]
		//record141: [a141, b1, c0]
		//record142: [a142, b0, c1]
		//record143: [a143, b1, c2]
		//record144: [a144, b0, c0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ygx", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 72, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 72);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("va6", cols3);
		String [][] records_va6 = new String[361][cols3.length];
		for(int i=0;i<361;i++)
		{
			records_va6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_va6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("va6", records_va6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9]
		String[] ConditionColumns3 = {"m","f","b","h","i","e"};
		String[] ConditionColumnsValues3 = {"m2","f3","b1","h5","i3","e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("va6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a93","b1","c0","d1","e3","f3","g2","h5","i3","j3","k5","l9","m2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("i52", cols4);
		String [][] records_i52 = new String[318][cols4.length];
		for(int i=0;i<318;i++)
		{
			records_i52[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_i52[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("i52", records_i52[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1]
		//record314: [a314, b0, c2, d2, e4, f2]
		//record315: [a315, b1, c0, d3, e0, f3]
		//record316: [a316, b0, c1, d0, e1, f4]
		//record317: [a317, b1, c2, d1, e2, f5]
		String[] ConditionColumns4 = {"f"};
		String[] ConditionColumnsValues4 = {"f1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("i52", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 53, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a13, b1, c1, d1, e3, f1]
		//record3: [a19, b1, c1, d3, e4, f1]
		//record4: [a25, b1, c1, d1, e0, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 53);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ax", cols0);
		String [][] records_ax = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			records_ax[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ax[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ax", records_ax[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		//record56: [a56, b0, c2, d0, e1, f2, g0]
		//record57: [a57, b1, c0, d1, e2, f3, g1]
		//record58: [a58, b0, c1, d2, e3, f4, g2]
		//record59: [a59, b1, c2, d3, e4, f5, g3]
		String[] ConditionColumns0 = {"c","g","f"};
		String[] ConditionColumnsValues0 = {"c1","g3","f1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ax", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rj", cols1);
		String [][] records_rj = new String[465][cols1.length];
		for(int i=0;i<465;i++)
		{
			records_rj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rj", records_rj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1, j0, k9, l4, m5, n12, o10]
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2, j1, k10, l5, m6, n13, o11]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0, l6, m7, n0, o12]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7, m8, n1, o13]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8, m9, n2, o14]
		String[] ConditionColumns1 = {"m","f","j","a"};
		String[] ConditionColumnsValues1 = {"m8","f1","j5","a385"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rj", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a385","b1","c1","d1","e0","f1","g0","h1","i7","j5","k0","l1","m8","n7","o10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cb", cols2);
		String [][] records_cb = new String[81][cols2.length];
		for(int i=0;i<81;i++)
		{
			records_cb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cb", records_cb[i]);
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
		String[] ConditionColumns2 = {"g","h"};
		String[] ConditionColumnsValues2 = {"g0","h6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("cb", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record1: [a70, b0, c1, d2, e0, f4, g0, h6, i7]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 2);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gl0", cols3);
		String [][] records_gl0 = new String[174][cols3.length];
		for(int i=0;i<174;i++)
		{
			records_gl0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gl0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gl0", records_gl0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5]
		String[] ConditionColumns3 = {"e","b"};
		String[] ConditionColumnsValues3 = {"e4","b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("gl0", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 17);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s60q", cols0);
		String [][] records_s60q = new String[319][cols0.length];
		for(int i=0;i<319;i++)
		{
			records_s60q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s60q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s60q", records_s60q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2, n6, o14]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7, o0]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s60q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 32, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 32);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5","n5","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9","n7","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1","l9","m6","n3","o0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("nh8xt", cols1);
		String [][] records_nh8xt = new String[442][cols1.length];
		for(int i=0;i<442;i++)
		{
			records_nh8xt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nh8xt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nh8xt", records_nh8xt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("nh8xt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 147, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 147);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zb2", cols2);
		String [][] records_zb2 = new String[275][cols2.length];
		for(int i=0;i<275;i++)
		{
			records_zb2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zb2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zb2", records_zb2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1]
		String[] ConditionColumns2 = {"h","a","f","j"};
		String[] ConditionColumnsValues2 = {"h2","a234","f0","j4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("zb2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a234","b0","c0","d2","e4","f0","g3","h2","i0","j4","k3","l6","m0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cnt", cols3);
		String [][] records_cnt = new String[78][cols3.length];
		for(int i=0;i<78;i++)
		{
			records_cnt[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_cnt[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("cnt", records_cnt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0]
		String[] ConditionColumns3 = {"h"};
		String[] ConditionColumnsValues3 = {"h3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("cnt", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 10, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 10);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ny8e3", cols0);
		String [][] records_ny8e3 = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			records_ny8e3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ny8e3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ny8e3", records_ny8e3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3, l7]
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4, l8]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ny8e3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		//record4: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k0h", cols1);
		String [][] records_k0h = new String[222][cols1.length];
		for(int i=0;i<222;i++)
		{
			records_k0h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k0h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k0h", records_k0h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k0h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 37);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("y2", cols2);
		String [][] records_y2 = new String[111][cols2.length];
		for(int i=0;i<111;i++)
		{
			records_y2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y2", records_y2[i]);
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
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("y2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 55, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 55);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w2s", cols3);
		String [][] records_w2s = new String[129][cols3.length];
		for(int i=0;i<129;i++)
		{
			records_w2s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w2s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w2s", records_w2s[i]);
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
		String[] ConditionColumns3 = {"a","g"};
		String[] ConditionColumnsValues3 = {"a20","g6"};
		ArrayList<String[]> tableSelect3 = DBApp.select("w2s", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("h674", cols0);
		String [][] records_h674 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_h674[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h674[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h674", records_h674[i]);
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
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("h674", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 5);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("o0", cols0);
		String [][] records_o0 = new String[250][cols0.length];
		for(int i=0;i<250;i++)
		{
			records_o0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0", records_o0[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record245: [a245, b1]
		//record246: [a246, b0]
		//record247: [a247, b1]
		//record248: [a248, b0]
		//record249: [a249, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 125, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 125);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j80", cols0);
		String [][] records_j80 = new String[446][cols0.length];
		for(int i=0;i<446;i++)
		{
			records_j80[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j80[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j80", records_j80[i]);
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
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j7"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j80", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 44, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 44);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("s40", cols0);
		String [][] records_s40 = new String[246][cols0.length];
		for(int i=0;i<246;i++)
		{
			records_s40[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s40[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s40", records_s40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record241: [a241, b1, c1, d1, e1, f1]
		//record242: [a242, b0, c2, d2, e2, f2]
		//record243: [a243, b1, c0, d3, e3, f3]
		//record244: [a244, b0, c1, d0, e4, f4]
		//record245: [a245, b1, c2, d1, e0, f5]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a108"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s40", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a108, b0, c0, d0, e3, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a108","b0","c0","d0","e3","f0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i659", cols0);
		String [][] records_i659 = new String[351][cols0.length];
		for(int i=0;i<351;i++)
		{
			records_i659[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i659[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i659", records_i659[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2]
		String[] ConditionColumns0 = {"d","i"};
		String[] ConditionColumnsValues0 = {"d3","i0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i659", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record1: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record2: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3]
		//record3: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3]
		//record4: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3","k8","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9","k0","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a135","b1","c0","d3","e0","f3","g2","h7","i0","j5","k3","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a171","b1","c0","d3","e1","f3","g3","h3","i0","j1","k6","l3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m9", cols0);
		String [][] records_m9 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			records_m9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m9", records_m9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("m9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 14);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("dv2", cols1);
		String [][] records_dv2 = new String[218][cols1.length];
		for(int i=0;i<218;i++)
		{
			records_dv2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dv2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dv2", records_dv2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record213: [a213, b1, c0, d1, e3, f3]
		//record214: [a214, b0, c1, d2, e4, f4]
		//record215: [a215, b1, c2, d3, e0, f5]
		//record216: [a216, b0, c0, d0, e1, f0]
		//record217: [a217, b1, c1, d1, e2, f1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("dv2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 73, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a6, b0, c0, d2, e1, f0]
		//record3: [a9, b1, c0, d1, e4, f3]
		//record4: [a12, b0, c0, d0, e2, f0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 73);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("eg", cols2);
		String [][] records_eg = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			records_eg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eg", records_eg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6]
		String[] ConditionColumns2 = {"b","i","e"};
		String[] ConditionColumnsValues2 = {"b1","i8","e2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("eg", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("f78gt", cols3);
		String [][] records_f78gt = new String[492][cols3.length];
		for(int i=0;i<492;i++)
		{
			records_f78gt[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f78gt[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f78gt", records_f78gt[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record487: [a487]
		//record488: [a488]
		//record489: [a489]
		//record490: [a490]
		//record491: [a491]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a202"};
		ArrayList<String[]> tableSelect3 = DBApp.select("f78gt", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a202]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a202"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("n8gz", cols0);
		String [][] records_n8gz = new String[261][cols0.length];
		for(int i=0;i<261;i++)
		{
			records_n8gz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n8gz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n8gz", records_n8gz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record256: [a256, b0, c1]
		//record257: [a257, b1, c2]
		//record258: [a258, b0, c0]
		//record259: [a259, b1, c1]
		//record260: [a260, b0, c2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("n8gz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 87, records:
		//record0: [a2, b0, c2]
		//record1: [a5, b1, c2]
		//record2: [a8, b0, c2]
		//record3: [a11, b1, c2]
		//record4: [a14, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 87);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("dwf", cols1);
		String [][] records_dwf = new String[438][cols1.length];
		for(int i=0;i<438;i++)
		{
			records_dwf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dwf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dwf", records_dwf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record433: [a433, b1, c1]
		//record434: [a434, b0, c2]
		//record435: [a435, b1, c0]
		//record436: [a436, b0, c1]
		//record437: [a437, b1, c2]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a249"};
		ArrayList<String[]> tableSelect1 = DBApp.select("dwf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a249, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a249","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("u80", cols2);
		String [][] records_u80 = new String[491][cols2.length];
		for(int i=0;i<491;i++)
		{
			records_u80[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u80[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u80", records_u80[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record486: [a486, b0, c0]
		//record487: [a487, b1, c1]
		//record488: [a488, b0, c2]
		//record489: [a489, b1, c0]
		//record490: [a490, b0, c1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("u80", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 246, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 246);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o9", cols0);
		String [][] records_o9 = new String[308][cols0.length];
		for(int i=0;i<308;i++)
		{
			records_o9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o9", records_o9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7]
		String[] ConditionColumns0 = {"f","g","c"};
		String[] ConditionColumnsValues0 = {"f0","g5","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record1: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4]
		//record2: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6]
		//record3: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8]
		//record4: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5","h0","i6","j6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a138","b0","c0","d2","e3","f0","g5","h2","i3","j8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a180","b0","c0","d0","e0","f0","g5","h4","i0","j0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ma02f", cols0);
		String [][] records_ma02f = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			records_ma02f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ma02f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ma02f", records_ma02f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		String[] ConditionColumns0 = {"a","l"};
		String[] ConditionColumnsValues0 = {"a8","l8"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ma02f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("mk", cols0);
		String [][] records_mk = new String[198][cols0.length];
		for(int i=0;i<198;i++)
		{
			records_mk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mk", records_mk[i]);
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
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("jn2", cols1);
		String [][] records_jn2 = new String[361][cols1.length];
		for(int i=0;i<361;i++)
		{
			records_jn2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jn2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jn2", records_jn2[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record356: [a356, b0]
		//record357: [a357, b1]
		//record358: [a358, b0]
		//record359: [a359, b1]
		//record360: [a360, b0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("jn2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 180, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 180);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v0", cols2);
		String [][] records_v0 = new String[152][cols2.length];
		for(int i=0;i<152;i++)
		{
			records_v0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v0", records_v0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("v0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 19, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 19);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("qu8", cols3);
		String [][] records_qu8 = new String[375][cols3.length];
		for(int i=0;i<375;i++)
		{
			records_qu8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qu8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qu8", records_qu8[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record370: [a370, b0]
		//record371: [a371, b1]
		//record372: [a372, b0]
		//record373: [a373, b1]
		//record374: [a374, b0]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a177"};
		ArrayList<String[]> tableSelect3 = DBApp.select("qu8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a177, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a177","b1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("t0", cols4);
		String [][] records_t0 = new String[223][cols4.length];
		for(int i=0;i<223;i++)
		{
			records_t0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_t0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("t0", records_t0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2]
		String[] ConditionColumns4 = {"d","a","f","h","j"};
		String[] ConditionColumnsValues4 = {"d0","a112","f4","h0","j2"};
		ArrayList<String[]> tableSelect4 = DBApp.select("t0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a112","b0","c1","d0","e2","f4","g0","h0","i4","j2","k2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("mk", cols0);
		String [][] records_mk = new String[440][cols0.length];
		for(int i=0;i<440;i++)
		{
			records_mk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mk", records_mk[i]);
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
		String[] ConditionColumns0 = {"c","j","e"};
		String[] ConditionColumnsValues0 = {"c0","j9","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record1: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		//record2: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9]
		//record3: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9]
		//record4: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 15);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5","i6","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a129","b1","c0","d1","e4","f3","g3","h1","i3","j9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s91", cols1);
		String [][] records_s91 = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			records_s91[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s91[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s91", records_s91[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("s91", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("hy3", cols0);
		String [][] records_hy3 = new String[216][cols0.length];
		for(int i=0;i<216;i++)
		{
			records_hy3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hy3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hy3", records_hy3[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record211: [a211, b1]
		//record212: [a212, b0]
		//record213: [a213, b1]
		//record214: [a214, b0]
		//record215: [a215, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("hy3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 108, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 108);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r4i7", cols1);
		String [][] records_r4i7 = new String[242][cols1.length];
		for(int i=0;i<242;i++)
		{
			records_r4i7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r4i7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r4i7", records_r4i7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m9"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r4i7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 18, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record1: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record3: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9]
		//record4: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 18);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0","i3","j8","k4","l0","m9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5","h5","i7","j1","k6","l1","m9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("n3a", cols2);
		String [][] records_n3a = new String[384][cols2.length];
		for(int i=0;i<384;i++)
		{
			records_n3a[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n3a[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n3a", records_n3a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record379: [a379, b1, c1, d3]
		//record380: [a380, b0, c2, d0]
		//record381: [a381, b1, c0, d1]
		//record382: [a382, b0, c1, d2]
		//record383: [a383, b1, c2, d3]
		String[] ConditionColumns2 = {"d","b"};
		String[] ConditionColumnsValues2 = {"d3","b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("n3a", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 96, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 96);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("d2", cols3);
		String [][] records_d2 = new String[167][cols3.length];
		for(int i=0;i<167;i++)
		{
			records_d2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_d2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("d2", records_d2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record162: [a162, b0, c0]
		//record163: [a163, b1, c1]
		//record164: [a164, b0, c2]
		//record165: [a165, b1, c0]
		//record166: [a166, b0, c1]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a103"};
		ArrayList<String[]> tableSelect3 = DBApp.select("d2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a103, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a103","b1","c1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l7116", cols0);
		String [][] records_l7116 = new String[338][cols0.length];
		for(int i=0;i<338;i++)
		{
			records_l7116[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l7116[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l7116", records_l7116[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l7116", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 38, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 38);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("o8", cols1);
		String [][] records_o8 = new String[240][cols1.length];
		for(int i=0;i<240;i++)
		{
			records_o8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o8", records_o8[i]);
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
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("o8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 27);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ltc", cols2);
		String [][] records_ltc = new String[124][cols2.length];
		for(int i=0;i<124;i++)
		{
			records_ltc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ltc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ltc", records_ltc[i]);
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
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ltc", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 21, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 21);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xds", cols3);
		String [][] records_xds = new String[419][cols3.length];
		for(int i=0;i<419;i++)
		{
			records_xds[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xds[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xds", records_xds[i]);
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
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g4"};
		ArrayList<String[]> tableSelect3 = DBApp.select("xds", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 60, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 60);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("z15", cols0);
		String [][] records_z15 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			records_z15[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z15[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z15", records_z15[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record63: [a63, b1, c0]
		//record64: [a64, b0, c1]
		//record65: [a65, b1, c2]
		//record66: [a66, b0, c0]
		//record67: [a67, b1, c1]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("z15", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 23, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 23);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("egd", cols1);
		String [][] records_egd = new String[471][cols1.length];
		for(int i=0;i<471;i++)
		{
			records_egd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_egd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("egd", records_egd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11, n4, o1]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12, n5, o2]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6, o3]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5]
		String[] ConditionColumns1 = {"f","j"};
		String[] ConditionColumnsValues1 = {"f4","j4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("egd", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 16, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record2: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4]
		//record3: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10, o4]
		//record4: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 16);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8","n6","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a64","b0","c1","d0","e4","f4","g1","h0","i1","j4","k9","l4","m12","n8","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a94","b0","c1","d2","e4","f4","g3","h6","i4","j4","k6","l10","m3","n10","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a124","b0","c1","d0","e4","f4","g5","h4","i7","j4","k3","l4","m7","n12","o4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ky", cols2);
		String [][] records_ky = new String[328][cols2.length];
		for(int i=0;i<328;i++)
		{
			records_ky[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ky[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ky", records_ky[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1, o8]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3, o10]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4, o11]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ky", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 109, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 109);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5","n5","o5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("nfr", cols3);
		String [][] records_nfr = new String[286][cols3.length];
		for(int i=0;i<286;i++)
		{
			records_nfr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_nfr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("nfr", records_nfr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("nfr", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 143, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 143);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ph0", cols0);
		String [][] records_ph0 = new String[333][cols0.length];
		for(int i=0;i<333;i++)
		{
			records_ph0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ph0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ph0", records_ph0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record328: [a328, b0, c1]
		//record329: [a329, b1, c2]
		//record330: [a330, b0, c0]
		//record331: [a331, b1, c1]
		//record332: [a332, b0, c2]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a71"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ph0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a71, b1, c2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a71","b1","c2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hft", cols1);
		String [][] records_hft = new String[188][cols1.length];
		for(int i=0;i<188;i++)
		{
			records_hft[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hft[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hft", records_hft[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0]
		String[] ConditionColumns1 = {"g","k"};
		String[] ConditionColumnsValues1 = {"g1","k4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("hft", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record1: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4]
		//record2: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a92","b0","c2","d0","e2","f2","g1","h4","i2","j2","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a169","b1","c1","d1","e4","f1","g1","h1","i7","j9","k4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mq1", cols0);
		String [][] records_mq1 = new String[427][cols0.length];
		for(int i=0;i<427;i++)
		{
			records_mq1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mq1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mq1", records_mq1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2, o2]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3, m7, n3, o3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8, n4, o4]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9, n5, o5]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6, o6]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("mq1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 54, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 54);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0","o0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2","o1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("d36", cols1);
		String [][] records_d36 = new String[419][cols1.length];
		for(int i=0;i<419;i++)
		{
			records_d36[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d36[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d36", records_d36[i]);
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
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("d36", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 47, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 47);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("lv56", cols2);
		String [][] records_lv56 = new String[251][cols2.length];
		for(int i=0;i<251;i++)
		{
			records_lv56[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lv56[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lv56", records_lv56[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1]
		//record247: [a247, b1, c1, d3, e2]
		//record248: [a248, b0, c2, d0, e3]
		//record249: [a249, b1, c0, d1, e4]
		//record250: [a250, b0, c1, d2, e0]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("lv56", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 50, records:
		//record0: [a3, b1, c0, d3, e3]
		//record1: [a8, b0, c2, d0, e3]
		//record2: [a13, b1, c1, d1, e3]
		//record3: [a18, b0, c0, d2, e3]
		//record4: [a23, b1, c2, d3, e3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 50);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xd11", cols3);
		String [][] records_xd11 = new String[11][cols3.length];
		for(int i=0;i<11;i++)
		{
			records_xd11[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xd11[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xd11", records_xd11[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("xd11", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 3, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a9, b1, c0, d1, e4, f3, g2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 3);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("zt", cols0);
		String [][] records_zt = new String[368][cols0.length];
		for(int i=0;i<368;i++)
		{
			records_zt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zt", records_zt[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record363: [a363]
		//record364: [a364]
		//record365: [a365]
		//record366: [a366]
		//record367: [a367]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a44"};
		ArrayList<String[]> tableSelect0 = DBApp.select("zt", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a44]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("av2", cols1);
		String [][] records_av2 = new String[393][cols1.length];
		for(int i=0;i<393;i++)
		{
			records_av2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_av2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("av2", records_av2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4]
		//record389: [a389, b1, c2, d1, e4, f5]
		//record390: [a390, b0, c0, d2, e0, f0]
		//record391: [a391, b1, c1, d3, e1, f1]
		//record392: [a392, b0, c2, d0, e2, f2]
		String[] ConditionColumns1 = {"a","c","f"};
		String[] ConditionColumnsValues1 = {"a172","c1","f4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("av2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a172, b0, c1, d0, e2, f4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a172","b0","c1","d0","e2","f4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r82", cols2);
		String [][] records_r82 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			records_r82[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r82[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r82", records_r82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("r82", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 6);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("qjmk", cols3);
		String [][] records_qjmk = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			records_qjmk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qjmk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qjmk", records_qjmk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2]
		//record43: [a43, b1, c1, d3, e3]
		//record44: [a44, b0, c2, d0, e4]
		//record45: [a45, b1, c0, d1, e0]
		//record46: [a46, b0, c1, d2, e1]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("qjmk", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 11, records:
		//record0: [a3, b1, c0, d3, e3]
		//record1: [a7, b1, c1, d3, e2]
		//record2: [a11, b1, c2, d3, e1]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a19, b1, c1, d3, e4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 11);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o47j0", cols0);
		String [][] records_o47j0 = new String[219][cols0.length];
		for(int i=0;i<219;i++)
		{
			records_o47j0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o47j0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o47j0", records_o47j0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o47j0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 55, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 55);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z758c", cols1);
		String [][] records_z758c = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_z758c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z758c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z758c", records_z758c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		String[] ConditionColumns1 = {"c","a","k","b","g"};
		String[] ConditionColumnsValues1 = {"c0","a3","k3","b1","g3"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z758c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("a3", cols2);
		String [][] records_a3 = new String[268][cols2.length];
		for(int i=0;i<268;i++)
		{
			records_a3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a3", records_a3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record263: [a263, b1, c2, d3, e3]
		//record264: [a264, b0, c0, d0, e4]
		//record265: [a265, b1, c1, d1, e0]
		//record266: [a266, b0, c2, d2, e1]
		//record267: [a267, b1, c0, d3, e2]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 134, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 134);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("x7k", cols3);
		String [][] records_x7k = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			records_x7k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x7k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x7k", records_x7k[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record32: [a32]
		//record33: [a33]
		//record34: [a34]
		//record35: [a35]
		//record36: [a36]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a17"};
		ArrayList<String[]> tableSelect3 = DBApp.select("x7k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a17]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("iqf", cols0);
		String [][] records_iqf = new String[323][cols0.length];
		for(int i=0;i<323;i++)
		{
			records_iqf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_iqf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("iqf", records_iqf[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record318: [a318]
		//record319: [a319]
		//record320: [a320]
		//record321: [a321]
		//record322: [a322]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a30"};
		ArrayList<String[]> tableSelect0 = DBApp.select("iqf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a30]
		int pageCount0 = (int)Math.ceil(323.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("iqf");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("ev3", cols1);
		String [][] records_ev3 = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			records_ev3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ev3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ev3", records_ev3[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record29: [a29, b1]
		//record30: [a30, b0]
		//record31: [a31, b1]
		//record32: [a32, b0]
		//record33: [a33, b1]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("ev3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 17, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount1 = (int)Math.ceil(34.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("ev3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("oj6", cols0);
		String [][] records_oj6 = new String[271][cols0.length];
		for(int i=0;i<271;i++)
		{
			records_oj6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oj6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oj6", records_oj6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0]
		String[] ConditionColumns0 = {"a","g"};
		String[] ConditionColumnsValues0 = {"a252","g0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("oj6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2]
		int pageCount0 = (int)Math.ceil(271.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("oj6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w7", cols1);
		String [][] records_w7 = new String[425][cols1.length];
		for(int i=0;i<425;i++)
		{
			records_w7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w7", records_w7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0, m4, n0]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1, m5, n1]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3, m7, n3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8, n4]
		String[] ConditionColumns1 = {"i","l","c"};
		String[] ConditionColumnsValues1 = {"i1","l1","c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 12, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record1: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record2: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1, m8, n3]
		//record3: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11]
		//record4: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5]
		int pageCount1 = (int)Math.ceil(425.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+12));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("cu21", cols2);
		String [][] records_cu21 = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			records_cu21[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cu21[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cu21", records_cu21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0]
		//record16: [a16, b0, c1, d0, e1]
		//record17: [a17, b1, c2, d1, e2]
		//record18: [a18, b0, c0, d2, e3]
		//record19: [a19, b1, c1, d3, e4]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a14"};
		ArrayList<String[]> tableSelect2 = DBApp.select("cu21", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a14, b0, c2, d2, e4]
		int pageCount2 = (int)Math.ceil(20.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("cu21");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y6y", cols3);
		String [][] records_y6y = new String[200][cols3.length];
		for(int i=0;i<200;i++)
		{
			records_y6y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y6y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y6y", records_y6y[i]);
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
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("y6y", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 67, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		int pageCount3 = (int)Math.ceil(200.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("y6y");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+67));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sn6e", cols0);
		String [][] records_sn6e = new String[242][cols0.length];
		for(int i=0;i<242;i++)
		{
			records_sn6e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sn6e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sn6e", records_sn6e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6, n2]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7, n3]
		String[] ConditionColumns0 = {"i","d","g","e"};
		String[] ConditionColumnsValues0 = {"i7","d1","g1","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("sn6e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1]
		int pageCount0 = (int)Math.ceil(242.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("sn6e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gu7oo", cols1);
		String [][] records_gu7oo = new String[430][cols1.length];
		for(int i=0;i<430;i++)
		{
			records_gu7oo[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gu7oo[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gu7oo", records_gu7oo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("gu7oo", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 143, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5]
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
		String selectTrace1 = DBApp.getLastTrace("gu7oo");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+143));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ifp41", cols2);
		String [][] records_ifp41 = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			records_ifp41[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ifp41[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ifp41", records_ifp41[i]);
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
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h2"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ifp41", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 11, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		int pageCount2 = (int)Math.ceil(87.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ifp41");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+11));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("r86", cols3);
		String [][] records_r86 = new String[330][cols3.length];
		for(int i=0;i<330;i++)
		{
			records_r86[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r86[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r86", records_r86[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record325: [a325, b1, c1, d1, e0, f1]
		//record326: [a326, b0, c2, d2, e1, f2]
		//record327: [a327, b1, c0, d3, e2, f3]
		//record328: [a328, b0, c1, d0, e3, f4]
		//record329: [a329, b1, c2, d1, e4, f5]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a303"};
		ArrayList<String[]> tableSelect3 = DBApp.select("r86", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a303, b1, c0, d3, e3, f3]
		int pageCount3 = (int)Math.ceil(330.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("r86");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("lm6", cols0);
		String [][] records_lm6 = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			records_lm6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lm6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lm6", records_lm6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("lm6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 16, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		int pageCount0 = (int)Math.ceil(83.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("lm6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+16));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a1d", cols1);
		String [][] records_a1d = new String[260][cols1.length];
		for(int i=0;i<260;i++)
		{
			records_a1d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a1d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a1d", records_a1d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7]
		String[] ConditionColumns1 = {"l","e"};
		String[] ConditionColumnsValues1 = {"l6","e1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("a1d", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record1: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10]
		//record2: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0]
		//record3: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4]
		//record4: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8]
		int pageCount1 = (int)Math.ceil(260.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("a1d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qb750", cols2);
		String [][] records_qb750 = new String[292][cols2.length];
		for(int i=0;i<292;i++)
		{
			records_qb750[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qb750[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qb750", records_qb750[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3]
		String[] ConditionColumns2 = {"f","i","h"};
		String[] ConditionColumnsValues2 = {"f2","i2","h4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qb750", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record1: [a92, b0, c2, d0, e2, f2, g1, h4, i2]
		//record2: [a164, b0, c2, d0, e4, f2, g3, h4, i2]
		//record3: [a236, b0, c2, d0, e1, f2, g5, h4, i2]
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
		String selectTrace2 = DBApp.getLastTrace("qb750");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+4));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("s9p26", cols0);
		String [][] records_s9p26 = new String[216][cols0.length];
		for(int i=0;i<216;i++)
		{
			records_s9p26[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s9p26[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s9p26", records_s9p26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1]
		//record212: [a212, b0, c2, d0, e2]
		//record213: [a213, b1, c0, d1, e3]
		//record214: [a214, b0, c1, d2, e4]
		//record215: [a215, b1, c2, d3, e0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("s9p26", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 72, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a4, b0, c1, d0, e4]
		//record2: [a7, b1, c1, d3, e2]
		//record3: [a10, b0, c1, d2, e0]
		//record4: [a13, b1, c1, d1, e3]
		int pageCount0 = (int)Math.ceil(216.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("s9p26");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h1l", cols1);
		String [][] records_h1l = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			records_h1l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h1l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h1l", records_h1l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a71"};
		ArrayList<String[]> tableSelect1 = DBApp.select("h1l", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a71, b1, c2, d3, e1, f5, g1, h7, i8]
		int pageCount1 = (int)Math.ceil(98.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("h1l");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("x3i", cols2);
		String [][] records_x3i = new String[207][cols2.length];
		for(int i=0;i<207;i++)
		{
			records_x3i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x3i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x3i", records_x3i[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record202: [a202]
		//record203: [a203]
		//record204: [a204]
		//record205: [a205]
		//record206: [a206]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a48"};
		ArrayList<String[]> tableSelect2 = DBApp.select("x3i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a48]
		int pageCount2 = (int)Math.ceil(207.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("x3i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p1z9h", cols3);
		String [][] records_p1z9h = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			records_p1z9h[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p1z9h[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p1z9h", records_p1z9h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9]
		String[] ConditionColumns3 = {"j","f","c"};
		String[] ConditionColumnsValues3 = {"j1","f5","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("p1z9h", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record1: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
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
		String selectTrace3 = DBApp.getLastTrace("p1z9h");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u46m0", cols4);
		String [][] records_u46m0 = new String[308][cols4.length];
		for(int i=0;i<308;i++)
		{
			records_u46m0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u46m0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u46m0", records_u46m0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b0"};
		ArrayList<String[]> tableSelect4 = DBApp.select("u46m0", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 154, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0]
		int pageCount4 = (int)Math.ceil(308.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("u46m0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+154));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("r8w22", cols0);
		String [][] records_r8w22 = new String[248][cols0.length];
		for(int i=0;i<248;i++)
		{
			records_r8w22[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r8w22[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r8w22", records_r8w22[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record243: [a243, b1]
		//record244: [a244, b0]
		//record245: [a245, b1]
		//record246: [a246, b0]
		//record247: [a247, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r8w22", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 124, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		int pageCount0 = (int)Math.ceil(248.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("r8w22");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+124));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k531", cols1);
		String [][] records_k531 = new String[348][cols1.length];
		for(int i=0;i<348;i++)
		{
			records_k531[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k531[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k531", records_k531[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5, n7, o13]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("k531", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 58, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		int pageCount1 = (int)Math.ceil(348.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("k531");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+58));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("o4pz", cols2);
		String [][] records_o4pz = new String[389][cols2.length];
		for(int i=0;i<389;i++)
		{
			records_o4pz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o4pz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o4pz", records_o4pz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1]
		String[] ConditionColumns2 = {"e","f","g"};
		String[] ConditionColumnsValues2 = {"e2","f3","g5"};
		ArrayList<String[]> tableSelect2 = DBApp.select("o4pz", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a117, b1, c0, d1, e2, f3, g5, h5, i0]
		//record1: [a327, b1, c0, d3, e2, f3, g5, h7, i3]
		int pageCount2 = (int)Math.ceil(389.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("o4pz");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+2));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mq1u3", cols3);
		String [][] records_mq1u3 = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			records_mq1u3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_mq1u3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("mq1u3", records_mq1u3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record80: [a80, b0, c2, d0, e0, f2, g3]
		//record81: [a81, b1, c0, d1, e1, f3, g4]
		//record82: [a82, b0, c1, d2, e2, f4, g5]
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("mq1u3", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a23, b1, c2, d3, e3, f5, g2]
		int pageCount3 = (int)Math.ceil(85.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("mq1u3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l03", cols0);
		String [][] records_l03 = new String[198][cols0.length];
		for(int i=0;i<198;i++)
		{
			records_l03[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l03[i][j] = cols0[j]+((i%(j+1)));
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
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2, n1]
		String[] ConditionColumns0 = {"n"};
		String[] ConditionColumnsValues0 = {"n3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("l03", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7, n3]
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
		String selectTrace0 = DBApp.getLastTrace("l03");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+14));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w8g", cols1);
		String [][] records_w8g = new String[233][cols1.length];
		for(int i=0;i<233;i++)
		{
			records_w8g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w8g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w8g", records_w8g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8]
		String[] ConditionColumns1 = {"e","k","i","l","n","m"};
		String[] ConditionColumnsValues1 = {"e4","k2","i8","l2","n8","m4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w8g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8]
		int pageCount1 = (int)Math.ceil(233.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w8g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("c9w", cols2);
		String [][] records_c9w = new String[237][cols2.length];
		for(int i=0;i<237;i++)
		{
			records_c9w[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c9w[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c9w", records_c9w[i]);
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
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("c9w", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 34, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		int pageCount2 = (int)Math.ceil(237.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("c9w");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+34));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("w9", cols3);
		String [][] records_w9 = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			records_w9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w9", records_w9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record44: [a44, b0, c2, d0, e4, f2]
		//record45: [a45, b1, c0, d1, e0, f3]
		//record46: [a46, b0, c1, d2, e1, f4]
		//record47: [a47, b1, c2, d3, e2, f5]
		//record48: [a48, b0, c0, d0, e3, f0]
		String[] ConditionColumns3 = {"c","b"};
		String[] ConditionColumnsValues3 = {"c0","b1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("w9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 8, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a15, b1, c0, d3, e0, f3]
		//record3: [a21, b1, c0, d1, e1, f3]
		//record4: [a27, b1, c0, d3, e2, f3]
		int pageCount3 = (int)Math.ceil(49.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("w9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+8));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qrj3", cols0);
		String [][] records_qrj3 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			records_qrj3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qrj3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qrj3", records_qrj3[i]);
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
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("qrj3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		int pageCount0 = (int)Math.ceil(86.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("qrj3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+9));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b1916", cols1);
		String [][] records_b1916 = new String[403][cols1.length];
		for(int i=0;i<403;i++)
		{
			records_b1916[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1916[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1916", records_b1916[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b1916", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 201, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		int pageCount1 = (int)Math.ceil(403.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("b1916");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+201));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("mhn", cols2);
		String [][] records_mhn = new String[482][cols2.length];
		for(int i=0;i<482;i++)
		{
			records_mhn[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mhn[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mhn", records_mhn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1]
		String[] ConditionColumns2 = {"g","k","c","e"};
		String[] ConditionColumnsValues2 = {"g5","k10","c2","e1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("mhn", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11]
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
		String selectTrace2 = DBApp.getLastTrace("mhn");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y1", cols0);
		String [][] records_y1 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			records_y1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y1", records_y1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("y1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
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
		String selectTrace0 = DBApp.getLastTrace("y1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+28));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mro8m", cols1);
		String [][] records_mro8m = new String[165][cols1.length];
		for(int i=0;i<165;i++)
		{
			records_mro8m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mro8m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mro8m", records_mro8m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8, o12]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14]
		String[] ConditionColumns1 = {"b","m","c","o"};
		String[] ConditionColumnsValues1 = {"b0","m3","c0","o0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("mro8m", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8, o0]
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
		String selectTrace1 = DBApp.getLastTrace("mro8m");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z66", cols2);
		String [][] records_z66 = new String[432][cols2.length];
		for(int i=0;i<432;i++)
		{
			records_z66[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z66[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z66", records_z66[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("z66", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 144, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4]
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
		String selectTrace2 = DBApp.getLastTrace("z66");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+144));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("dzg", cols3);
		String [][] records_dzg = new String[369][cols3.length];
		for(int i=0;i<369;i++)
		{
			records_dzg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_dzg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("dzg", records_dzg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("dzg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 74, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		int pageCount3 = (int)Math.ceil(369.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("dzg");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+74));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("g7", cols4);
		String [][] records_g7 = new String[220][cols4.length];
		for(int i=0;i<220;i++)
		{
			records_g7[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g7[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g7", records_g7[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record215: [a215]
		//record216: [a216]
		//record217: [a217]
		//record218: [a218]
		//record219: [a219]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a22"};
		ArrayList<String[]> tableSelect4 = DBApp.select("g7", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a22]
		int pageCount4 = (int)Math.ceil(220.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("g7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p4op", cols0);
		String [][] records_p4op = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			records_p4op[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p4op[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p4op", records_p4op[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		String[] ConditionColumns0 = {"f","k","a","d","b"};
		String[] ConditionColumnsValues0 = {"f1","k1","a1","d1","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("p4op", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		int pageCount0 = (int)Math.ceil(4.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("p4op");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bu51", cols1);
		String [][] records_bu51 = new String[374][cols1.length];
		for(int i=0;i<374;i++)
		{
			records_bu51[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bu51[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bu51", records_bu51[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5]
		String[] ConditionColumns1 = {"b","h","d"};
		String[] ConditionColumnsValues1 = {"b0","h2","d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("bu51", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 47, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2]
		int pageCount1 = (int)Math.ceil(374.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("bu51");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+47));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b7", cols0);
		String [][] records_b7 = new String[148][cols0.length];
		for(int i=0;i<148;i++)
		{
			records_b7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b7", records_b7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k10"};
		ArrayList<String[]> tableSelect0 = DBApp.select("b7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10]
		int pageCount0 = (int)Math.ceil(148.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("b7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("coa", cols0);
		String [][] records_coa = new String[286][cols0.length];
		for(int i=0;i<286;i++)
		{
			records_coa[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_coa[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("coa", records_coa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5, m8, n1, o11]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9, n2, o12]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3, o13]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4, o14]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5, o0]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("coa", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 24, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record3: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
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
		String selectTrace0 = DBApp.getLastTrace("coa");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+24));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("a74e", cols0);
		String [][] records_a74e = new String[493][cols0.length];
		for(int i=0;i<493;i++)
		{
			records_a74e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a74e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a74e", records_a74e[i]);
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
		String[] ConditionColumnsValues0 = {"a398"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a74e", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a398, b0, c2]
		int pageCount0 = (int)Math.ceil(493.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a74e");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i9", cols1);
		String [][] records_i9 = new String[345][cols1.length];
		for(int i=0;i<345;i++)
		{
			records_i9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k6"};
		ArrayList<String[]> tableSelect1 = DBApp.select("i9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 31, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record4: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11]
		int pageCount1 = (int)Math.ceil(345.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("i9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jr", cols2);
		String [][] records_jr = new String[290][cols2.length];
		for(int i=0;i<290;i++)
		{
			records_jr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jr", records_jr[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record285: [a285, b1]
		//record286: [a286, b0]
		//record287: [a287, b1]
		//record288: [a288, b0]
		//record289: [a289, b1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("jr", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 145, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount2 = (int)Math.ceil(290.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("jr");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+145));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("w0k", cols0);
		String [][] records_w0k = new String[289][cols0.length];
		for(int i=0;i<289;i++)
		{
			records_w0k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w0k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w0k", records_w0k[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record284: [a284, b0]
		//record285: [a285, b1]
		//record286: [a286, b0]
		//record287: [a287, b1]
		//record288: [a288, b0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a149"};
		ArrayList<String[]> tableSelect0 = DBApp.select("w0k", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a149, b1]
		int pageCount0 = (int)Math.ceil(289.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("w0k");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l39a", cols1);
		String [][] records_l39a = new String[310][cols1.length];
		for(int i=0;i<310;i++)
		{
			records_l39a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l39a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l39a", records_l39a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("l39a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 78, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		int pageCount1 = (int)Math.ceil(310.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("l39a");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+78));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ai24x", cols2);
		String [][] records_ai24x = new String[185][cols2.length];
		for(int i=0;i<185;i++)
		{
			records_ai24x[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ai24x[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ai24x", records_ai24x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record180: [a180, b0, c0, d0, e0, f0, g5]
		//record181: [a181, b1, c1, d1, e1, f1, g6]
		//record182: [a182, b0, c2, d2, e2, f2, g0]
		//record183: [a183, b1, c0, d3, e3, f3, g1]
		//record184: [a184, b0, c1, d0, e4, f4, g2]
		String[] ConditionColumns2 = {"e","b"};
		String[] ConditionColumnsValues2 = {"e1","b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ai24x", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 18, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a26, b0, c2, d2, e1, f2, g5]
		//record3: [a36, b0, c0, d0, e1, f0, g1]
		//record4: [a46, b0, c1, d2, e1, f4, g4]
		int pageCount2 = (int)Math.ceil(185.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ai24x");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+18));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("vo0", cols0);
		String [][] records_vo0 = new String[299][cols0.length];
		for(int i=0;i<299;i++)
		{
			records_vo0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vo0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vo0", records_vo0[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record294: [a294]
		//record295: [a295]
		//record296: [a296]
		//record297: [a297]
		//record298: [a298]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a24"};
		ArrayList<String[]> tableSelect0 = DBApp.select("vo0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a24]
		int pageCount0 = (int)Math.ceil(299.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("vo0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("d1", cols1);
		String [][] records_d1 = new String[441][cols1.length];
		for(int i=0;i<441;i++)
		{
			records_d1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d1", records_d1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("d1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 55, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5]
		int pageCount1 = (int)Math.ceil(441.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("d1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+55));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("pmp6", cols0);
		String [][] records_pmp6 = new String[173][cols0.length];
		for(int i=0;i<173;i++)
		{
			records_pmp6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pmp6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pmp6", records_pmp6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k9"};
		ArrayList<String[]> tableSelect0 = DBApp.select("pmp6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 15, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		int pageCount0 = (int)Math.ceil(173.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("pmp6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+15));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("j5yq", cols1);
		String [][] records_j5yq = new String[153][cols1.length];
		for(int i=0;i<153;i++)
		{
			records_j5yq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j5yq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j5yq", records_j5yq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record148: [a148, b0, c1]
		//record149: [a149, b1, c2]
		//record150: [a150, b0, c0]
		//record151: [a151, b1, c1]
		//record152: [a152, b0, c2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("j5yq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 77, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
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
		String selectTrace1 = DBApp.getLastTrace("j5yq");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+77));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("m2u", cols2);
		String [][] records_m2u = new String[383][cols2.length];
		for(int i=0;i<383;i++)
		{
			records_m2u[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m2u[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m2u", records_m2u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record378: [a378, b0, c0, d2, e3, f0]
		//record379: [a379, b1, c1, d3, e4, f1]
		//record380: [a380, b0, c2, d0, e0, f2]
		//record381: [a381, b1, c0, d1, e1, f3]
		//record382: [a382, b0, c1, d2, e2, f4]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("m2u", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 64, records:
		//record0: [a3, b1, c0, d3, e3, f3]
		//record1: [a9, b1, c0, d1, e4, f3]
		//record2: [a15, b1, c0, d3, e0, f3]
		//record3: [a21, b1, c0, d1, e1, f3]
		//record4: [a27, b1, c0, d3, e2, f3]
		int pageCount2 = (int)Math.ceil(383.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("m2u");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+64));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qc8", cols3);
		String [][] records_qc8 = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_qc8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qc8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qc8", records_qc8[i]);
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
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("qc8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 3, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		int pageCount3 = (int)Math.ceil(16.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("qc8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+3));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("i9", cols0);
		String [][] records_i9 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_i9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record21: [a21, b1]
		//record22: [a22, b0]
		//record23: [a23, b1]
		//record24: [a24, b0]
		//record25: [a25, b1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("i9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
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
		String selectTrace0 = DBApp.getLastTrace("i9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("vv885", cols1);
		String [][] records_vv885 = new String[314][cols1.length];
		for(int i=0;i<314;i++)
		{
			records_vv885[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vv885[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vv885", records_vv885[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record309: [a309]
		//record310: [a310]
		//record311: [a311]
		//record312: [a312]
		//record313: [a313]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a205"};
		ArrayList<String[]> tableSelect1 = DBApp.select("vv885", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a205]
		int pageCount1 = (int)Math.ceil(314.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("vv885");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("cjp", cols0);
		String [][] records_cjp = new String[402][cols0.length];
		for(int i=0;i<402;i++)
		{
			records_cjp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cjp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cjp", records_cjp[i]);
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
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g5"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cjp", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 57, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
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
		String selectTrace0 = DBApp.getLastTrace("cjp");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+57));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("xk", cols1);
		String [][] records_xk = new String[334][cols1.length];
		for(int i=0;i<334;i++)
		{
			records_xk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xk", records_xk[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record329: [a329, b1]
		//record330: [a330, b0]
		//record331: [a331, b1]
		//record332: [a332, b0]
		//record333: [a333, b1]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a159"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xk", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a159, b1]
		int pageCount1 = (int)Math.ceil(334.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("xk");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("rkp7z", cols2);
		String [][] records_rkp7z = new String[167][cols2.length];
		for(int i=0;i<167;i++)
		{
			records_rkp7z[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rkp7z[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rkp7z", records_rkp7z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6]
		String[] ConditionColumns2 = {"j"};
		String[] ConditionColumnsValues2 = {"j6"};
		ArrayList<String[]> tableSelect2 = DBApp.select("rkp7z", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 17, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6]
		int pageCount2 = (int)Math.ceil(167.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("rkp7z");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("lud4i", cols3);
		String [][] records_lud4i = new String[471][cols3.length];
		for(int i=0;i<471;i++)
		{
			records_lud4i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lud4i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lud4i", records_lud4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11, n4]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12, n5]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8]
		String[] ConditionColumns3 = {"h","j","n","i"};
		String[] ConditionColumnsValues3 = {"h6","j4","n12","i0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("lud4i", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		int pageCount3 = (int)Math.ceil(471.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("lud4i");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("r4d", cols0);
		String [][] records_r4d = new String[257][cols0.length];
		for(int i=0;i<257;i++)
		{
			records_r4d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r4d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r4d", records_r4d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record252: [a252, b0, c0, d0, e2]
		//record253: [a253, b1, c1, d1, e3]
		//record254: [a254, b0, c2, d2, e4]
		//record255: [a255, b1, c0, d3, e0]
		//record256: [a256, b0, c1, d0, e1]
		String[] ConditionColumns0 = {"c","e"};
		String[] ConditionColumnsValues0 = {"c0","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r4d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a9, b1, c0, d1, e4]
		//record1: [a24, b0, c0, d0, e4]
		//record2: [a39, b1, c0, d3, e4]
		//record3: [a54, b0, c0, d2, e4]
		//record4: [a69, b1, c0, d1, e4]
		int pageCount0 = (int)Math.ceil(257.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("r4d");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("c0", cols1);
		String [][] records_c0 = new String[155][cols1.length];
		for(int i=0;i<155;i++)
		{
			records_c0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c0", records_c0[i]);
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
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("c0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 78, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount1 = (int)Math.ceil(155.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("c0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+78));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("ey6", cols2);
		String [][] records_ey6 = new String[146][cols2.length];
		for(int i=0;i<146;i++)
		{
			records_ey6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ey6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ey6", records_ey6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record141: [a141, b1, c0, d1]
		//record142: [a142, b0, c1, d2]
		//record143: [a143, b1, c2, d3]
		//record144: [a144, b0, c0, d0]
		//record145: [a145, b1, c1, d1]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.select("ey6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 36, records:
		//record0: [a3, b1, c0, d3]
		//record1: [a7, b1, c1, d3]
		//record2: [a11, b1, c2, d3]
		//record3: [a15, b1, c0, d3]
		//record4: [a19, b1, c1, d3]
		int pageCount2 = (int)Math.ceil(146.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("ey6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+36));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cdrf", cols0);
		String [][] records_cdrf = new String[378][cols0.length];
		for(int i=0;i<378;i++)
		{
			records_cdrf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cdrf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cdrf", records_cdrf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cdrf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 189, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
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
		String selectTrace0 = DBApp.getLastTrace("cdrf");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+189));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("c08", cols1);
		String [][] records_c08 = new String[326][cols1.length];
		for(int i=0;i<326;i++)
		{
			records_c08[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c08[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c08", records_c08[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record321: [a321, b1, c0, d1]
		//record322: [a322, b0, c1, d2]
		//record323: [a323, b1, c2, d3]
		//record324: [a324, b0, c0, d0]
		//record325: [a325, b1, c1, d1]
		String[] ConditionColumns1 = {"a","d"};
		String[] ConditionColumnsValues1 = {"a96","d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("c08", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a96, b0, c0, d0]
		int pageCount1 = (int)Math.ceil(326.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("c08");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g6qax", cols2);
		String [][] records_g6qax = new String[281][cols2.length];
		for(int i=0;i<281;i++)
		{
			records_g6qax[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g6qax[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g6qax", records_g6qax[i]);
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
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g4"};
		ArrayList<String[]> tableSelect2 = DBApp.select("g6qax", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 40, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a18, b0, c0, d2, e3, f0, g4]
		//record3: [a25, b1, c1, d1, e0, f1, g4]
		//record4: [a32, b0, c2, d0, e2, f2, g4]
		int pageCount2 = (int)Math.ceil(281.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("g6qax");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+40));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q0", cols3);
		String [][] records_q0 = new String[410][cols3.length];
		for(int i=0;i<410;i++)
		{
			records_q0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q0", records_q0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9]
		String[] ConditionColumns3 = {"j","h","i","g"};
		String[] ConditionColumnsValues3 = {"j5","h1","i2","g2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("q0", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5]
		int pageCount3 = (int)Math.ceil(410.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("q0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("g1vp1", cols0);
		String [][] records_g1vp1 = new String[471][cols0.length];
		for(int i=0;i<471;i++)
		{
			records_g1vp1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g1vp1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g1vp1", records_g1vp1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record466: [a466, b0, c1, d2]
		//record467: [a467, b1, c2, d3]
		//record468: [a468, b0, c0, d0]
		//record469: [a469, b1, c1, d1]
		//record470: [a470, b0, c2, d2]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g1vp1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 118, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a5, b1, c2, d1]
		//record2: [a9, b1, c0, d1]
		//record3: [a13, b1, c1, d1]
		//record4: [a17, b1, c2, d1]
		int pageCount0 = (int)Math.ceil(471.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("g1vp1");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+118));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i7", cols1);
		String [][] records_i7 = new String[373][cols1.length];
		for(int i=0;i<373;i++)
		{
			records_i7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i7", records_i7[i]);
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
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("i7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 75, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record3: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record4: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		int pageCount1 = (int)Math.ceil(373.0/DBApp.dataPageSize);
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
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+75));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("uv9", cols2);
		String [][] records_uv9 = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			records_uv9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_uv9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("uv9", records_uv9[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record48: [a48, b0]
		//record49: [a49, b1]
		//record50: [a50, b0]
		//record51: [a51, b1]
		//record52: [a52, b0]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		ArrayList<String[]> tableSelect2 = DBApp.select("uv9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 27, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		int pageCount2 = (int)Math.ceil(53.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("uv9");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+27));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("x9zpf", cols0);
		String [][] records_x9zpf = new String[244][cols0.length];
		for(int i=0;i<244;i++)
		{
			records_x9zpf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x9zpf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x9zpf", records_x9zpf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record239: [a239, b1, c2]
		//record240: [a240, b0, c0]
		//record241: [a241, b1, c1]
		//record242: [a242, b0, c2]
		//record243: [a243, b1, c0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a115"};
		ArrayList<String[]> tableSelect0 = DBApp.select("x9zpf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a115, b1, c1]
		int pageCount0 = (int)Math.ceil(244.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("x9zpf");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("zoh", cols1);
		String [][] records_zoh = new String[170][cols1.length];
		for(int i=0;i<170;i++)
		{
			records_zoh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zoh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zoh", records_zoh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record165: [a165, b1, c0, d1]
		//record166: [a166, b0, c1, d2]
		//record167: [a167, b1, c2, d3]
		//record168: [a168, b0, c0, d0]
		//record169: [a169, b1, c1, d1]
		String[] ConditionColumns1 = {"a","d"};
		String[] ConditionColumnsValues1 = {"a162","d2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("zoh", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a162, b0, c0, d2]
		int pageCount1 = (int)Math.ceil(170.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("zoh");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("a66", cols0);
		String [][] records_a66 = new String[359][cols0.length];
		for(int i=0;i<359;i++)
		{
			records_a66[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a66[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a66", records_a66[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record354: [a354, b0, c0, d2, e4]
		//record355: [a355, b1, c1, d3, e0]
		//record356: [a356, b0, c2, d0, e1]
		//record357: [a357, b1, c0, d1, e2]
		//record358: [a358, b0, c1, d2, e3]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		ArrayList<String[]> tableSelect0 = DBApp.select("a66", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 72, records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a5, b1, c2, d1, e0]
		//record2: [a10, b0, c1, d2, e0]
		//record3: [a15, b1, c0, d3, e0]
		//record4: [a20, b0, c2, d0, e0]
		int pageCount0 = (int)Math.ceil(359.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("a66");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("b6g", cols1);
		String [][] records_b6g = new String[261][cols1.length];
		for(int i=0;i<261;i++)
		{
			records_b6g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b6g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b6g", records_b6g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record256: [a256, b0, c1, d0]
		//record257: [a257, b1, c2, d1]
		//record258: [a258, b0, c0, d2]
		//record259: [a259, b1, c1, d3]
		//record260: [a260, b0, c2, d0]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("b6g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 87, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a5, b1, c2, d1]
		//record2: [a8, b0, c2, d0]
		//record3: [a11, b1, c2, d3]
		//record4: [a14, b0, c2, d2]
		int pageCount1 = (int)Math.ceil(261.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("b6g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+87));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("rc07p", cols2);
		String [][] records_rc07p = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_rc07p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rc07p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rc07p", records_rc07p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record35: [a35, b1, c2, d3]
		//record36: [a36, b0, c0, d0]
		//record37: [a37, b1, c1, d1]
		//record38: [a38, b0, c2, d2]
		//record39: [a39, b1, c0, d3]
		String[] ConditionColumns2 = {"d","a"};
		String[] ConditionColumnsValues2 = {"d2","a34"};
		ArrayList<String[]> tableSelect2 = DBApp.select("rc07p", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a34, b0, c1, d2]
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
		String selectTrace2 = DBApp.getLastTrace("rc07p");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("o1c", cols3);
		String [][] records_o1c = new String[127][cols3.length];
		for(int i=0;i<127;i++)
		{
			records_o1c[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o1c[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o1c", records_o1c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2]
		//record123: [a123, b1, c0, d3, e3, f3]
		//record124: [a124, b0, c1, d0, e4, f4]
		//record125: [a125, b1, c2, d1, e0, f5]
		//record126: [a126, b0, c0, d2, e1, f0]
		String[] ConditionColumns3 = {"c","b"};
		String[] ConditionColumnsValues3 = {"c0","b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("o1c", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 22, records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a6, b0, c0, d2, e1, f0]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a18, b0, c0, d2, e3, f0]
		//record4: [a24, b0, c0, d0, e4, f0]
		int pageCount3 = (int)Math.ceil(127.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("o1c");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+22));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("cq0lm", cols0);
		String [][] records_cq0lm = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_cq0lm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cq0lm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cq0lm", records_cq0lm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record178: [a178, b0, c1, d2]
		//record179: [a179, b1, c2, d3]
		//record180: [a180, b0, c0, d0]
		//record181: [a181, b1, c1, d1]
		//record182: [a182, b0, c2, d2]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d2"};
		ArrayList<String[]> tableSelect0 = DBApp.select("cq0lm", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 46, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a6, b0, c0, d2]
		//record2: [a10, b0, c1, d2]
		//record3: [a14, b0, c2, d2]
		//record4: [a18, b0, c0, d2]
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
		String selectTrace0 = DBApp.getLastTrace("cq0lm");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+46));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("azy", cols1);
		String [][] records_azy = new String[150][cols1.length];
		for(int i=0;i<150;i++)
		{
			records_azy[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_azy[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("azy", records_azy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record145: [a145, b1, c1, d1, e0, f1, g5]
		//record146: [a146, b0, c2, d2, e1, f2, g6]
		//record147: [a147, b1, c0, d3, e2, f3, g0]
		//record148: [a148, b0, c1, d0, e3, f4, g1]
		//record149: [a149, b1, c2, d1, e4, f5, g2]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("azy", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 75, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		int pageCount1 = (int)Math.ceil(150.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("azy");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+75));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r5674", cols0);
		String [][] records_r5674 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			records_r5674[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r5674[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r5674", records_r5674[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record59: [a59, b1, c2, d3, e4, f5, g3]
		//record60: [a60, b0, c0, d0, e0, f0, g4]
		//record61: [a61, b1, c1, d1, e1, f1, g5]
		//record62: [a62, b0, c2, d2, e2, f2, g6]
		//record63: [a63, b1, c0, d3, e3, f3, g0]
		String[] ConditionColumns0 = {"f","c"};
		String[] ConditionColumnsValues0 = {"f1","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("r5674", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		int pageCount0 = (int)Math.ceil(64.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("r5674");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+11));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r3q", cols1);
		String [][] records_r3q = new String[495][cols1.length];
		for(int i=0;i<495;i++)
		{
			records_r3q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r3q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r3q", records_r3q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4]
		String[] ConditionColumns1 = {"a","f","d"};
		String[] ConditionColumnsValues1 = {"a192","f0","d0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("r3q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2]
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
		String selectTrace1 = DBApp.getLastTrace("r3q");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tj7", cols2);
		String [][] records_tj7 = new String[109][cols2.length];
		for(int i=0;i<109;i++)
		{
			records_tj7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tj7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tj7", records_tj7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0]
		String[] ConditionColumns2 = {"j","i","f","d","l"};
		String[] ConditionColumnsValues2 = {"j7","i4","f1","d3","l7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("tj7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7]
		int pageCount2 = (int)Math.ceil(109.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("tj7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("o1i0", cols3);
		String [][] records_o1i0 = new String[474][cols3.length];
		for(int i=0;i<474;i++)
		{
			records_o1i0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o1i0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o1i0", records_o1i0[i]);
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
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("o1i0", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 95, records:
		//record0: [a2, b0, c2, d2, e2, f2]
		//record1: [a7, b1, c1, d3, e2, f1]
		//record2: [a12, b0, c0, d0, e2, f0]
		//record3: [a17, b1, c2, d1, e2, f5]
		//record4: [a22, b0, c1, d2, e2, f4]
		int pageCount3 = (int)Math.ceil(474.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("o1i0");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+95));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("h47", cols4);
		String [][] records_h47 = new String[229][cols4.length];
		for(int i=0;i<229;i++)
		{
			records_h47[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_h47[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("h47", records_h47[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8]
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("h47", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 46, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		int pageCount4 = (int)Math.ceil(229.0/DBApp.dataPageSize);
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
		String selectTrace4 = DBApp.getLastTrace("h47");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace4.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace4.contains(Arrays.toString(ConditionColumns4)+"->"+Arrays.toString(ConditionColumnsValues4)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace4.contains(pagesCounts4.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace4.contains("records:"+46));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xy8", cols0);
		String [][] records_xy8 = new String[387][cols0.length];
		for(int i=0;i<387;i++)
		{
			records_xy8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xy8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xy8", records_xy8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		//record384: [a384, b0, c0, d0, e4, f0, g6]
		//record385: [a385, b1, c1, d1, e0, f1, g0]
		//record386: [a386, b0, c2, d2, e1, f2, g1]
		String[] ConditionColumns0 = {"e","b","a"};
		String[] ConditionColumnsValues0 = {"e0","b0","a110"};
		ArrayList<String[]> tableSelect0 = DBApp.select("xy8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a110, b0, c2, d2, e0, f2, g5]
		int pageCount0 = (int)Math.ceil(387.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("xy8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w167", cols1);
		String [][] records_w167 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			records_w167[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w167[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w167", records_w167[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m12"};
		ArrayList<String[]> tableSelect1 = DBApp.select("w167", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12]
		//record4: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		int pageCount1 = (int)Math.ceil(68.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("w167");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+5));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qh3", cols2);
		String [][] records_qh3 = new String[499][cols2.length];
		for(int i=0;i<499;i++)
		{
			records_qh3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qh3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qh3", records_qh3[i]);
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
		String[] ConditionColumns2 = {"l"};
		String[] ConditionColumnsValues2 = {"l8"};
		ArrayList<String[]> tableSelect2 = DBApp.select("qh3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 41, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		int pageCount2 = (int)Math.ceil(499.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("qh3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+41));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rn8g", cols0);
		String [][] records_rn8g = new String[317][cols0.length];
		for(int i=0;i<317;i++)
		{
			records_rn8g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rn8g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rn8g", records_rn8g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5, l1, m1]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4]
		String[] ConditionColumns0 = {"m","h","e","c"};
		String[] ConditionColumnsValues0 = {"m5","h2","e1","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rn8g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5]
		int pageCount0 = (int)Math.ceil(317.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("rn8g");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qvq3", cols0);
		String [][] records_qvq3 = new String[479][cols0.length];
		for(int i=0;i<479;i++)
		{
			records_qvq3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qvq3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qvq3", records_qvq3[i]);
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
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("qvq3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 80, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a9, b1, c0, d1, e4, f3, g2]
		//record2: [a15, b1, c0, d3, e0, f3, g1]
		//record3: [a21, b1, c0, d1, e1, f3, g0]
		//record4: [a27, b1, c0, d3, e2, f3, g6]
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
		String selectTrace0 = DBApp.getLastTrace("qvq3");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+80));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("rd50p", cols1);
		String [][] records_rd50p = new String[150][cols1.length];
		for(int i=0;i<150;i++)
		{
			records_rd50p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rd50p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rd50p", records_rd50p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record145: [a145, b1, c1, d1]
		//record146: [a146, b0, c2, d2]
		//record147: [a147, b1, c0, d3]
		//record148: [a148, b0, c1, d0]
		//record149: [a149, b1, c2, d1]
		String[] ConditionColumns1 = {"d","c"};
		String[] ConditionColumnsValues1 = {"d2","c2"};
		ArrayList<String[]> tableSelect1 = DBApp.select("rd50p", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 13, records:
		//record0: [a2, b0, c2, d2]
		//record1: [a14, b0, c2, d2]
		//record2: [a26, b0, c2, d2]
		//record3: [a38, b0, c2, d2]
		//record4: [a50, b0, c2, d2]
		int pageCount1 = (int)Math.ceil(150.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("rd50p");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+13));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("iop", cols2);
		String [][] records_iop = new String[377][cols2.length];
		for(int i=0;i<377;i++)
		{
			records_iop[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_iop[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("iop", records_iop[i]);
		}
		//first 5 records:
		//record0: [a0]
		//record1: [a1]
		//record2: [a2]
		//record3: [a3]
		//record4: [a4]
		//last 5 records:
		//record372: [a372]
		//record373: [a373]
		//record374: [a374]
		//record375: [a375]
		//record376: [a376]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a96"};
		ArrayList<String[]> tableSelect2 = DBApp.select("iop", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a96]
		int pageCount2 = (int)Math.ceil(377.0/DBApp.dataPageSize);
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
		String selectTrace2 = DBApp.getLastTrace("iop");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("u8e6", cols3);
		String [][] records_u8e6 = new String[383][cols3.length];
		for(int i=0;i<383;i++)
		{
			records_u8e6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u8e6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u8e6", records_u8e6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0]
		//record1: [a1, b1, c1]
		//record2: [a2, b0, c2]
		//record3: [a3, b1, c0]
		//record4: [a4, b0, c1]
		//last 5 records:
		//record378: [a378, b0, c0]
		//record379: [a379, b1, c1]
		//record380: [a380, b0, c2]
		//record381: [a381, b1, c0]
		//record382: [a382, b0, c1]
		String[] ConditionColumns3 = {"b"};
		String[] ConditionColumnsValues3 = {"b0"};
		ArrayList<String[]> tableSelect3 = DBApp.select("u8e6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 192, records:
		//record0: [a0, b0, c0]
		//record1: [a2, b0, c2]
		//record2: [a4, b0, c1]
		//record3: [a6, b0, c0]
		//record4: [a8, b0, c2]
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
		String selectTrace3 = DBApp.getLastTrace("u8e6");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+192));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("o9a", cols0);
		String [][] records_o9a = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			records_o9a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o9a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o9a", records_o9a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j4"};
		ArrayList<String[]> tableSelect0 = DBApp.select("o9a", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		int pageCount0 = (int)Math.ceil(25.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("o9a");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p8", cols1);
		String [][] records_p8 = new String[487][cols1.length];
		for(int i=0;i<487;i++)
		{
			records_p8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p8", records_p8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9, l2, m1]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3, m2]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5, m4]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5]
		String[] ConditionColumns1 = {"i","h","b","j","l","f"};
		String[] ConditionColumnsValues1 = {"i5","h1","b1","j5","l5","f5"};
		ArrayList<String[]> tableSelect1 = DBApp.select("p8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3]
		int pageCount1 = (int)Math.ceil(487.0/DBApp.dataPageSize);
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
		String selectTrace1 = DBApp.getLastTrace("p8");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace1.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace1.contains(Arrays.toString(ConditionColumns1)+"->"+Arrays.toString(ConditionColumnsValues1)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace1.contains(pagesCounts1.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cm7", cols2);
		String [][] records_cm7 = new String[339][cols2.length];
		for(int i=0;i<339;i++)
		{
			records_cm7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cm7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cm7", records_cm7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("cm7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 42, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
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
		String selectTrace2 = DBApp.getLastTrace("cm7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace2.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace2.contains(Arrays.toString(ConditionColumns2)+"->"+Arrays.toString(ConditionColumnsValues2)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace2.contains(pagesCounts2.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace2.contains("records:"+42));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v13d7", cols3);
		String [][] records_v13d7 = new String[121][cols3.length];
		for(int i=0;i<121;i++)
		{
			records_v13d7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v13d7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v13d7", records_v13d7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12, n4, o11]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9, m0, n5, o12]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1, n6, o13]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9, l11, m2, n7, o14]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8, o0]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g3"};
		ArrayList<String[]> tableSelect3 = DBApp.select("v13d7", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		int pageCount3 = (int)Math.ceil(121.0/DBApp.dataPageSize);
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
		String selectTrace3 = DBApp.getLastTrace("v13d7");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace3.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace3.contains(Arrays.toString(ConditionColumns3)+"->"+Arrays.toString(ConditionColumnsValues3)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace3.contains(pagesCounts3.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace3.contains("records:"+17));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(240);
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