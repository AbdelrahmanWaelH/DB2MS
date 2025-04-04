package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests06
{

	@Test(timeout = 1000000)
	public void test76TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w8", cols0);
		String [][] records_w8 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w8", records_w8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(76));
		for(int i=0;i<2;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("w8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_w8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q1428", cols1);
		String [][] records_q1428 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q1428[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q1428", records_q1428[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(49));
		for(int i=0;i<49;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("q1428",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_q1428[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p11", cols2);
		String [][] records_p11 = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p11[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p11", records_p11[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(380));
		for(int i=0;i<12;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("p11",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_p11[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("qm3", cols3);
		String [][] records_qm3 = new String[88][cols3.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_qm3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qm3", records_qm3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(370));
		for(int i=0;i<88;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("qm3",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_qm3[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g3o", cols4);
		String [][] records_g3o = new String[94][cols4.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g3o[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g3o", records_g3o[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(74));
		for(int i=0;i<94;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("g3o",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_g3o[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("o1z9", cols0);
		String [][] records_o1z9 = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o1z9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o1z9", records_o1z9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(618));
		for(int i=0;i<62;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o1z9",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o1z9[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r5", cols1);
		String [][] records_r5 = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r5", records_r5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(470));
		for(int i=0;i<29;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("r5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_r5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z025", cols2);
		String [][] records_z025 = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z025[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z025", records_z025[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(513));
		for(int i=0;i<6;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("z025",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_z025[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("oaj", cols3);
		String [][] records_oaj = new String[93][cols3.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_oaj[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oaj", records_oaj[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(159));
		for(int i=0;i<93;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("oaj",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_oaj[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("v0c", cols4);
		String [][] records_v0c = new String[62][cols4.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_v0c[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v0c", records_v0c[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(574));
		for(int i=0;i<62;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("v0c",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_v0c[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s76", cols0);
		String [][] records_s76 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s76[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s76", records_s76[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(729));
		for(int i=0;i<40;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("s76",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_s76[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("gy5", cols1);
		String [][] records_gy5 = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gy5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gy5", records_gy5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(612));
		for(int i=0;i<64;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("gy5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_gy5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("o1amd", cols2);
		String [][] records_o1amd = new String[93][cols2.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o1amd[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o1amd", records_o1amd[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(230));
		for(int i=0;i<93;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("o1amd",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_o1amd[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y16", cols3);
		String [][] records_y16 = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y16[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y16", records_y16[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<55; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(420));
		for(int i=0;i<55;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("y16",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_y16[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("b7i", cols0);
		String [][] records_b7i = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b7i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b7i", records_b7i[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(943));
		for(int i=0;i<68;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("b7i",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_b7i[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("zb", cols1);
		String [][] records_zb = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zb", records_zb[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(998));
		for(int i=0;i<90;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("zb",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_zb[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cr2c", cols0);
		String [][] records_cr2c = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cr2c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cr2c", records_cr2c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(197));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("cr2c",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_cr2c[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("n9fu9", cols1);
		String [][] records_n9fu9 = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n9fu9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9fu9", records_n9fu9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(882));
		for(int i=0;i<99;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("n9fu9",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_n9fu9[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("xsi", cols2);
		String [][] records_xsi = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xsi[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xsi", records_xsi[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(182));
		for(int i=0;i<67;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("xsi",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_xsi[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("hm6", cols3);
		String [][] records_hm6 = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_hm6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hm6", records_hm6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(176));
		for(int i=0;i<83;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("hm6",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_hm6[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("noi", cols0);
		String [][] records_noi = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_noi[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("noi", records_noi[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(963));
		for(int i=0;i<49;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("noi",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_noi[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("m78", cols0);
		String [][] records_m78 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m78[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m78", records_m78[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(232));
		for(int i=0;i<32;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("m78",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_m78[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g86o", cols0);
		String [][] records_g86o = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g86o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g86o", records_g86o[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(14));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("g86o",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_g86o[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("f56g9", cols1);
		String [][] records_f56g9 = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f56g9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f56g9", records_f56g9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(25));
		for(int i=0;i<46;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("f56g9",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_f56g9[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rj429", cols2);
		String [][] records_rj429 = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_rj429[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rj429", records_rj429[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(704));
		for(int i=0;i<21;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("rj429",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_rj429[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("c23", cols3);
		String [][] records_c23 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c23[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c23", records_c23[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(616));
		for(int i=0;i<1;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("c23",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_c23[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pp", cols0);
		String [][] records_pp = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pp", records_pp[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(82));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("pp",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_pp[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z545", cols1);
		String [][] records_z545 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z545[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z545", records_z545[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(262));
		for(int i=0;i<89;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("z545",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_z545[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r56xr", cols2);
		String [][] records_r56xr = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r56xr[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r56xr", records_r56xr[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(14));
		for(int i=0;i<61;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("r56xr",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_r56xr[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("bn3", cols3);
		String [][] records_bn3 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bn3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn3", records_bn3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(32));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("bn3",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_bn3[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b14d", cols4);
		String [][] records_b14d = new String[68][cols4.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b14d[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b14d", records_b14d[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(400));
		for(int i=0;i<68;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("b14d",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_b14d[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rxb", cols0);
		String [][] records_rxb = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rxb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rxb", records_rxb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(943));
		for(int i=0;i<92;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("rxb",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_rxb[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("w93t7", cols1);
		String [][] records_w93t7 = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w93t7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w93t7", records_w93t7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(337));
		for(int i=0;i<10;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("w93t7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_w93t7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("l369r", cols2);
		String [][] records_l369r = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l369r[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l369r", records_l369r[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(908));
		for(int i=0;i<18;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("l369r",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_l369r[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w6g00", cols3);
		String [][] records_w6g00 = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w6g00[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w6g00", records_w6g00[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(485));
		for(int i=0;i<37;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("w6g00",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_w6g00[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("no", cols4);
		String [][] records_no = new String[23][cols4.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_no[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("no", records_no[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(962));
		for(int i=0;i<23;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("no",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_no[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("p21", cols0);
		String [][] records_p21 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p21[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p21", records_p21[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(280));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p21",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p21[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p3", cols0);
		String [][] records_p3 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3", records_p3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(932));
		for(int i=0;i<36;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("djg", cols0);
		String [][] records_djg = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_djg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("djg", records_djg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(415));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("djg",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_djg[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("vwb67", cols1);
		String [][] records_vwb67 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vwb67[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vwb67", records_vwb67[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(852));
		for(int i=0;i<35;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("vwb67",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_vwb67[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("ll", cols2);
		String [][] records_ll = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ll[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ll", records_ll[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(269));
		for(int i=0;i<94;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("ll",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ll[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("bn", cols3);
		String [][] records_bn = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bn[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn", records_bn[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(453));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("bn",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_bn[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("rei5", cols4);
		String [][] records_rei5 = new String[4][cols4.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_rei5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rei5", records_rei5[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(882));
		for(int i=0;i<4;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("rei5",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_rei5[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ur16", cols0);
		String [][] records_ur16 = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ur16[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ur16", records_ur16[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<87; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(85));
		for(int i=0;i<87;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ur16",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ur16[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w2l6", cols1);
		String [][] records_w2l6 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w2l6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w2l6", records_w2l6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(925));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("w2l6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_w2l6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b5h8", cols2);
		String [][] records_b5h8 = new String[29][cols2.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b5h8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b5h8", records_b5h8[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(627));
		for(int i=0;i<29;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("b5h8",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_b5h8[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qy", cols0);
		String [][] records_qy = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qy", records_qy[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(140));
		for(int i=0;i<69;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("qy",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_qy[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("g3", cols1);
		String [][] records_g3 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g3", records_g3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(27));
		for(int i=0;i<79;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("g3",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_g3[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g2e", cols2);
		String [][] records_g2e = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g2e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g2e", records_g2e[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(383));
		for(int i=0;i<46;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("g2e",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_g2e[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mp", cols3);
		String [][] records_mp = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_mp[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mp", records_mp[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(150));
		for(int i=0;i<23;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("mp",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_mp[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("u3cxy", cols0);
		String [][] records_u3cxy = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u3cxy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u3cxy", records_u3cxy[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(420));
		for(int i=0;i<29;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("u3cxy",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_u3cxy[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("icf0l", cols1);
		String [][] records_icf0l = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_icf0l[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("icf0l", records_icf0l[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(110));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("icf0l",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_icf0l[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("x47", cols0);
		String [][] records_x47 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x47[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x47", records_x47[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(914));
		for(int i=0;i<5;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("x47",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_x47[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("nse1", cols0);
		String [][] records_nse1 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nse1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nse1", records_nse1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(90));
		for(int i=0;i<27;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("nse1",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_nse1[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rl60", cols1);
		String [][] records_rl60 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rl60[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rl60", records_rl60[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(427));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("rl60",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_rl60[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("e39", cols2);
		String [][] records_e39 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e39[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e39", records_e39[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(212));
		for(int i=0;i<23;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("e39",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_e39[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("sin", cols3);
		String [][] records_sin = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sin[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sin", records_sin[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(595));
		for(int i=0;i<77;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("sin",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_sin[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("cdu", cols4);
		String [][] records_cdu = new String[12][cols4.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_cdu[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cdu", records_cdu[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(320));
		for(int i=0;i<12;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("cdu",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_cdu[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r219", cols0);
		String [][] records_r219 = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r219[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r219", records_r219[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(811));
		for(int i=0;i<83;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("r219",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_r219[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("si68x", cols1);
		String [][] records_si68x = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_si68x[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("si68x", records_si68x[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(313));
		for(int i=0;i<79;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("si68x",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_si68x[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tp", cols2);
		String [][] records_tp = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tp[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tp", records_tp[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(819));
		for(int i=0;i<12;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("tp",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_tp[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p82n2", cols3);
		String [][] records_p82n2 = new String[96][cols3.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p82n2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p82n2", records_p82n2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(215));
		for(int i=0;i<96;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("p82n2",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_p82n2[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d5690", cols4);
		String [][] records_d5690 = new String[68][cols4.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_d5690[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d5690", records_d5690[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(81));
		for(int i=0;i<68;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("d5690",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_d5690[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("s91", cols0);
		String [][] records_s91 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s91[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s91", records_s91[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(557));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("s91",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_s91[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("bf8", cols1);
		String [][] records_bf8 = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bf8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bf8", records_bf8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(659));
		for(int i=0;i<84;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("bf8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_bf8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xn0", cols2);
		String [][] records_xn0 = new String[71][cols2.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xn0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xn0", records_xn0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(255));
		for(int i=0;i<71;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("xn0",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_xn0[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("isj", cols3);
		String [][] records_isj = new String[74][cols3.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_isj[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("isj", records_isj[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(518));
		for(int i=0;i<74;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("isj",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_isj[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("i70c", cols0);
		String [][] records_i70c = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i70c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i70c", records_i70c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(869));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i70c",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i70c[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("b6", cols1);
		String [][] records_b6 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b6", records_b6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(801));
		for(int i=0;i<35;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("b6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_b6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("h82", cols2);
		String [][] records_h82 = new String[39][cols2.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h82[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h82", records_h82[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(243));
		for(int i=0;i<39;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("h82",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_h82[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("o95m2", cols0);
		String [][] records_o95m2 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o95m2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o95m2", records_o95m2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(421));
		for(int i=0;i<51;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o95m2",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o95m2[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("we4q8", cols0);
		String [][] records_we4q8 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_we4q8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("we4q8", records_we4q8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(706));
		for(int i=0;i<33;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("we4q8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_we4q8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("z36", cols1);
		String [][] records_z36 = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z36[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z36", records_z36[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(709));
		for(int i=0;i<8;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("z36",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_z36[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("jv263", cols2);
		String [][] records_jv263 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jv263[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jv263", records_jv263[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(463));
		for(int i=0;i<50;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("jv263",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_jv263[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k49", cols3);
		String [][] records_k49 = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k49[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k49", records_k49[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(445));
		for(int i=0;i<42;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("k49",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_k49[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("bv59", cols0);
		String [][] records_bv59 = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bv59[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bv59", records_bv59[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(868));
		for(int i=0;i<62;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("bv59",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_bv59[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("n70", cols1);
		String [][] records_n70 = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n70[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n70", records_n70[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(954));
		for(int i=0;i<94;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("n70",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_n70[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("m5h8o", cols2);
		String [][] records_m5h8o = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m5h8o[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m5h8o", records_m5h8o[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(484));
		for(int i=0;i<25;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("m5h8o",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_m5h8o[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("snr", cols3);
		String [][] records_snr = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_snr[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("snr", records_snr[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(859));
		for(int i=0;i<77;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("snr",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_snr[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p0", cols0);
		String [][] records_p0 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p0", records_p0[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(246));
		for(int i=0;i<67;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p0",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p0[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("or5w", cols1);
		String [][] records_or5w = new String[61][cols1.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_or5w[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("or5w", records_or5w[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(999));
		for(int i=0;i<61;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("or5w",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_or5w[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("nzf1k", cols2);
		String [][] records_nzf1k = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_nzf1k[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nzf1k", records_nzf1k[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(463));
		for(int i=0;i<32;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("nzf1k",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_nzf1k[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("jqs", cols3);
		String [][] records_jqs = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jqs[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jqs", records_jqs[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(890));
		for(int i=0;i<46;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("jqs",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_jqs[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("dvk4i", cols4);
		String [][] records_dvk4i = new String[37][cols4.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_dvk4i[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dvk4i", records_dvk4i[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(203));
		for(int i=0;i<37;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("dvk4i",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_dvk4i[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ej", cols0);
		String [][] records_ej = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ej[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ej", records_ej[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(267));
		for(int i=0;i<7;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ej",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ej[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("ix9", cols0);
		String [][] records_ix9 = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ix9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ix9", records_ix9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(640));
		for(int i=0;i<9;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ix9",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ix9[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ql6wp", cols0);
		String [][] records_ql6wp = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ql6wp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ql6wp", records_ql6wp[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(976));
		for(int i=0;i<24;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ql6wp",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ql6wp[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("i94", cols1);
		String [][] records_i94 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i94[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i94", records_i94[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(91));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("i94",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_i94[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("q6p", cols2);
		String [][] records_q6p = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q6p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q6p", records_q6p[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(974));
		for(int i=0;i<70;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("q6p",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_q6p[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e14", cols3);
		String [][] records_e14 = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e14[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e14", records_e14[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(429));
		for(int i=0;i<10;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("e14",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_e14[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l7", cols4);
		String [][] records_l7 = new String[85][cols4.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_l7[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7", records_l7[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(753));
		for(int i=0;i<85;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("l7",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_l7[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xjpg", cols0);
		String [][] records_xjpg = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xjpg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xjpg", records_xjpg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(479));
		for(int i=0;i<13;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xjpg",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xjpg[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("o9g", cols1);
		String [][] records_o9g = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o9g[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o9g", records_o9g[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(391));
		for(int i=0;i<1;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("o9g",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_o9g[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("i1759", cols2);
		String [][] records_i1759 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i1759[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i1759", records_i1759[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(327));
		for(int i=0;i<89;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("i1759",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_i1759[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("py", cols3);
		String [][] records_py = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_py[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("py", records_py[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(495));
		for(int i=0;i<24;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("py",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_py[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u7o", cols4);
		String [][] records_u7o = new String[7][cols4.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_u7o[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u7o", records_u7o[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(608));
		for(int i=0;i<7;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("u7o",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_u7o[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("j3z", cols0);
		String [][] records_j3z = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j3z[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j3z", records_j3z[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(474));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("j3z",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_j3z[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("pm849", cols0);
		String [][] records_pm849 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pm849[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pm849", records_pm849[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(946));
		for(int i=0;i<47;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("pm849",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_pm849[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("xv", cols0);
		String [][] records_xv = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xv", records_xv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(347));
		for(int i=0;i<17;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xv",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xv[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("wg", cols1);
		String [][] records_wg = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wg", records_wg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(443));
		for(int i=0;i<81;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("wg",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_wg[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("c1yvd", cols2);
		String [][] records_c1yvd = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c1yvd[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c1yvd", records_c1yvd[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(418));
		for(int i=0;i<57;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("c1yvd",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_c1yvd[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b9", cols3);
		String [][] records_b9 = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(293));
		for(int i=0;i<20;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("b9",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_b9[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("il", cols0);
		String [][] records_il = new String[100][cols0.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_il[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("il", records_il[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(413));
		for(int i=0;i<100;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("il",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_il[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e5", cols1);
		String [][] records_e5 = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e5", records_e5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(426));
		for(int i=0;i<44;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("e5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_e5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("lp9", cols2);
		String [][] records_lp9 = new String[33][cols2.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_lp9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lp9", records_lp9[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(204));
		for(int i=0;i<33;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("lp9",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_lp9[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fi9", cols3);
		String [][] records_fi9 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fi9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fi9", records_fi9[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(536));
		for(int i=0;i<44;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("fi9",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_fi9[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ak", cols4);
		String [][] records_ak = new String[38][cols4.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ak[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ak", records_ak[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(682));
		for(int i=0;i<38;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("ak",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_ak[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y6p9", cols0);
		String [][] records_y6p9 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y6p9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y6p9", records_y6p9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(938));
		for(int i=0;i<31;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("y6p9",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_y6p9[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("in", cols0);
		String [][] records_in = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_in[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("in", records_in[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(72));
		for(int i=0;i<33;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("in",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_in[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r46", cols1);
		String [][] records_r46 = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r46[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r46", records_r46[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(766));
		for(int i=0;i<63;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("r46",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_r46[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("jtru", cols2);
		String [][] records_jtru = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jtru[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jtru", records_jtru[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(291));
		for(int i=0;i<66;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("jtru",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_jtru[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("b7", cols0);
		String [][] records_b7 = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b7", records_b7[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(860));
		for(int i=0;i<13;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("b7",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_b7[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zk", cols1);
		String [][] records_zk = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zk", records_zk[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(56));
		for(int i=0;i<90;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("zk",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_zk[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("zw05x", cols2);
		String [][] records_zw05x = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zw05x[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zw05x", records_zw05x[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(379));
		for(int i=0;i<67;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("zw05x",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_zw05x[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r79", cols3);
		String [][] records_r79 = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r79[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r79", records_r79[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(704));
		for(int i=0;i<6;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("r79",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_r79[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("pgkl1", cols4);
		String [][] records_pgkl1 = new String[88][cols4.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_pgkl1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pgkl1", records_pgkl1[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(564));
		for(int i=0;i<88;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("pgkl1",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_pgkl1[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("bg5", cols0);
		String [][] records_bg5 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bg5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bg5", records_bg5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(984));
		for(int i=0;i<7;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("bg5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_bg5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z65c0", cols1);
		String [][] records_z65c0 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z65c0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z65c0", records_z65c0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(667));
		for(int i=0;i<11;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("z65c0",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_z65c0[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("v5sp", cols2);
		String [][] records_v5sp = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v5sp[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v5sp", records_v5sp[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(527));
		for(int i=0;i<97;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("v5sp",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_v5sp[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("s9q", cols3);
		String [][] records_s9q = new String[96][cols3.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s9q[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s9q", records_s9q[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(195));
		for(int i=0;i<96;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("s9q",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_s9q[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tma", cols4);
		String [][] records_tma = new String[84][cols4.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_tma[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tma", records_tma[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(890));
		for(int i=0;i<84;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("tma",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_tma[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rf", cols0);
		String [][] records_rf = new String[100][cols0.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rf[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rf", records_rf[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(46));
		for(int i=0;i<100;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("rf",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_rf[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("m0hum", cols1);
		String [][] records_m0hum = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m0hum[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m0hum", records_m0hum[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(987));
		for(int i=0;i<35;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("m0hum",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_m0hum[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("on7", cols2);
		String [][] records_on7 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_on7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("on7", records_on7[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(347));
		for(int i=0;i<23;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("on7",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_on7[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("t02", cols3);
		String [][] records_t02 = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t02[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t02", records_t02[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(664));
		for(int i=0;i<58;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("t02",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_t02[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b0", cols4);
		String [][] records_b0 = new String[12][cols4.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b0[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b0", records_b0[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(27));
		for(int i=0;i<12;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("b0",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_b0[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o0f5o", cols0);
		String [][] records_o0f5o = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o0f5o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o0f5o", records_o0f5o[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(120));
		for(int i=0;i<72;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o0f5o",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o0f5o[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s93", cols1);
		String [][] records_s93 = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s93[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s93", records_s93[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(222));
		for(int i=0;i<15;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("s93",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_s93[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("e87", cols2);
		String [][] records_e87 = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e87[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e87", records_e87[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(509));
		for(int i=0;i<20;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("e87",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_e87[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e537", cols3);
		String [][] records_e537 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e537[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e537", records_e537[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(9));
		for(int i=0;i<1;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("e537",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_e537[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u46a", cols0);
		String [][] records_u46a = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u46a[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u46a", records_u46a[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(332));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("u46a",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_u46a[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("t1by", cols0);
		String [][] records_t1by = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t1by[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t1by", records_t1by[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(253));
		for(int i=0;i<20;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t1by",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t1by[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("t8", cols1);
		String [][] records_t8 = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t8", records_t8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(71));
		for(int i=0;i<94;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("t8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_t8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("i5m8", cols0);
		String [][] records_i5m8 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i5m8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i5m8", records_i5m8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(198));
		for(int i=0;i<92;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i5m8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i5m8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("cb", cols1);
		String [][] records_cb = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cb", records_cb[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(373));
		for(int i=0;i<22;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("cb",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_cb[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("c2", cols0);
		String [][] records_c2 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c2", records_c2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(389));
		for(int i=0;i<15;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("c2",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_c2[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("qu0es", cols1);
		String [][] records_qu0es = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qu0es[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qu0es", records_qu0es[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(897));
		for(int i=0;i<58;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("qu0es",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_qu0es[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("yq3y", cols2);
		String [][] records_yq3y = new String[63][cols2.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yq3y[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yq3y", records_yq3y[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(465));
		for(int i=0;i<63;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("yq3y",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_yq3y[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("pbo20", cols0);
		String [][] records_pbo20 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pbo20[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pbo20", records_pbo20[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(73));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("pbo20",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_pbo20[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l49b8", cols1);
		String [][] records_l49b8 = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l49b8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l49b8", records_l49b8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(83));
		for(int i=0;i<94;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("l49b8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_l49b8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("js4p", cols2);
		String [][] records_js4p = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_js4p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("js4p", records_js4p[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(45));
		for(int i=0;i<69;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("js4p",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_js4p[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("kx", cols3);
		String [][] records_kx = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_kx[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kx", records_kx[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(985));
		for(int i=0;i<44;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("kx",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_kx[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gr", cols4);
		String [][] records_gr = new String[81][cols4.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_gr[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gr", records_gr[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(585));
		for(int i=0;i<81;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("gr",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_gr[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("k7676", cols0);
		String [][] records_k7676 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k7676[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k7676", records_k7676[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(404));
		for(int i=0;i<77;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("k7676",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_k7676[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m90", cols1);
		String [][] records_m90 = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m90[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m90", records_m90[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(227));
		for(int i=0;i<77;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("m90",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_m90[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("nwm", cols0);
		String [][] records_nwm = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nwm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nwm", records_nwm[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(214));
		for(int i=0;i<65;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("nwm",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_nwm[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("kd37", cols1);
		String [][] records_kd37 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kd37[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kd37", records_kd37[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(532));
		for(int i=0;i<12;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("kd37",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_kd37[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dc5", cols2);
		String [][] records_dc5 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dc5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dc5", records_dc5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(608));
		for(int i=0;i<85;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("dc5",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_dc5[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("a6", cols3);
		String [][] records_a6 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a6", records_a6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(40));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("a6",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_a6[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("g54", cols4);
		String [][] records_g54 = new String[21][cols4.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g54[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g54", records_g54[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(937));
		for(int i=0;i<21;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("g54",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_g54[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i31", cols0);
		String [][] records_i31 = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i31[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i31", records_i31[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(130));
		for(int i=0;i<19;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i31",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i31[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("i2ngw", cols1);
		String [][] records_i2ngw = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i2ngw[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i2ngw", records_i2ngw[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(483));
		for(int i=0;i<47;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("i2ngw",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_i2ngw[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("bsb", cols2);
		String [][] records_bsb = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bsb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bsb", records_bsb[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(831));
		for(int i=0;i<89;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("bsb",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_bsb[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("bom58", cols0);
		String [][] records_bom58 = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bom58[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bom58", records_bom58[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(340));
		for(int i=0;i<13;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("bom58",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_bom58[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xz", cols0);
		String [][] records_xz = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xz", records_xz[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(567));
		for(int i=0;i<98;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xz",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xz[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("q0xy9", cols1);
		String [][] records_q0xy9 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q0xy9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q0xy9", records_q0xy9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(351));
		for(int i=0;i<19;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("q0xy9",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_q0xy9[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hwr", cols2);
		String [][] records_hwr = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hwr[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hwr", records_hwr[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(491));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("hwr",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_hwr[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("h53oh", cols3);
		String [][] records_h53oh = new String[72][cols3.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h53oh[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h53oh", records_h53oh[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(787));
		for(int i=0;i<72;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("h53oh",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_h53oh[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s52", cols0);
		String [][] records_s52 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s52[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s52", records_s52[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(388));
		for(int i=0;i<66;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("s52",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_s52[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("it", cols0);
		String [][] records_it = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_it[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("it", records_it[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(660));
		for(int i=0;i<37;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("it",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_it[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("en39e", cols1);
		String [][] records_en39e = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_en39e[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("en39e", records_en39e[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(908));
		for(int i=0;i<26;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("en39e",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_en39e[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("k241", cols0);
		String [][] records_k241 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k241[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k241", records_k241[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(140));
		for(int i=0;i<25;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("k241",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_k241[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h1", cols1);
		String [][] records_h1 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h1", records_h1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(534));
		for(int i=0;i<89;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("h1",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_h1[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n67g", cols2);
		String [][] records_n67g = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n67g[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n67g", records_n67g[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(155));
		for(int i=0;i<57;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("n67g",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_n67g[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("ev2", cols3);
		String [][] records_ev2 = new String[9][cols3.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ev2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ev2", records_ev2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(390));
		for(int i=0;i<9;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ev2",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ev2[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("myf", cols4);
		String [][] records_myf = new String[26][cols4.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_myf[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("myf", records_myf[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(376));
		for(int i=0;i<26;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("myf",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_myf[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("o304", cols0);
		String [][] records_o304 = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o304[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o304", records_o304[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(696));
		for(int i=0;i<37;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o304",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o304[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("icjd", cols0);
		String [][] records_icjd = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_icjd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("icjd", records_icjd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(688));
		for(int i=0;i<47;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("icjd",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_icjd[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("l639", cols1);
		String [][] records_l639 = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l639[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l639", records_l639[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(708));
		for(int i=0;i<15;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("l639",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_l639[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ep", cols0);
		String [][] records_ep = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ep[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ep", records_ep[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(552));
		for(int i=0;i<28;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ep",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ep[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("lhcg", cols1);
		String [][] records_lhcg = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lhcg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lhcg", records_lhcg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(811));
		for(int i=0;i<57;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("lhcg",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_lhcg[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("i4", cols2);
		String [][] records_i4 = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i4", records_i4[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(924));
		for(int i=0;i<30;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("i4",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_i4[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("r83", cols0);
		String [][] records_r83 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r83[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r83", records_r83[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(299));
		for(int i=0;i<74;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("r83",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_r83[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("x00f3", cols0);
		String [][] records_x00f3 = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x00f3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x00f3", records_x00f3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(884));
		for(int i=0;i<79;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("x00f3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_x00f3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("pmc", cols1);
		String [][] records_pmc = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pmc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pmc", records_pmc[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(164));
		for(int i=0;i<51;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("pmc",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_pmc[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("g5", cols2);
		String [][] records_g5 = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g5", records_g5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(132));
		for(int i=0;i<70;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("g5",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_g5[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ha6b", cols0);
		String [][] records_ha6b = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ha6b[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ha6b", records_ha6b[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(96));
		for(int i=0;i<32;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ha6b",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ha6b[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("m30", cols0);
		String [][] records_m30 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m30[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m30", records_m30[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(842));
		for(int i=0;i<68;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("m30",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_m30[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("gj6", cols0);
		String [][] records_gj6 = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gj6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gj6", records_gj6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(340));
		for(int i=0;i<56;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("gj6",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_gj6[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dr0", cols1);
		String [][] records_dr0 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dr0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dr0", records_dr0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(470));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("dr0",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_dr0[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("g62g", cols0);
		String [][] records_g62g = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g62g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g62g", records_g62g[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(991));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("g62g",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_g62g[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("dc7", cols1);
		String [][] records_dc7 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dc7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dc7", records_dc7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(490));
		for(int i=0;i<35;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("dc7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_dc7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g5r80", cols0);
		String [][] records_g5r80 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g5r80[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g5r80", records_g5r80[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<86; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(614));
		for(int i=0;i<86;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("g5r80",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_g5r80[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("if", cols1);
		String [][] records_if = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_if[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("if", records_if[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(692));
		for(int i=0;i<23;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("if",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_if[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("qs", cols2);
		String [][] records_qs = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qs[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qs", records_qs[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(769));
		for(int i=0;i<5;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("qs",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_qs[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("yb2", cols3);
		String [][] records_yb2 = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_yb2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yb2", records_yb2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(183));
		for(int i=0;i<13;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("yb2",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_yb2[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("i7f", cols4);
		String [][] records_i7f = new String[80][cols4.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_i7f[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i7f", records_i7f[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(718));
		for(int i=0;i<80;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("i7f",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_i7f[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p3174", cols0);
		String [][] records_p3174 = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p3174[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3174", records_p3174[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(771));
		for(int i=0;i<73;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p3174",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p3174[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ok1", cols1);
		String [][] records_ok1 = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ok1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ok1", records_ok1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(923));
		for(int i=0;i<38;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ok1",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ok1[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("m0b", cols2);
		String [][] records_m0b = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m0b[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m0b", records_m0b[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(279));
		for(int i=0;i<99;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("m0b",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_m0b[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("ep", cols3);
		String [][] records_ep = new String[4][cols3.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ep[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ep", records_ep[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(200));
		for(int i=0;i<4;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ep",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ep[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("q3b", cols4);
		String [][] records_q3b = new String[85][cols4.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_q3b[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q3b", records_q3b[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(241));
		for(int i=0;i<85;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("q3b",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_q3b[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p4", cols0);
		String [][] records_p4 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p4", records_p4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(742));
		for(int i=0;i<7;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p4",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p4[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fx", cols0);
		String [][] records_fx = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fx", records_fx[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(619));
		for(int i=0;i<50;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("fx",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_fx[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e6", cols1);
		String [][] records_e6 = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e6", records_e6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(371));
		for(int i=0;i<18;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("e6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_e6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("mk0l7", cols2);
		String [][] records_mk0l7 = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mk0l7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mk0l7", records_mk0l7[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(168));
		for(int i=0;i<61;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("mk0l7",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_mk0l7[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p4", cols3);
		String [][] records_p4 = new String[36][cols3.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p4", records_p4[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(957));
		for(int i=0;i<36;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("p4",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_p4[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("ige", cols4);
		String [][] records_ige = new String[81][cols4.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ige[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ige", records_ige[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(875));
		for(int i=0;i<81;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("ige",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_ige[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("w71", cols0);
		String [][] records_w71 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w71[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w71", records_w71[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(187));
		for(int i=0;i<99;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("w71",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_w71[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("fy5", cols1);
		String [][] records_fy5 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fy5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fy5", records_fy5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(753));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("fy5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_fy5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("mz0", cols2);
		String [][] records_mz0 = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mz0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mz0", records_mz0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(933));
		for(int i=0;i<64;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("mz0",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_mz0[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p1h", cols3);
		String [][] records_p1h = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p1h[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p1h", records_p1h[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(544));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("p1h",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_p1h[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ea5", cols0);
		String [][] records_ea5 = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ea5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ea5", records_ea5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(614));
		for(int i=0;i<90;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ea5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ea5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("us8", cols0);
		String [][] records_us8 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_us8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("us8", records_us8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(230));
		for(int i=0;i<38;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("us8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_us8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z3", cols0);
		String [][] records_z3 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z3", records_z3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(828));
		for(int i=0;i<32;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("z3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_z3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("sth", cols1);
		String [][] records_sth = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sth[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sth", records_sth[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(82));
		for(int i=0;i<5;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("sth",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_sth[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("v1v74", cols2);
		String [][] records_v1v74 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v1v74[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v1v74", records_v1v74[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(53));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("v1v74",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_v1v74[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sl6tz", cols0);
		String [][] records_sl6tz = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sl6tz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sl6tz", records_sl6tz[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(633));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("sl6tz",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_sl6tz[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e5x", cols1);
		String [][] records_e5x = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e5x[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e5x", records_e5x[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(803));
		for(int i=0;i<100;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("e5x",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_e5x[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("iw", cols2);
		String [][] records_iw = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_iw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iw", records_iw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(860));
		for(int i=0;i<19;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("iw",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_iw[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("s4p73", cols3);
		String [][] records_s4p73 = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s4p73[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4p73", records_s4p73[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(671));
		for(int i=0;i<18;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("s4p73",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_s4p73[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("oeyv", cols0);
		String [][] records_oeyv = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_oeyv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oeyv", records_oeyv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(881));
		for(int i=0;i<85;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("oeyv",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_oeyv[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("vj068", cols1);
		String [][] records_vj068 = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vj068[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vj068", records_vj068[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(209));
		for(int i=0;i<62;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("vj068",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_vj068[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("oh8", cols2);
		String [][] records_oh8 = new String[84][cols2.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_oh8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oh8", records_oh8[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(341));
		for(int i=0;i<84;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("oh8",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_oh8[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("ve", cols3);
		String [][] records_ve = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ve[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ve", records_ve[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(738));
		for(int i=0;i<20;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ve",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ve[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g4d8", cols4);
		String [][] records_g4d8 = new String[9][cols4.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g4d8[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g4d8", records_g4d8[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(511));
		for(int i=0;i<9;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("g4d8",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_g4d8[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z2457", cols0);
		String [][] records_z2457 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z2457[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z2457", records_z2457[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(13));
		for(int i=0;i<31;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("z2457",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_z2457[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("jm0", cols1);
		String [][] records_jm0 = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jm0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jm0", records_jm0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(201));
		for(int i=0;i<53;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("jm0",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_jm0[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("fsv", cols0);
		String [][] records_fsv = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fsv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fsv", records_fsv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(13));
		for(int i=0;i<49;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("fsv",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_fsv[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("v1086", cols1);
		String [][] records_v1086 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v1086[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v1086", records_v1086[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(149));
		for(int i=0;i<23;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("v1086",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_v1086[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dx5j", cols2);
		String [][] records_dx5j = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dx5j[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dx5j", records_dx5j[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(146));
		for(int i=0;i<27;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("dx5j",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_dx5j[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("as3", cols3);
		String [][] records_as3 = new String[61][cols3.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_as3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("as3", records_as3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(873));
		for(int i=0;i<61;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("as3",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_as3[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("bf0", cols4);
		String [][] records_bf0 = new String[96][cols4.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_bf0[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bf0", records_bf0[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(492));
		for(int i=0;i<96;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("bf0",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_bf0[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xu479", cols0);
		String [][] records_xu479 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xu479[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xu479", records_xu479[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(932));
		for(int i=0;i<18;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xu479",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xu479[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h0ck7", cols1);
		String [][] records_h0ck7 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h0ck7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h0ck7", records_h0ck7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(526));
		for(int i=0;i<13;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("h0ck7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_h0ck7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i7m", cols2);
		String [][] records_i7m = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i7m[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i7m", records_i7m[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(402));
		for(int i=0;i<14;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("i7m",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_i7m[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("y59o", cols3);
		String [][] records_y59o = new String[57][cols3.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y59o[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y59o", records_y59o[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(162));
		for(int i=0;i<57;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("y59o",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_y59o[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("xdk", cols0);
		String [][] records_xdk = new String[76][cols0.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xdk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xdk", records_xdk[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(240));
		for(int i=0;i<76;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xdk",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xdk[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ko71", cols1);
		String [][] records_ko71 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ko71[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ko71", records_ko71[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(817));
		for(int i=0;i<71;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ko71",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ko71[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("dib3", cols0);
		String [][] records_dib3 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dib3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dib3", records_dib3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(947));
		for(int i=0;i<36;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("dib3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_dib3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("tc", cols1);
		String [][] records_tc = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tc", records_tc[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(85));
		for(int i=0;i<48;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("tc",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_tc[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("pi86", cols2);
		String [][] records_pi86 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pi86[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pi86", records_pi86[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(393));
		for(int i=0;i<48;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("pi86",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_pi86[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e78", cols3);
		String [][] records_e78 = new String[21][cols3.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e78[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e78", records_e78[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(409));
		for(int i=0;i<21;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("e78",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_e78[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("jaq", cols4);
		String [][] records_jaq = new String[13][cols4.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_jaq[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jaq", records_jaq[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(786));
		for(int i=0;i<13;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("jaq",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_jaq[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("bu3", cols0);
		String [][] records_bu3 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bu3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bu3", records_bu3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(653));
		for(int i=0;i<51;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("bu3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_bu3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("lzrl1", cols1);
		String [][] records_lzrl1 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lzrl1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lzrl1", records_lzrl1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(988));
		for(int i=0;i<12;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("lzrl1",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_lzrl1[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("aro", cols0);
		String [][] records_aro = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_aro[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("aro", records_aro[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(710));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("aro",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_aro[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("d5a", cols1);
		String [][] records_d5a = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d5a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d5a", records_d5a[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(105));
		for(int i=0;i<44;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("d5a",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_d5a[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("a7l3", cols2);
		String [][] records_a7l3 = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_a7l3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a7l3", records_a7l3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(13));
		for(int i=0;i<98;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("a7l3",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_a7l3[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("zxz6t", cols3);
		String [][] records_zxz6t = new String[59][cols3.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zxz6t[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zxz6t", records_zxz6t[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(257));
		for(int i=0;i<59;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("zxz6t",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_zxz6t[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("mnn", cols4);
		String [][] records_mnn = new String[1][cols4.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_mnn[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mnn", records_mnn[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(889));
		for(int i=0;i<1;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("mnn",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_mnn[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xcey7", cols0);
		String [][] records_xcey7 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xcey7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xcey7", records_xcey7[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(148));
		for(int i=0;i<23;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xcey7",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xcey7[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("ai1", cols0);
		String [][] records_ai1 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ai1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ai1", records_ai1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(201));
		for(int i=0;i<74;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ai1",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ai1[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p69p", cols1);
		String [][] records_p69p = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p69p[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p69p", records_p69p[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(511));
		for(int i=0;i<4;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("p69p",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_p69p[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vz62q", cols2);
		String [][] records_vz62q = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vz62q[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vz62q", records_vz62q[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(968));
		for(int i=0;i<14;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("vz62q",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_vz62q[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("sne74", cols0);
		String [][] records_sne74 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sne74[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sne74", records_sne74[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(451));
		for(int i=0;i<22;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("sne74",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_sne74[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vh024", cols0);
		String [][] records_vh024 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vh024[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vh024", records_vh024[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(903));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("vh024",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_vh024[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mcleo", cols1);
		String [][] records_mcleo = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mcleo[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mcleo", records_mcleo[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(915));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("mcleo",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_mcleo[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("s67", cols2);
		String [][] records_s67 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s67[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s67", records_s67[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(246));
		for(int i=0;i<73;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("s67",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_s67[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i98i1", cols3);
		String [][] records_i98i1 = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i98i1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i98i1", records_i98i1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(80));
		for(int i=0;i<23;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("i98i1",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_i98i1[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("leh06", cols0);
		String [][] records_leh06 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_leh06[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("leh06", records_leh06[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(37));
		for(int i=0;i<20;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("leh06",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_leh06[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("m4d2", cols1);
		String [][] records_m4d2 = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m4d2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m4d2", records_m4d2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(336));
		for(int i=0;i<29;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("m4d2",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_m4d2[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("xyo2m", cols2);
		String [][] records_xyo2m = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xyo2m[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xyo2m", records_xyo2m[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(185));
		for(int i=0;i<5;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("xyo2m",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_xyo2m[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bxi", cols3);
		String [][] records_bxi = new String[26][cols3.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bxi[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bxi", records_bxi[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(265));
		for(int i=0;i<26;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("bxi",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_bxi[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("vs3", cols4);
		String [][] records_vs3 = new String[88][cols4.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vs3[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vs3", records_vs3[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(388));
		for(int i=0;i<88;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("vs3",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_vs3[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("jf6g", cols0);
		String [][] records_jf6g = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jf6g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jf6g", records_jf6g[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(165));
		for(int i=0;i<49;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("jf6g",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_jf6g[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("obu", cols1);
		String [][] records_obu = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_obu[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("obu", records_obu[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(943));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("obu",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_obu[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("o1", cols2);
		String [][] records_o1 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o1", records_o1[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(800));
		for(int i=0;i<73;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("o1",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_o1[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("g14", cols3);
		String [][] records_g14 = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g14[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g14", records_g14[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(409));
		for(int i=0;i<6;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("g14",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_g14[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("zf84", cols0);
		String [][] records_zf84 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zf84[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zf84", records_zf84[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(817));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("zf84",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_zf84[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i5", cols1);
		String [][] records_i5 = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i5", records_i5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(964));
		for(int i=0;i<64;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("i5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_i5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tvq", cols2);
		String [][] records_tvq = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tvq[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tvq", records_tvq[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(783));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("tvq",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_tvq[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wvh", cols3);
		String [][] records_wvh = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wvh[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wvh", records_wvh[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(133));
		for(int i=0;i<52;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("wvh",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_wvh[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ay212", cols4);
		String [][] records_ay212 = new String[33][cols4.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ay212[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ay212", records_ay212[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(195));
		for(int i=0;i<33;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("ay212",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_ay212[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("v0j64", cols0);
		String [][] records_v0j64 = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v0j64[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v0j64", records_v0j64[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(83));
		for(int i=0;i<56;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("v0j64",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_v0j64[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("aqv", cols1);
		String [][] records_aqv = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_aqv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("aqv", records_aqv[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(703));
		for(int i=0;i<94;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("aqv",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_aqv[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b9", cols0);
		String [][] records_b9 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<86; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(142));
		for(int i=0;i<86;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("b9",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_b9[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("t6nj", cols1);
		String [][] records_t6nj = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t6nj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6nj", records_t6nj[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(216));
		for(int i=0;i<12;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("t6nj",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_t6nj[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("m64", cols2);
		String [][] records_m64 = new String[31][cols2.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m64[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m64", records_m64[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(882));
		for(int i=0;i<31;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("m64",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_m64[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("ny2", cols3);
		String [][] records_ny2 = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ny2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ny2", records_ny2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(955));
		for(int i=0;i<34;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ny2",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ny2[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xy64", cols4);
		String [][] records_xy64 = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xy64[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xy64", records_xy64[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(981));
		for(int i=0;i<10;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("xy64",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_xy64[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("mmm", cols0);
		String [][] records_mmm = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mmm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mmm", records_mmm[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(699));
		for(int i=0;i<96;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("mmm",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_mmm[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("rn", cols1);
		String [][] records_rn = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rn", records_rn[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(126));
		for(int i=0;i<53;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("rn",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_rn[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("fz3", cols2);
		String [][] records_fz3 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_fz3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fz3", records_fz3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(667));
		for(int i=0;i<32;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("fz3",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_fz3[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b2ey", cols3);
		String [][] records_b2ey = new String[76][cols3.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b2ey[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b2ey", records_b2ey[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(797));
		for(int i=0;i<76;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("b2ey",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_b2ey[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p71", cols0);
		String [][] records_p71 = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p71[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p71", records_p71[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(385));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("p71",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_p71[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("z40", cols1);
		String [][] records_z40 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z40[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z40", records_z40[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(740));
		for(int i=0;i<71;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("z40",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_z40[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o462j", cols0);
		String [][] records_o462j = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o462j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o462j", records_o462j[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(729));
		for(int i=0;i<51;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o462j",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o462j[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hh6", cols1);
		String [][] records_hh6 = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hh6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hh6", records_hh6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(815));
		for(int i=0;i<54;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("hh6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_hh6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yu163", cols0);
		String [][] records_yu163 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yu163[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yu163", records_yu163[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(481));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("yu163",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_yu163[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("e3f", cols1);
		String [][] records_e3f = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e3f[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e3f", records_e3f[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(17));
		for(int i=0;i<54;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("e3f",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_e3f[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hu90", cols2);
		String [][] records_hu90 = new String[93][cols2.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hu90[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hu90", records_hu90[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(273));
		for(int i=0;i<93;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("hu90",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_hu90[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("i3k6", cols0);
		String [][] records_i3k6 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i3k6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i3k6", records_i3k6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(716));
		for(int i=0;i<31;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i3k6",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i3k6[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("d8f1e", cols1);
		String [][] records_d8f1e = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d8f1e[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d8f1e", records_d8f1e[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(246));
		for(int i=0;i<45;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("d8f1e",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_d8f1e[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("yh4", cols0);
		String [][] records_yh4 = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yh4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yh4", records_yh4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(314));
		for(int i=0;i<97;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("yh4",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_yh4[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("tg806", cols1);
		String [][] records_tg806 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tg806[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tg806", records_tg806[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(280));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("tg806",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_tg806[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("qeb", cols2);
		String [][] records_qeb = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qeb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qeb", records_qeb[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<16; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(461));
		for(int i=0;i<16;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("qeb",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_qeb[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("d1", cols3);
		String [][] records_d1 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d1", records_d1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(715));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("d1",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_d1[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("h35c8", cols0);
		String [][] records_h35c8 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h35c8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h35c8", records_h35c8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(261));
		for(int i=0;i<77;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("h35c8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_h35c8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ov", cols1);
		String [][] records_ov = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ov[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ov", records_ov[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(213));
		for(int i=0;i<48;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ov",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ov[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("eb", cols2);
		String [][] records_eb = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_eb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eb", records_eb[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(550));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("eb",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_eb[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("eitn5", cols3);
		String [][] records_eitn5 = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_eitn5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eitn5", records_eitn5[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(953));
		for(int i=0;i<73;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("eitn5",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_eitn5[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k338", cols4);
		String [][] records_k338 = new String[40][cols4.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_k338[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k338", records_k338[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(452));
		for(int i=0;i<40;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("k338",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_k338[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("l2", cols0);
		String [][] records_l2 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l2", records_l2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(695));
		for(int i=0;i<2;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l2",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("pa122", cols0);
		String [][] records_pa122 = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pa122[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pa122", records_pa122[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(478));
		for(int i=0;i<54;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("pa122",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("pa122");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v9tj", cols1);
		String [][] records_v9tj = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v9tj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v9tj", records_v9tj[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(826));
		for(int i=0;i<5;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("v9tj",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("v9tj");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("kh6", cols2);
		String [][] records_kh6 = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kh6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kh6", records_kh6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(251));
		for(int i=0;i<42;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("kh6",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("kh6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("o6", cols0);
		String [][] records_o6 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o6", records_o6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(735));
		for(int i=0;i<5;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("o6",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("o6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lq", cols1);
		String [][] records_lq = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lq", records_lq[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(24));
		for(int i=0;i<49;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("lq",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("lq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("m8h47", cols2);
		String [][] records_m8h47 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m8h47[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m8h47", records_m8h47[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(193));
		for(int i=0;i<7;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("m8h47",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("m8h47");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("md33", cols0);
		String [][] records_md33 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_md33[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("md33", records_md33[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(861));
		for(int i=0;i<47;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("md33",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("md33");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qty", cols1);
		String [][] records_qty = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qty[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qty", records_qty[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(650));
		for(int i=0;i<83;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("qty",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("qty");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("dv237", cols0);
		String [][] records_dv237 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dv237[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dv237", records_dv237[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(175));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("dv237",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("dv237");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("tzx33", cols1);
		String [][] records_tzx33 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tzx33[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tzx33", records_tzx33[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(605));
		for(int i=0;i<12;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("tzx33",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("tzx33");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l57", cols0);
		String [][] records_l57 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l57[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l57", records_l57[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(640));
		for(int i=0;i<5;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l57",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l57");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("xq5", cols1);
		String [][] records_xq5 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xq5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xq5", records_xq5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(400));
		for(int i=0;i<13;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("xq5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("xq5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("rk5u", cols0);
		String [][] records_rk5u = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rk5u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rk5u", records_rk5u[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(495));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("rk5u",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("rk5u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("vd5", cols1);
		String [][] records_vd5 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vd5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vd5", records_vd5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(125));
		for(int i=0;i<92;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("vd5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("vd5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("p76", cols0);
		String [][] records_p76 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p76[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p76", records_p76[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(628));
		for(int i=0;i<80;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("p76",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("p76");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("b1mp", cols0);
		String [][] records_b1mp = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b1mp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b1mp", records_b1mp[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(426));
		for(int i=0;i<96;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("b1mp",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("b1mp");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jg9", cols1);
		String [][] records_jg9 = new String[61][cols1.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jg9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jg9", records_jg9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(874));
		for(int i=0;i<61;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("jg9",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("jg9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("uka", cols2);
		String [][] records_uka = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uka[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uka", records_uka[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(362));
		for(int i=0;i<35;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("uka",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("uka");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y6f", cols3);
		String [][] records_y6f = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y6f[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y6f", records_y6f[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(195));
		for(int i=0;i<38;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("y6f",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("y6f");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("cfn", cols0);
		String [][] records_cfn = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cfn[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cfn", records_cfn[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(422));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("cfn",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("cfn");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("o08", cols0);
		String [][] records_o08 = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o08[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o08", records_o08[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(313));
		for(int i=0;i<59;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("o08",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("o08");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("ph9", cols1);
		String [][] records_ph9 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ph9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ph9", records_ph9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(930));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ph9",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ph9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("b987", cols2);
		String [][] records_b987 = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b987[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b987", records_b987[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(768));
		for(int i=0;i<47;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("b987",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("b987");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("qn7", cols3);
		String [][] records_qn7 = new String[35][cols3.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_qn7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qn7", records_qn7[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(786));
		for(int i=0;i<35;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("qn7",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("qn7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v76", cols4);
		String [][] records_v76 = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_v76[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v76", records_v76[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(314));
		for(int i=0;i<10;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("v76",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("v76");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("l1e", cols0);
		String [][] records_l1e = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l1e[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l1e", records_l1e[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(803));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l1e",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l1e");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("d4", cols0);
		String [][] records_d4 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d4", records_d4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(49));
		for(int i=0;i<35;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("ec9", cols1);
		String [][] records_ec9 = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ec9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ec9", records_ec9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(151));
		for(int i=0;i<48;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ec9",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ec9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("p71", cols0);
		String [][] records_p71 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p71[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p71", records_p71[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(153));
		for(int i=0;i<23;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("p71",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("p71");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("c3fk", cols1);
		String [][] records_c3fk = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c3fk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c3fk", records_c3fk[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(93));
		for(int i=0;i<14;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("c3fk",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("c3fk");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("sa3", cols2);
		String [][] records_sa3 = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_sa3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sa3", records_sa3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(456));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("sa3",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("sa3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m0x", cols3);
		String [][] records_m0x = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m0x[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m0x", records_m0x[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(944));
		for(int i=0;i<70;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("m0x",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("m0x");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("sr5", cols4);
		String [][] records_sr5 = new String[57][cols4.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_sr5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sr5", records_sr5[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(486));
		for(int i=0;i<57;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("sr5",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("sr5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("so55", cols0);
		String [][] records_so55 = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_so55[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("so55", records_so55[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(698));
		for(int i=0;i<42;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("so55",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("so55");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xbl", cols1);
		String [][] records_xbl = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xbl[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xbl", records_xbl[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(521));
		for(int i=0;i<53;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("xbl",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("xbl");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zx7gw", cols2);
		String [][] records_zx7gw = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zx7gw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zx7gw", records_zx7gw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(969));
		for(int i=0;i<60;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("zx7gw",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("zx7gw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("e4t", cols3);
		String [][] records_e4t = new String[35][cols3.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e4t[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e4t", records_e4t[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(499));
		for(int i=0;i<35;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("e4t",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("e4t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("by8", cols0);
		String [][] records_by8 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_by8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("by8", records_by8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(790));
		for(int i=0;i<91;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("by8",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("by8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y4a8", cols0);
		String [][] records_y4a8 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y4a8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y4a8", records_y4a8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(789));
		for(int i=0;i<77;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("y4a8",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("y4a8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xn", cols1);
		String [][] records_xn = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xn", records_xn[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(124));
		for(int i=0;i<82;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("xn",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("xn");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d33", cols2);
		String [][] records_d33 = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d33[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d33", records_d33[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(34));
		for(int i=0;i<18;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("d33",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("d33");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("c1", cols0);
		String [][] records_c1 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c1", records_c1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(185));
		for(int i=0;i<65;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("c1",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("c1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("he893", cols1);
		String [][] records_he893 = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_he893[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("he893", records_he893[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(168));
		for(int i=0;i<83;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("he893",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("he893");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("m7w", cols2);
		String [][] records_m7w = new String[43][cols2.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m7w[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m7w", records_m7w[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<43; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(317));
		for(int i=0;i<43;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("m7w",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("m7w");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j0", cols3);
		String [][] records_j0 = new String[72][cols3.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j0[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j0", records_j0[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(839));
		for(int i=0;i<72;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("j0",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("j0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kd6u", cols4);
		String [][] records_kd6u = new String[84][cols4.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_kd6u[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kd6u", records_kd6u[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(633));
		for(int i=0;i<84;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("kd6u",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("kd6u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o6q", cols0);
		String [][] records_o6q = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o6q[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o6q", records_o6q[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(725));
		for(int i=0;i<2;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("o6q",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("o6q");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vda", cols1);
		String [][] records_vda = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vda[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vda", records_vda[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(143));
		for(int i=0;i<39;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("vda",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("vda");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ksvq", cols0);
		String [][] records_ksvq = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ksvq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ksvq", records_ksvq[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(570));
		for(int i=0;i<54;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ksvq",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ksvq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("azto", cols0);
		String [][] records_azto = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_azto[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("azto", records_azto[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(590));
		for(int i=0;i<98;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("azto",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("azto");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w7", cols1);
		String [][] records_w7 = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w7", records_w7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(255));
		for(int i=0;i<77;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("w7",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("w7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z2", cols2);
		String [][] records_z2 = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z2", records_z2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(545));
		for(int i=0;i<56;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("z2",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("z2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p3", cols3);
		String [][] records_p3 = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3", records_p3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(220));
		for(int i=0;i<15;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("p3",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("p3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ivbj", cols0);
		String [][] records_ivbj = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ivbj[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ivbj", records_ivbj[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(948));
		for(int i=0;i<51;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ivbj",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ivbj");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p67", cols1);
		String [][] records_p67 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p67[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p67", records_p67[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(4));
		for(int i=0;i<89;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("p67",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("p67");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zub5", cols2);
		String [][] records_zub5 = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zub5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zub5", records_zub5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(56));
		for(int i=0;i<5;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("zub5",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("zub5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("sn03", cols0);
		String [][] records_sn03 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sn03[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sn03", records_sn03[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(189));
		for(int i=0;i<31;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sn03",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sn03");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("sp", cols1);
		String [][] records_sp = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sp[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sp", records_sp[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(46));
		for(int i=0;i<62;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("sp",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("sp");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("y0", cols2);
		String [][] records_y0 = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y0", records_y0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(918));
		for(int i=0;i<5;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("y0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("y0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("d6s", cols0);
		String [][] records_d6s = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d6s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d6s", records_d6s[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(74));
		for(int i=0;i<6;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d6s",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d6s");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("d47s0", cols1);
		String [][] records_d47s0 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d47s0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d47s0", records_d47s0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(422));
		for(int i=0;i<35;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("d47s0",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("d47s0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("yc", cols2);
		String [][] records_yc = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yc[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yc", records_yc[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(775));
		for(int i=0;i<99;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("yc",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("yc");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("vaxa", cols3);
		String [][] records_vaxa = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vaxa[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vaxa", records_vaxa[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(676));
		for(int i=0;i<56;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("vaxa",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("vaxa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("xm58", cols0);
		String [][] records_xm58 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xm58[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xm58", records_xm58[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(745));
		for(int i=0;i<50;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("xm58",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("xm58");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mrht3", cols1);
		String [][] records_mrht3 = new String[65][cols1.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mrht3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mrht3", records_mrht3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(193));
		for(int i=0;i<65;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("mrht3",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("mrht3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p6", cols2);
		String [][] records_p6 = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p6", records_p6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(800));
		for(int i=0;i<12;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("p6",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("p6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("c33", cols3);
		String [][] records_c33 = new String[26][cols3.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c33[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c33", records_c33[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(890));
		for(int i=0;i<26;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("c33",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("c33");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("c2c39", cols0);
		String [][] records_c2c39 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c2c39[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c2c39", records_c2c39[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<16; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(856));
		for(int i=0;i<16;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("c2c39",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("c2c39");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ws6tr", cols1);
		String [][] records_ws6tr = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ws6tr[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ws6tr", records_ws6tr[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(496));
		for(int i=0;i<72;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ws6tr",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ws6tr");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("iob", cols2);
		String [][] records_iob = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_iob[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iob", records_iob[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(301));
		for(int i=0;i<45;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("iob",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("iob");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tux3", cols0);
		String [][] records_tux3 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tux3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tux3", records_tux3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(766));
		for(int i=0;i<18;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("tux3",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("tux3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("c67", cols1);
		String [][] records_c67 = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c67[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c67", records_c67[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(570));
		for(int i=0;i<63;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("c67",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("c67");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("yq4", cols2);
		String [][] records_yq4 = new String[80][cols2.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yq4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yq4", records_yq4[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(242));
		for(int i=0;i<80;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("yq4",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("yq4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("bzby5", cols3);
		String [][] records_bzby5 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bzby5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bzby5", records_bzby5[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(215));
		for(int i=0;i<44;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("bzby5",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("bzby5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u6m1", cols4);
		String [][] records_u6m1 = new String[15][cols4.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_u6m1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u6m1", records_u6m1[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(721));
		for(int i=0;i<15;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("u6m1",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("u6m1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vgq", cols0);
		String [][] records_vgq = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vgq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vgq", records_vgq[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(180));
		for(int i=0;i<36;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("vgq",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("vgq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("o45o", cols1);
		String [][] records_o45o = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o45o[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o45o", records_o45o[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(517));
		for(int i=0;i<14;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("o45o",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("o45o");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("i0", cols2);
		String [][] records_i0 = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i0", records_i0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(232));
		for(int i=0;i<75;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("csm", cols0);
		String [][] records_csm = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_csm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("csm", records_csm[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(325));
		for(int i=0;i<60;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("csm",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("csm");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("jnd", cols0);
		String [][] records_jnd = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jnd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jnd", records_jnd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(182));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("jnd",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("jnd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("awa", cols0);
		String [][] records_awa = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_awa[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("awa", records_awa[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(888));
		for(int i=0;i<17;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("awa",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("awa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("vqa", cols1);
		String [][] records_vqa = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vqa[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vqa", records_vqa[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(887));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("vqa",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("vqa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("wn14", cols2);
		String [][] records_wn14 = new String[10][cols2.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wn14[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wn14", records_wn14[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(833));
		for(int i=0;i<10;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("wn14",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("wn14");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("f64l6", cols3);
		String [][] records_f64l6 = new String[17][cols3.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f64l6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f64l6", records_f64l6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(992));
		for(int i=0;i<17;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("f64l6",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("f64l6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ac5", cols0);
		String [][] records_ac5 = new String[94][cols0.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ac5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ac5", records_ac5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(300));
		for(int i=0;i<94;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ac5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ac5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("dik", cols0);
		String [][] records_dik = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dik[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dik", records_dik[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(661));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("dik",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("dik");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("rt5", cols0);
		String [][] records_rt5 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rt5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rt5", records_rt5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(855));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("rt5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("rt5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("e16", cols1);
		String [][] records_e16 = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e16[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e16", records_e16[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(89));
		for(int i=0;i<96;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("e16",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("e16");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("n84k5", cols0);
		String [][] records_n84k5 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n84k5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n84k5", records_n84k5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(719));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("n84k5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("n84k5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r8m0", cols1);
		String [][] records_r8m0 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r8m0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8m0", records_r8m0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(788));
		for(int i=0;i<56;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r8m0",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r8m0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a7m3z", cols0);
		String [][] records_a7m3z = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a7m3z[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a7m3z", records_a7m3z[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(218));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("a7m3z",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("a7m3z");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h8d", cols1);
		String [][] records_h8d = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h8d[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h8d", records_h8d[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(579));
		for(int i=0;i<8;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("h8d",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("h8d");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("omv", cols2);
		String [][] records_omv = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_omv[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("omv", records_omv[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(179));
		for(int i=0;i<61;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("omv",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("omv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("i9", cols3);
		String [][] records_i9 = new String[29][cols3.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(202));
		for(int i=0;i<29;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("i9",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("i9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("tt0", cols0);
		String [][] records_tt0 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tt0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tt0", records_tt0[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(915));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("tt0",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("tt0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kr2t", cols1);
		String [][] records_kr2t = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kr2t[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kr2t", records_kr2t[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(527));
		for(int i=0;i<13;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("kr2t",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("kr2t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("z6", cols2);
		String [][] records_z6 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z6", records_z6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(473));
		for(int i=0;i<51;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("z6",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("z6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("z0", cols3);
		String [][] records_z0 = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z0[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z0", records_z0[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(327));
		for(int i=0;i<65;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("z0",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("z0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dtj0t", cols4);
		String [][] records_dtj0t = new String[40][cols4.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_dtj0t[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dtj0t", records_dtj0t[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(804));
		for(int i=0;i<40;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("dtj0t",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("dtj0t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("a8", cols0);
		String [][] records_a8 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a8", records_a8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<43; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(336));
		for(int i=0;i<43;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("a8",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("a8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wkb", cols0);
		String [][] records_wkb = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wkb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wkb", records_wkb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(877));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("wkb",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("wkb");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("dx", cols1);
		String [][] records_dx = new String[28][cols1.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dx", records_dx[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(423));
		for(int i=0;i<28;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("dx",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("dx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("i406o", cols2);
		String [][] records_i406o = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i406o[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i406o", records_i406o[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(287));
		for(int i=0;i<69;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i406o",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i406o");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ztdl", cols3);
		String [][] records_ztdl = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ztdl[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ztdl", records_ztdl[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(90));
		for(int i=0;i<83;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ztdl",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ztdl");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("c9y", cols4);
		String [][] records_c9y = new String[54][cols4.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_c9y[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c9y", records_c9y[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(538));
		for(int i=0;i<54;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("c9y",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("c9y");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d1k6h", cols0);
		String [][] records_d1k6h = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d1k6h[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d1k6h", records_d1k6h[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(465));
		for(int i=0;i<83;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d1k6h",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d1k6h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fv", cols0);
		String [][] records_fv = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fv", records_fv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(976));
		for(int i=0;i<4;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("fv",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("fv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("d1h", cols0);
		String [][] records_d1h = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d1h[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d1h", records_d1h[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(508));
		for(int i=0;i<38;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d1h",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d1h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("ntf", cols1);
		String [][] records_ntf = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ntf[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ntf", records_ntf[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(562));
		for(int i=0;i<78;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ntf",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ntf");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y29", cols2);
		String [][] records_y29 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y29[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y29", records_y29[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(231));
		for(int i=0;i<88;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("y29",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("y29");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wq0r1", cols3);
		String [][] records_wq0r1 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wq0r1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wq0r1", records_wq0r1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(239));
		for(int i=0;i<44;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("wq0r1",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("wq0r1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("aa2jv", cols4);
		String [][] records_aa2jv = new String[42][cols4.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_aa2jv[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("aa2jv", records_aa2jv[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(870));
		for(int i=0;i<42;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("aa2jv",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("aa2jv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("cpg", cols0);
		String [][] records_cpg = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cpg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cpg", records_cpg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(554));
		for(int i=0;i<72;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("cpg",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("cpg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("l97", cols0);
		String [][] records_l97 = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l97[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l97", records_l97[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(309));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l97",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l97");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("mfk", cols0);
		String [][] records_mfk = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mfk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mfk", records_mfk[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(170));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("mfk",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("mfk");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l2la", cols0);
		String [][] records_l2la = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l2la[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l2la", records_l2la[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(59));
		for(int i=0;i<57;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l2la",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l2la");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("bkrt", cols0);
		String [][] records_bkrt = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bkrt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bkrt", records_bkrt[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(899));
		for(int i=0;i<88;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("bkrt",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("bkrt");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("lnw5", cols1);
		String [][] records_lnw5 = new String[52][cols1.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lnw5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lnw5", records_lnw5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(562));
		for(int i=0;i<52;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("lnw5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("lnw5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g4h", cols2);
		String [][] records_g4h = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g4h[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g4h", records_g4h[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(965));
		for(int i=0;i<45;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("g4h",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("g4h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("xy43r", cols3);
		String [][] records_xy43r = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xy43r[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xy43r", records_xy43r[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(98));
		for(int i=0;i<24;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("xy43r",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("xy43r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("q9n", cols4);
		String [][] records_q9n = new String[61][cols4.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_q9n[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q9n", records_q9n[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(270));
		for(int i=0;i<61;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("q9n",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("q9n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r6h", cols0);
		String [][] records_r6h = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r6h[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r6h", records_r6h[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(340));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("r6h",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("r6h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("m62n", cols1);
		String [][] records_m62n = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m62n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m62n", records_m62n[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(755));
		for(int i=0;i<38;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("m62n",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("m62n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("z968s", cols0);
		String [][] records_z968s = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z968s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z968s", records_z968s[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(497));
		for(int i=0;i<42;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("z968s",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("z968s");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("ve", cols1);
		String [][] records_ve = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ve[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ve", records_ve[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<41; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(209));
		for(int i=0;i<41;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ve",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ve");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("jix4", cols0);
		String [][] records_jix4 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jix4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jix4", records_jix4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(485));
		for(int i=0;i<58;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("jix4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("jix4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("u2eu4", cols1);
		String [][] records_u2eu4 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u2eu4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u2eu4", records_u2eu4[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(622));
		for(int i=0;i<19;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("u2eu4",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("u2eu4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("aqn", cols0);
		String [][] records_aqn = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_aqn[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("aqn", records_aqn[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(442));
		for(int i=0;i<25;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("aqn",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("aqn");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("k2f", cols1);
		String [][] records_k2f = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k2f[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2f", records_k2f[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(956));
		for(int i=0;i<73;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("k2f",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("k2f");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p5n", cols2);
		String [][] records_p5n = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p5n[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p5n", records_p5n[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(461));
		for(int i=0;i<64;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("p5n",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("p5n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("sf", cols0);
		String [][] records_sf = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sf[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sf", records_sf[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(279));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sf",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sf");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("u8y", cols1);
		String [][] records_u8y = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u8y[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u8y", records_u8y[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(588));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("u8y",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("u8y");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ewi2", cols2);
		String [][] records_ewi2 = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ewi2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ewi2", records_ewi2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(705));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("ewi2",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("ewi2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i155", cols0);
		String [][] records_i155 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i155[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i155", records_i155[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(495));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("i155",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("i155");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ulv3", cols0);
		String [][] records_ulv3 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ulv3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ulv3", records_ulv3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(414));
		for(int i=0;i<46;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ulv3",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ulv3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k95", cols1);
		String [][] records_k95 = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k95[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k95", records_k95[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(997));
		for(int i=0;i<50;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("k95",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("k95");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xs37q", cols0);
		String [][] records_xs37q = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xs37q[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xs37q", records_xs37q[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(918));
		for(int i=0;i<66;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("xs37q",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("xs37q");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("r45q7", cols1);
		String [][] records_r45q7 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r45q7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r45q7", records_r45q7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(669));
		for(int i=0;i<12;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r45q7",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r45q7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("y0", cols2);
		String [][] records_y0 = new String[93][cols2.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y0", records_y0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(339));
		for(int i=0;i<93;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("y0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("y0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("psh", cols3);
		String [][] records_psh = new String[9][cols3.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_psh[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("psh", records_psh[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(707));
		for(int i=0;i<9;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("psh",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("psh");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w5cw", cols0);
		String [][] records_w5cw = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w5cw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w5cw", records_w5cw[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(608));
		for(int i=0;i<3;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("w5cw",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("w5cw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vg8i", cols1);
		String [][] records_vg8i = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vg8i[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vg8i", records_vg8i[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(809));
		for(int i=0;i<34;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("vg8i",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("vg8i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wh7r", cols2);
		String [][] records_wh7r = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wh7r[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wh7r", records_wh7r[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(803));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("wh7r",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("wh7r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("oj8", cols3);
		String [][] records_oj8 = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_oj8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oj8", records_oj8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(105));
		for(int i=0;i<33;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("oj8",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("oj8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(801);
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