package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests07
{

	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("m24", cols0);
		String [][] records_m24 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m24[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m24", records_m24[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(990));
		for(int i=0;i<38;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("m24",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("m24");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("n9y34", cols0);
		String [][] records_n9y34 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n9y34[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9y34", records_n9y34[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(600));
		for(int i=0;i<48;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("n9y34",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("n9y34");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("o92", cols1);
		String [][] records_o92 = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o92[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o92", records_o92[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(236));
		for(int i=0;i<62;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("o92",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("o92");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s1mn9", cols0);
		String [][] records_s1mn9 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s1mn9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s1mn9", records_s1mn9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(273));
		for(int i=0;i<99;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("s1mn9",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("s1mn9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u34g", cols1);
		String [][] records_u34g = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u34g[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u34g", records_u34g[i]);
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
			DBApp.select("u34g",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("u34g");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("k597", cols2);
		String [][] records_k597 = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k597[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k597", records_k597[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(473));
		for(int i=0;i<72;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("k597",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("k597");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("t22m", cols3);
		String [][] records_t22m = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t22m[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t22m", records_t22m[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<16; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(996));
		for(int i=0;i<16;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("t22m",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("t22m");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("r6", cols0);
		String [][] records_r6 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r6", records_r6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(75));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("r6",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("r6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("rs", cols0);
		String [][] records_rs = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rs[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rs", records_rs[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(624));
		for(int i=0;i<54;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("rs",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("rs");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h47i", cols0);
		String [][] records_h47i = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h47i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h47i", records_h47i[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(189));
		for(int i=0;i<85;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("h47i",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("h47i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("bogv", cols1);
		String [][] records_bogv = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bogv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bogv", records_bogv[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(74));
		for(int i=0;i<38;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("bogv",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("bogv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("b59", cols0);
		String [][] records_b59 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b59[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b59", records_b59[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(438));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("b59",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("b59");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("og9b", cols1);
		String [][] records_og9b = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_og9b[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("og9b", records_og9b[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(849));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("og9b",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("og9b");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("i9", cols2);
		String [][] records_i9 = new String[39][cols2.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(621));
		for(int i=0;i<39;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i9",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("ki", cols3);
		String [][] records_ki = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ki[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ki", records_ki[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(908));
		for(int i=0;i<52;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ki",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ki");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sumx5", cols4);
		String [][] records_sumx5 = new String[92][cols4.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_sumx5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sumx5", records_sumx5[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(471));
		for(int i=0;i<92;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("sumx5",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("sumx5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("u0d0", cols0);
		String [][] records_u0d0 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u0d0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u0d0", records_u0d0[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(255));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u0d0",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u0d0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a2c", cols0);
		String [][] records_a2c = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a2c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a2c", records_a2c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(321));
		for(int i=0;i<62;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("a2c",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("a2c");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ay2c", cols1);
		String [][] records_ay2c = new String[95][cols1.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ay2c[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ay2c", records_ay2c[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(861));
		for(int i=0;i<95;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ay2c",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ay2c");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jy1", cols2);
		String [][] records_jy1 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jy1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jy1", records_jy1[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(809));
		for(int i=0;i<48;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("jy1",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("jy1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x2s", cols3);
		String [][] records_x2s = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x2s[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x2s", records_x2s[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(3));
		for(int i=0;i<70;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("x2s",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("x2s");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("sr3y1", cols0);
		String [][] records_sr3y1 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sr3y1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sr3y1", records_sr3y1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(973));
		for(int i=0;i<17;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sr3y1",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sr3y1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("bv6n", cols1);
		String [][] records_bv6n = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bv6n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bv6n", records_bv6n[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(904));
		for(int i=0;i<63;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("bv6n",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("bv6n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hu2", cols2);
		String [][] records_hu2 = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hu2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hu2", records_hu2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(8));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("hu2",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("hu2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mt5", cols0);
		String [][] records_mt5 = new String[94][cols0.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mt5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mt5", records_mt5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(100));
		for(int i=0;i<94;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("mt5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("mt5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o47c8", cols1);
		String [][] records_o47c8 = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o47c8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o47c8", records_o47c8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(349));
		for(int i=0;i<7;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("o47c8",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("o47c8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v8gn", cols2);
		String [][] records_v8gn = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v8gn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v8gn", records_v8gn[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(295));
		for(int i=0;i<22;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("v8gn",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("v8gn");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("z8he", cols3);
		String [][] records_z8he = new String[76][cols3.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z8he[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z8he", records_z8he[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(325));
		for(int i=0;i<76;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("z8he",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("z8he");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("sgdgz", cols0);
		String [][] records_sgdgz = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sgdgz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sgdgz", records_sgdgz[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(942));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sgdgz",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sgdgz");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ey1", cols1);
		String [][] records_ey1 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ey1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ey1", records_ey1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(126));
		for(int i=0;i<49;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ey1",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ey1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("u35", cols0);
		String [][] records_u35 = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u35[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u35", records_u35[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(511));
		for(int i=0;i<69;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u35",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u35");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f95", cols1);
		String [][] records_f95 = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f95[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f95", records_f95[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(902));
		for(int i=0;i<50;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("f95",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("f95");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("nj5", cols2);
		String [][] records_nj5 = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_nj5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nj5", records_nj5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(883));
		for(int i=0;i<58;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("nj5",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("nj5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k0d17", cols3);
		String [][] records_k0d17 = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k0d17[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k0d17", records_k0d17[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(334));
		for(int i=0;i<50;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("k0d17",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("k0d17");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("wc10", cols4);
		String [][] records_wc10 = new String[42][cols4.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_wc10[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wc10", records_wc10[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(65));
		for(int i=0;i<42;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("wc10",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("wc10");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("te0", cols0);
		String [][] records_te0 = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_te0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("te0", records_te0[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(336));
		for(int i=0;i<61;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("te0",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("te0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t20", cols1);
		String [][] records_t20 = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t20[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t20", records_t20[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(731));
		for(int i=0;i<84;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("t20",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("t20");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("yzw", cols0);
		String [][] records_yzw = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yzw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yzw", records_yzw[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(582));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("yzw",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("yzw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ykl", cols1);
		String [][] records_ykl = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ykl[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ykl", records_ykl[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(322));
		for(int i=0;i<75;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ykl",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ykl");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("nuc", cols2);
		String [][] records_nuc = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_nuc[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nuc", records_nuc[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(50));
		for(int i=0;i<12;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("nuc",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("nuc");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z39k1", cols3);
		String [][] records_z39k1 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z39k1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z39k1", records_z39k1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(766));
		for(int i=0;i<44;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("z39k1",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("z39k1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("atp34", cols0);
		String [][] records_atp34 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_atp34[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("atp34", records_atp34[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(447));
		for(int i=0;i<39;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("atp34",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("atp34");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r9dg", cols1);
		String [][] records_r9dg = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r9dg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r9dg", records_r9dg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<87; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(867));
		for(int i=0;i<87;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r9dg",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r9dg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("pk", cols2);
		String [][] records_pk = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pk[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pk", records_pk[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(704));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("pk",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("pk");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("vxku6", cols3);
		String [][] records_vxku6 = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vxku6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vxku6", records_vxku6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(517));
		for(int i=0;i<24;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("vxku6",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("vxku6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("z5n", cols0);
		String [][] records_z5n = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z5n[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z5n", records_z5n[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(65));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("z5n",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("z5n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("g4l1r", cols1);
		String [][] records_g4l1r = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g4l1r[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g4l1r", records_g4l1r[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(934));
		for(int i=0;i<11;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("g4l1r",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("g4l1r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("zl2", cols2);
		String [][] records_zl2 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zl2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zl2", records_zl2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(14));
		for(int i=0;i<73;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("zl2",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("zl2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ime", cols3);
		String [][] records_ime = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ime[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ime", records_ime[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(420));
		for(int i=0;i<66;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ime",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ime");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("vpn4q", cols4);
		String [][] records_vpn4q = new String[6][cols4.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vpn4q[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vpn4q", records_vpn4q[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(384));
		for(int i=0;i<6;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("vpn4q",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("vpn4q");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ty", cols0);
		String [][] records_ty = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ty[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ty", records_ty[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(909));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ty",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ty");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w5v95", cols1);
		String [][] records_w5v95 = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w5v95[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w5v95", records_w5v95[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(994));
		for(int i=0;i<77;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("w5v95",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("w5v95");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l24", cols2);
		String [][] records_l24 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l24[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l24", records_l24[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(982));
		for(int i=0;i<89;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("l24",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("l24");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h7l", cols3);
		String [][] records_h7l = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h7l[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h7l", records_h7l[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(290));
		for(int i=0;i<66;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("h7l",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("h7l");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("om", cols4);
		String [][] records_om = new String[19][cols4.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_om[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("om", records_om[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(564));
		for(int i=0;i<19;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("om",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("om");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("zk3", cols0);
		String [][] records_zk3 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zk3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zk3", records_zk3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(204));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("zk3",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("zk3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g7i", cols0);
		String [][] records_g7i = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g7i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g7i", records_g7i[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(968));
		for(int i=0;i<14;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("g7i",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("g7i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y45", cols0);
		String [][] records_y45 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y45[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y45", records_y45[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(103));
		for(int i=0;i<67;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("y45",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("y45");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zw3h", cols1);
		String [][] records_zw3h = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zw3h[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zw3h", records_zw3h[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(586));
		for(int i=0;i<44;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("zw3h",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("zw3h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("f3zm", cols2);
		String [][] records_f3zm = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f3zm[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f3zm", records_f3zm[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(391));
		for(int i=0;i<75;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("f3zm",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("f3zm");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w6", cols3);
		String [][] records_w6 = new String[95][cols3.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w6", records_w6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(735));
		for(int i=0;i<95;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("w6",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("w6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("txyz", cols0);
		String [][] records_txyz = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_txyz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("txyz", records_txyz[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(817));
		for(int i=0;i<6;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("txyz",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("txyz");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ba4", cols0);
		String [][] records_ba4 = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ba4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ba4", records_ba4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(428));
		for(int i=0;i<59;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ba4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ba4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r6", cols1);
		String [][] records_r6 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r6", records_r6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(500));
		for(int i=0;i<11;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r6",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("gqq52", cols0);
		String [][] records_gqq52 = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gqq52[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gqq52", records_gqq52[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(999));
		for(int i=0;i<81;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("gqq52",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("gqq52");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("j2q79", cols1);
		String [][] records_j2q79 = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j2q79[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j2q79", records_j2q79[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(590));
		for(int i=0;i<21;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("j2q79",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("j2q79");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("vx6d", cols2);
		String [][] records_vx6d = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vx6d[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vx6d", records_vx6d[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(168));
		for(int i=0;i<99;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("vx6d",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("vx6d");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jx", cols3);
		String [][] records_jx = new String[39][cols3.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jx[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jx", records_jx[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(303));
		for(int i=0;i<39;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("jx",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("jx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("nu54", cols0);
		String [][] records_nu54 = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nu54[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nu54", records_nu54[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(306));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("nu54",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("nu54");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n5", cols1);
		String [][] records_n5 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n5", records_n5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(224));
		for(int i=0;i<3;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("n5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("n5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("va", cols2);
		String [][] records_va = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_va[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("va", records_va[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(624));
		for(int i=0;i<76;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("va",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("va");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("uh1", cols3);
		String [][] records_uh1 = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_uh1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uh1", records_uh1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(106));
		for(int i=0;i<97;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("uh1",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("uh1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jg9", cols4);
		String [][] records_jg9 = new String[67][cols4.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_jg9[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jg9", records_jg9[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(705));
		for(int i=0;i<67;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("jg9",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("jg9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u2", cols0);
		String [][] records_u2 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u2", records_u2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(660));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u2",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("v50", cols1);
		String [][] records_v50 = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v50[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v50", records_v50[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(278));
		for(int i=0;i<81;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("v50",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("v50");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("u4yd", cols0);
		String [][] records_u4yd = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u4yd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u4yd", records_u4yd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(684));
		for(int i=0;i<99;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u4yd",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u4yd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j5lg", cols1);
		String [][] records_j5lg = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j5lg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j5lg", records_j5lg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(988));
		for(int i=0;i<99;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("j5lg",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("j5lg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p6325", cols0);
		String [][] records_p6325 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p6325[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p6325", records_p6325[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(152));
		for(int i=0;i<10;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("p6325",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("p6325");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k153j", cols1);
		String [][] records_k153j = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k153j[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k153j", records_k153j[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(174));
		for(int i=0;i<20;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("k153j",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("k153j");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("kn34", cols2);
		String [][] records_kn34 = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kn34[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kn34", records_kn34[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(734));
		for(int i=0;i<53;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("kn34",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("kn34");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rcp", cols3);
		String [][] records_rcp = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_rcp[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rcp", records_rcp[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(766));
		for(int i=0;i<37;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("rcp",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("rcp");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k82", cols0);
		String [][] records_k82 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k82[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k82", records_k82[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(237));
		for(int i=0;i<23;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("k82",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("k82");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("xdvg", cols0);
		String [][] records_xdvg = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xdvg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xdvg", records_xdvg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(161));
		for(int i=0;i<97;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("xdvg",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("xdvg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a2qs", cols1);
		String [][] records_a2qs = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a2qs[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a2qs", records_a2qs[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(175));
		for(int i=0;i<96;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("a2qs",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("a2qs");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("oz", cols2);
		String [][] records_oz = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_oz[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oz", records_oz[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(337));
		for(int i=0;i<9;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("oz",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("oz");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("j721u", cols3);
		String [][] records_j721u = new String[21][cols3.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j721u[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j721u", records_j721u[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(714));
		for(int i=0;i<21;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("j721u",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("j721u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zp5", cols0);
		String [][] records_zp5 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zp5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zp5", records_zp5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(574));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("zp5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("zp5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ckfnk", cols1);
		String [][] records_ckfnk = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ckfnk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ckfnk", records_ckfnk[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(177));
		for(int i=0;i<8;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ckfnk",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ckfnk");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("bz32i", cols2);
		String [][] records_bz32i = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bz32i[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bz32i", records_bz32i[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(145));
		for(int i=0;i<2;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("bz32i",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("bz32i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eduo8", cols0);
		String [][] records_eduo8 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_eduo8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eduo8", records_eduo8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(806));
		for(int i=0;i<68;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("eduo8",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("eduo8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("dxu", cols1);
		String [][] records_dxu = new String[95][cols1.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dxu[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dxu", records_dxu[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(564));
		for(int i=0;i<95;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("dxu",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("dxu");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("z97o", cols2);
		String [][] records_z97o = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z97o[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z97o", records_z97o[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(655));
		for(int i=0;i<40;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("z97o",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("z97o");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("g81", cols3);
		String [][] records_g81 = new String[75][cols3.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g81[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g81", records_g81[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(191));
		for(int i=0;i<75;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("g81",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("g81");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k1e", cols0);
		String [][] records_k1e = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k1e[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k1e", records_k1e[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(134));
		for(int i=0;i<39;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("k1e",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("k1e");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("gs05u", cols0);
		String [][] records_gs05u = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gs05u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gs05u", records_gs05u[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(247));
		for(int i=0;i<68;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("gs05u",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("gs05u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("x2h3j", cols0);
		String [][] records_x2h3j = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x2h3j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x2h3j", records_x2h3j[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(821));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("x2h3j",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("x2h3j");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("j77f", cols1);
		String [][] records_j77f = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j77f[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j77f", records_j77f[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(627));
		for(int i=0;i<47;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("j77f",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("j77f");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("d12", cols2);
		String [][] records_d12 = new String[3][cols2.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d12[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d12", records_d12[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(342));
		for(int i=0;i<3;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("d12",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("d12");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("llw9", cols3);
		String [][] records_llw9 = new String[100][cols3.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_llw9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("llw9", records_llw9[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(89));
		for(int i=0;i<100;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("llw9",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("llw9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("vmwc1", cols4);
		String [][] records_vmwc1 = new String[82][cols4.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vmwc1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vmwc1", records_vmwc1[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(151));
		for(int i=0;i<82;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("vmwc1",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("vmwc1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("x3c", cols0);
		String [][] records_x3c = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x3c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x3c", records_x3c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<78; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(432));
		for(int i=0;i<78;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("x3c",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("x3c");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k2", cols0);
		String [][] records_k2 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2", records_k2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(182));
		for(int i=0;i<8;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("k2",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("k2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("sb", cols1);
		String [][] records_sb = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sb", records_sb[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(552));
		for(int i=0;i<98;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("sb",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("sb");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("tf9", cols0);
		String [][] records_tf9 = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tf9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tf9", records_tf9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(862));
		for(int i=0;i<71;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("tf9",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("tf9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i7i", cols0);
		String [][] records_i7i = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i7i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i7i", records_i7i[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(886));
		for(int i=0;i<81;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("i7i",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("i7i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("ykv", cols1);
		String [][] records_ykv = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ykv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ykv", records_ykv[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(899));
		for(int i=0;i<83;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ykv",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ykv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("nri", cols0);
		String [][] records_nri = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nri[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nri", records_nri[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(255));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("nri",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("nri");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z96", cols1);
		String [][] records_z96 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z96[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z96", records_z96[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(931));
		for(int i=0;i<93;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("z96",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("z96");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xa", cols2);
		String [][] records_xa = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xa[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xa", records_xa[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(236));
		for(int i=0;i<50;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xa",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d02", cols3);
		String [][] records_d02 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d02[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d02", records_d02[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(717));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("d02",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("d02");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ilt", cols0);
		String [][] records_ilt = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ilt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ilt", records_ilt[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(499));
		for(int i=0;i<46;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ilt",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ilt");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ncn", cols1);
		String [][] records_ncn = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ncn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ncn", records_ncn[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(724));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ncn",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ncn");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("roy", cols2);
		String [][] records_roy = new String[86][cols2.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_roy[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("roy", records_roy[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<86; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(897));
		for(int i=0;i<86;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("roy",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("roy");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a1", cols3);
		String [][] records_a1 = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a1", records_a1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(995));
		for(int i=0;i<58;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("a1",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("a1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v8l4", cols0);
		String [][] records_v8l4 = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v8l4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v8l4", records_v8l4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(388));
		for(int i=0;i<62;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("v8l4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("v8l4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("by", cols1);
		String [][] records_by = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_by[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("by", records_by[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(35));
		for(int i=0;i<45;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("by",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("by");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ta", cols2);
		String [][] records_ta = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ta[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ta", records_ta[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(779));
		for(int i=0;i<89;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("ta",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("ta");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ae", cols3);
		String [][] records_ae = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ae[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ae", records_ae[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(537));
		for(int i=0;i<58;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ae",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ae");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("x970", cols4);
		String [][] records_x970 = new String[37][cols4.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x970[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x970", records_x970[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(535));
		for(int i=0;i<37;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("x970",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("x970");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("i9r", cols0);
		String [][] records_i9r = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i9r[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9r", records_i9r[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(678));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("i9r",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("i9r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("e2", cols0);
		String [][] records_e2 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e2", records_e2[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(431));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("e2",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("e2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("pxa", cols1);
		String [][] records_pxa = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pxa[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pxa", records_pxa[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(850));
		for(int i=0;i<13;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("pxa",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("pxa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("e3ctw", cols2);
		String [][] records_e3ctw = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e3ctw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e3ctw", records_e3ctw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(67));
		for(int i=0;i<49;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("e3ctw",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("e3ctw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("z66t", cols3);
		String [][] records_z66t = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z66t[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z66t", records_z66t[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(126));
		for(int i=0;i<99;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("z66t",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("z66t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("sq", cols0);
		String [][] records_sq = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sq", records_sq[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(533));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sq",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("fr", cols1);
		String [][] records_fr = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fr[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fr", records_fr[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(421));
		for(int i=0;i<84;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("fr",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("fr");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("we926", cols2);
		String [][] records_we926 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_we926[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("we926", records_we926[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(830));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("we926",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("we926");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("nb8a8", cols3);
		String [][] records_nb8a8 = new String[30][cols3.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_nb8a8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nb8a8", records_nb8a8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(823));
		for(int i=0;i<30;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("nb8a8",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("nb8a8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("u85ek", cols0);
		String [][] records_u85ek = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u85ek[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u85ek", records_u85ek[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(166));
		for(int i=0;i<46;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u85ek",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u85ek");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("avi", cols1);
		String [][] records_avi = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_avi[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("avi", records_avi[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(514));
		for(int i=0;i<63;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("avi",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("avi");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("f0f74", cols2);
		String [][] records_f0f74 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f0f74[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f0f74", records_f0f74[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(476));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("f0f74",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("f0f74");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("r48", cols3);
		String [][] records_r48 = new String[62][cols3.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r48[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r48", records_r48[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(284));
		for(int i=0;i<62;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("r48",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("r48");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("t2q8", cols4);
		String [][] records_t2q8 = new String[74][cols4.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_t2q8[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t2q8", records_t2q8[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(658));
		for(int i=0;i<74;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("t2q8",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("t2q8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("tiy6", cols0);
		String [][] records_tiy6 = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tiy6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tiy6", records_tiy6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(186));
		for(int i=0;i<85;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("tiy6",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("tiy6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p5", cols0);
		String [][] records_p5 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p5", records_p5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(386));
		for(int i=0;i<12;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("p5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("p5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f5e6", cols1);
		String [][] records_f5e6 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f5e6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f5e6", records_f5e6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(137));
		for(int i=0;i<19;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("f5e6",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("f5e6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lb", cols0);
		String [][] records_lb = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lb", records_lb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(727));
		for(int i=0;i<91;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("lb",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("lb");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("c9n", cols1);
		String [][] records_c9n = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c9n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c9n", records_c9n[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(216));
		for(int i=0;i<44;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("c9n",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("c9n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("wp", cols2);
		String [][] records_wp = new String[74][cols2.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wp[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wp", records_wp[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(707));
		for(int i=0;i<74;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("wp",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("wp");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("r2h7m", cols0);
		String [][] records_r2h7m = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r2h7m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r2h7m", records_r2h7m[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(358));
		for(int i=0;i<56;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("r2h7m",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("r2h7m");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("r4ja", cols1);
		String [][] records_r4ja = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r4ja[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4ja", records_r4ja[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(597));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r4ja",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r4ja");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xkw", cols2);
		String [][] records_xkw = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xkw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xkw", records_xkw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(204));
		for(int i=0;i<85;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xkw",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xkw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("plk2", cols3);
		String [][] records_plk2 = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_plk2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("plk2", records_plk2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(49));
		for(int i=0;i<42;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("plk2",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("plk2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x1", cols0);
		String [][] records_x1 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x1", records_x1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(480));
		for(int i=0;i<10;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("x1",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("x1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("w3k", cols1);
		String [][] records_w3k = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w3k[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w3k", records_w3k[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(494));
		for(int i=0;i<22;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("w3k",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("w3k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("uf6", cols0);
		String [][] records_uf6 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_uf6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uf6", records_uf6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(229));
		for(int i=0;i<68;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("uf6",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("uf6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("dc", cols1);
		String [][] records_dc = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dc", records_dc[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(775));
		for(int i=0;i<10;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("dc",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("dc");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("i8y", cols0);
		String [][] records_i8y = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i8y[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8y", records_i8y[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(158));
		for(int i=0;i<21;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("i8y",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("i8y");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("z7", cols0);
		String [][] records_z7 = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z7", records_z7[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(720));
		for(int i=0;i<69;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("z7",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("z7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("iz2", cols1);
		String [][] records_iz2 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_iz2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iz2", records_iz2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(60));
		for(int i=0;i<25;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("iz2",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("iz2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("cj3", cols2);
		String [][] records_cj3 = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_cj3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cj3", records_cj3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(524));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("cj3",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("cj3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oc", cols3);
		String [][] records_oc = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_oc[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oc", records_oc[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(471));
		for(int i=0;i<73;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("oc",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("oc");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("vz", cols0);
		String [][] records_vz = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vz", records_vz[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<21; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(754));
		for(int i=0;i<21;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("vz",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("vz");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("ze1", cols1);
		String [][] records_ze1 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ze1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ze1", records_ze1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(188));
		for(int i=0;i<23;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ze1",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ze1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s5z3", cols2);
		String [][] records_s5z3 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s5z3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s5z3", records_s5z3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(54));
		for(int i=0;i<88;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("s5z3",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("s5z3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uzrv", cols0);
		String [][] records_uzrv = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_uzrv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uzrv", records_uzrv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(242));
		for(int i=0;i<24;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("uzrv",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("uzrv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n6", cols1);
		String [][] records_n6 = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n6", records_n6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(476));
		for(int i=0;i<46;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("n6",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("n6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("y540", cols2);
		String [][] records_y540 = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y540[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y540", records_y540[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(531));
		for(int i=0;i<19;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("y540",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("y540");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h469", cols0);
		String [][] records_h469 = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h469[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h469", records_h469[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(844));
		for(int i=0;i<95;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("h469",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("h469");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w1", cols0);
		String [][] records_w1 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w1", records_w1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(226));
		for(int i=0;i<50;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("w1",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("w1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("as6j", cols1);
		String [][] records_as6j = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_as6j[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("as6j", records_as6j[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(322));
		for(int i=0;i<14;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("as6j",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("as6j");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xg", cols2);
		String [][] records_xg = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xg", records_xg[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(818));
		for(int i=0;i<70;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xg",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("squ", cols0);
		String [][] records_squ = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_squ[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("squ", records_squ[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(743));
		for(int i=0;i<49;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("squ",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("squ");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h3", cols1);
		String [][] records_h3 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h3", records_h3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(864));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("h3",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("h3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("d9", cols2);
		String [][] records_d9 = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d9", records_d9[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(978));
		for(int i=0;i<56;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("d9",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("d9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("sjb", cols0);
		String [][] records_sjb = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sjb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sjb", records_sjb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(644));
		for(int i=0;i<13;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sjb",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sjb");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ru60", cols1);
		String [][] records_ru60 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ru60[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ru60", records_ru60[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(621));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ru60",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ru60");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xwg", cols2);
		String [][] records_xwg = new String[84][cols2.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xwg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xwg", records_xwg[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(322));
		for(int i=0;i<84;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xwg",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xwg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("r434", cols0);
		String [][] records_r434 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r434[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r434", records_r434[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(927));
		for(int i=0;i<92;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("r434",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("r434");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ih2t", cols1);
		String [][] records_ih2t = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ih2t[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ih2t", records_ih2t[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(854));
		for(int i=0;i<80;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ih2t",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ih2t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e7s3c", cols0);
		String [][] records_e7s3c = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e7s3c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e7s3c", records_e7s3c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(901));
		for(int i=0;i<54;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("e7s3c",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("e7s3c");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x1d", cols1);
		String [][] records_x1d = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x1d[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x1d", records_x1d[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(109));
		for(int i=0;i<1;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("x1d",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("x1d");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y3ue", cols2);
		String [][] records_y3ue = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y3ue[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y3ue", records_y3ue[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(300));
		for(int i=0;i<40;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("y3ue",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("y3ue");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("x4dy", cols3);
		String [][] records_x4dy = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x4dy[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x4dy", records_x4dy[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(168));
		for(int i=0;i<27;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("x4dy",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("x4dy");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hl", cols4);
		String [][] records_hl = new String[97][cols4.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_hl[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hl", records_hl[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(443));
		for(int i=0;i<97;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("hl",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("hl");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("o8554", cols0);
		String [][] records_o8554 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o8554[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o8554", records_o8554[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(156));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("o8554",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("o8554");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n28sj", cols1);
		String [][] records_n28sj = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n28sj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n28sj", records_n28sj[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<16; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(987));
		for(int i=0;i<16;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("n28sj",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("n28sj");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e7t", cols2);
		String [][] records_e7t = new String[80][cols2.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e7t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e7t", records_e7t[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(905));
		for(int i=0;i<80;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("e7t",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("e7t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j8", cols3);
		String [][] records_j8 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j8", records_j8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(670));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("j8",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("j8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("r1i", cols4);
		String [][] records_r1i = new String[70][cols4.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r1i[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r1i", records_r1i[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(601));
		for(int i=0;i<70;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("r1i",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("r1i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("e3a3", cols0);
		String [][] records_e3a3 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e3a3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e3a3", records_e3a3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(808));
		for(int i=0;i<23;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("e3a3",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("e3a3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("pyf", cols1);
		String [][] records_pyf = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pyf[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pyf", records_pyf[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(575));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("pyf",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("pyf");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("h1k", cols2);
		String [][] records_h1k = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h1k[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h1k", records_h1k[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(843));
		for(int i=0;i<18;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("h1k",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("h1k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nfm", cols3);
		String [][] records_nfm = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_nfm[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nfm", records_nfm[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(869));
		for(int i=0;i<82;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("nfm",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("nfm");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("nd", cols0);
		String [][] records_nd = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nd", records_nd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(336));
		for(int i=0;i<4;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("nd",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("nd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ix", cols1);
		String [][] records_ix = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ix[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ix", records_ix[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(471));
		for(int i=0;i<90;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ix",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ix");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("to0", cols0);
		String [][] records_to0 = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_to0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("to0", records_to0[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(987));
		for(int i=0;i<61;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("to0",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("to0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("eg", cols1);
		String [][] records_eg = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eg", records_eg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(451));
		for(int i=0;i<99;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("eg",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("eg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b5y", cols2);
		String [][] records_b5y = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b5y[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b5y", records_b5y[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(284));
		for(int i=0;i<23;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("b5y",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("b5y");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("t27", cols3);
		String [][] records_t27 = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t27[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t27", records_t27[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(567));
		for(int i=0;i<49;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("t27",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("t27");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v766", cols4);
		String [][] records_v766 = new String[91][cols4.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_v766[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v766", records_v766[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(375));
		for(int i=0;i<91;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("v766",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("v766");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("l2625", cols0);
		String [][] records_l2625 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l2625[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l2625", records_l2625[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(262));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l2625",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l2625");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fgj", cols1);
		String [][] records_fgj = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fgj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fgj", records_fgj[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<87; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(978));
		for(int i=0;i<87;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("fgj",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("fgj");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e5ht", cols2);
		String [][] records_e5ht = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e5ht[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e5ht", records_e5ht[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(918));
		for(int i=0;i<76;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("e5ht",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("e5ht");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("cf76", cols0);
		String [][] records_cf76 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cf76[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cf76", records_cf76[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(56));
		for(int i=0;i<17;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("cf76",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("cf76");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s9e", cols1);
		String [][] records_s9e = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s9e[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s9e", records_s9e[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(841));
		for(int i=0;i<3;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("s9e",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("s9e");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s6w2z", cols2);
		String [][] records_s6w2z = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s6w2z[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s6w2z", records_s6w2z[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(317));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("s6w2z",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("s6w2z");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("bd", cols3);
		String [][] records_bd = new String[14][cols3.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bd[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bd", records_bd[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(113));
		for(int i=0;i<14;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("bd",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("bd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kg2", cols4);
		String [][] records_kg2 = new String[63][cols4.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_kg2[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kg2", records_kg2[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(314));
		for(int i=0;i<63;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("kg2",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("kg2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g68ri", cols0);
		String [][] records_g68ri = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g68ri[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g68ri", records_g68ri[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(29));
		for(int i=0;i<19;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("g68ri",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("g68ri");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rgqri", cols1);
		String [][] records_rgqri = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rgqri[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rgqri", records_rgqri[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(513));
		for(int i=0;i<70;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("rgqri",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("rgqri");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rd70t", cols2);
		String [][] records_rd70t = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_rd70t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rd70t", records_rd70t[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(662));
		for(int i=0;i<45;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("rd70t",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("rd70t");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("ckrq", cols3);
		String [][] records_ckrq = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ckrq[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ckrq", records_ckrq[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<43; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(759));
		for(int i=0;i<43;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ckrq",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ckrq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("r74r", cols4);
		String [][] records_r74r = new String[76][cols4.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r74r[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r74r", records_r74r[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(902));
		for(int i=0;i<76;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("r74r",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("r74r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("t32", cols0);
		String [][] records_t32 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t32[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t32", records_t32[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(12));
		for(int i=0;i<92;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("t32",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("t32");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("x0h", cols0);
		String [][] records_x0h = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x0h[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x0h", records_x0h[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(398));
		for(int i=0;i<80;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("x0h",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("x0h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fty", cols1);
		String [][] records_fty = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fty[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fty", records_fty[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(383));
		for(int i=0;i<53;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("fty",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("fty");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j7fu8", cols2);
		String [][] records_j7fu8 = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j7fu8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j7fu8", records_j7fu8[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(475));
		for(int i=0;i<100;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("j7fu8",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("j7fu8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("n9bx", cols0);
		String [][] records_n9bx = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n9bx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9bx", records_n9bx[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(917));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("n9bx",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("n9bx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ow97", cols0);
		String [][] records_ow97 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ow97[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ow97", records_ow97[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(56));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ow97",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ow97");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("eq", cols1);
		String [][] records_eq = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eq", records_eq[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(29));
		for(int i=0;i<25;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("eq",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("eq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i8", cols2);
		String [][] records_i8 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8", records_i8[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(796));
		for(int i=0;i<27;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i8",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("up", cols3);
		String [][] records_up = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_up[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("up", records_up[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(973));
		for(int i=0;i<52;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("up",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("up");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("ln5", cols0);
		String [][] records_ln5 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ln5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ln5", records_ln5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<86; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(258));
		for(int i=0;i<86;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ln5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ln5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qo2da", cols1);
		String [][] records_qo2da = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qo2da[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qo2da", records_qo2da[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(664));
		for(int i=0;i<68;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("qo2da",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("qo2da");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qp6ml", cols2);
		String [][] records_qp6ml = new String[54][cols2.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qp6ml[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qp6ml", records_qp6ml[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(408));
		for(int i=0;i<54;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("qp6ml",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("qp6ml");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("c3", cols3);
		String [][] records_c3 = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c3", records_c3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(334));
		for(int i=0;i<2;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("c3",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("c3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("s4", cols0);
		String [][] records_s4 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(104));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("s4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("s4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qct", cols1);
		String [][] records_qct = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qct[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qct", records_qct[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(295));
		for(int i=0;i<83;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("qct",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("qct");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("j7nt", cols2);
		String [][] records_j7nt = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j7nt[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j7nt", records_j7nt[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<16; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(87));
		for(int i=0;i<16;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("j7nt",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("j7nt");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u7", cols3);
		String [][] records_u7 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_u7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u7", records_u7[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(503));
		for(int i=0;i<77;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("u7",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("u7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("yww3o", cols4);
		String [][] records_yww3o = new String[79][cols4.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_yww3o[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yww3o", records_yww3o[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(117));
		for(int i=0;i<79;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("yww3o",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("yww3o");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z43c", cols0);
		String [][] records_z43c = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z43c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z43c", records_z43c[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(758));
		for(int i=0;i<85;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("z43c",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("z43c");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("h77r", cols1);
		String [][] records_h77r = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h77r[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h77r", records_h77r[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(647));
		for(int i=0;i<70;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("h77r",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("h77r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p6s", cols2);
		String [][] records_p6s = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p6s[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p6s", records_p6s[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(536));
		for(int i=0;i<64;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("p6s",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("p6s");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jk5", cols0);
		String [][] records_jk5 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jk5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jk5", records_jk5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(854));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("jk5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("jk5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h48k", cols0);
		String [][] records_h48k = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h48k[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h48k", records_h48k[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(465));
		for(int i=0;i<74;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("h48k",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("h48k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("mxa", cols1);
		String [][] records_mxa = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mxa[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mxa", records_mxa[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(295));
		for(int i=0;i<46;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("mxa",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("mxa");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("ef", cols0);
		String [][] records_ef = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ef[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ef", records_ef[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(952));
		for(int i=0;i<92;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ef",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ef");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kc0w", cols1);
		String [][] records_kc0w = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kc0w[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kc0w", records_kc0w[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(166));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("kc0w",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("kc0w");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xg3", cols2);
		String [][] records_xg3 = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xg3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xg3", records_xg3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<14; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(983));
		for(int i=0;i<14;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xg3",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xg3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("x92", cols3);
		String [][] records_x92 = new String[87][cols3.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x92[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x92", records_x92[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<87; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(865));
		for(int i=0;i<87;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("x92",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("x92");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d326", cols0);
		String [][] records_d326 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d326[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d326", records_d326[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(175));
		for(int i=0;i<66;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d326",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d326");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f0", cols1);
		String [][] records_f0 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f0", records_f0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<49; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(128));
		for(int i=0;i<49;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("f0",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("f0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i3by2", cols2);
		String [][] records_i3by2 = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i3by2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i3by2", records_i3by2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(869));
		for(int i=0;i<98;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i3by2",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i3by2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("zd5y", cols3);
		String [][] records_zd5y = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zd5y[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zd5y", records_zd5y[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<56; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(401));
		for(int i=0;i<56;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("zd5y",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("zd5y");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e49", cols0);
		String [][] records_e49 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e49[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e49", records_e49[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(978));
		for(int i=0;i<58;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("e49",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("e49");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ke0", cols1);
		String [][] records_ke0 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ke0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ke0", records_ke0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(317));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ke0",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ke0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("a69", cols2);
		String [][] records_a69 = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_a69[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a69", records_a69[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(719));
		for(int i=0;i<83;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("a69",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("a69");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("g917", cols3);
		String [][] records_g917 = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g917[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g917", records_g917[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(625));
		for(int i=0;i<73;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("g917",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("g917");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b38", cols4);
		String [][] records_b38 = new String[17][cols4.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b38[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b38", records_b38[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(859));
		for(int i=0;i<17;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("b38",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("b38");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("d777", cols0);
		String [][] records_d777 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d777[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d777", records_d777[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(53));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("d777",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("d777");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("qz7", cols1);
		String [][] records_qz7 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qz7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qz7", records_qz7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(624));
		for(int i=0;i<11;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("qz7",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("qz7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xy0", cols2);
		String [][] records_xy0 = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xy0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xy0", records_xy0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(741));
		for(int i=0;i<18;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("xy0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("xy0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c4iy0", cols3);
		String [][] records_c4iy0 = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c4iy0[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c4iy0", records_c4iy0[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(48));
		for(int i=0;i<66;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("c4iy0",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("c4iy0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("eq1", cols0);
		String [][] records_eq1 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_eq1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eq1", records_eq1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(567));
		for(int i=0;i<64;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("eq1",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("eq1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gb449", cols1);
		String [][] records_gb449 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gb449[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gb449", records_gb449[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(673));
		for(int i=0;i<5;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("gb449",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("gb449");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p0", cols2);
		String [][] records_p0 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p0", records_p0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(997));
		for(int i=0;i<51;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("p0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("p0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("r8", cols3);
		String [][] records_r8 = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8", records_r8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(377));
		for(int i=0;i<82;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("r8",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("r8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l2f", cols0);
		String [][] records_l2f = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l2f[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l2f", records_l2f[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(911));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("l2f",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("l2f");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ot7b5", cols1);
		String [][] records_ot7b5 = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ot7b5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ot7b5", records_ot7b5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(368));
		for(int i=0;i<54;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("ot7b5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("ot7b5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("le", cols0);
		String [][] records_le = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_le[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("le", records_le[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(322));
		for(int i=0;i<26;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("le",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("le");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q3l", cols1);
		String [][] records_q3l = new String[55][cols1.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q3l[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q3l", records_q3l[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<55; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(220));
		for(int i=0;i<55;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("q3l",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("q3l");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gow", cols2);
		String [][] records_gow = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_gow[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gow", records_gow[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(184));
		for(int i=0;i<59;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("gow",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("gow");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("gesvx", cols0);
		String [][] records_gesvx = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gesvx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gesvx", records_gesvx[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(737));
		for(int i=0;i<77;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("gesvx",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("gesvx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s8k", cols0);
		String [][] records_s8k = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s8k[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s8k", records_s8k[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(243));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("s8k",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("s8k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("oui3", cols1);
		String [][] records_oui3 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oui3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oui3", records_oui3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(252));
		for(int i=0;i<51;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("oui3",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("oui3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m04", cols2);
		String [][] records_m04 = new String[68][cols2.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m04[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m04", records_m04[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(168));
		for(int i=0;i<68;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("m04",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("m04");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z7", cols3);
		String [][] records_z7 = new String[87][cols3.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z7", records_z7[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<87; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(405));
		for(int i=0;i<87;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("z7",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("z7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("h93w", cols0);
		String [][] records_h93w = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h93w[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h93w", records_h93w[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(437));
		for(int i=0;i<42;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("h93w",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("h93w");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k2", cols1);
		String [][] records_k2 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2", records_k2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(200));
		for(int i=0;i<68;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("k2",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("k2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u3ew", cols2);
		String [][] records_u3ew = new String[39][cols2.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u3ew[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u3ew", records_u3ew[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(4));
		for(int i=0;i<39;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("u3ew",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("u3ew");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y05e", cols3);
		String [][] records_y05e = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y05e[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y05e", records_y05e[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(129));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("y05e",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("y05e");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yc", cols4);
		String [][] records_yc = new String[3][cols4.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_yc[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yc", records_yc[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(597));
		for(int i=0;i<3;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("yc",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("yc");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j5q", cols0);
		String [][] records_j5q = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j5q[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j5q", records_j5q[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(553));
		for(int i=0;i<10;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("j5q",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("j5q");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k15", cols1);
		String [][] records_k15 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k15[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k15", records_k15[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(339));
		for(int i=0;i<45;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("k15",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("k15");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("i5v", cols2);
		String [][] records_i5v = new String[10][cols2.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i5v[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i5v", records_i5v[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(791));
		for(int i=0;i<10;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("i5v",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("i5v");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tb9", cols0);
		String [][] records_tb9 = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tb9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tb9", records_tb9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(517));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("tb9",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("tb9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("e67fy", cols1);
		String [][] records_e67fy = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e67fy[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e67fy", records_e67fy[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(460));
		for(int i=0;i<80;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("e67fy",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("e67fy");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r4fp", cols0);
		String [][] records_r4fp = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r4fp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4fp", records_r4fp[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(454));
		for(int i=0;i<26;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("r4fp",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("r4fp");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r68", cols1);
		String [][] records_r68 = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r68[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r68", records_r68[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(710));
		for(int i=0;i<98;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r68",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r68");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tq4u", cols2);
		String [][] records_tq4u = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tq4u[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tq4u", records_tq4u[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(946));
		for(int i=0;i<15;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("tq4u",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("tq4u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c56j", cols3);
		String [][] records_c56j = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c56j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c56j", records_c56j[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(544));
		for(int i=0;i<65;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("c56j",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("c56j");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("of338", cols0);
		String [][] records_of338 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_of338[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("of338", records_of338[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(13));
		for(int i=0;i<57;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("of338",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("of338");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("t45", cols1);
		String [][] records_t45 = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t45[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t45", records_t45[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(161));
		for(int i=0;i<60;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("t45",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("t45");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("yqo", cols2);
		String [][] records_yqo = new String[10][cols2.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yqo[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yqo", records_yqo[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(945));
		for(int i=0;i<10;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("yqo",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("yqo");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("q4kzy", cols0);
		String [][] records_q4kzy = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q4kzy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q4kzy", records_q4kzy[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(975));
		for(int i=0;i<3;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("q4kzy",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("q4kzy");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("o3", cols1);
		String [][] records_o3 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o3", records_o3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(679));
		for(int i=0;i<13;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("o3",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("o3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z1k", cols2);
		String [][] records_z1k = new String[31][cols2.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z1k[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z1k", records_z1k[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(841));
		for(int i=0;i<31;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("z1k",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("z1k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("jo8", cols0);
		String [][] records_jo8 = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jo8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jo8", records_jo8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(749));
		for(int i=0;i<60;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("jo8",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("jo8");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("a2", cols1);
		String [][] records_a2 = new String[76][cols1.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a2", records_a2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<76; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(316));
		for(int i=0;i<76;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("a2",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("a2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ea", cols2);
		String [][] records_ea = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ea[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ea", records_ea[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(316));
		for(int i=0;i<70;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("ea",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("ea");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s8908", cols0);
		String [][] records_s8908 = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s8908[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s8908", records_s8908[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(917));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("s8908",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("s8908");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("pd", cols1);
		String [][] records_pd = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pd", records_pd[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(236));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("pd",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("pd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("qw5", cols2);
		String [][] records_qw5 = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qw5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qw5", records_qw5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(877));
		for(int i=0;i<53;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("qw5",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("qw5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("z2u", cols0);
		String [][] records_z2u = new String[63][cols0.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z2u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z2u", records_z2u[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(478));
		for(int i=0;i<63;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("z2u",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("z2u");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("n4r", cols0);
		String [][] records_n4r = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n4r[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n4r", records_n4r[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(408));
		for(int i=0;i<12;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("n4r",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("n4r");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a45", cols1);
		String [][] records_a45 = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a45[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a45", records_a45[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(44));
		for(int i=0;i<59;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("a45",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("a45");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q436", cols2);
		String [][] records_q436 = new String[82][cols2.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q436[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q436", records_q436[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(466));
		for(int i=0;i<82;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("q436",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("q436");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("baf", cols3);
		String [][] records_baf = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_baf[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("baf", records_baf[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(200));
		for(int i=0;i<2;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("baf",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("baf");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qh", cols0);
		String [][] records_qh = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qh", records_qh[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(54));
		for(int i=0;i<24;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("qh",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("qh");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("r80", cols1);
		String [][] records_r80 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r80[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r80", records_r80[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(654));
		for(int i=0;i<89;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r80",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r80");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("hph", cols2);
		String [][] records_hph = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hph[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hph", records_hph[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(768));
		for(int i=0;i<30;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("hph",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("hph");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ok0", cols3);
		String [][] records_ok0 = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ok0[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ok0", records_ok0[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(970));
		for(int i=0;i<6;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("ok0",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("ok0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y1b8i", cols4);
		String [][] records_y1b8i = new String[23][cols4.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_y1b8i[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y1b8i", records_y1b8i[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(506));
		for(int i=0;i<23;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("y1b8i",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("y1b8i");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("c4nmq", cols0);
		String [][] records_c4nmq = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c4nmq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c4nmq", records_c4nmq[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(989));
		for(int i=0;i<95;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("c4nmq",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("c4nmq");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b21", cols0);
		String [][] records_b21 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b21[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b21", records_b21[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(202));
		for(int i=0;i<75;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("b21",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("b21");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("b4l5", cols1);
		String [][] records_b4l5 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b4l5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b4l5", records_b4l5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(338));
		for(int i=0;i<37;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("b4l5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("b4l5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("eo14", cols2);
		String [][] records_eo14 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_eo14[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eo14", records_eo14[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(619));
		for(int i=0;i<36;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("eo14",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("eo14");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("o04sr", cols3);
		String [][] records_o04sr = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_o04sr[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o04sr", records_o04sr[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(245));
		for(int i=0;i<83;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("o04sr",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("o04sr");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("jk5", cols0);
		String [][] records_jk5 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jk5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jk5", records_jk5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(880));
		for(int i=0;i<17;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("jk5",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("jk5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("xbg", cols1);
		String [][] records_xbg = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xbg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xbg", records_xbg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(832));
		for(int i=0;i<99;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("xbg",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("xbg");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("qmv", cols0);
		String [][] records_qmv = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qmv[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qmv", records_qmv[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(545));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("qmv",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("qmv");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gzk1", cols1);
		String [][] records_gzk1 = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gzk1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gzk1", records_gzk1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(503));
		for(int i=0;i<46;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("gzk1",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("gzk1");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("rh04", cols2);
		String [][] records_rh04 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_rh04[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rh04", records_rh04[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(301));
		for(int i=0;i<37;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("rh04",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("rh04");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u4", cols0);
		String [][] records_u4 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u4", records_u4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(474));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("u4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("u4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s1n", cols1);
		String [][] records_s1n = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s1n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s1n", records_s1n[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(155));
		for(int i=0;i<30;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("s1n",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("s1n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("c0", cols2);
		String [][] records_c0 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c0", records_c0[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(231));
		for(int i=0;i<36;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("c0",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("c0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("db7ep", cols3);
		String [][] records_db7ep = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_db7ep[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("db7ep", records_db7ep[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(719));
		for(int i=0;i<77;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("db7ep",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("db7ep");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("qglhd", cols0);
		String [][] records_qglhd = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qglhd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qglhd", records_qglhd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(453));
		for(int i=0;i<97;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("qglhd",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("qglhd");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dh", cols1);
		String [][] records_dh = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dh[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dh", records_dh[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(390));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("dh",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("dh");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o73", cols2);
		String [][] records_o73 = new String[3][cols2.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o73[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o73", records_o73[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(173));
		for(int i=0;i<3;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("o73",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("o73");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("r9aw", cols3);
		String [][] records_r9aw = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r9aw[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r9aw", records_r9aw[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(42));
		for(int i=0;i<82;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("r9aw",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("r9aw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("re", cols0);
		String [][] records_re = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_re[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("re", records_re[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(703));
		for(int i=0;i<37;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("re",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("re");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("de7h", cols1);
		String [][] records_de7h = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_de7h[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("de7h", records_de7h[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(378));
		for(int i=0;i<32;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("de7h",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("de7h");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wx", cols2);
		String [][] records_wx = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wx[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wx", records_wx[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(770));
		for(int i=0;i<79;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("wx",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("wx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q95", cols3);
		String [][] records_q95 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_q95[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q95", records_q95[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(134));
		for(int i=0;i<77;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("q95",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("q95");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ddzkt", cols0);
		String [][] records_ddzkt = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ddzkt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ddzkt", records_ddzkt[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(39));
		for(int i=0;i<30;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("ddzkt",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("ddzkt");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dme", cols0);
		String [][] records_dme = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dme[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dme", records_dme[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<3; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(391));
		for(int i=0;i<3;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("dme",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("dme");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("i0", cols1);
		String [][] records_i0 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i0", records_i0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(318));
		for(int i=0;i<20;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("i0",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("i0");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("yc55", cols0);
		String [][] records_yc55 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yc55[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yc55", records_yc55[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(180));
		for(int i=0;i<18;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("yc55",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("yc55");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r7n5", cols1);
		String [][] records_r7n5 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r7n5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r7n5", records_r7n5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(480));
		for(int i=0;i<69;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("r7n5",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("r7n5");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("bj32s", cols2);
		String [][] records_bj32s = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bj32s[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bj32s", records_bj32s[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(518));
		for(int i=0;i<18;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("bj32s",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("bj32s");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("yyh", cols0);
		String [][] records_yyh = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yyh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yyh", records_yyh[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(574));
		for(int i=0;i<80;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("yyh",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("yyh");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("wx", cols1);
		String [][] records_wx = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wx", records_wx[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(48));
		for(int i=0;i<8;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("wx",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("wx");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("equ", cols2);
		String [][] records_equ = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_equ[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("equ", records_equ[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(121));
		for(int i=0;i<94;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("equ",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("equ");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("sbq0k", cols0);
		String [][] records_sbq0k = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sbq0k[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sbq0k", records_sbq0k[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<55; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(852));
		for(int i=0;i<55;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("sbq0k",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("sbq0k");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o265", cols1);
		String [][] records_o265 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o265[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o265", records_o265[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(869));
		for(int i=0;i<19;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("o265",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("o265");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("ssi6", cols2);
		String [][] records_ssi6 = new String[68][cols2.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ssi6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ssi6", records_ssi6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(591));
		for(int i=0;i<68;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("ssi6",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("ssi6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k90e", cols0);
		String [][] records_k90e = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k90e[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k90e", records_k90e[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(169));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("k90e",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("k90e");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i4385", cols1);
		String [][] records_i4385 = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i4385[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i4385", records_i4385[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(842));
		for(int i=0;i<98;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("i4385",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("i4385");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s6n", cols2);
		String [][] records_s6n = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s6n[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s6n", records_s6n[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(900));
		for(int i=0;i<6;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("s6n",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("s6n");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gqa7", cols3);
		String [][] records_gqa7 = new String[80][cols3.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gqa7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gqa7", records_gqa7[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(968));
		for(int i=0;i<80;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("gqa7",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("gqa7");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("ieq6", cols4);
		String [][] records_ieq6 = new String[45][cols4.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ieq6[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ieq6", records_ieq6[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(908));
		for(int i=0;i<45;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			DBApp.select("ieq6",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("ieq6");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h8051", cols0);
		String [][] records_h8051 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h8051[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h8051", records_h8051[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(604));
		for(int i=0;i<65;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("h8051",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("h8051");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u3y3", cols1);
		String [][] records_u3y3 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u3y3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u3y3", records_u3y3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<5; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(63));
		for(int i=0;i<5;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("u3y3",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("u3y3");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("q056p", cols2);
		String [][] records_q056p = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q056p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q056p", records_q056p[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(870));
		for(int i=0;i<35;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("q056p",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("q056p");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b559", cols0);
		String [][] records_b559 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b559[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b559", records_b559[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(353));
		for(int i=0;i<74;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("b559",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("b559");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y5623", cols1);
		String [][] records_y5623 = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y5623[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y5623", records_y5623[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(274));
		for(int i=0;i<30;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			DBApp.select("y5623",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("y5623");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("qb9", cols2);
		String [][] records_qb9 = new String[82][cols2.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qb9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qb9", records_qb9[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(276));
		for(int i=0;i<82;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			DBApp.select("qb9",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("qb9");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p1v", cols3);
		String [][] records_p1v = new String[90][cols3.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p1v[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p1v", records_p1v[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(287));
		for(int i=0;i<90;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			DBApp.select("p1v",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("p1v");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("c024v", cols0);
		String [][] records_c024v = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c024v[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c024v", records_c024v[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(905));
		for(int i=0;i<2;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			DBApp.select("c024v",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("c024v");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("gf", cols0);
		String [][] records_gf = new String[335][cols0.length];
		for(int i=0;i<335;i++)
		{
			records_gf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gf", records_gf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("gf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 112, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 112);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v5tk", cols1);
		String [][] records_v5tk = new String[341][cols1.length];
		for(int i=0;i<341;i++)
		{
			records_v5tk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v5tk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v5tk", records_v5tk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record336: [a336, b0, c0, d0, e1, f0, g0]
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("v5tk", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 114, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 114);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("a4", cols2);
		String [][] records_a4 = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			records_a4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a4", records_a4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0]
		//record1: [a1, b1, c1, d1]
		//record2: [a2, b0, c2, d2]
		//record3: [a3, b1, c0, d3]
		//record4: [a4, b0, c1, d0]
		//last 5 records:
		//record25: [a25, b1, c1, d1]
		//record26: [a26, b0, c2, d2]
		//record27: [a27, b1, c0, d3]
		//record28: [a28, b0, c1, d0]
		//record29: [a29, b1, c2, d1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("a4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 15, records:
		//record0: [a1, b1, c1, d1]
		//record1: [a3, b1, c0, d3]
		//record2: [a5, b1, c2, d1]
		//record3: [a7, b1, c1, d3]
		//record4: [a9, b1, c0, d1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 15);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g9kt6", cols0);
		String [][] records_g9kt6 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			records_g9kt6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g9kt6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g9kt6", records_g9kt6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h3"};
		ArrayList<String[]> tableSelect0 = DBApp.select("g9kt6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record4: [a35, b1, c2, d3, e0, f5, g0, h3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z2q", cols1);
		String [][] records_z2q = new String[113][cols1.length];
		for(int i=0;i<113;i++)
		{
			records_z2q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z2q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z2q", records_z2q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e4"};
		ArrayList<String[]> tableSelect1 = DBApp.select("z2q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 22, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 22);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5","o4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vy14", cols2);
		String [][] records_vy14 = new String[304][cols2.length];
		for(int i=0;i<304;i++)
		{
			records_vy14[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vy14[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vy14", records_vy14[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h7"};
		ArrayList<String[]> tableSelect2 = DBApp.select("vy14", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 38, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 38);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o7p43", cols3);
		String [][] records_o7p43 = new String[354][cols3.length];
		for(int i=0;i<354;i++)
		{
			records_o7p43[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o7p43[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o7p43", records_o7p43[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0, n1]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1, n2]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2, n3]
		String[] ConditionColumns3 = {"l","c"};
		String[] ConditionColumnsValues3 = {"l8","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.select("o7p43", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 29, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 29);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5","n2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionConditionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("j14lk", cols0);
		String [][] records_j14lk = new String[401][cols0.length];
		for(int i=0;i<401;i++)
		{
			records_j14lk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j14lk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j14lk", records_j14lk[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record396: [a396, b0]
		//record397: [a397, b1]
		//record398: [a398, b0]
		//record399: [a399, b1]
		//record400: [a400, b0]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("j14lk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 200, records:
		//record0: [a1, b1]
		//record1: [a3, b1]
		//record2: [a5, b1]
		//record3: [a7, b1]
		//record4: [a9, b1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 200);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("f75", cols1);
		String [][] records_f75 = new String[310][cols1.length];
		for(int i=0;i<310;i++)
		{
			records_f75[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f75[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f75", records_f75[i]);
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
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c1"};
		ArrayList<String[]> tableSelect1 = DBApp.select("f75", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 103, records:
		//record0: [a1, b1, c1]
		//record1: [a4, b0, c1]
		//record2: [a7, b1, c1]
		//record3: [a10, b0, c1]
		//record4: [a13, b1, c1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 103);
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



	private static int genRandNum(int max) 
	{
		Random random = new Random(1498);
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