package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests04
{

	@Test(timeout = 1000000)
	public void test76TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ad4", cols0);
		String [][] records_ad4 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ad4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ad4", records_ad4[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ad4");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ad4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("y29", cols1);
		String [][] records_y29 = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y29[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y29", records_y29[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("y29");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_y29[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("sjlg", cols0);
		String [][] records_sjlg = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sjlg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sjlg", records_sjlg[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("sjlg");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_sjlg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("i681k", cols1);
		String [][] records_i681k = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i681k[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i681k", records_i681k[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("i681k");
		for(int i=0;i<36;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_i681k[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("l1r", cols2);
		String [][] records_l1r = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l1r[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l1r", records_l1r[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("l1r");
		for(int i=0;i<88;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_l1r[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ac89", cols3);
		String [][] records_ac89 = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ac89[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ac89", records_ac89[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("ac89");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_ac89[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("g370", cols4);
		String [][] records_g370 = new String[30][cols4.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g370[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g370", records_g370[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("g370");
		for(int i=0;i<30;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_g370[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h1v", cols0);
		String [][] records_h1v = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h1v[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h1v", records_h1v[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("h1v");
		for(int i=0;i<84;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_h1v[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("e425a", cols1);
		String [][] records_e425a = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e425a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e425a", records_e425a[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("e425a");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_e425a[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("r2", cols2);
		String [][] records_r2 = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r2", records_r2[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("r2");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_r2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yz483", cols0);
		String [][] records_yz483 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yz483[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yz483", records_yz483[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("yz483");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_yz483[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("p77se", cols1);
		String [][] records_p77se = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p77se[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p77se", records_p77se[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p77se");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p77se[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ut9", cols2);
		String [][] records_ut9 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ut9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ut9", records_ut9[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ut9");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ut9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("btin", cols3);
		String [][] records_btin = new String[31][cols3.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_btin[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("btin", records_btin[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("btin");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_btin[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("o8amp", cols4);
		String [][] records_o8amp = new String[4][cols4.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_o8amp[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o8amp", records_o8amp[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("o8amp");
		for(int i=0;i<4;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_o8amp[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("uxm", cols0);
		String [][] records_uxm = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_uxm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uxm", records_uxm[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("uxm");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_uxm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("dj5", cols1);
		String [][] records_dj5 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dj5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dj5", records_dj5[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("dj5");
		for(int i=0;i<22;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_dj5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yk7", cols2);
		String [][] records_yk7 = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yk7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yk7", records_yk7[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yk7");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yk7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("en7t", cols0);
		String [][] records_en7t = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_en7t[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("en7t", records_en7t[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("en7t");
		for(int i=0;i<99;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_en7t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jc50", cols1);
		String [][] records_jc50 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jc50[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jc50", records_jc50[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("jc50");
		for(int i=0;i<6;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_jc50[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("h9g", cols0);
		String [][] records_h9g = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h9g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h9g", records_h9g[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("h9g");
		for(int i=0;i<82;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_h9g[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j00", cols1);
		String [][] records_j00 = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j00[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j00", records_j00[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("j00");
		for(int i=0;i<100;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_j00[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zj1", cols2);
		String [][] records_zj1 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zj1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zj1", records_zj1[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("zj1");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_zj1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r4vn2", cols0);
		String [][] records_r4vn2 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r4vn2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4vn2", records_r4vn2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("r4vn2");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_r4vn2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kjlg", cols1);
		String [][] records_kjlg = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kjlg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kjlg", records_kjlg[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("kjlg");
		for(int i=0;i<54;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_kjlg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("j5", cols2);
		String [][] records_j5 = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j5", records_j5[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("j5");
		for(int i=0;i<16;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_j5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v7k4t", cols3);
		String [][] records_v7k4t = new String[45][cols3.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_v7k4t[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v7k4t", records_v7k4t[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("v7k4t");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_v7k4t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cj72u", cols4);
		String [][] records_cj72u = new String[54][cols4.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_cj72u[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cj72u", records_cj72u[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("cj72u");
		for(int i=0;i<54;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_cj72u[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("djm2a", cols0);
		String [][] records_djm2a = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_djm2a[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("djm2a", records_djm2a[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("djm2a");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_djm2a[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("fe1q", cols1);
		String [][] records_fe1q = new String[61][cols1.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fe1q[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fe1q", records_fe1q[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("fe1q");
		for(int i=0;i<61;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_fe1q[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o4", cols2);
		String [][] records_o4 = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o4", records_o4[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o4");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q6", cols3);
		String [][] records_q6 = new String[90][cols3.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_q6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q6", records_q6[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("q6");
		for(int i=0;i<90;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_q6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t3fb8", cols4);
		String [][] records_t3fb8 = new String[17][cols4.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_t3fb8[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t3fb8", records_t3fb8[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("t3fb8");
		for(int i=0;i<17;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_t3fb8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zw0oi", cols0);
		String [][] records_zw0oi = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zw0oi[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zw0oi", records_zw0oi[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("zw0oi");
		for(int i=0;i<41;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_zw0oi[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("t9", cols1);
		String [][] records_t9 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t9", records_t9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("t9");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_t9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p585l", cols0);
		String [][] records_p585l = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p585l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p585l", records_p585l[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p585l");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p585l[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("t885", cols1);
		String [][] records_t885 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t885[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t885", records_t885[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("t885");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_t885[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zn", cols2);
		String [][] records_zn = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zn", records_zn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("zn");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_zn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x7n", cols3);
		String [][] records_x7n = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x7n[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7n", records_x7n[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("x7n");
		for(int i=0;i<50;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_x7n[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n9g38", cols0);
		String [][] records_n9g38 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n9g38[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9g38", records_n9g38[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("n9g38");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_n9g38[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("lf7", cols1);
		String [][] records_lf7 = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lf7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lf7", records_lf7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("lf7");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_lf7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ai7", cols2);
		String [][] records_ai7 = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ai7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ai7", records_ai7[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ai7");
		for(int i=0;i<9;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ai7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("no3j", cols3);
		String [][] records_no3j = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_no3j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("no3j", records_no3j[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("no3j");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_no3j[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("efe", cols0);
		String [][] records_efe = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_efe[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("efe", records_efe[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("efe");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_efe[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("cz2", cols1);
		String [][] records_cz2 = new String[85][cols1.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cz2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cz2", records_cz2[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cz2");
		for(int i=0;i<85;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cz2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fx", cols2);
		String [][] records_fx = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_fx[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fx", records_fx[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("fx");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_fx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("x8j", cols3);
		String [][] records_x8j = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x8j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x8j", records_x8j[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("x8j");
		for(int i=0;i<99;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_x8j[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("b5moo", cols4);
		String [][] records_b5moo = new String[3][cols4.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b5moo[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b5moo", records_b5moo[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("b5moo");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_b5moo[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("id33", cols0);
		String [][] records_id33 = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_id33[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("id33", records_id33[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("id33");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_id33[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ur", cols1);
		String [][] records_ur = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ur[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ur", records_ur[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("ur");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_ur[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("m11", cols2);
		String [][] records_m11 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m11[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m11", records_m11[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("m11");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_m11[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("pur3h", cols0);
		String [][] records_pur3h = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pur3h[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pur3h", records_pur3h[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("pur3h");
		for(int i=0;i<88;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_pur3h[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q54", cols1);
		String [][] records_q54 = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q54[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q54", records_q54[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("q54");
		for(int i=0;i<99;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_q54[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f90", cols2);
		String [][] records_f90 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f90[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f90", records_f90[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("f90");
		for(int i=0;i<51;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_f90[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("w9j", cols3);
		String [][] records_w9j = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w9j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w9j", records_w9j[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("w9j");
		for(int i=0;i<49;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_w9j[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("epl", cols0);
		String [][] records_epl = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_epl[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("epl", records_epl[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("epl");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_epl[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qx", cols0);
		String [][] records_qx = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qx", records_qx[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("qx");
		for(int i=0;i<68;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_qx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x9", cols1);
		String [][] records_x9 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x9", records_x9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("x9");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_x9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k2v18", cols2);
		String [][] records_k2v18 = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k2v18[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2v18", records_k2v18[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("k2v18");
		for(int i=0;i<64;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_k2v18[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("j10", cols3);
		String [][] records_j10 = new String[58][cols3.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j10[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j10", records_j10[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("j10");
		for(int i=0;i<58;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_j10[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("alnu", cols0);
		String [][] records_alnu = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_alnu[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("alnu", records_alnu[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("alnu");
		for(int i=0;i<78;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_alnu[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("mn", cols1);
		String [][] records_mn = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mn", records_mn[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mn");
		for(int i=0;i<32;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("mc4", cols2);
		String [][] records_mc4 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mc4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mc4", records_mc4[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("mc4");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_mc4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sj4", cols3);
		String [][] records_sj4 = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sj4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sj4", records_sj4[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("sj4");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_sj4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c89", cols0);
		String [][] records_c89 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c89[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c89", records_c89[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c89");
		for(int i=0;i<44;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c89[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wj93", cols1);
		String [][] records_wj93 = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wj93[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wj93", records_wj93[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("wj93");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_wj93[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lt5", cols2);
		String [][] records_lt5 = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_lt5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lt5", records_lt5[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("lt5");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_lt5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("nmjt", cols0);
		String [][] records_nmjt = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nmjt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nmjt", records_nmjt[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("nmjt");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_nmjt[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("cgd", cols1);
		String [][] records_cgd = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cgd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cgd", records_cgd[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cgd");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cgd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("pq", cols2);
		String [][] records_pq = new String[31][cols2.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pq[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pq", records_pq[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("pq");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_pq[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i8co", cols0);
		String [][] records_i8co = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i8co[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8co", records_i8co[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("i8co");
		for(int i=0;i<17;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_i8co[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("xy", cols1);
		String [][] records_xy = new String[95][cols1.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xy[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xy", records_xy[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("xy");
		for(int i=0;i<95;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_xy[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("r8", cols0);
		String [][] records_r8 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8", records_r8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("r8");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_r8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("c1dce", cols1);
		String [][] records_c1dce = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c1dce[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c1dce", records_c1dce[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("c1dce");
		for(int i=0;i<51;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_c1dce[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o84g", cols2);
		String [][] records_o84g = new String[65][cols2.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o84g[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o84g", records_o84g[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o84g");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o84g[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u9v", cols0);
		String [][] records_u9v = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u9v[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u9v", records_u9v[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("u9v");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_u9v[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n2", cols1);
		String [][] records_n2 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n2", records_n2[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("n2");
		for(int i=0;i<71;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_n2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y8", cols2);
		String [][] records_y8 = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y8", records_y8[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("y8");
		for(int i=0;i<79;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_y8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v5w", cols0);
		String [][] records_v5w = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v5w[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v5w", records_v5w[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("v5w");
		for(int i=0;i<35;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_v5w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("d86p", cols1);
		String [][] records_d86p = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d86p[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d86p", records_d86p[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("d86p");
		for(int i=0;i<84;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_d86p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ir75", cols2);
		String [][] records_ir75 = new String[41][cols2.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ir75[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ir75", records_ir75[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ir75");
		for(int i=0;i<41;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ir75[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zl", cols3);
		String [][] records_zl = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zl[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zl", records_zl[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("zl");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_zl[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("dv4", cols0);
		String [][] records_dv4 = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dv4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dv4", records_dv4[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("dv4");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_dv4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("y06u", cols1);
		String [][] records_y06u = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y06u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y06u", records_y06u[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("y06u");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_y06u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("c34", cols2);
		String [][] records_c34 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c34[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c34", records_c34[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("c34");
		for(int i=0;i<36;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_c34[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zd", cols3);
		String [][] records_zd = new String[25][cols3.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zd[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zd", records_zd[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("zd");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_zd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("i8s", cols4);
		String [][] records_i8s = new String[43][cols4.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_i8s[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8s", records_i8s[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("i8s");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_i8s[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("gb4", cols0);
		String [][] records_gb4 = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gb4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gb4", records_gb4[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("gb4");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_gb4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("cp", cols1);
		String [][] records_cp = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cp[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cp", records_cp[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cp");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cp[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("fvt", cols0);
		String [][] records_fvt = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fvt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fvt", records_fvt[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fvt");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fvt[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("l7k", cols1);
		String [][] records_l7k = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l7k[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7k", records_l7k[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("l7k");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_l7k[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("jjh", cols2);
		String [][] records_jjh = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jjh[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jjh", records_jjh[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("jjh");
		for(int i=0;i<49;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_jjh[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x3", cols3);
		String [][] records_x3 = new String[45][cols3.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x3", records_x3[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("x3");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_x3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("gqo", cols4);
		String [][] records_gqo = new String[46][cols4.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_gqo[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gqo", records_gqo[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("gqo");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_gqo[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("c34", cols0);
		String [][] records_c34 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c34[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c34", records_c34[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c34");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c34[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rt9", cols1);
		String [][] records_rt9 = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rt9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rt9", records_rt9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("rt9");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_rt9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("ytw", cols2);
		String [][] records_ytw = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ytw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ytw", records_ytw[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ytw");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ytw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dv", cols3);
		String [][] records_dv = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_dv[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dv", records_dv[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("dv");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_dv[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rs17l", cols4);
		String [][] records_rs17l = new String[87][cols4.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_rs17l[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rs17l", records_rs17l[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("rs17l");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_rs17l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("c2", cols0);
		String [][] records_c2 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c2", records_c2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c2");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("p3c", cols1);
		String [][] records_p3c = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p3c[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3c", records_p3c[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p3c");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p3c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l7a", cols2);
		String [][] records_l7a = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l7a[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7a", records_l7a[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("l7a");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_l7a[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("po1", cols0);
		String [][] records_po1 = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_po1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("po1", records_po1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("po1");
		for(int i=0;i<29;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_po1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vn", cols1);
		String [][] records_vn = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vn", records_vn[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("vn");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_vn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("oh", cols2);
		String [][] records_oh = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_oh[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oh", records_oh[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("oh");
		for(int i=0;i<37;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_oh[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g18", cols3);
		String [][] records_g18 = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g18[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g18", records_g18[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("g18");
		for(int i=0;i<97;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_g18[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jz", cols0);
		String [][] records_jz = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jz", records_jz[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("jz");
		for(int i=0;i<86;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_jz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cgr", cols1);
		String [][] records_cgr = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cgr[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cgr", records_cgr[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cgr");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cgr[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("o239", cols2);
		String [][] records_o239 = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o239[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o239", records_o239[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o239");
		for(int i=0;i<1;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o239[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sn2", cols3);
		String [][] records_sn2 = new String[76][cols3.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sn2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sn2", records_sn2[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("sn2");
		for(int i=0;i<76;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_sn2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("j2", cols0);
		String [][] records_j2 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j2", records_j2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("j2");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_j2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f43", cols1);
		String [][] records_f43 = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f43[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f43", records_f43[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("f43");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_f43[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("j4", cols2);
		String [][] records_j4 = new String[95][cols2.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j4", records_j4[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("j4");
		for(int i=0;i<95;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_j4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g9d04", cols3);
		String [][] records_g9d04 = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g9d04[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g9d04", records_g9d04[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("g9d04");
		for(int i=0;i<8;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_g9d04[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("ddn", cols4);
		String [][] records_ddn = new String[89][cols4.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ddn[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ddn", records_ddn[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("ddn");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_ddn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x0", cols0);
		String [][] records_x0 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x0", records_x0[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x0");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hd", cols1);
		String [][] records_hd = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hd", records_hd[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("hd");
		for(int i=0;i<1;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_hd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("cgg", cols2);
		String [][] records_cgg = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_cgg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cgg", records_cgg[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("cgg");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_cgg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jlt", cols3);
		String [][] records_jlt = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jlt[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jlt", records_jlt[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("jlt");
		for(int i=0;i<97;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_jlt[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ww5", cols0);
		String [][] records_ww5 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ww5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ww5", records_ww5[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ww5");
		for(int i=0;i<74;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ww5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j07o", cols0);
		String [][] records_j07o = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j07o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j07o", records_j07o[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("j07o");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_j07o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("l2547", cols1);
		String [][] records_l2547 = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l2547[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l2547", records_l2547[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("l2547");
		for(int i=0;i<62;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_l2547[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qc4", cols2);
		String [][] records_qc4 = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qc4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qc4", records_qc4[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("qc4");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_qc4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p2", cols0);
		String [][] records_p2 = new String[63][cols0.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p2", records_p2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p2");
		for(int i=0;i<63;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wu246", cols1);
		String [][] records_wu246 = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wu246[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wu246", records_wu246[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("wu246");
		for(int i=0;i<82;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_wu246[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("b7af", cols2);
		String [][] records_b7af = new String[4][cols2.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b7af[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b7af", records_b7af[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("b7af");
		for(int i=0;i<4;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_b7af[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lr20", cols3);
		String [][] records_lr20 = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_lr20[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lr20", records_lr20[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("lr20");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_lr20[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h197", cols0);
		String [][] records_h197 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h197[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h197", records_h197[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("h197");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_h197[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("p3e0", cols1);
		String [][] records_p3e0 = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p3e0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3e0", records_p3e0[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p3e0");
		for(int i=0;i<58;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p3e0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x7n", cols2);
		String [][] records_x7n = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x7n[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7n", records_x7n[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("x7n");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_x7n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("df57", cols3);
		String [][] records_df57 = new String[95][cols3.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_df57[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("df57", records_df57[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("df57");
		for(int i=0;i<95;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_df57[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("px77", cols0);
		String [][] records_px77 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_px77[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("px77", records_px77[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("px77");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_px77[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("is5", cols1);
		String [][] records_is5 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_is5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("is5", records_is5[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("is5");
		for(int i=0;i<49;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_is5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("f30", cols2);
		String [][] records_f30 = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f30[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f30", records_f30[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("f30");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_f30[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e3d", cols0);
		String [][] records_e3d = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e3d[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e3d", records_e3d[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("e3d");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_e3d[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("d8x56", cols1);
		String [][] records_d8x56 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d8x56[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d8x56", records_d8x56[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("d8x56");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_d8x56[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("d0", cols2);
		String [][] records_d0 = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d0", records_d0[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("d0");
		for(int i=0;i<100;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_d0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("itc", cols3);
		String [][] records_itc = new String[92][cols3.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_itc[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("itc", records_itc[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("itc");
		for(int i=0;i<92;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_itc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("lf", cols0);
		String [][] records_lf = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lf[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lf", records_lf[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("lf");
		for(int i=0;i<96;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_lf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c5179", cols1);
		String [][] records_c5179 = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c5179[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c5179", records_c5179[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("c5179");
		for(int i=0;i<39;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_c5179[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p8b", cols2);
		String [][] records_p8b = new String[52][cols2.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p8b[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p8b", records_p8b[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("p8b");
		for(int i=0;i<52;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_p8b[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vkz", cols3);
		String [][] records_vkz = new String[35][cols3.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vkz[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vkz", records_vkz[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("vkz");
		for(int i=0;i<35;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_vkz[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("a1n8g", cols0);
		String [][] records_a1n8g = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a1n8g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a1n8g", records_a1n8g[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("a1n8g");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_a1n8g[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("fq14", cols1);
		String [][] records_fq14 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fq14[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fq14", records_fq14[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("fq14");
		for(int i=0;i<80;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_fq14[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("o18t", cols2);
		String [][] records_o18t = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o18t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o18t", records_o18t[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o18t");
		for(int i=0;i<19;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o18t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("vtv01", cols3);
		String [][] records_vtv01 = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vtv01[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vtv01", records_vtv01[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("vtv01");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_vtv01[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wv1", cols4);
		String [][] records_wv1 = new String[60][cols4.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_wv1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wv1", records_wv1[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("wv1");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_wv1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("z4t7", cols0);
		String [][] records_z4t7 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z4t7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z4t7", records_z4t7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("z4t7");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_z4t7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("pb4l", cols1);
		String [][] records_pb4l = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pb4l[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pb4l", records_pb4l[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("pb4l");
		for(int i=0;i<90;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_pb4l[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("m18", cols2);
		String [][] records_m18 = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m18[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m18", records_m18[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("m18");
		for(int i=0;i<19;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_m18[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("p3", cols3);
		String [][] records_p3 = new String[39][cols3.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3", records_p3[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("p3");
		for(int i=0;i<39;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_p3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("fht8", cols4);
		String [][] records_fht8 = new String[94][cols4.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_fht8[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fht8", records_fht8[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("fht8");
		for(int i=0;i<94;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_fht8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("a6n5v", cols0);
		String [][] records_a6n5v = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a6n5v[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a6n5v", records_a6n5v[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("a6n5v");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_a6n5v[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("bik98", cols0);
		String [][] records_bik98 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bik98[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bik98", records_bik98[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("bik98");
		for(int i=0;i<17;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_bik98[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("t16", cols0);
		String [][] records_t16 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t16[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t16", records_t16[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t16");
		for(int i=0;i<39;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t16[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("rk", cols1);
		String [][] records_rk = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rk", records_rk[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("rk");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_rk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gc6", cols2);
		String [][] records_gc6 = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_gc6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gc6", records_gc6[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("gc6");
		for(int i=0;i<21;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_gc6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("np4h", cols3);
		String [][] records_np4h = new String[74][cols3.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_np4h[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("np4h", records_np4h[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("np4h");
		for(int i=0;i<74;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_np4h[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x232", cols0);
		String [][] records_x232 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x232[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x232", records_x232[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x232");
		for(int i=0;i<11;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x232[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bw4k2", cols1);
		String [][] records_bw4k2 = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bw4k2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bw4k2", records_bw4k2[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("bw4k2");
		for(int i=0;i<99;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_bw4k2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ylg", cols0);
		String [][] records_ylg = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ylg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ylg", records_ylg[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ylg");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ylg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("mirx", cols1);
		String [][] records_mirx = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mirx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mirx", records_mirx[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mirx");
		for(int i=0;i<88;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mirx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v157", cols2);
		String [][] records_v157 = new String[4][cols2.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v157[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v157", records_v157[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("v157");
		for(int i=0;i<4;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_v157[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("gx5", cols3);
		String [][] records_gx5 = new String[25][cols3.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gx5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gx5", records_gx5[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("gx5");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_gx5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("a2p", cols0);
		String [][] records_a2p = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a2p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a2p", records_a2p[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("a2p");
		for(int i=0;i<71;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_a2p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u30", cols1);
		String [][] records_u30 = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u30[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u30", records_u30[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("u30");
		for(int i=0;i<54;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_u30[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("wn7", cols0);
		String [][] records_wn7 = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wn7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wn7", records_wn7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("wn7");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_wn7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p4w", cols1);
		String [][] records_p4w = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p4w[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p4w", records_p4w[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p4w");
		for(int i=0;i<96;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p4w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("har1l", cols2);
		String [][] records_har1l = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_har1l[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("har1l", records_har1l[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("har1l");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_har1l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l4298", cols0);
		String [][] records_l4298 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l4298[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l4298", records_l4298[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("l4298");
		for(int i=0;i<91;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_l4298[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("wt", cols1);
		String [][] records_wt = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wt[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wt", records_wt[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("wt");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_wt[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("fm", cols0);
		String [][] records_fm = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fm", records_fm[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fm");
		for(int i=0;i<84;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s8y12", cols1);
		String [][] records_s8y12 = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s8y12[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s8y12", records_s8y12[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("s8y12");
		for(int i=0;i<84;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_s8y12[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qs", cols2);
		String [][] records_qs = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qs[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qs", records_qs[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("qs");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_qs[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("m84", cols3);
		String [][] records_m84 = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m84[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m84", records_m84[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("m84");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_m84[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j0j", cols4);
		String [][] records_j0j = new String[40][cols4.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_j0j[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j0j", records_j0j[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("j0j");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_j0j[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f9", cols0);
		String [][] records_f9 = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f9", records_f9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("f9");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_f9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("c7", cols0);
		String [][] records_c7 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c7", records_c7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c7");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rd", cols0);
		String [][] records_rd = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rd", records_rd[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("rd");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_rd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k2hi", cols1);
		String [][] records_k2hi = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k2hi[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2hi", records_k2hi[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("k2hi");
		for(int i=0;i<93;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_k2hi[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fnxi", cols0);
		String [][] records_fnxi = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fnxi[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fnxi", records_fnxi[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fnxi");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fnxi[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gy", cols0);
		String [][] records_gy = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gy", records_gy[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("gy");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_gy[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n3", cols0);
		String [][] records_n3 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n3", records_n3[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("n3");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_n3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("mrys", cols1);
		String [][] records_mrys = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mrys[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mrys", records_mrys[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mrys");
		for(int i=0;i<30;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mrys[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j86", cols2);
		String [][] records_j86 = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j86[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j86", records_j86[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("j86");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_j86[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("if63", cols0);
		String [][] records_if63 = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_if63[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("if63", records_if63[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("if63");
		for(int i=0;i<78;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_if63[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("c9471", cols1);
		String [][] records_c9471 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c9471[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c9471", records_c9471[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("c9471");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_c9471[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("e07", cols2);
		String [][] records_e07 = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e07[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e07", records_e07[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("e07");
		for(int i=0;i<30;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_e07[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zcr", cols0);
		String [][] records_zcr = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zcr[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zcr", records_zcr[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("zcr");
		for(int i=0;i<68;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_zcr[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("py", cols0);
		String [][] records_py = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_py[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("py", records_py[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("py");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_py[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("n00", cols1);
		String [][] records_n00 = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n00[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n00", records_n00[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("n00");
		for(int i=0;i<58;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_n00[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p37s5", cols2);
		String [][] records_p37s5 = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p37s5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p37s5", records_p37s5[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("p37s5");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_p37s5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fl0rc", cols3);
		String [][] records_fl0rc = new String[74][cols3.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fl0rc[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fl0rc", records_fl0rc[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("fl0rc");
		for(int i=0;i<74;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_fl0rc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vrcm", cols0);
		String [][] records_vrcm = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vrcm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vrcm", records_vrcm[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vrcm");
		for(int i=0;i<21;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vrcm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x6w", cols1);
		String [][] records_x6w = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x6w[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x6w", records_x6w[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("x6w");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_x6w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d1w69", cols2);
		String [][] records_d1w69 = new String[52][cols2.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d1w69[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d1w69", records_d1w69[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("d1w69");
		for(int i=0;i<52;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_d1w69[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("vi", cols3);
		String [][] records_vi = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vi[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vi", records_vi[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("vi");
		for(int i=0;i<33;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_vi[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("qn49", cols4);
		String [][] records_qn49 = new String[59][cols4.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_qn49[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qn49", records_qn49[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("qn49");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_qn49[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d13", cols0);
		String [][] records_d13 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d13[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d13", records_d13[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("d13");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_d13[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v1", cols1);
		String [][] records_v1 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v1", records_v1[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("v1");
		for(int i=0;i<88;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_v1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yv9fn", cols2);
		String [][] records_yv9fn = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yv9fn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yv9fn", records_yv9fn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yv9fn");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yv9fn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b1ct", cols3);
		String [][] records_b1ct = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b1ct[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b1ct", records_b1ct[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("b1ct");
		for(int i=0;i<97;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_b1ct[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("g99", cols0);
		String [][] records_g99 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g99[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g99", records_g99[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("g99");
		for(int i=0;i<58;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_g99[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("c2", cols0);
		String [][] records_c2 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c2", records_c2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c2");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("kr815", cols0);
		String [][] records_kr815 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kr815[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kr815", records_kr815[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("kr815");
		for(int i=0;i<6;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_kr815[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("zkx8", cols1);
		String [][] records_zkx8 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zkx8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zkx8", records_zkx8[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zkx8");
		for(int i=0;i<11;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zkx8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("o158", cols2);
		String [][] records_o158 = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o158[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o158", records_o158[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o158");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o158[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k2z", cols3);
		String [][] records_k2z = new String[95][cols3.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k2z[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k2z", records_k2z[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("k2z");
		for(int i=0;i<95;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_k2z[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j8", cols0);
		String [][] records_j8 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j8", records_j8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("j8");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_j8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wgw", cols0);
		String [][] records_wgw = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wgw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wgw", records_wgw[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("wgw");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_wgw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cfye", cols1);
		String [][] records_cfye = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cfye[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cfye", records_cfye[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cfye");
		for(int i=0;i<29;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cfye[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sf", cols0);
		String [][] records_sf = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sf[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sf", records_sf[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("sf");
		for(int i=0;i<50;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_sf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("rd8", cols1);
		String [][] records_rd8 = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rd8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rd8", records_rd8[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("rd8");
		for(int i=0;i<7;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_rd8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("w0ytm", cols2);
		String [][] records_w0ytm = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w0ytm[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w0ytm", records_w0ytm[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("w0ytm");
		for(int i=0;i<85;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_w0ytm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("nt78", cols3);
		String [][] records_nt78 = new String[22][cols3.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_nt78[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nt78", records_nt78[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("nt78");
		for(int i=0;i<22;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_nt78[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rmmsm", cols0);
		String [][] records_rmmsm = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rmmsm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rmmsm", records_rmmsm[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("rmmsm");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_rmmsm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("f369a", cols1);
		String [][] records_f369a = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f369a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f369a", records_f369a[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("f369a");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_f369a[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w8e", cols0);
		String [][] records_w8e = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w8e[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w8e", records_w8e[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("w8e");
		for(int i=0;i<22;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_w8e[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("t73", cols1);
		String [][] records_t73 = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t73[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t73", records_t73[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("t73");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_t73[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("za41o", cols2);
		String [][] records_za41o = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_za41o[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("za41o", records_za41o[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("za41o");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_za41o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s459k", cols3);
		String [][] records_s459k = new String[48][cols3.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s459k[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s459k", records_s459k[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("s459k");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_s459k[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hrk", cols0);
		String [][] records_hrk = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hrk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hrk", records_hrk[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("hrk");
		for(int i=0;i<81;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_hrk[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("t95i", cols0);
		String [][] records_t95i = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t95i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t95i", records_t95i[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t95i");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t95i[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o1l7", cols1);
		String [][] records_o1l7 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o1l7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o1l7", records_o1l7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("o1l7");
		for(int i=0;i<11;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_o1l7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("rg", cols2);
		String [][] records_rg = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_rg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rg", records_rg[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("rg");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_rg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t90", cols3);
		String [][] records_t90 = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t90[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t90", records_t90[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("t90");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_t90[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("lt57", cols4);
		String [][] records_lt57 = new String[98][cols4.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_lt57[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lt57", records_lt57[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("lt57");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_lt57[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w086", cols0);
		String [][] records_w086 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w086[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w086", records_w086[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("w086");
		for(int i=0;i<64;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_w086[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sxpr", cols1);
		String [][] records_sxpr = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sxpr[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sxpr", records_sxpr[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("sxpr");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_sxpr[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cf1", cols0);
		String [][] records_cf1 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cf1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cf1", records_cf1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("cf1");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_cf1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r0", cols1);
		String [][] records_r0 = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r0", records_r0[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("r0");
		for(int i=0;i<58;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_r0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w3", cols2);
		String [][] records_w3 = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w3", records_w3[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("w3");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_w3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("ajq", cols0);
		String [][] records_ajq = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ajq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ajq", records_ajq[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ajq");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ajq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("qt7", cols1);
		String [][] records_qt7 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qt7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qt7", records_qt7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("qt7");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_qt7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("ejer7", cols2);
		String [][] records_ejer7 = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ejer7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ejer7", records_ejer7[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ejer7");
		for(int i=0;i<53;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ejer7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q5", cols3);
		String [][] records_q5 = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_q5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q5", records_q5[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("q5");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_q5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("k44w7", cols4);
		String [][] records_k44w7 = new String[62][cols4.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_k44w7[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k44w7", records_k44w7[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("k44w7");
		for(int i=0;i<62;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_k44w7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("dd", cols0);
		String [][] records_dd = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dd", records_dd[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("dd");
		for(int i=0;i<37;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_dd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n62u", cols1);
		String [][] records_n62u = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n62u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n62u", records_n62u[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("n62u");
		for(int i=0;i<1;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_n62u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h4l3w", cols2);
		String [][] records_h4l3w = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h4l3w[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h4l3w", records_h4l3w[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("h4l3w");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_h4l3w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("o9", cols3);
		String [][] records_o9 = new String[89][cols3.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_o9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o9", records_o9[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("o9");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_o9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q3i", cols4);
		String [][] records_q3i = new String[92][cols4.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_q3i[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q3i", records_q3i[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("q3i");
		for(int i=0;i<92;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_q3i[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("y8u", cols0);
		String [][] records_y8u = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y8u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y8u", records_y8u[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("y8u");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_y8u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("zjc", cols1);
		String [][] records_zjc = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zjc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zjc", records_zjc[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zjc");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zjc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jfc", cols2);
		String [][] records_jfc = new String[65][cols2.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jfc[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jfc", records_jfc[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("jfc");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_jfc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("mx0", cols0);
		String [][] records_mx0 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mx0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mx0", records_mx0[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("mx0");
		for(int i=0;i<12;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_mx0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("ie100", cols1);
		String [][] records_ie100 = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ie100[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ie100", records_ie100[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("ie100");
		for(int i=0;i<100;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_ie100[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zbx", cols2);
		String [][] records_zbx = new String[3][cols2.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zbx[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zbx", records_zbx[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("zbx");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_zbx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("eby75", cols3);
		String [][] records_eby75 = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_eby75[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eby75", records_eby75[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("eby75");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_eby75[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("x29l6", cols4);
		String [][] records_x29l6 = new String[20][cols4.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x29l6[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x29l6", records_x29l6[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("x29l6");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_x29l6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("umt", cols0);
		String [][] records_umt = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_umt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("umt", records_umt[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("umt");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_umt[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("gb", cols1);
		String [][] records_gb = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gb", records_gb[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("gb");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_gb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("v9", cols2);
		String [][] records_v9 = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v9", records_v9[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("v9");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_v9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("g37o5", cols3);
		String [][] records_g37o5 = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g37o5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g37o5", records_g37o5[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("g37o5");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_g37o5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("y3n", cols4);
		String [][] records_y3n = new String[45][cols4.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_y3n[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y3n", records_y3n[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("y3n");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_y3n[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("t53x", cols0);
		String [][] records_t53x = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t53x[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t53x", records_t53x[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t53x");
		for(int i=0;i<16;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t53x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ep1", cols1);
		String [][] records_ep1 = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ep1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ep1", records_ep1[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("ep1");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_ep1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z6qz", cols2);
		String [][] records_z6qz = new String[62][cols2.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z6qz[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z6qz", records_z6qz[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("z6qz");
		for(int i=0;i<62;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_z6qz[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vrv2g", cols0);
		String [][] records_vrv2g = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vrv2g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vrv2g", records_vrv2g[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vrv2g");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vrv2g[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("p13", cols1);
		String [][] records_p13 = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p13[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p13", records_p13[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p13");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p13[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x4p", cols0);
		String [][] records_x4p = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x4p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x4p", records_x4p[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x4p");
		for(int i=0;i<54;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x4p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fec5", cols1);
		String [][] records_fec5 = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fec5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fec5", records_fec5[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("fec5");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_fec5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b2j22", cols2);
		String [][] records_b2j22 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b2j22[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b2j22", records_b2j22[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("b2j22");
		for(int i=0;i<94;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_b2j22[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ntb", cols0);
		String [][] records_ntb = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ntb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ntb", records_ntb[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ntb");
		for(int i=0;i<50;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ntb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("x2o", cols1);
		String [][] records_x2o = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x2o[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x2o", records_x2o[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("x2o");
		for(int i=0;i<41;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_x2o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xd64", cols2);
		String [][] records_xd64 = new String[44][cols2.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xd64[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xd64", records_xd64[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("xd64");
		for(int i=0;i<44;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_xd64[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("p0780", cols0);
		String [][] records_p0780 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p0780[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p0780", records_p0780[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p0780");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p0780[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p4s3", cols1);
		String [][] records_p4s3 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p4s3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p4s3", records_p4s3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("p4s3");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_p4s3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xzz", cols2);
		String [][] records_xzz = new String[55][cols2.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xzz[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xzz", records_xzz[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("xzz");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_xzz[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("rz1", cols0);
		String [][] records_rz1 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rz1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rz1", records_rz1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("rz1");
		for(int i=0;i<53;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_rz1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("t4j7", cols1);
		String [][] records_t4j7 = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t4j7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t4j7", records_t4j7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("t4j7");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_t4j7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("c5kn", cols2);
		String [][] records_c5kn = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c5kn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c5kn", records_c5kn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("c5kn");
		for(int i=0;i<19;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_c5kn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("f05fv", cols3);
		String [][] records_f05fv = new String[68][cols3.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f05fv[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f05fv", records_f05fv[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("f05fv");
		for(int i=0;i<68;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_f05fv[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vi91", cols4);
		String [][] records_vi91 = new String[67][cols4.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vi91[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vi91", records_vi91[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("vi91");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_vi91[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("po1", cols0);
		String [][] records_po1 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_po1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("po1", records_po1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("po1");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_po1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("w0hd", cols1);
		String [][] records_w0hd = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w0hd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w0hd", records_w0hd[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("w0hd");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_w0hd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z339p", cols2);
		String [][] records_z339p = new String[55][cols2.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z339p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z339p", records_z339p[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("z339p");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_z339p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("pda6o", cols3);
		String [][] records_pda6o = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_pda6o[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pda6o", records_pda6o[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("pda6o");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_pda6o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("t5", cols4);
		String [][] records_t5 = new String[77][cols4.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_t5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t5", records_t5[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("t5");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_t5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("vk9", cols0);
		String [][] records_vk9 = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vk9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vk9", records_vk9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vk9");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vk9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("v7", cols1);
		String [][] records_v7 = new String[67][cols1.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v7", records_v7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("v7");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_v7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("pn", cols2);
		String [][] records_pn = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pn", records_pn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("pn");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_pn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wq", cols3);
		String [][] records_wq = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wq[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wq", records_wq[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("wq");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_wq[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("mk", cols0);
		String [][] records_mk = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mk", records_mk[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("mk");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_mk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("o75", cols1);
		String [][] records_o75 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o75[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o75", records_o75[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("o75");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_o75[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u5", cols2);
		String [][] records_u5 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u5", records_u5[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("u5");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_u5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("j9gk", cols3);
		String [][] records_j9gk = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j9gk[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j9gk", records_j9gk[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("j9gk");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_j9gk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("r5f", cols4);
		String [][] records_r5f = new String[14][cols4.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r5f[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r5f", records_r5f[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("r5f");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_r5f[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ql", cols0);
		String [][] records_ql = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ql[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ql", records_ql[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ql");
		for(int i=0;i<44;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ql[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uwkp4", cols1);
		String [][] records_uwkp4 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_uwkp4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uwkp4", records_uwkp4[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("uwkp4");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_uwkp4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ao20e", cols2);
		String [][] records_ao20e = new String[80][cols2.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ao20e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ao20e", records_ao20e[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ao20e");
		for(int i=0;i<80;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ao20e[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("yha", cols0);
		String [][] records_yha = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yha[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yha", records_yha[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("yha");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_yha[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("pap5", cols1);
		String [][] records_pap5 = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pap5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pap5", records_pap5[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("pap5");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_pap5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("td", cols2);
		String [][] records_td = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_td[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("td", records_td[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("td");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_td[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ms4mn", cols3);
		String [][] records_ms4mn = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ms4mn[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ms4mn", records_ms4mn[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("ms4mn");
		for(int i=0;i<79;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_ms4mn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("fd6", cols4);
		String [][] records_fd6 = new String[2][cols4.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_fd6[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fd6", records_fd6[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("fd6");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_fd6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("k6b", cols0);
		String [][] records_k6b = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k6b[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k6b", records_k6b[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("k6b");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_k6b[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("mo182", cols1);
		String [][] records_mo182 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mo182[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mo182", records_mo182[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mo182");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mo182[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("t24u7", cols2);
		String [][] records_t24u7 = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_t24u7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t24u7", records_t24u7[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("t24u7");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_t24u7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fi338", cols3);
		String [][] records_fi338 = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fi338[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fi338", records_fi338[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("fi338");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_fi338[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dus", cols0);
		String [][] records_dus = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dus[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dus", records_dus[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("dus");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_dus[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nn", cols0);
		String [][] records_nn = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nn[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nn", records_nn[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("nn");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_nn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e04", cols1);
		String [][] records_e04 = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e04[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e04", records_e04[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("e04");
		for(int i=0;i<34;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_e04[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("uf45u", cols2);
		String [][] records_uf45u = new String[8][cols2.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uf45u[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uf45u", records_uf45u[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("uf45u");
		for(int i=0;i<8;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_uf45u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sy46x", cols3);
		String [][] records_sy46x = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sy46x[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sy46x", records_sy46x[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("sy46x");
		for(int i=0;i<47;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_sy46x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("kf5", cols4);
		String [][] records_kf5 = new String[29][cols4.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_kf5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kf5", records_kf5[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("kf5");
		for(int i=0;i<29;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_kf5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("th2", cols0);
		String [][] records_th2 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_th2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("th2", records_th2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("th2");
		for(int i=0;i<39;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_th2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("w21", cols1);
		String [][] records_w21 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w21[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w21", records_w21[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("w21");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_w21[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i5", cols2);
		String [][] records_i5 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i5", records_i5[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("i5");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_i5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("pw2z", cols3);
		String [][] records_pw2z = new String[91][cols3.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_pw2z[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pw2z", records_pw2z[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("pw2z");
		for(int i=0;i<91;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_pw2z[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s38", cols0);
		String [][] records_s38 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s38[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s38", records_s38[i]);
		}
		DBApp.select("s38");
		String selectTrace0 = DBApp.getLastTrace("s38");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+15));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("oer", cols1);
		String [][] records_oer = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oer[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oer", records_oer[i]);
		}
		DBApp.select("oer");
		String selectTrace1 = DBApp.getLastTrace("oer");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("xb", cols2);
		String [][] records_xb = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xb", records_xb[i]);
		}
		DBApp.select("xb");
		String selectTrace2 = DBApp.getLastTrace("xb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(76.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+76));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("g6", cols3);
		String [][] records_g6 = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g6", records_g6[i]);
		}
		DBApp.select("g6");
		String selectTrace3 = DBApp.getLastTrace("g6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("itp12", cols0);
		String [][] records_itp12 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_itp12[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("itp12", records_itp12[i]);
		}
		DBApp.select("itp12");
		String selectTrace0 = DBApp.getLastTrace("itp12");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("e89", cols1);
		String [][] records_e89 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e89[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e89", records_e89[i]);
		}
		DBApp.select("e89");
		String selectTrace1 = DBApp.getLastTrace("e89");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("y0", cols2);
		String [][] records_y0 = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y0", records_y0[i]);
		}
		DBApp.select("y0");
		String selectTrace2 = DBApp.getLastTrace("y0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("pkl0j", cols3);
		String [][] records_pkl0j = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_pkl0j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pkl0j", records_pkl0j[i]);
		}
		DBApp.select("pkl0j");
		String selectTrace3 = DBApp.getLastTrace("pkl0j");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xt1", cols4);
		String [][] records_xt1 = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xt1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xt1", records_xt1[i]);
		}
		DBApp.select("xt1");
		String selectTrace4 = DBApp.getLastTrace("xt1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+10));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("iu", cols0);
		String [][] records_iu = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_iu[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iu", records_iu[i]);
		}
		DBApp.select("iu");
		String selectTrace0 = DBApp.getLastTrace("iu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("t0a", cols1);
		String [][] records_t0a = new String[67][cols1.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t0a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t0a", records_t0a[i]);
		}
		DBApp.select("t0a");
		String selectTrace1 = DBApp.getLastTrace("t0a");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("m0764", cols2);
		String [][] records_m0764 = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m0764[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m0764", records_m0764[i]);
		}
		DBApp.select("m0764");
		String selectTrace2 = DBApp.getLastTrace("m0764");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xk51", cols3);
		String [][] records_xk51 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xk51[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xk51", records_xk51[i]);
		}
		DBApp.select("xk51");
		String selectTrace3 = DBApp.getLastTrace("xk51");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("e9w6", cols4);
		String [][] records_e9w6 = new String[74][cols4.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_e9w6[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e9w6", records_e9w6[i]);
		}
		DBApp.select("e9w6");
		String selectTrace4 = DBApp.getLastTrace("e9w6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(74.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+74));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("u4o2", cols0);
		String [][] records_u4o2 = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u4o2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u4o2", records_u4o2[i]);
		}
		DBApp.select("u4o2");
		String selectTrace0 = DBApp.getLastTrace("u4o2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("x4n", cols1);
		String [][] records_x4n = new String[33][cols1.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x4n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x4n", records_x4n[i]);
		}
		DBApp.select("x4n");
		String selectTrace1 = DBApp.getLastTrace("x4n");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(33.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+33));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ew", cols2);
		String [][] records_ew = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ew[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ew", records_ew[i]);
		}
		DBApp.select("ew");
		String selectTrace2 = DBApp.getLastTrace("ew");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("a918", cols0);
		String [][] records_a918 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a918[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a918", records_a918[i]);
		}
		DBApp.select("a918");
		String selectTrace0 = DBApp.getLastTrace("a918");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("c505i", cols1);
		String [][] records_c505i = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c505i[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c505i", records_c505i[i]);
		}
		DBApp.select("c505i");
		String selectTrace1 = DBApp.getLastTrace("c505i");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ub", cols0);
		String [][] records_ub = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ub[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ub", records_ub[i]);
		}
		DBApp.select("ub");
		String selectTrace0 = DBApp.getLastTrace("ub");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e9", cols1);
		String [][] records_e9 = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e9", records_e9[i]);
		}
		DBApp.select("e9");
		String selectTrace1 = DBApp.getLastTrace("e9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+81));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qwt", cols2);
		String [][] records_qwt = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qwt[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qwt", records_qwt[i]);
		}
		DBApp.select("qwt");
		String selectTrace2 = DBApp.getLastTrace("qwt");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("a27ef", cols3);
		String [][] records_a27ef = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a27ef[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a27ef", records_a27ef[i]);
		}
		DBApp.select("a27ef");
		String selectTrace3 = DBApp.getLastTrace("a27ef");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s0", cols4);
		String [][] records_s0 = new String[64][cols4.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_s0[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s0", records_s0[i]);
		}
		DBApp.select("s0");
		String selectTrace4 = DBApp.getLastTrace("s0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s5e", cols0);
		String [][] records_s5e = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s5e[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s5e", records_s5e[i]);
		}
		DBApp.select("s5e");
		String selectTrace0 = DBApp.getLastTrace("s5e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+81));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dy6", cols0);
		String [][] records_dy6 = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dy6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dy6", records_dy6[i]);
		}
		DBApp.select("dy6");
		String selectTrace0 = DBApp.getLastTrace("dy6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+69));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cwe", cols1);
		String [][] records_cwe = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cwe[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cwe", records_cwe[i]);
		}
		DBApp.select("cwe");
		String selectTrace1 = DBApp.getLastTrace("cwe");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i9m", cols0);
		String [][] records_i9m = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i9m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9m", records_i9m[i]);
		}
		DBApp.select("i9m");
		String selectTrace0 = DBApp.getLastTrace("i9m");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+2));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s08", cols1);
		String [][] records_s08 = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s08[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s08", records_s08[i]);
		}
		DBApp.select("s08");
		String selectTrace1 = DBApp.getLastTrace("s08");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("seqp1", cols0);
		String [][] records_seqp1 = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_seqp1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("seqp1", records_seqp1[i]);
		}
		DBApp.select("seqp1");
		String selectTrace0 = DBApp.getLastTrace("seqp1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+73));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o83", cols1);
		String [][] records_o83 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o83[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o83", records_o83[i]);
		}
		DBApp.select("o83");
		String selectTrace1 = DBApp.getLastTrace("o83");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("i8", cols2);
		String [][] records_i8 = new String[62][cols2.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8", records_i8[i]);
		}
		DBApp.select("i8");
		String selectTrace2 = DBApp.getLastTrace("i8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("s4", cols3);
		String [][] records_s4 = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
		DBApp.select("s4");
		String selectTrace3 = DBApp.getLastTrace("s4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ib3", cols4);
		String [][] records_ib3 = new String[71][cols4.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ib3[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ib3", records_ib3[i]);
		}
		DBApp.select("ib3");
		String selectTrace4 = DBApp.getLastTrace("ib3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("sq63", cols0);
		String [][] records_sq63 = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sq63[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sq63", records_sq63[i]);
		}
		DBApp.select("sq63");
		String selectTrace0 = DBApp.getLastTrace("sq63");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("yrk", cols0);
		String [][] records_yrk = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yrk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yrk", records_yrk[i]);
		}
		DBApp.select("yrk");
		String selectTrace0 = DBApp.getLastTrace("yrk");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("g180", cols1);
		String [][] records_g180 = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g180[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g180", records_g180[i]);
		}
		DBApp.select("g180");
		String selectTrace1 = DBApp.getLastTrace("g180");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("h1", cols2);
		String [][] records_h1 = new String[63][cols2.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h1", records_h1[i]);
		}
		DBApp.select("h1");
		String selectTrace2 = DBApp.getLastTrace("h1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+63));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("upe", cols3);
		String [][] records_upe = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_upe[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("upe", records_upe[i]);
		}
		DBApp.select("upe");
		String selectTrace3 = DBApp.getLastTrace("upe");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("iu", cols4);
		String [][] records_iu = new String[50][cols4.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_iu[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iu", records_iu[i]);
		}
		DBApp.select("iu");
		String selectTrace4 = DBApp.getLastTrace("iu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("v24s", cols0);
		String [][] records_v24s = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v24s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v24s", records_v24s[i]);
		}
		DBApp.select("v24s");
		String selectTrace0 = DBApp.getLastTrace("v24s");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ji1", cols0);
		String [][] records_ji1 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ji1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ji1", records_ji1[i]);
		}
		DBApp.select("ji1");
		String selectTrace0 = DBApp.getLastTrace("ji1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ip24f", cols1);
		String [][] records_ip24f = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ip24f[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ip24f", records_ip24f[i]);
		}
		DBApp.select("ip24f");
		String selectTrace1 = DBApp.getLastTrace("ip24f");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("l9ob", cols2);
		String [][] records_l9ob = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l9ob[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l9ob", records_l9ob[i]);
		}
		DBApp.select("l9ob");
		String selectTrace2 = DBApp.getLastTrace("l9ob");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("vl3", cols3);
		String [][] records_vl3 = new String[86][cols3.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vl3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vl3", records_vl3[i]);
		}
		DBApp.select("vl3");
		String selectTrace3 = DBApp.getLastTrace("vl3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+86));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("djk7y", cols4);
		String [][] records_djk7y = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_djk7y[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("djk7y", records_djk7y[i]);
		}
		DBApp.select("djk7y");
		String selectTrace4 = DBApp.getLastTrace("djk7y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+48));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("s4y", cols0);
		String [][] records_s4y = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s4y[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4y", records_s4y[i]);
		}
		DBApp.select("s4y");
		String selectTrace0 = DBApp.getLastTrace("s4y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zryf", cols1);
		String [][] records_zryf = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zryf[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zryf", records_zryf[i]);
		}
		DBApp.select("zryf");
		String selectTrace1 = DBApp.getLastTrace("zryf");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+14));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("lyq0t", cols0);
		String [][] records_lyq0t = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lyq0t[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lyq0t", records_lyq0t[i]);
		}
		DBApp.select("lyq0t");
		String selectTrace0 = DBApp.getLastTrace("lyq0t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pdhx4", cols0);
		String [][] records_pdhx4 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pdhx4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pdhx4", records_pdhx4[i]);
		}
		DBApp.select("pdhx4");
		String selectTrace0 = DBApp.getLastTrace("pdhx4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e876", cols1);
		String [][] records_e876 = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e876[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e876", records_e876[i]);
		}
		DBApp.select("e876");
		String selectTrace1 = DBApp.getLastTrace("e876");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+63));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("esx", cols2);
		String [][] records_esx = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_esx[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("esx", records_esx[i]);
		}
		DBApp.select("esx");
		String selectTrace2 = DBApp.getLastTrace("esx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("td965", cols3);
		String [][] records_td965 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_td965[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("td965", records_td965[i]);
		}
		DBApp.select("td965");
		String selectTrace3 = DBApp.getLastTrace("td965");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k6", cols0);
		String [][] records_k6 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k6", records_k6[i]);
		}
		DBApp.select("k6");
		String selectTrace0 = DBApp.getLastTrace("k6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("sa", cols1);
		String [][] records_sa = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sa[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sa", records_sa[i]);
		}
		DBApp.select("sa");
		String selectTrace1 = DBApp.getLastTrace("sa");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y74a", cols2);
		String [][] records_y74a = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y74a[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y74a", records_y74a[i]);
		}
		DBApp.select("y74a");
		String selectTrace2 = DBApp.getLastTrace("y74a");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+69));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("t76", cols0);
		String [][] records_t76 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t76[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t76", records_t76[i]);
		}
		DBApp.select("t76");
		String selectTrace0 = DBApp.getLastTrace("t76");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+92));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zqe0", cols0);
		String [][] records_zqe0 = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zqe0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zqe0", records_zqe0[i]);
		}
		DBApp.select("zqe0");
		String selectTrace0 = DBApp.getLastTrace("zqe0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+54));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("j87", cols1);
		String [][] records_j87 = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j87[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j87", records_j87[i]);
		}
		DBApp.select("j87");
		String selectTrace1 = DBApp.getLastTrace("j87");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o17", cols2);
		String [][] records_o17 = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o17[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o17", records_o17[i]);
		}
		DBApp.select("o17");
		String selectTrace2 = DBApp.getLastTrace("o17");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t1", cols3);
		String [][] records_t1 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t1", records_t1[i]);
		}
		DBApp.select("t1");
		String selectTrace3 = DBApp.getLastTrace("t1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("ski", cols4);
		String [][] records_ski = new String[100][cols4.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ski[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ski", records_ski[i]);
		}
		DBApp.select("ski");
		String selectTrace4 = DBApp.getLastTrace("ski");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+100));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("j9", cols0);
		String [][] records_j9 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j9", records_j9[i]);
		}
		DBApp.select("j9");
		String selectTrace0 = DBApp.getLastTrace("j9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+96));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("w2u", cols1);
		String [][] records_w2u = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w2u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w2u", records_w2u[i]);
		}
		DBApp.select("w2u");
		String selectTrace1 = DBApp.getLastTrace("w2u");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+1));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("z7r", cols2);
		String [][] records_z7r = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z7r[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z7r", records_z7r[i]);
		}
		DBApp.select("z7r");
		String selectTrace2 = DBApp.getLastTrace("z7r");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("s8ax", cols3);
		String [][] records_s8ax = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s8ax[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s8ax", records_s8ax[i]);
		}
		DBApp.select("s8ax");
		String selectTrace3 = DBApp.getLastTrace("s8ax");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("csit6", cols4);
		String [][] records_csit6 = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_csit6[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("csit6", records_csit6[i]);
		}
		DBApp.select("csit6");
		String selectTrace4 = DBApp.getLastTrace("csit6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+48));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ya1", cols0);
		String [][] records_ya1 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ya1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ya1", records_ya1[i]);
		}
		DBApp.select("ya1");
		String selectTrace0 = DBApp.getLastTrace("ya1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("dj3", cols1);
		String [][] records_dj3 = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dj3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dj3", records_dj3[i]);
		}
		DBApp.select("dj3");
		String selectTrace1 = DBApp.getLastTrace("dj3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("w3", cols0);
		String [][] records_w3 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w3", records_w3[i]);
		}
		DBApp.select("w3");
		String selectTrace0 = DBApp.getLastTrace("w3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ejq", cols1);
		String [][] records_ejq = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ejq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ejq", records_ejq[i]);
		}
		DBApp.select("ejq");
		String selectTrace1 = DBApp.getLastTrace("ejq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+63));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s1a6", cols2);
		String [][] records_s1a6 = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s1a6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s1a6", records_s1a6[i]);
		}
		DBApp.select("s1a6");
		String selectTrace2 = DBApp.getLastTrace("s1a6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+90));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("yq9", cols0);
		String [][] records_yq9 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yq9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yq9", records_yq9[i]);
		}
		DBApp.select("yq9");
		String selectTrace0 = DBApp.getLastTrace("yq9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(33.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+33));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("t6ax9", cols1);
		String [][] records_t6ax9 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t6ax9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6ax9", records_t6ax9[i]);
		}
		DBApp.select("t6ax9");
		String selectTrace1 = DBApp.getLastTrace("t6ax9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mne", cols0);
		String [][] records_mne = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mne[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mne", records_mne[i]);
		}
		DBApp.select("mne");
		String selectTrace0 = DBApp.getLastTrace("mne");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ru7y", cols1);
		String [][] records_ru7y = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ru7y[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ru7y", records_ru7y[i]);
		}
		DBApp.select("ru7y");
		String selectTrace1 = DBApp.getLastTrace("ru7y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ywk", cols0);
		String [][] records_ywk = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ywk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ywk", records_ywk[i]);
		}
		DBApp.select("ywk");
		String selectTrace0 = DBApp.getLastTrace("ywk");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+61));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k33", cols1);
		String [][] records_k33 = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k33[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k33", records_k33[i]);
		}
		DBApp.select("k33");
		String selectTrace1 = DBApp.getLastTrace("k33");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k108", cols2);
		String [][] records_k108 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k108[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k108", records_k108[i]);
		}
		DBApp.select("k108");
		String selectTrace2 = DBApp.getLastTrace("k108");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+7));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yd", cols0);
		String [][] records_yd = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yd", records_yd[i]);
		}
		DBApp.select("yd");
		String selectTrace0 = DBApp.getLastTrace("yd");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+7));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("na", cols1);
		String [][] records_na = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_na[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("na", records_na[i]);
		}
		DBApp.select("na");
		String selectTrace1 = DBApp.getLastTrace("na");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("uq7", cols2);
		String [][] records_uq7 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uq7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uq7", records_uq7[i]);
		}
		DBApp.select("uq7");
		String selectTrace2 = DBApp.getLastTrace("uq7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+48));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("r4l", cols3);
		String [][] records_r4l = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r4l[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4l", records_r4l[i]);
		}
		DBApp.select("r4l");
		String selectTrace3 = DBApp.getLastTrace("r4l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("at", cols4);
		String [][] records_at = new String[32][cols4.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_at[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("at", records_at[i]);
		}
		DBApp.select("at");
		String selectTrace4 = DBApp.getLastTrace("at");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("r62m", cols0);
		String [][] records_r62m = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r62m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r62m", records_r62m[i]);
		}
		DBApp.select("r62m");
		String selectTrace0 = DBApp.getLastTrace("r62m");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ou983", cols0);
		String [][] records_ou983 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ou983[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ou983", records_ou983[i]);
		}
		DBApp.select("ou983");
		String selectTrace0 = DBApp.getLastTrace("ou983");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+17));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("t3", cols1);
		String [][] records_t3 = new String[85][cols1.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t3", records_t3[i]);
		}
		DBApp.select("t3");
		String selectTrace1 = DBApp.getLastTrace("t3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("un", cols2);
		String [][] records_un = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_un[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("un", records_un[i]);
		}
		DBApp.select("un");
		String selectTrace2 = DBApp.getLastTrace("un");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("ba", cols3);
		String [][] records_ba = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ba[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ba", records_ba[i]);
		}
		DBApp.select("ba");
		String selectTrace3 = DBApp.getLastTrace("ba");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("q0ew0", cols4);
		String [][] records_q0ew0 = new String[35][cols4.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_q0ew0[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q0ew0", records_q0ew0[i]);
		}
		DBApp.select("q0ew0");
		String selectTrace4 = DBApp.getLastTrace("q0ew0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z3", cols0);
		String [][] records_z3 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z3", records_z3[i]);
		}
		DBApp.select("z3");
		String selectTrace0 = DBApp.getLastTrace("z3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b7dv", cols1);
		String [][] records_b7dv = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b7dv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b7dv", records_b7dv[i]);
		}
		DBApp.select("b7dv");
		String selectTrace1 = DBApp.getLastTrace("b7dv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g696", cols0);
		String [][] records_g696 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g696[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g696", records_g696[i]);
		}
		DBApp.select("g696");
		String selectTrace0 = DBApp.getLastTrace("g696");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+39));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w4", cols1);
		String [][] records_w4 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w4", records_w4[i]);
		}
		DBApp.select("w4");
		String selectTrace1 = DBApp.getLastTrace("w4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+93));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ym2i", cols0);
		String [][] records_ym2i = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ym2i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ym2i", records_ym2i[i]);
		}
		DBApp.select("ym2i");
		String selectTrace0 = DBApp.getLastTrace("ym2i");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+10));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("kmh", cols1);
		String [][] records_kmh = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kmh[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kmh", records_kmh[i]);
		}
		DBApp.select("kmh");
		String selectTrace1 = DBApp.getLastTrace("kmh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(66.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+66));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("tc", cols2);
		String [][] records_tc = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tc[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tc", records_tc[i]);
		}
		DBApp.select("tc");
		String selectTrace2 = DBApp.getLastTrace("tc");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+75));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v51", cols3);
		String [][] records_v51 = new String[41][cols3.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_v51[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v51", records_v51[i]);
		}
		DBApp.select("v51");
		String selectTrace3 = DBApp.getLastTrace("v51");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+41));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("f65", cols4);
		String [][] records_f65 = new String[94][cols4.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_f65[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f65", records_f65[i]);
		}
		DBApp.select("f65");
		String selectTrace4 = DBApp.getLastTrace("f65");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+94));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("or686", cols0);
		String [][] records_or686 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_or686[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("or686", records_or686[i]);
		}
		DBApp.select("or686");
		String selectTrace0 = DBApp.getLastTrace("or686");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+96));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("x85i4", cols1);
		String [][] records_x85i4 = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x85i4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x85i4", records_x85i4[i]);
		}
		DBApp.select("x85i4");
		String selectTrace1 = DBApp.getLastTrace("x85i4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i0", cols2);
		String [][] records_i0 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i0", records_i0[i]);
		}
		DBApp.select("i0");
		String selectTrace2 = DBApp.getLastTrace("i0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(28.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+28));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("lfekq", cols0);
		String [][] records_lfekq = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lfekq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lfekq", records_lfekq[i]);
		}
		DBApp.select("lfekq");
		String selectTrace0 = DBApp.getLastTrace("lfekq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+43));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hdcu0", cols1);
		String [][] records_hdcu0 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hdcu0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hdcu0", records_hdcu0[i]);
		}
		DBApp.select("hdcu0");
		String selectTrace1 = DBApp.getLastTrace("hdcu0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wn5", cols2);
		String [][] records_wn5 = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wn5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wn5", records_wn5[i]);
		}
		DBApp.select("wn5");
		String selectTrace2 = DBApp.getLastTrace("wn5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("wge7l", cols3);
		String [][] records_wge7l = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wge7l[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wge7l", records_wge7l[i]);
		}
		DBApp.select("wge7l");
		String selectTrace3 = DBApp.getLastTrace("wge7l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("m78t3", cols4);
		String [][] records_m78t3 = new String[56][cols4.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_m78t3[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m78t3", records_m78t3[i]);
		}
		DBApp.select("m78t3");
		String selectTrace4 = DBApp.getLastTrace("m78t3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+56));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("f8j", cols0);
		String [][] records_f8j = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f8j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f8j", records_f8j[i]);
		}
		DBApp.select("f8j");
		String selectTrace0 = DBApp.getLastTrace("f8j");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("us1", cols0);
		String [][] records_us1 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_us1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("us1", records_us1[i]);
		}
		DBApp.select("us1");
		String selectTrace0 = DBApp.getLastTrace("us1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+22));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lh67s", cols1);
		String [][] records_lh67s = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lh67s[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lh67s", records_lh67s[i]);
		}
		DBApp.select("lh67s");
		String selectTrace1 = DBApp.getLastTrace("lh67s");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("do4", cols2);
		String [][] records_do4 = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_do4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("do4", records_do4[i]);
		}
		DBApp.select("do4");
		String selectTrace2 = DBApp.getLastTrace("do4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+100));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d258", cols3);
		String [][] records_d258 = new String[30][cols3.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d258[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d258", records_d258[i]);
		}
		DBApp.select("d258");
		String selectTrace3 = DBApp.getLastTrace("d258");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b1", cols0);
		String [][] records_b1 = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b1", records_b1[i]);
		}
		DBApp.select("b1");
		String selectTrace0 = DBApp.getLastTrace("b1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+90));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("wc1", cols1);
		String [][] records_wc1 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wc1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wc1", records_wc1[i]);
		}
		DBApp.select("wc1");
		String selectTrace1 = DBApp.getLastTrace("wc1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("l7", cols2);
		String [][] records_l7 = new String[91][cols2.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7", records_l7[i]);
		}
		DBApp.select("l7");
		String selectTrace2 = DBApp.getLastTrace("l7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+91));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("aag4", cols3);
		String [][] records_aag4 = new String[86][cols3.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_aag4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("aag4", records_aag4[i]);
		}
		DBApp.select("aag4");
		String selectTrace3 = DBApp.getLastTrace("aag4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+86));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x7f", cols4);
		String [][] records_x7f = new String[16][cols4.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x7f[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7f", records_x7f[i]);
		}
		DBApp.select("x7f");
		String selectTrace4 = DBApp.getLastTrace("x7f");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("et", cols0);
		String [][] records_et = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_et[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("et", records_et[i]);
		}
		DBApp.select("et");
		String selectTrace0 = DBApp.getLastTrace("et");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+59));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("p54u", cols0);
		String [][] records_p54u = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p54u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p54u", records_p54u[i]);
		}
		DBApp.select("p54u");
		String selectTrace0 = DBApp.getLastTrace("p54u");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xx", cols1);
		String [][] records_xx = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xx", records_xx[i]);
		}
		DBApp.select("xx");
		String selectTrace1 = DBApp.getLastTrace("xx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("c01ha", cols2);
		String [][] records_c01ha = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c01ha[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c01ha", records_c01ha[i]);
		}
		DBApp.select("c01ha");
		String selectTrace2 = DBApp.getLastTrace("c01ha");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+26));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("f8c", cols0);
		String [][] records_f8c = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f8c[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f8c", records_f8c[i]);
		}
		DBApp.select("f8c");
		String selectTrace0 = DBApp.getLastTrace("f8c");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("y2p", cols1);
		String [][] records_y2p = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y2p[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y2p", records_y2p[i]);
		}
		DBApp.select("y2p");
		String selectTrace1 = DBApp.getLastTrace("y2p");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+58));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ui4", cols2);
		String [][] records_ui4 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ui4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ui4", records_ui4[i]);
		}
		DBApp.select("ui4");
		String selectTrace2 = DBApp.getLastTrace("ui4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mha", cols0);
		String [][] records_mha = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mha[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mha", records_mha[i]);
		}
		DBApp.select("mha");
		String selectTrace0 = DBApp.getLastTrace("mha");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+55));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t2db2", cols1);
		String [][] records_t2db2 = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t2db2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t2db2", records_t2db2[i]);
		}
		DBApp.select("t2db2");
		String selectTrace1 = DBApp.getLastTrace("t2db2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+43));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("l07", cols2);
		String [][] records_l07 = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l07[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l07", records_l07[i]);
		}
		DBApp.select("l07");
		String selectTrace2 = DBApp.getLastTrace("l07");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+20));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("zvvk", cols3);
		String [][] records_zvvk = new String[87][cols3.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zvvk[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zvvk", records_zvvk[i]);
		}
		DBApp.select("zvvk");
		String selectTrace3 = DBApp.getLastTrace("zvvk");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b25if", cols4);
		String [][] records_b25if = new String[49][cols4.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b25if[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b25if", records_b25if[i]);
		}
		DBApp.select("b25if");
		String selectTrace4 = DBApp.getLastTrace("b25if");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("a1", cols0);
		String [][] records_a1 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a1", records_a1[i]);
		}
		DBApp.select("a1");
		String selectTrace0 = DBApp.getLastTrace("a1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("op2", cols1);
		String [][] records_op2 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_op2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("op2", records_op2[i]);
		}
		DBApp.select("op2");
		String selectTrace1 = DBApp.getLastTrace("op2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a6k", cols2);
		String [][] records_a6k = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_a6k[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a6k", records_a6k[i]);
		}
		DBApp.select("a6k");
		String selectTrace2 = DBApp.getLastTrace("a6k");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+7));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("fiu", cols3);
		String [][] records_fiu = new String[29][cols3.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fiu[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fiu", records_fiu[i]);
		}
		DBApp.select("fiu");
		String selectTrace3 = DBApp.getLastTrace("fiu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(29.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+29));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wu", cols4);
		String [][] records_wu = new String[21][cols4.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_wu[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wu", records_wu[i]);
		}
		DBApp.select("wu");
		String selectTrace4 = DBApp.getLastTrace("wu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+21));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("wq3", cols0);
		String [][] records_wq3 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wq3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wq3", records_wq3[i]);
		}
		DBApp.select("wq3");
		String selectTrace0 = DBApp.getLastTrace("wq3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("f0", cols0);
		String [][] records_f0 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f0", records_f0[i]);
		}
		DBApp.select("f0");
		String selectTrace0 = DBApp.getLastTrace("f0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("r520", cols1);
		String [][] records_r520 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r520[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r520", records_r520[i]);
		}
		DBApp.select("r520");
		String selectTrace1 = DBApp.getLastTrace("r520");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("l24", cols0);
		String [][] records_l24 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l24[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l24", records_l24[i]);
		}
		DBApp.select("l24");
		String selectTrace0 = DBApp.getLastTrace("l24");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("mw02", cols1);
		String [][] records_mw02 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mw02[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mw02", records_mw02[i]);
		}
		DBApp.select("mw02");
		String selectTrace1 = DBApp.getLastTrace("mw02");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("p40y0", cols2);
		String [][] records_p40y0 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p40y0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p40y0", records_p40y0[i]);
		}
		DBApp.select("p40y0");
		String selectTrace2 = DBApp.getLastTrace("p40y0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(28.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+28));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ok1u", cols3);
		String [][] records_ok1u = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ok1u[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ok1u", records_ok1u[i]);
		}
		DBApp.select("ok1u");
		String selectTrace3 = DBApp.getLastTrace("ok1u");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qa2", cols0);
		String [][] records_qa2 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qa2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qa2", records_qa2[i]);
		}
		DBApp.select("qa2");
		String selectTrace0 = DBApp.getLastTrace("qa2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+15));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j7h", cols1);
		String [][] records_j7h = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j7h[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j7h", records_j7h[i]);
		}
		DBApp.select("j7h");
		String selectTrace1 = DBApp.getLastTrace("j7h");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bi6", cols2);
		String [][] records_bi6 = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bi6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bi6", records_bi6[i]);
		}
		DBApp.select("bi6");
		String selectTrace2 = DBApp.getLastTrace("bi6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h3", cols3);
		String [][] records_h3 = new String[19][cols3.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h3", records_h3[i]);
		}
		DBApp.select("h3");
		String selectTrace3 = DBApp.getLastTrace("h3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(19.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+19));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("n94t", cols4);
		String [][] records_n94t = new String[82][cols4.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_n94t[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n94t", records_n94t[i]);
		}
		DBApp.select("n94t");
		String selectTrace4 = DBApp.getLastTrace("n94t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("kbx", cols0);
		String [][] records_kbx = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kbx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kbx", records_kbx[i]);
		}
		DBApp.select("kbx");
		String selectTrace0 = DBApp.getLastTrace("kbx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d1xu4", cols1);
		String [][] records_d1xu4 = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d1xu4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d1xu4", records_d1xu4[i]);
		}
		DBApp.select("d1xu4");
		String selectTrace1 = DBApp.getLastTrace("d1xu4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+84));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("yl26h", cols2);
		String [][] records_yl26h = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yl26h[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yl26h", records_yl26h[i]);
		}
		DBApp.select("yl26h");
		String selectTrace2 = DBApp.getLastTrace("yl26h");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+90));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("bgp2", cols3);
		String [][] records_bgp2 = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bgp2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bgp2", records_bgp2[i]);
		}
		DBApp.select("bgp2");
		String selectTrace3 = DBApp.getLastTrace("bgp2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(33.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+33));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("irb", cols0);
		String [][] records_irb = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_irb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("irb", records_irb[i]);
		}
		DBApp.select("irb");
		String selectTrace0 = DBApp.getLastTrace("irb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xi4", cols1);
		String [][] records_xi4 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xi4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xi4", records_xi4[i]);
		}
		DBApp.select("xi4");
		String selectTrace1 = DBApp.getLastTrace("xi4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m61a3", cols2);
		String [][] records_m61a3 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m61a3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m61a3", records_m61a3[i]);
		}
		DBApp.select("m61a3");
		String selectTrace2 = DBApp.getLastTrace("m61a3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+11));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ym8", cols0);
		String [][] records_ym8 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ym8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ym8", records_ym8[i]);
		}
		DBApp.select("ym8");
		String selectTrace0 = DBApp.getLastTrace("ym8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tjs", cols1);
		String [][] records_tjs = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tjs[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tjs", records_tjs[i]);
		}
		DBApp.select("tjs");
		String selectTrace1 = DBApp.getLastTrace("tjs");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("vxkr", cols0);
		String [][] records_vxkr = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vxkr[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vxkr", records_vxkr[i]);
		}
		DBApp.select("vxkr");
		String selectTrace0 = DBApp.getLastTrace("vxkr");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+97));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("c0fx", cols1);
		String [][] records_c0fx = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c0fx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c0fx", records_c0fx[i]);
		}
		DBApp.select("c0fx");
		String selectTrace1 = DBApp.getLastTrace("c0fx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+84));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mkfh", cols0);
		String [][] records_mkfh = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mkfh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mkfh", records_mkfh[i]);
		}
		DBApp.select("mkfh");
		String selectTrace0 = DBApp.getLastTrace("mkfh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+39));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y54", cols0);
		String [][] records_y54 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y54[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y54", records_y54[i]);
		}
		DBApp.select("y54");
		String selectTrace0 = DBApp.getLastTrace("y54");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+18));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("d2", cols1);
		String [][] records_d2 = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d2", records_d2[i]);
		}
		DBApp.select("d2");
		String selectTrace1 = DBApp.getLastTrace("d2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nd7", cols2);
		String [][] records_nd7 = new String[54][cols2.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_nd7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nd7", records_nd7[i]);
		}
		DBApp.select("nd7");
		String selectTrace2 = DBApp.getLastTrace("nd7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+54));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("p41", cols3);
		String [][] records_p41 = new String[60][cols3.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_p41[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p41", records_p41[i]);
		}
		DBApp.select("p41");
		String selectTrace3 = DBApp.getLastTrace("p41");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("u5y", cols4);
		String [][] records_u5y = new String[94][cols4.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_u5y[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u5y", records_u5y[i]);
		}
		DBApp.select("u5y");
		String selectTrace4 = DBApp.getLastTrace("u5y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+94));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kd3", cols0);
		String [][] records_kd3 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kd3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kd3", records_kd3[i]);
		}
		DBApp.select("kd3");
		String selectTrace0 = DBApp.getLastTrace("kd3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("cri6", cols1);
		String [][] records_cri6 = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cri6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cri6", records_cri6[i]);
		}
		DBApp.select("cri6");
		String selectTrace1 = DBApp.getLastTrace("cri6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+91));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("hmo", cols0);
		String [][] records_hmo = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hmo[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hmo", records_hmo[i]);
		}
		DBApp.select("hmo");
		String selectTrace0 = DBApp.getLastTrace("hmo");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+75));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d6", cols1);
		String [][] records_d6 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d6", records_d6[i]);
		}
		DBApp.select("d6");
		String selectTrace1 = DBApp.getLastTrace("d6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+92));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dxl", cols2);
		String [][] records_dxl = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dxl[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dxl", records_dxl[i]);
		}
		DBApp.select("dxl");
		String selectTrace2 = DBApp.getLastTrace("dxl");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kqe", cols3);
		String [][] records_kqe = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_kqe[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kqe", records_kqe[i]);
		}
		DBApp.select("kqe");
		String selectTrace3 = DBApp.getLastTrace("kqe");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("e9m59", cols0);
		String [][] records_e9m59 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e9m59[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e9m59", records_e9m59[i]);
		}
		DBApp.select("e9m59");
		String selectTrace0 = DBApp.getLastTrace("e9m59");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+44));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rk", cols1);
		String [][] records_rk = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rk", records_rk[i]);
		}
		DBApp.select("rk");
		String selectTrace1 = DBApp.getLastTrace("rk");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+92));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("o0i8", cols0);
		String [][] records_o0i8 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o0i8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o0i8", records_o0i8[i]);
		}
		DBApp.select("o0i8");
		String selectTrace0 = DBApp.getLastTrace("o0i8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+86));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("iw70", cols1);
		String [][] records_iw70 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_iw70[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iw70", records_iw70[i]);
		}
		DBApp.select("iw70");
		String selectTrace1 = DBApp.getLastTrace("iw70");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+56));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("k933", cols0);
		String [][] records_k933 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k933[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k933", records_k933[i]);
		}
		DBApp.select("k933");
		String selectTrace0 = DBApp.getLastTrace("k933");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("gi8b", cols1);
		String [][] records_gi8b = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gi8b[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gi8b", records_gi8b[i]);
		}
		DBApp.select("gi8b");
		String selectTrace1 = DBApp.getLastTrace("gi8b");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ft", cols2);
		String [][] records_ft = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ft[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ft", records_ft[i]);
		}
		DBApp.select("ft");
		String selectTrace2 = DBApp.getLastTrace("ft");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("nf8l", cols0);
		String [][] records_nf8l = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nf8l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nf8l", records_nf8l[i]);
		}
		DBApp.select("nf8l");
		String selectTrace0 = DBApp.getLastTrace("nf8l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("vi", cols1);
		String [][] records_vi = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vi[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vi", records_vi[i]);
		}
		DBApp.select("vi");
		String selectTrace1 = DBApp.getLastTrace("vi");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+58));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("y2d", cols2);
		String [][] records_y2d = new String[30][cols2.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y2d[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y2d", records_y2d[i]);
		}
		DBApp.select("y2d");
		String selectTrace2 = DBApp.getLastTrace("y2d");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rqm", cols3);
		String [][] records_rqm = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_rqm[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rqm", records_rqm[i]);
		}
		DBApp.select("rqm");
		String selectTrace3 = DBApp.getLastTrace("rqm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b2", cols0);
		String [][] records_b2 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b2", records_b2[i]);
		}
		DBApp.select("b2");
		String selectTrace0 = DBApp.getLastTrace("b2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+7));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u46", cols1);
		String [][] records_u46 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u46[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u46", records_u46[i]);
		}
		DBApp.select("u46");
		String selectTrace1 = DBApp.getLastTrace("u46");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+92));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("os", cols2);
		String [][] records_os = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_os[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("os", records_os[i]);
		}
		DBApp.select("os");
		String selectTrace2 = DBApp.getLastTrace("os");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("sh5", cols3);
		String [][] records_sh5 = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sh5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sh5", records_sh5[i]);
		}
		DBApp.select("sh5");
		String selectTrace3 = DBApp.getLastTrace("sh5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("jtg", cols0);
		String [][] records_jtg = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jtg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jtg", records_jtg[i]);
		}
		DBApp.select("jtg");
		String selectTrace0 = DBApp.getLastTrace("jtg");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i73m", cols0);
		String [][] records_i73m = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i73m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i73m", records_i73m[i]);
		}
		DBApp.select("i73m");
		String selectTrace0 = DBApp.getLastTrace("i73m");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("g01e", cols1);
		String [][] records_g01e = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g01e[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g01e", records_g01e[i]);
		}
		DBApp.select("g01e");
		String selectTrace1 = DBApp.getLastTrace("g01e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+4));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("s7", cols2);
		String [][] records_s7 = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s7", records_s7[i]);
		}
		DBApp.select("s7");
		String selectTrace2 = DBApp.getLastTrace("s7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+18));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("r78q", cols3);
		String [][] records_r78q = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r78q[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r78q", records_r78q[i]);
		}
		DBApp.select("r78q");
		String selectTrace3 = DBApp.getLastTrace("r78q");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ve172", cols4);
		String [][] records_ve172 = new String[33][cols4.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ve172[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ve172", records_ve172[i]);
		}
		DBApp.select("ve172");
		String selectTrace4 = DBApp.getLastTrace("ve172");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(33.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+33));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("i2j", cols0);
		String [][] records_i2j = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i2j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i2j", records_i2j[i]);
		}
		DBApp.select("i2j");
		String selectTrace0 = DBApp.getLastTrace("i2j");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("m38", cols0);
		String [][] records_m38 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m38[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m38", records_m38[i]);
		}
		DBApp.select("m38");
		String selectTrace0 = DBApp.getLastTrace("m38");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+26));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("c0l6", cols1);
		String [][] records_c0l6 = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c0l6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c0l6", records_c0l6[i]);
		}
		DBApp.select("c0l6");
		String selectTrace1 = DBApp.getLastTrace("c0l6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("z3", cols2);
		String [][] records_z3 = new String[80][cols2.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z3", records_z3[i]);
		}
		DBApp.select("z3");
		String selectTrace2 = DBApp.getLastTrace("z3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(80.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+80));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l0umy", cols3);
		String [][] records_l0umy = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_l0umy[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l0umy", records_l0umy[i]);
		}
		DBApp.select("l0umy");
		String selectTrace3 = DBApp.getLastTrace("l0umy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+44));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("tm", cols0);
		String [][] records_tm = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tm[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tm", records_tm[i]);
		}
		DBApp.select("tm");
		String selectTrace0 = DBApp.getLastTrace("tm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("o58", cols1);
		String [][] records_o58 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o58[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o58", records_o58[i]);
		}
		DBApp.select("o58");
		String selectTrace1 = DBApp.getLastTrace("o58");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("eoi9", cols2);
		String [][] records_eoi9 = new String[44][cols2.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_eoi9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eoi9", records_eoi9[i]);
		}
		DBApp.select("eoi9");
		String selectTrace2 = DBApp.getLastTrace("eoi9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+44));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e6", cols3);
		String [][] records_e6 = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e6", records_e6[i]);
		}
		DBApp.select("e6");
		String selectTrace3 = DBApp.getLastTrace("e6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+97));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wl", cols0);
		String [][] records_wl = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wl[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wl", records_wl[i]);
		}
		DBApp.select("wl");
		String selectTrace0 = DBApp.getLastTrace("wl");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("z7y", cols0);
		String [][] records_z7y = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z7y[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z7y", records_z7y[i]);
		}
		DBApp.select("z7y");
		String selectTrace0 = DBApp.getLastTrace("z7y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+96));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qd", cols1);
		String [][] records_qd = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qd", records_qd[i]);
		}
		DBApp.select("qd");
		String selectTrace1 = DBApp.getLastTrace("qd");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e16", cols0);
		String [][] records_e16 = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e16[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e16", records_e16[i]);
		}
		DBApp.select("e16");
		String selectTrace0 = DBApp.getLastTrace("e16");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+81));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t01", cols1);
		String [][] records_t01 = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t01[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t01", records_t01[i]);
		}
		DBApp.select("t01");
		String selectTrace1 = DBApp.getLastTrace("t01");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+59));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fj5", cols2);
		String [][] records_fj5 = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_fj5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fj5", records_fj5[i]);
		}
		DBApp.select("fj5");
		String selectTrace2 = DBApp.getLastTrace("fj5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("umjq", cols3);
		String [][] records_umjq = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_umjq[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("umjq", records_umjq[i]);
		}
		DBApp.select("umjq");
		String selectTrace3 = DBApp.getLastTrace("umjq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("o07pb", cols0);
		String [][] records_o07pb = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o07pb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o07pb", records_o07pb[i]);
		}
		DBApp.select("o07pb");
		String selectTrace0 = DBApp.getLastTrace("o07pb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ry", cols1);
		String [][] records_ry = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ry[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ry", records_ry[i]);
		}
		DBApp.select("ry");
		String selectTrace1 = DBApp.getLastTrace("ry");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+21));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("dg62l", cols2);
		String [][] records_dg62l = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dg62l[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dg62l", records_dg62l[i]);
		}
		DBApp.select("dg62l");
		String selectTrace2 = DBApp.getLastTrace("dg62l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+15));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("x0dh0", cols0);
		String [][] records_x0dh0 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x0dh0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x0dh0", records_x0dh0[i]);
		}
		DBApp.select("x0dh0");
		String selectTrace0 = DBApp.getLastTrace("x0dh0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d80rc", cols1);
		String [][] records_d80rc = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d80rc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d80rc", records_d80rc[i]);
		}
		DBApp.select("d80rc");
		String selectTrace1 = DBApp.getLastTrace("d80rc");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("b4e71", cols2);
		String [][] records_b4e71 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b4e71[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b4e71", records_b4e71[i]);
		}
		DBApp.select("b4e71");
		String selectTrace2 = DBApp.getLastTrace("b4e71");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("b59", cols3);
		String [][] records_b59 = new String[63][cols3.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b59[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b59", records_b59[i]);
		}
		DBApp.select("b59");
		String selectTrace3 = DBApp.getLastTrace("b59");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+63));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("ecv", cols4);
		String [][] records_ecv = new String[85][cols4.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ecv[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ecv", records_ecv[i]);
		}
		DBApp.select("ecv");
		String selectTrace4 = DBApp.getLastTrace("ecv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("hef", cols0);
		String [][] records_hef = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hef[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hef", records_hef[i]);
		}
		DBApp.select("hef");
		String selectTrace0 = DBApp.getLastTrace("hef");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("gx2yo", cols1);
		String [][] records_gx2yo = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gx2yo[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gx2yo", records_gx2yo[i]);
		}
		DBApp.select("gx2yo");
		String selectTrace1 = DBApp.getLastTrace("gx2yo");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m7j", cols2);
		String [][] records_m7j = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m7j[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m7j", records_m7j[i]);
		}
		DBApp.select("m7j");
		String selectTrace2 = DBApp.getLastTrace("m7j");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gua", cols0);
		String [][] records_gua = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gua[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gua", records_gua[i]);
		}
		DBApp.select("gua");
		String selectTrace0 = DBApp.getLastTrace("gua");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f7o", cols1);
		String [][] records_f7o = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f7o[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f7o", records_f7o[i]);
		}
		DBApp.select("f7o");
		String selectTrace1 = DBApp.getLastTrace("f7o");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gdj", cols2);
		String [][] records_gdj = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_gdj[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gdj", records_gdj[i]);
		}
		DBApp.select("gdj");
		String selectTrace2 = DBApp.getLastTrace("gdj");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(28.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+28));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rp", cols0);
		String [][] records_rp = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rp", records_rp[i]);
		}
		DBApp.select("rp");
		String selectTrace0 = DBApp.getLastTrace("rp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("iq4za", cols1);
		String [][] records_iq4za = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_iq4za[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iq4za", records_iq4za[i]);
		}
		DBApp.select("iq4za");
		String selectTrace1 = DBApp.getLastTrace("iq4za");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+81));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("j72", cols0);
		String [][] records_j72 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j72[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j72", records_j72[i]);
		}
		DBApp.select("j72");
		String selectTrace0 = DBApp.getLastTrace("j72");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("q2t", cols1);
		String [][] records_q2t = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q2t[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q2t", records_q2t[i]);
		}
		DBApp.select("q2t");
		String selectTrace1 = DBApp.getLastTrace("q2t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("m7", cols0);
		String [][] records_m7 = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m7", records_m7[i]);
		}
		DBApp.select("m7");
		String selectTrace0 = DBApp.getLastTrace("m7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+41));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w8c", cols1);
		String [][] records_w8c = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w8c[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w8c", records_w8c[i]);
		}
		DBApp.select("w8c");
		String selectTrace1 = DBApp.getLastTrace("w8c");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("unv", cols2);
		String [][] records_unv = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_unv[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("unv", records_unv[i]);
		}
		DBApp.select("unv");
		String selectTrace2 = DBApp.getLastTrace("unv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+40));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cc", cols3);
		String [][] records_cc = new String[84][cols3.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_cc[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cc", records_cc[i]);
		}
		DBApp.select("cc");
		String selectTrace3 = DBApp.getLastTrace("cc");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+84));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xuf48", cols4);
		String [][] records_xuf48 = new String[18][cols4.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xuf48[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xuf48", records_xuf48[i]);
		}
		DBApp.select("xuf48");
		String selectTrace4 = DBApp.getLastTrace("xuf48");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+18));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("t80", cols0);
		String [][] records_t80 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t80[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t80", records_t80[i]);
		}
		DBApp.select("t80");
		String selectTrace0 = DBApp.getLastTrace("t80");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+10));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("d68", cols1);
		String [][] records_d68 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d68[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d68", records_d68[i]);
		}
		DBApp.select("d68");
		String selectTrace1 = DBApp.getLastTrace("d68");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("ze0x9", cols2);
		String [][] records_ze0x9 = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ze0x9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ze0x9", records_ze0x9[i]);
		}
		DBApp.select("ze0x9");
		String selectTrace2 = DBApp.getLastTrace("ze0x9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n94", cols3);
		String [][] records_n94 = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n94[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n94", records_n94[i]);
		}
		DBApp.select("n94");
		String selectTrace3 = DBApp.getLastTrace("n94");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qdk54", cols4);
		String [][] records_qdk54 = new String[56][cols4.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_qdk54[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qdk54", records_qdk54[i]);
		}
		DBApp.select("qdk54");
		String selectTrace4 = DBApp.getLastTrace("qdk54");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+56));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xs3", cols0);
		String [][] records_xs3 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xs3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xs3", records_xs3[i]);
		}
		DBApp.select("xs3");
		String selectTrace0 = DBApp.getLastTrace("xs3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+39));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("py3z", cols1);
		String [][] records_py3z = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_py3z[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("py3z", records_py3z[i]);
		}
		DBApp.select("py3z");
		String selectTrace1 = DBApp.getLastTrace("py3z");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(98.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+98));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("t5lva", cols2);
		String [][] records_t5lva = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_t5lva[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t5lva", records_t5lva[i]);
		}
		DBApp.select("t5lva");
		String selectTrace2 = DBApp.getLastTrace("t5lva");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+96));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("n6", cols0);
		String [][] records_n6 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n6", records_n6[i]);
		}
		DBApp.select("n6");
		String selectTrace0 = DBApp.getLastTrace("n6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+89));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a3", cols1);
		String [][] records_a3 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a3", records_a3[i]);
		}
		DBApp.select("a3");
		String selectTrace1 = DBApp.getLastTrace("a3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("bn25", cols2);
		String [][] records_bn25 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bn25[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn25", records_bn25[i]);
		}
		DBApp.select("bn25");
		String selectTrace2 = DBApp.getLastTrace("bn25");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("h8z8b", cols3);
		String [][] records_h8z8b = new String[98][cols3.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h8z8b[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h8z8b", records_h8z8b[i]);
		}
		DBApp.select("h8z8b");
		String selectTrace3 = DBApp.getLastTrace("h8z8b");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(98.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+98));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j25", cols4);
		String [][] records_j25 = new String[43][cols4.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_j25[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j25", records_j25[i]);
		}
		DBApp.select("j25");
		String selectTrace4 = DBApp.getLastTrace("j25");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+43));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("kq8b", cols0);
		String [][] records_kq8b = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kq8b[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kq8b", records_kq8b[i]);
		}
		DBApp.select("kq8b");
		String selectTrace0 = DBApp.getLastTrace("kq8b");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("shh", cols1);
		String [][] records_shh = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_shh[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("shh", records_shh[i]);
		}
		DBApp.select("shh");
		String selectTrace1 = DBApp.getLastTrace("shh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+100));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("c29dp", cols2);
		String [][] records_c29dp = new String[91][cols2.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c29dp[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c29dp", records_c29dp[i]);
		}
		DBApp.select("c29dp");
		String selectTrace2 = DBApp.getLastTrace("c29dp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+91));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("no", cols0);
		String [][] records_no = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_no[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("no", records_no[i]);
		}
		DBApp.select("no");
		String selectTrace0 = DBApp.getLastTrace("no");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+27));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("yz23", cols0);
		String [][] records_yz23 = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yz23[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yz23", records_yz23[i]);
		}
		DBApp.select("yz23");
		String selectTrace0 = DBApp.getLastTrace("yz23");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(29.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+29));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("wqn", cols1);
		String [][] records_wqn = new String[52][cols1.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wqn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wqn", records_wqn[i]);
		}
		DBApp.select("wqn");
		String selectTrace1 = DBApp.getLastTrace("wqn");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+52));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gq3", cols0);
		String [][] records_gq3 = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gq3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gq3", records_gq3[i]);
		}
		DBApp.select("gq3");
		String selectTrace0 = DBApp.getLastTrace("gq3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r8fb7", cols0);
		String [][] records_r8fb7 = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r8fb7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8fb7", records_r8fb7[i]);
		}
		DBApp.select("r8fb7");
		String selectTrace0 = DBApp.getLastTrace("r8fb7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d3", cols1);
		String [][] records_d3 = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d3", records_d3[i]);
		}
		DBApp.select("d3");
		String selectTrace1 = DBApp.getLastTrace("d3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(29.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+29));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d9vt", cols0);
		String [][] records_d9vt = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d9vt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d9vt", records_d9vt[i]);
		}
		DBApp.select("d9vt");
		String selectTrace0 = DBApp.getLastTrace("d9vt");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+69));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c27", cols1);
		String [][] records_c27 = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c27[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c27", records_c27[i]);
		}
		DBApp.select("c27");
		String selectTrace1 = DBApp.getLastTrace("c27");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("x7", cols2);
		String [][] records_x7 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7", records_x7[i]);
		}
		DBApp.select("x7");
		String selectTrace2 = DBApp.getLastTrace("x7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("t027w", cols3);
		String [][] records_t027w = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t027w[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t027w", records_t027w[i]);
		}
		DBApp.select("t027w");
		String selectTrace3 = DBApp.getLastTrace("t027w");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k1sy", cols0);
		String [][] records_k1sy = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k1sy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k1sy", records_k1sy[i]);
		}
		DBApp.select("k1sy");
		String selectTrace0 = DBApp.getLastTrace("k1sy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("m59b7", cols1);
		String [][] records_m59b7 = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m59b7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m59b7", records_m59b7[i]);
		}
		DBApp.select("m59b7");
		String selectTrace1 = DBApp.getLastTrace("m59b7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jy", cols2);
		String [][] records_jy = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jy[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jy", records_jy[i]);
		}
		DBApp.select("jy");
		String selectTrace2 = DBApp.getLastTrace("jy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vjk6", cols3);
		String [][] records_vjk6 = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vjk6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vjk6", records_vjk6[i]);
		}
		DBApp.select("vjk6");
		String selectTrace3 = DBApp.getLastTrace("vjk6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("x6r", cols4);
		String [][] records_x6r = new String[36][cols4.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x6r[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x6r", records_x6r[i]);
		}
		DBApp.select("x6r");
		String selectTrace4 = DBApp.getLastTrace("x6r");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("h00", cols0);
		String [][] records_h00 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h00[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h00", records_h00[i]);
		}
		DBApp.select("h00");
		String selectTrace0 = DBApp.getLastTrace("h00");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("sj", cols1);
		String [][] records_sj = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sj", records_sj[i]);
		}
		DBApp.select("sj");
		String selectTrace1 = DBApp.getLastTrace("sj");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+68));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p60", cols0);
		String [][] records_p60 = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p60[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p60", records_p60[i]);
		}
		DBApp.select("p60");
		String selectTrace0 = DBApp.getLastTrace("p60");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("s2", cols1);
		String [][] records_s2 = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s2", records_s2[i]);
		}
		DBApp.select("s2");
		String selectTrace1 = DBApp.getLastTrace("s2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+70));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s0r", cols2);
		String [][] records_s0r = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s0r[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s0r", records_s0r[i]);
		}
		DBApp.select("s0r");
		String selectTrace2 = DBApp.getLastTrace("s0r");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("li8as", cols0);
		String [][] records_li8as = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_li8as[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("li8as", records_li8as[i]);
		}
		DBApp.select("li8as");
		String selectTrace0 = DBApp.getLastTrace("li8as");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+40));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1469);
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