package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests05
{

	@Test(timeout = 1000000)
	public void test87TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qaw", cols0);
		String [][] records_qaw = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qaw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qaw", records_qaw[i]);
		}
		DBApp.select("qaw");
		String selectTrace0 = DBApp.getLastTrace("qaw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b46s", cols1);
		String [][] records_b46s = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b46s[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b46s", records_b46s[i]);
		}
		DBApp.select("b46s");
		String selectTrace1 = DBApp.getLastTrace("b46s");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("q5l", cols2);
		String [][] records_q5l = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q5l[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q5l", records_q5l[i]);
		}
		DBApp.select("q5l");
		String selectTrace2 = DBApp.getLastTrace("q5l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hc", cols3);
		String [][] records_hc = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_hc[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hc", records_hc[i]);
		}
		DBApp.select("hc");
		String selectTrace3 = DBApp.getLastTrace("hc");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ih5", cols0);
		String [][] records_ih5 = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ih5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ih5", records_ih5[i]);
		}
		DBApp.select("ih5");
		String selectTrace0 = DBApp.getLastTrace("ih5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+41));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("uil", cols1);
		String [][] records_uil = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_uil[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uil", records_uil[i]);
		}
		DBApp.select("uil");
		String selectTrace1 = DBApp.getLastTrace("uil");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("atz20", cols2);
		String [][] records_atz20 = new String[53][cols2.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_atz20[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("atz20", records_atz20[i]);
		}
		DBApp.select("atz20");
		String selectTrace2 = DBApp.getLastTrace("atz20");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("bzf3", cols3);
		String [][] records_bzf3 = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bzf3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bzf3", records_bzf3[i]);
		}
		DBApp.select("bzf3");
		String selectTrace3 = DBApp.getLastTrace("bzf3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ih", cols0);
		String [][] records_ih = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ih[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ih", records_ih[i]);
		}
		DBApp.select("ih");
		String selectTrace0 = DBApp.getLastTrace("ih");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ke8", cols1);
		String [][] records_ke8 = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ke8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ke8", records_ke8[i]);
		}
		DBApp.select("ke8");
		String selectTrace1 = DBApp.getLastTrace("ke8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("i40", cols2);
		String [][] records_i40 = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i40[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i40", records_i40[i]);
		}
		DBApp.select("i40");
		String selectTrace2 = DBApp.getLastTrace("i40");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("me", cols3);
		String [][] records_me = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_me[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("me", records_me[i]);
		}
		DBApp.select("me");
		String selectTrace3 = DBApp.getLastTrace("me");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cnm", cols4);
		String [][] records_cnm = new String[83][cols4.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_cnm[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cnm", records_cnm[i]);
		}
		DBApp.select("cnm");
		String selectTrace4 = DBApp.getLastTrace("cnm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("mrz7i", cols0);
		String [][] records_mrz7i = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mrz7i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mrz7i", records_mrz7i[i]);
		}
		DBApp.select("mrz7i");
		String selectTrace0 = DBApp.getLastTrace("mrz7i");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+20));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("rxh0", cols0);
		String [][] records_rxh0 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rxh0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rxh0", records_rxh0[i]);
		}
		DBApp.select("rxh0");
		String selectTrace0 = DBApp.getLastTrace("rxh0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ctx", cols1);
		String [][] records_ctx = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ctx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ctx", records_ctx[i]);
		}
		DBApp.select("ctx");
		String selectTrace1 = DBApp.getLastTrace("ctx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p0ji", cols2);
		String [][] records_p0ji = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p0ji[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p0ji", records_p0ji[i]);
		}
		DBApp.select("p0ji");
		String selectTrace2 = DBApp.getLastTrace("p0ji");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("r3f", cols3);
		String [][] records_r3f = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r3f[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r3f", records_r3f[i]);
		}
		DBApp.select("r3f");
		String selectTrace3 = DBApp.getLastTrace("r3f");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+55));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vua", cols4);
		String [][] records_vua = new String[47][cols4.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vua[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vua", records_vua[i]);
		}
		DBApp.select("vua");
		String selectTrace4 = DBApp.getLastTrace("vua");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("yw0", cols0);
		String [][] records_yw0 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yw0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yw0", records_yw0[i]);
		}
		DBApp.select("yw0");
		String selectTrace0 = DBApp.getLastTrace("yw0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("s30", cols1);
		String [][] records_s30 = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s30[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s30", records_s30[i]);
		}
		DBApp.select("s30");
		String selectTrace1 = DBApp.getLastTrace("s30");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("w00", cols0);
		String [][] records_w00 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w00[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w00", records_w00[i]);
		}
		DBApp.select("w00");
		String selectTrace0 = DBApp.getLastTrace("w00");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+68));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d06", cols0);
		String [][] records_d06 = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d06[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d06", records_d06[i]);
		}
		DBApp.select("d06");
		String selectTrace0 = DBApp.getLastTrace("d06");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("p85zw", cols1);
		String [][] records_p85zw = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_p85zw[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p85zw", records_p85zw[i]);
		}
		DBApp.select("p85zw");
		String selectTrace1 = DBApp.getLastTrace("p85zw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("f309s", cols2);
		String [][] records_f309s = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f309s[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f309s", records_f309s[i]);
		}
		DBApp.select("f309s");
		String selectTrace2 = DBApp.getLastTrace("f309s");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+6));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gs4", cols3);
		String [][] records_gs4 = new String[72][cols3.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gs4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gs4", records_gs4[i]);
		}
		DBApp.select("gs4");
		String selectTrace3 = DBApp.getLastTrace("gs4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("oe470", cols4);
		String [][] records_oe470 = new String[50][cols4.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_oe470[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oe470", records_oe470[i]);
		}
		DBApp.select("oe470");
		String selectTrace4 = DBApp.getLastTrace("oe470");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("j32", cols0);
		String [][] records_j32 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j32[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j32", records_j32[i]);
		}
		DBApp.select("j32");
		String selectTrace0 = DBApp.getLastTrace("j32");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ox", cols1);
		String [][] records_ox = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ox[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ox", records_ox[i]);
		}
		DBApp.select("ox");
		String selectTrace1 = DBApp.getLastTrace("ox");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("xp4", cols2);
		String [][] records_xp4 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xp4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xp4", records_xp4[i]);
		}
		DBApp.select("xp4");
		String selectTrace2 = DBApp.getLastTrace("xp4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ly3", cols3);
		String [][] records_ly3 = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ly3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ly3", records_ly3[i]);
		}
		DBApp.select("ly3");
		String selectTrace3 = DBApp.getLastTrace("ly3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+52));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kso2", cols0);
		String [][] records_kso2 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kso2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kso2", records_kso2[i]);
		}
		DBApp.select("kso2");
		String selectTrace0 = DBApp.getLastTrace("kso2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("h5", cols1);
		String [][] records_h5 = new String[27][cols1.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h5", records_h5[i]);
		}
		DBApp.select("h5");
		String selectTrace1 = DBApp.getLastTrace("h5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+27));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y6", cols0);
		String [][] records_y6 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y6", records_y6[i]);
		}
		DBApp.select("y6");
		String selectTrace0 = DBApp.getLastTrace("y6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hm", cols1);
		String [][] records_hm = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hm[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hm", records_hm[i]);
		}
		DBApp.select("hm");
		String selectTrace1 = DBApp.getLastTrace("hm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jlp2z", cols2);
		String [][] records_jlp2z = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jlp2z[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jlp2z", records_jlp2z[i]);
		}
		DBApp.select("jlp2z");
		String selectTrace2 = DBApp.getLastTrace("jlp2z");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("fxj", cols3);
		String [][] records_fxj = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fxj[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fxj", records_fxj[i]);
		}
		DBApp.select("fxj");
		String selectTrace3 = DBApp.getLastTrace("fxj");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+8));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("h4xco", cols0);
		String [][] records_h4xco = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h4xco[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h4xco", records_h4xco[i]);
		}
		DBApp.select("h4xco");
		String selectTrace0 = DBApp.getLastTrace("h4xco");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+69));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a4tcy", cols1);
		String [][] records_a4tcy = new String[76][cols1.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a4tcy[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a4tcy", records_a4tcy[i]);
		}
		DBApp.select("a4tcy");
		String selectTrace1 = DBApp.getLastTrace("a4tcy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(76.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+76));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("uq", cols2);
		String [][] records_uq = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uq[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uq", records_uq[i]);
		}
		DBApp.select("uq");
		String selectTrace2 = DBApp.getLastTrace("uq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+21));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("re", cols3);
		String [][] records_re = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_re[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("re", records_re[i]);
		}
		DBApp.select("re");
		String selectTrace3 = DBApp.getLastTrace("re");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v04", cols4);
		String [][] records_v04 = new String[44][cols4.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_v04[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v04", records_v04[i]);
		}
		DBApp.select("v04");
		String selectTrace4 = DBApp.getLastTrace("v04");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+44));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ulj", cols0);
		String [][] records_ulj = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ulj[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ulj", records_ulj[i]);
		}
		DBApp.select("ulj");
		String selectTrace0 = DBApp.getLastTrace("ulj");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+96));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("th7", cols1);
		String [][] records_th7 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_th7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("th7", records_th7[i]);
		}
		DBApp.select("th7");
		String selectTrace1 = DBApp.getLastTrace("th7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vth", cols2);
		String [][] records_vth = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vth[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vth", records_vth[i]);
		}
		DBApp.select("vth");
		String selectTrace2 = DBApp.getLastTrace("vth");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("kj3", cols0);
		String [][] records_kj3 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kj3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kj3", records_kj3[i]);
		}
		DBApp.select("kj3");
		String selectTrace0 = DBApp.getLastTrace("kj3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("pt5", cols0);
		String [][] records_pt5 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pt5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pt5", records_pt5[i]);
		}
		DBApp.select("pt5");
		String selectTrace0 = DBApp.getLastTrace("pt5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+8));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e12", cols1);
		String [][] records_e12 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e12[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e12", records_e12[i]);
		}
		DBApp.select("e12");
		String selectTrace1 = DBApp.getLastTrace("e12");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+17));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vtqv", cols2);
		String [][] records_vtqv = new String[62][cols2.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vtqv[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vtqv", records_vtqv[i]);
		}
		DBApp.select("vtqv");
		String selectTrace2 = DBApp.getLastTrace("vtqv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("wftn3", cols3);
		String [][] records_wftn3 = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wftn3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wftn3", records_wftn3[i]);
		}
		DBApp.select("wftn3");
		String selectTrace3 = DBApp.getLastTrace("wftn3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("per", cols4);
		String [][] records_per = new String[94][cols4.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_per[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("per", records_per[i]);
		}
		DBApp.select("per");
		String selectTrace4 = DBApp.getLastTrace("per");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+94));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("qp35", cols0);
		String [][] records_qp35 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qp35[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qp35", records_qp35[i]);
		}
		DBApp.select("qp35");
		String selectTrace0 = DBApp.getLastTrace("qp35");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f6", cols0);
		String [][] records_f6 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f6", records_f6[i]);
		}
		DBApp.select("f6");
		String selectTrace0 = DBApp.getLastTrace("f6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("kn3", cols0);
		String [][] records_kn3 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kn3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kn3", records_kn3[i]);
		}
		DBApp.select("kn3");
		String selectTrace0 = DBApp.getLastTrace("kn3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+89));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("u9", cols1);
		String [][] records_u9 = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u9", records_u9[i]);
		}
		DBApp.select("u9");
		String selectTrace1 = DBApp.getLastTrace("u9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("zh1", cols2);
		String [][] records_zh1 = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zh1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zh1", records_zh1[i]);
		}
		DBApp.select("zh1");
		String selectTrace2 = DBApp.getLastTrace("zh1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+97));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i9g", cols0);
		String [][] records_i9g = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i9g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9g", records_i9g[i]);
		}
		DBApp.select("i9g");
		String selectTrace0 = DBApp.getLastTrace("i9g");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+68));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("db12i", cols1);
		String [][] records_db12i = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_db12i[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("db12i", records_db12i[i]);
		}
		DBApp.select("db12i");
		String selectTrace1 = DBApp.getLastTrace("db12i");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("n089", cols2);
		String [][] records_n089 = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n089[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n089", records_n089[i]);
		}
		DBApp.select("n089");
		String selectTrace2 = DBApp.getLastTrace("n089");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+59));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ekr", cols0);
		String [][] records_ekr = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ekr[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ekr", records_ekr[i]);
		}
		DBApp.select("ekr");
		String selectTrace0 = DBApp.getLastTrace("ekr");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wn7", cols1);
		String [][] records_wn7 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wn7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wn7", records_wn7[i]);
		}
		DBApp.select("wn7");
		String selectTrace1 = DBApp.getLastTrace("wn7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x21", cols2);
		String [][] records_x21 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x21[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x21", records_x21[i]);
		}
		DBApp.select("x21");
		String selectTrace2 = DBApp.getLastTrace("x21");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+73));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z6", cols3);
		String [][] records_z6 = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z6", records_z6[i]);
		}
		DBApp.select("z6");
		String selectTrace3 = DBApp.getLastTrace("z6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bkn", cols0);
		String [][] records_bkn = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bkn[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bkn", records_bkn[i]);
		}
		DBApp.select("bkn");
		String selectTrace0 = DBApp.getLastTrace("bkn");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("u07", cols0);
		String [][] records_u07 = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u07[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u07", records_u07[i]);
		}
		DBApp.select("u07");
		String selectTrace0 = DBApp.getLastTrace("u07");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+55));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("oa", cols0);
		String [][] records_oa = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_oa[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oa", records_oa[i]);
		}
		DBApp.select("oa");
		String selectTrace0 = DBApp.getLastTrace("oa");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+84));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("c4an", cols1);
		String [][] records_c4an = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c4an[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c4an", records_c4an[i]);
		}
		DBApp.select("c4an");
		String selectTrace1 = DBApp.getLastTrace("c4an");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("je", cols0);
		String [][] records_je = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_je[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("je", records_je[i]);
		}
		DBApp.select("je");
		String selectTrace0 = DBApp.getLastTrace("je");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("yh", cols1);
		String [][] records_yh = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_yh[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yh", records_yh[i]);
		}
		DBApp.select("yh");
		String selectTrace1 = DBApp.getLastTrace("yh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("kj0e", cols2);
		String [][] records_kj0e = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kj0e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kj0e", records_kj0e[i]);
		}
		DBApp.select("kj0e");
		String selectTrace2 = DBApp.getLastTrace("kj0e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+73));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qh9x", cols0);
		String [][] records_qh9x = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qh9x[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qh9x", records_qh9x[i]);
		}
		DBApp.select("qh9x");
		String selectTrace0 = DBApp.getLastTrace("qh9x");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("e67c", cols1);
		String [][] records_e67c = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e67c[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e67c", records_e67c[i]);
		}
		DBApp.select("e67c");
		String selectTrace1 = DBApp.getLastTrace("e67c");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+6));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xxx", cols2);
		String [][] records_xxx = new String[10][cols2.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xxx[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xxx", records_xxx[i]);
		}
		DBApp.select("xxx");
		String selectTrace2 = DBApp.getLastTrace("xxx");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+10));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("t7", cols3);
		String [][] records_t7 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t7", records_t7[i]);
		}
		DBApp.select("t7");
		String selectTrace3 = DBApp.getLastTrace("t7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ikp", cols0);
		String [][] records_ikp = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ikp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ikp", records_ikp[i]);
		}
		DBApp.select("ikp");
		String selectTrace0 = DBApp.getLastTrace("ikp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("t84u", cols0);
		String [][] records_t84u = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t84u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t84u", records_t84u[i]);
		}
		DBApp.select("t84u");
		String selectTrace0 = DBApp.getLastTrace("t84u");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+54));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("l46", cols1);
		String [][] records_l46 = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l46[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l46", records_l46[i]);
		}
		DBApp.select("l46");
		String selectTrace1 = DBApp.getLastTrace("l46");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f99s", cols0);
		String [][] records_f99s = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f99s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f99s", records_f99s[i]);
		}
		DBApp.select("f99s");
		String selectTrace0 = DBApp.getLastTrace("f99s");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g4", cols0);
		String [][] records_g4 = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g4", records_g4[i]);
		}
		DBApp.select("g4");
		String selectTrace0 = DBApp.getLastTrace("g4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("sm3r", cols0);
		String [][] records_sm3r = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sm3r[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sm3r", records_sm3r[i]);
		}
		DBApp.select("sm3r");
		String selectTrace0 = DBApp.getLastTrace("sm3r");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("k3mm", cols1);
		String [][] records_k3mm = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k3mm[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k3mm", records_k3mm[i]);
		}
		DBApp.select("k3mm");
		String selectTrace1 = DBApp.getLastTrace("k3mm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+8));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("gtv8o", cols0);
		String [][] records_gtv8o = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gtv8o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gtv8o", records_gtv8o[i]);
		}
		DBApp.select("gtv8o");
		String selectTrace0 = DBApp.getLastTrace("gtv8o");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+73));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q1w", cols1);
		String [][] records_q1w = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q1w[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q1w", records_q1w[i]);
		}
		DBApp.select("q1w");
		String selectTrace1 = DBApp.getLastTrace("q1w");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("de", cols2);
		String [][] records_de = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_de[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("de", records_de[i]);
		}
		DBApp.select("de");
		String selectTrace2 = DBApp.getLastTrace("de");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("d00", cols3);
		String [][] records_d00 = new String[93][cols3.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d00[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d00", records_d00[i]);
		}
		DBApp.select("d00");
		String selectTrace3 = DBApp.getLastTrace("d00");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+93));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xbw", cols0);
		String [][] records_xbw = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xbw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xbw", records_xbw[i]);
		}
		DBApp.select("xbw");
		String selectTrace0 = DBApp.getLastTrace("xbw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("eu", cols0);
		String [][] records_eu = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_eu[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eu", records_eu[i]);
		}
		DBApp.select("eu");
		String selectTrace0 = DBApp.getLastTrace("eu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+54));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("vyz", cols1);
		String [][] records_vyz = new String[74][cols1.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vyz[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vyz", records_vyz[i]);
		}
		DBApp.select("vyz");
		String selectTrace1 = DBApp.getLastTrace("vyz");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(74.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+74));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y1c8", cols2);
		String [][] records_y1c8 = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y1c8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y1c8", records_y1c8[i]);
		}
		DBApp.select("y1c8");
		String selectTrace2 = DBApp.getLastTrace("y1c8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+2));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("icu", cols0);
		String [][] records_icu = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_icu[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("icu", records_icu[i]);
		}
		DBApp.select("icu");
		String selectTrace0 = DBApp.getLastTrace("icu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("n40", cols1);
		String [][] records_n40 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n40[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n40", records_n40[i]);
		}
		DBApp.select("n40");
		String selectTrace1 = DBApp.getLastTrace("n40");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+17));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r9q", cols2);
		String [][] records_r9q = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r9q[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r9q", records_r9q[i]);
		}
		DBApp.select("r9q");
		String selectTrace2 = DBApp.getLastTrace("r9q");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k60", cols3);
		String [][] records_k60 = new String[87][cols3.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k60[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k60", records_k60[i]);
		}
		DBApp.select("k60");
		String selectTrace3 = DBApp.getLastTrace("k60");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("lpe", cols4);
		String [][] records_lpe = new String[73][cols4.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_lpe[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lpe", records_lpe[i]);
		}
		DBApp.select("lpe");
		String selectTrace4 = DBApp.getLastTrace("lpe");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+73));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("yj2p", cols0);
		String [][] records_yj2p = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yj2p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yj2p", records_yj2p[i]);
		}
		DBApp.select("yj2p");
		String selectTrace0 = DBApp.getLastTrace("yj2p");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+2));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("x8fz4", cols1);
		String [][] records_x8fz4 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x8fz4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x8fz4", records_x8fz4[i]);
		}
		DBApp.select("x8fz4");
		String selectTrace1 = DBApp.getLastTrace("x8fz4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l1y7t", cols2);
		String [][] records_l1y7t = new String[52][cols2.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l1y7t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l1y7t", records_l1y7t[i]);
		}
		DBApp.select("l1y7t");
		String selectTrace2 = DBApp.getLastTrace("l1y7t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+52));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f05", cols0);
		String [][] records_f05 = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f05[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f05", records_f05[i]);
		}
		DBApp.select("f05");
		String selectTrace0 = DBApp.getLastTrace("f05");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(98.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+98));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z47", cols1);
		String [][] records_z47 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z47[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z47", records_z47[i]);
		}
		DBApp.select("z47");
		String selectTrace1 = DBApp.getLastTrace("z47");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+89));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("as", cols2);
		String [][] records_as = new String[82][cols2.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_as[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("as", records_as[i]);
		}
		DBApp.select("as");
		String selectTrace2 = DBApp.getLastTrace("as");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("soqy", cols0);
		String [][] records_soqy = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_soqy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("soqy", records_soqy[i]);
		}
		DBApp.select("soqy");
		String selectTrace0 = DBApp.getLastTrace("soqy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+60));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ek17", cols1);
		String [][] records_ek17 = new String[95][cols1.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ek17[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ek17", records_ek17[i]);
		}
		DBApp.select("ek17");
		String selectTrace1 = DBApp.getLastTrace("ek17");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(95.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+95));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("w9", cols2);
		String [][] records_w9 = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w9", records_w9[i]);
		}
		DBApp.select("w9");
		String selectTrace2 = DBApp.getLastTrace("w9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(46.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+46));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yo3", cols3);
		String [][] records_yo3 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_yo3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yo3", records_yo3[i]);
		}
		DBApp.select("yo3");
		String selectTrace3 = DBApp.getLastTrace("yo3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+1));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ux4", cols0);
		String [][] records_ux4 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ux4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ux4", records_ux4[i]);
		}
		DBApp.select("ux4");
		String selectTrace0 = DBApp.getLastTrace("ux4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x4t", cols1);
		String [][] records_x4t = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x4t[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x4t", records_x4t[i]);
		}
		DBApp.select("x4t");
		String selectTrace1 = DBApp.getLastTrace("x4t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u62p", cols2);
		String [][] records_u62p = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u62p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u62p", records_u62p[i]);
		}
		DBApp.select("u62p");
		String selectTrace2 = DBApp.getLastTrace("u62p");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mu", cols0);
		String [][] records_mu = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mu[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mu", records_mu[i]);
		}
		DBApp.select("mu");
		String selectTrace0 = DBApp.getLastTrace("mu");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("hq", cols1);
		String [][] records_hq = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hq", records_hq[i]);
		}
		DBApp.select("hq");
		String selectTrace1 = DBApp.getLastTrace("hq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+22));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u9", cols0);
		String [][] records_u9 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u9", records_u9[i]);
		}
		DBApp.select("u9");
		String selectTrace0 = DBApp.getLastTrace("u9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("v9", cols1);
		String [][] records_v9 = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v9", records_v9[i]);
		}
		DBApp.select("v9");
		String selectTrace1 = DBApp.getLastTrace("v9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("tg", cols2);
		String [][] records_tg = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tg", records_tg[i]);
		}
		DBApp.select("tg");
		String selectTrace2 = DBApp.getLastTrace("tg");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v9l", cols3);
		String [][] records_v9l = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_v9l[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v9l", records_v9l[i]);
		}
		DBApp.select("v9l");
		String selectTrace3 = DBApp.getLastTrace("v9l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("hui", cols0);
		String [][] records_hui = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hui[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hui", records_hui[i]);
		}
		DBApp.select("hui");
		String selectTrace0 = DBApp.getLastTrace("hui");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b20", cols1);
		String [][] records_b20 = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b20[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b20", records_b20[i]);
		}
		DBApp.select("b20");
		String selectTrace1 = DBApp.getLastTrace("b20");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+64));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("wh", cols0);
		String [][] records_wh = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wh", records_wh[i]);
		}
		DBApp.select("wh");
		String selectTrace0 = DBApp.getLastTrace("wh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("sg0", cols0);
		String [][] records_sg0 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sg0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sg0", records_sg0[i]);
		}
		DBApp.select("sg0");
		String selectTrace0 = DBApp.getLastTrace("sg0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("ryp49", cols1);
		String [][] records_ryp49 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ryp49[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ryp49", records_ryp49[i]);
		}
		DBApp.select("ryp49");
		String selectTrace1 = DBApp.getLastTrace("ryp49");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("w97", cols2);
		String [][] records_w97 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w97[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w97", records_w97[i]);
		}
		DBApp.select("w97");
		String selectTrace2 = DBApp.getLastTrace("w97");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("b0s96", cols3);
		String [][] records_b0s96 = new String[53][cols3.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b0s96[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b0s96", records_b0s96[i]);
		}
		DBApp.select("b0s96");
		String selectTrace3 = DBApp.getLastTrace("b0s96");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+53));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("xq", cols0);
		String [][] records_xq = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xq", records_xq[i]);
		}
		DBApp.select("xq");
		String selectTrace0 = DBApp.getLastTrace("xq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+43));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("v11u", cols1);
		String [][] records_v11u = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v11u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v11u", records_v11u[i]);
		}
		DBApp.select("v11u");
		String selectTrace1 = DBApp.getLastTrace("v11u");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+11));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("bfa", cols2);
		String [][] records_bfa = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bfa[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bfa", records_bfa[i]);
		}
		DBApp.select("bfa");
		String selectTrace2 = DBApp.getLastTrace("bfa");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bn3dt", cols3);
		String [][] records_bn3dt = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bn3dt[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn3dt", records_bn3dt[i]);
		}
		DBApp.select("bn3dt");
		String selectTrace3 = DBApp.getLastTrace("bn3dt");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r0rko", cols4);
		String [][] records_r0rko = new String[75][cols4.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r0rko[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r0rko", records_r0rko[i]);
		}
		DBApp.select("r0rko");
		String selectTrace4 = DBApp.getLastTrace("r0rko");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+75));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("r2ug6", cols0);
		String [][] records_r2ug6 = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r2ug6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r2ug6", records_r2ug6[i]);
		}
		DBApp.select("r2ug6");
		String selectTrace0 = DBApp.getLastTrace("r2ug6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("ktik9", cols1);
		String [][] records_ktik9 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ktik9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ktik9", records_ktik9[i]);
		}
		DBApp.select("ktik9");
		String selectTrace1 = DBApp.getLastTrace("ktik9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+71));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rhkb", cols2);
		String [][] records_rhkb = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_rhkb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rhkb", records_rhkb[i]);
		}
		DBApp.select("rhkb");
		String selectTrace2 = DBApp.getLastTrace("rhkb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+17));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qfr05", cols3);
		String [][] records_qfr05 = new String[36][cols3.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_qfr05[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qfr05", records_qfr05[i]);
		}
		DBApp.select("qfr05");
		String selectTrace3 = DBApp.getLastTrace("qfr05");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ze7", cols4);
		String [][] records_ze7 = new String[56][cols4.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ze7[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ze7", records_ze7[i]);
		}
		DBApp.select("ze7");
		String selectTrace4 = DBApp.getLastTrace("ze7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+56));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e1i", cols0);
		String [][] records_e1i = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e1i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e1i", records_e1i[i]);
		}
		DBApp.select("e1i");
		String selectTrace0 = DBApp.getLastTrace("e1i");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hx3", cols1);
		String [][] records_hx3 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hx3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hx3", records_hx3[i]);
		}
		DBApp.select("hx3");
		String selectTrace1 = DBApp.getLastTrace("hx3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sk", cols2);
		String [][] records_sk = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_sk[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sk", records_sk[i]);
		}
		DBApp.select("sk");
		String selectTrace2 = DBApp.getLastTrace("sk");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+94));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("li", cols3);
		String [][] records_li = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_li[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("li", records_li[i]);
		}
		DBApp.select("li");
		String selectTrace3 = DBApp.getLastTrace("li");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ygn", cols4);
		String [][] records_ygn = new String[24][cols4.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ygn[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ygn", records_ygn[i]);
		}
		DBApp.select("ygn");
		String selectTrace4 = DBApp.getLastTrace("ygn");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("b9g", cols0);
		String [][] records_b9g = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b9g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9g", records_b9g[i]);
		}
		DBApp.select("b9g");
		String selectTrace0 = DBApp.getLastTrace("b9g");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+58));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ma", cols1);
		String [][] records_ma = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ma[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ma", records_ma[i]);
		}
		DBApp.select("ma");
		String selectTrace1 = DBApp.getLastTrace("ma");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+39));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("n34", cols2);
		String [][] records_n34 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n34[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n34", records_n34[i]);
		}
		DBApp.select("n34");
		String selectTrace2 = DBApp.getLastTrace("n34");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r3q", cols3);
		String [][] records_r3q = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r3q[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r3q", records_r3q[i]);
		}
		DBApp.select("r3q");
		String selectTrace3 = DBApp.getLastTrace("r3q");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dw", cols0);
		String [][] records_dw = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dw", records_dw[i]);
		}
		DBApp.select("dw");
		String selectTrace0 = DBApp.getLastTrace("dw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("tdbp", cols0);
		String [][] records_tdbp = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tdbp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tdbp", records_tdbp[i]);
		}
		DBApp.select("tdbp");
		String selectTrace0 = DBApp.getLastTrace("tdbp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+14));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("n2v", cols1);
		String [][] records_n2v = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n2v[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n2v", records_n2v[i]);
		}
		DBApp.select("n2v");
		String selectTrace1 = DBApp.getLastTrace("n2v");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+100));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("nkp3", cols2);
		String [][] records_nkp3 = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_nkp3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nkp3", records_nkp3[i]);
		}
		DBApp.select("nkp3");
		String selectTrace2 = DBApp.getLastTrace("nkp3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(19.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+19));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("mz24n", cols3);
		String [][] records_mz24n = new String[25][cols3.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_mz24n[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mz24n", records_mz24n[i]);
		}
		DBApp.select("mz24n");
		String selectTrace3 = DBApp.getLastTrace("mz24n");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+25));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k3", cols0);
		String [][] records_k3 = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k3", records_k3[i]);
		}
		DBApp.select("k3");
		String selectTrace0 = DBApp.getLastTrace("k3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+41));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("dacw", cols1);
		String [][] records_dacw = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dacw[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dacw", records_dacw[i]);
		}
		DBApp.select("dacw");
		String selectTrace1 = DBApp.getLastTrace("dacw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("iiv", cols2);
		String [][] records_iiv = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_iiv[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iiv", records_iiv[i]);
		}
		DBApp.select("iiv");
		String selectTrace2 = DBApp.getLastTrace("iiv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+47));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("edrf", cols3);
		String [][] records_edrf = new String[12][cols3.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_edrf[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("edrf", records_edrf[i]);
		}
		DBApp.select("edrf");
		String selectTrace3 = DBApp.getLastTrace("edrf");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s8", cols0);
		String [][] records_s8 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s8", records_s8[i]);
		}
		DBApp.select("s8");
		String selectTrace0 = DBApp.getLastTrace("s8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hb2ou", cols1);
		String [][] records_hb2ou = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hb2ou[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hb2ou", records_hb2ou[i]);
		}
		DBApp.select("hb2ou");
		String selectTrace1 = DBApp.getLastTrace("hb2ou");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("kp", cols2);
		String [][] records_kp = new String[62][cols2.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kp[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kp", records_kp[i]);
		}
		DBApp.select("kp");
		String selectTrace2 = DBApp.getLastTrace("kp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r8f", cols3);
		String [][] records_r8f = new String[78][cols3.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_r8f[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8f", records_r8f[i]);
		}
		DBApp.select("r8f");
		String selectTrace3 = DBApp.getLastTrace("r8f");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(78.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+78));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("ix", cols4);
		String [][] records_ix = new String[49][cols4.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ix[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ix", records_ix[i]);
		}
		DBApp.select("ix");
		String selectTrace4 = DBApp.getLastTrace("ix");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r1y", cols0);
		String [][] records_r1y = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r1y[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r1y", records_r1y[i]);
		}
		DBApp.select("r1y");
		String selectTrace0 = DBApp.getLastTrace("r1y");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+58));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("rq", cols1);
		String [][] records_rq = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rq", records_rq[i]);
		}
		DBApp.select("rq");
		String selectTrace1 = DBApp.getLastTrace("rq");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+100));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w46", cols2);
		String [][] records_w46 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w46[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w46", records_w46[i]);
		}
		DBApp.select("w46");
		String selectTrace2 = DBApp.getLastTrace("w46");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ry1", cols0);
		String [][] records_ry1 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ry1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ry1", records_ry1[i]);
		}
		DBApp.select("ry1");
		String selectTrace0 = DBApp.getLastTrace("ry1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(66.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+66));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("c24", cols0);
		String [][] records_c24 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c24[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c24", records_c24[i]);
		}
		DBApp.select("c24");
		String selectTrace0 = DBApp.getLastTrace("c24");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+93));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f63", cols1);
		String [][] records_f63 = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f63[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f63", records_f63[i]);
		}
		DBApp.select("f63");
		String selectTrace1 = DBApp.getLastTrace("f63");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q224", cols0);
		String [][] records_q224 = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q224[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q224", records_q224[i]);
		}
		DBApp.select("q224");
		String selectTrace0 = DBApp.getLastTrace("q224");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+61));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ri", cols1);
		String [][] records_ri = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ri[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ri", records_ri[i]);
		}
		DBApp.select("ri");
		String selectTrace1 = DBApp.getLastTrace("ri");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+9));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("s0e", cols2);
		String [][] records_s0e = new String[86][cols2.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s0e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s0e", records_s0e[i]);
		}
		DBApp.select("s0e");
		String selectTrace2 = DBApp.getLastTrace("s0e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+86));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("e55", cols0);
		String [][] records_e55 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e55[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e55", records_e55[i]);
		}
		DBApp.select("e55");
		String selectTrace0 = DBApp.getLastTrace("e55");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("udp", cols1);
		String [][] records_udp = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_udp[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("udp", records_udp[i]);
		}
		DBApp.select("udp");
		String selectTrace1 = DBApp.getLastTrace("udp");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("l5uv8", cols2);
		String [][] records_l5uv8 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l5uv8[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l5uv8", records_l5uv8[i]);
		}
		DBApp.select("l5uv8");
		String selectTrace2 = DBApp.getLastTrace("l5uv8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("f01f8", cols0);
		String [][] records_f01f8 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f01f8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f01f8", records_f01f8[i]);
		}
		DBApp.select("f01f8");
		String selectTrace0 = DBApp.getLastTrace("f01f8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("m8", cols0);
		String [][] records_m8 = new String[63][cols0.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m8", records_m8[i]);
		}
		DBApp.select("m8");
		String selectTrace0 = DBApp.getLastTrace("m8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+63));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ts2", cols0);
		String [][] records_ts2 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ts2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ts2", records_ts2[i]);
		}
		DBApp.select("ts2");
		String selectTrace0 = DBApp.getLastTrace("ts2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l9d", cols1);
		String [][] records_l9d = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l9d[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l9d", records_l9d[i]);
		}
		DBApp.select("l9d");
		String selectTrace1 = DBApp.getLastTrace("l9d");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+4));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("pvw47", cols2);
		String [][] records_pvw47 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pvw47[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pvw47", records_pvw47[i]);
		}
		DBApp.select("pvw47");
		String selectTrace2 = DBApp.getLastTrace("pvw47");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+11));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n9c", cols3);
		String [][] records_n9c = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n9c[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9c", records_n9c[i]);
		}
		DBApp.select("n9c");
		String selectTrace3 = DBApp.getLastTrace("n9c");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+97));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o65", cols0);
		String [][] records_o65 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o65[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o65", records_o65[i]);
		}
		DBApp.select("o65");
		String selectTrace0 = DBApp.getLastTrace("o65");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+30));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("nw60", cols1);
		String [][] records_nw60 = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_nw60[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nw60", records_nw60[i]);
		}
		DBApp.select("nw60");
		String selectTrace1 = DBApp.getLastTrace("nw60");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+50));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("exb", cols2);
		String [][] records_exb = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_exb[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("exb", records_exb[i]);
		}
		DBApp.select("exb");
		String selectTrace2 = DBApp.getLastTrace("exb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(19.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+19));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("k9c6", cols3);
		String [][] records_k9c6 = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k9c6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k9c6", records_k9c6[i]);
		}
		DBApp.select("k9c6");
		String selectTrace3 = DBApp.getLastTrace("k9c6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n7p", cols0);
		String [][] records_n7p = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n7p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n7p", records_n7p[i]);
		}
		DBApp.select("n7p");
		String selectTrace0 = DBApp.getLastTrace("n7p");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+58));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("r68k7", cols1);
		String [][] records_r68k7 = new String[74][cols1.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r68k7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r68k7", records_r68k7[i]);
		}
		DBApp.select("r68k7");
		String selectTrace1 = DBApp.getLastTrace("r68k7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(74.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+74));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("xql0", cols2);
		String [][] records_xql0 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xql0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xql0", records_xql0[i]);
		}
		DBApp.select("xql0");
		String selectTrace2 = DBApp.getLastTrace("xql0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+11));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("a9xk7", cols3);
		String [][] records_a9xk7 = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a9xk7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a9xk7", records_a9xk7[i]);
		}
		DBApp.select("a9xk7");
		String selectTrace3 = DBApp.getLastTrace("a9xk7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+16));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("hn4", cols0);
		String [][] records_hn4 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hn4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hn4", records_hn4[i]);
		}
		DBApp.select("hn4");
		String selectTrace0 = DBApp.getLastTrace("hn4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("h63h", cols1);
		String [][] records_h63h = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h63h[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h63h", records_h63h[i]);
		}
		DBApp.select("h63h");
		String selectTrace1 = DBApp.getLastTrace("h63h");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(82.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+82));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("wp6", cols0);
		String [][] records_wp6 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wp6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wp6", records_wp6[i]);
		}
		DBApp.select("wp6");
		String selectTrace0 = DBApp.getLastTrace("wp6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+11));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("jv", cols1);
		String [][] records_jv = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jv", records_jv[i]);
		}
		DBApp.select("jv");
		String selectTrace1 = DBApp.getLastTrace("jv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(29.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+29));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r4la6", cols2);
		String [][] records_r4la6 = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r4la6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4la6", records_r4la6[i]);
		}
		DBApp.select("r4la6");
		String selectTrace2 = DBApp.getLastTrace("r4la6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("zw", cols3);
		String [][] records_zw = new String[90][cols3.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_zw[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zw", records_zw[i]);
		}
		DBApp.select("zw");
		String selectTrace3 = DBApp.getLastTrace("zw");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+90));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u2", cols4);
		String [][] records_u2 = new String[57][cols4.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_u2[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u2", records_u2[i]);
		}
		DBApp.select("u2");
		String selectTrace4 = DBApp.getLastTrace("u2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q1", cols0);
		String [][] records_q1 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q1", records_q1[i]);
		}
		DBApp.select("q1");
		String selectTrace0 = DBApp.getLastTrace("q1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+6));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("k56t", cols0);
		String [][] records_k56t = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k56t[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k56t", records_k56t[i]);
		}
		DBApp.select("k56t");
		String selectTrace0 = DBApp.getLastTrace("k56t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("l46l", cols1);
		String [][] records_l46l = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l46l[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l46l", records_l46l[i]);
		}
		DBApp.select("l46l");
		String selectTrace1 = DBApp.getLastTrace("l46l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+62));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g89h", cols2);
		String [][] records_g89h = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g89h[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g89h", records_g89h[i]);
		}
		DBApp.select("g89h");
		String selectTrace2 = DBApp.getLastTrace("g89h");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+18));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("t6", cols3);
		String [][] records_t6 = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6", records_t6[i]);
		}
		DBApp.select("t6");
		String selectTrace3 = DBApp.getLastTrace("t6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+15));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("h23l", cols0);
		String [][] records_h23l = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h23l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h23l", records_h23l[i]);
		}
		DBApp.select("h23l");
		String selectTrace0 = DBApp.getLastTrace("h23l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+4));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ln26a", cols0);
		String [][] records_ln26a = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ln26a[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ln26a", records_ln26a[i]);
		}
		DBApp.select("ln26a");
		String selectTrace0 = DBApp.getLastTrace("ln26a");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+61));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s31", cols1);
		String [][] records_s31 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s31[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s31", records_s31[i]);
		}
		DBApp.select("s31");
		String selectTrace1 = DBApp.getLastTrace("s31");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+45));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mdn", cols0);
		String [][] records_mdn = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mdn[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mdn", records_mdn[i]);
		}
		DBApp.select("mdn");
		String selectTrace0 = DBApp.getLastTrace("mdn");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("otv", cols1);
		String [][] records_otv = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_otv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("otv", records_otv[i]);
		}
		DBApp.select("otv");
		String selectTrace1 = DBApp.getLastTrace("otv");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+22));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w6t7", cols0);
		String [][] records_w6t7 = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w6t7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w6t7", records_w6t7[i]);
		}
		DBApp.select("w6t7");
		String selectTrace0 = DBApp.getLastTrace("w6t7");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+13));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("rhy", cols0);
		String [][] records_rhy = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rhy[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rhy", records_rhy[i]);
		}
		DBApp.select("rhy");
		String selectTrace0 = DBApp.getLastTrace("rhy");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k1188", cols1);
		String [][] records_k1188 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k1188[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k1188", records_k1188[i]);
		}
		DBApp.select("k1188");
		String selectTrace1 = DBApp.getLastTrace("k1188");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+92));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xxmo", cols0);
		String [][] records_xxmo = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xxmo[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xxmo", records_xxmo[i]);
		}
		DBApp.select("xxmo");
		String selectTrace0 = DBApp.getLastTrace("xxmo");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+35));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("w18", cols1);
		String [][] records_w18 = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w18[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w18", records_w18[i]);
		}
		DBApp.select("w18");
		String selectTrace1 = DBApp.getLastTrace("w18");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+4));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("o8po1", cols0);
		String [][] records_o8po1 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o8po1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o8po1", records_o8po1[i]);
		}
		DBApp.select("o8po1");
		String selectTrace0 = DBApp.getLastTrace("o8po1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("z94", cols1);
		String [][] records_z94 = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z94[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z94", records_z94[i]);
		}
		DBApp.select("z94");
		String selectTrace1 = DBApp.getLastTrace("z94");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+57));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("cj2", cols0);
		String [][] records_cj2 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cj2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cj2", records_cj2[i]);
		}
		DBApp.select("cj2");
		String selectTrace0 = DBApp.getLastTrace("cj2");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+24));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qvs70", cols1);
		String [][] records_qvs70 = new String[97][cols1.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qvs70[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qvs70", records_qvs70[i]);
		}
		DBApp.select("qvs70");
		String selectTrace1 = DBApp.getLastTrace("qvs70");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+97));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("fj1", cols2);
		String [][] records_fj1 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_fj1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fj1", records_fj1[i]);
		}
		DBApp.select("fj1");
		String selectTrace2 = DBApp.getLastTrace("fj1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+51));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i65q", cols3);
		String [][] records_i65q = new String[21][cols3.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i65q[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i65q", records_i65q[i]);
		}
		DBApp.select("i65q");
		String selectTrace3 = DBApp.getLastTrace("i65q");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+21));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f4", cols4);
		String [][] records_f4 = new String[37][cols4.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_f4[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f4", records_f4[i]);
		}
		DBApp.select("f4");
		String selectTrace4 = DBApp.getLastTrace("f4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("db6", cols0);
		String [][] records_db6 = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_db6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("db6", records_db6[i]);
		}
		DBApp.select("db6");
		String selectTrace0 = DBApp.getLastTrace("db6");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(95.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+95));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("oj1r1", cols1);
		String [][] records_oj1r1 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oj1r1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oj1r1", records_oj1r1[i]);
		}
		DBApp.select("oj1r1");
		String selectTrace1 = DBApp.getLastTrace("oj1r1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+6));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("kre35", cols2);
		String [][] records_kre35 = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kre35[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kre35", records_kre35[i]);
		}
		DBApp.select("kre35");
		String selectTrace2 = DBApp.getLastTrace("kre35");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+2));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("fzk8", cols3);
		String [][] records_fzk8 = new String[91][cols3.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fzk8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fzk8", records_fzk8[i]);
		}
		DBApp.select("fzk8");
		String selectTrace3 = DBApp.getLastTrace("fzk8");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+91));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b9e", cols4);
		String [][] records_b9e = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b9e[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9e", records_b9e[i]);
		}
		DBApp.select("b9e");
		String selectTrace4 = DBApp.getLastTrace("b9e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ih7l", cols0);
		String [][] records_ih7l = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ih7l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ih7l", records_ih7l[i]);
		}
		DBApp.select("ih7l");
		String selectTrace0 = DBApp.getLastTrace("ih7l");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+79));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("hnzb9", cols1);
		String [][] records_hnzb9 = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hnzb9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hnzb9", records_hnzb9[i]);
		}
		DBApp.select("hnzb9");
		String selectTrace1 = DBApp.getLastTrace("hnzb9");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+59));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("e871p", cols2);
		String [][] records_e871p = new String[43][cols2.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e871p[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e871p", records_e871p[i]);
		}
		DBApp.select("e871p");
		String selectTrace2 = DBApp.getLastTrace("e871p");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+43));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("vr0", cols3);
		String [][] records_vr0 = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vr0[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vr0", records_vr0[i]);
		}
		DBApp.select("vr0");
		String selectTrace3 = DBApp.getLastTrace("vr0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+8));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("sbo20", cols0);
		String [][] records_sbo20 = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sbo20[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sbo20", records_sbo20[i]);
		}
		DBApp.select("sbo20");
		String selectTrace0 = DBApp.getLastTrace("sbo20");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+3));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("m8kh3", cols1);
		String [][] records_m8kh3 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m8kh3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m8kh3", records_m8kh3[i]);
		}
		DBApp.select("m8kh3");
		String selectTrace1 = DBApp.getLastTrace("m8kh3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+36));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("c4t", cols2);
		String [][] records_c4t = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c4t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c4t", records_c4t[i]);
		}
		DBApp.select("c4t");
		String selectTrace2 = DBApp.getLastTrace("c4t");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+67));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("m3j3", cols3);
		String [][] records_m3j3 = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m3j3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m3j3", records_m3j3[i]);
		}
		DBApp.select("m3j3");
		String selectTrace3 = DBApp.getLastTrace("m3j3");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+34));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("b1", cols4);
		String [][] records_b1 = new String[38][cols4.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_b1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b1", records_b1[i]);
		}
		DBApp.select("b1");
		String selectTrace4 = DBApp.getLastTrace("b1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+38));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qdb", cols0);
		String [][] records_qdb = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qdb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qdb", records_qdb[i]);
		}
		DBApp.select("qdb");
		String selectTrace0 = DBApp.getLastTrace("qdb");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+31));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("emgd", cols1);
		String [][] records_emgd = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_emgd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("emgd", records_emgd[i]);
		}
		DBApp.select("emgd");
		String selectTrace1 = DBApp.getLastTrace("emgd");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+99));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("opc1", cols2);
		String [][] records_opc1 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_opc1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("opc1", records_opc1[i]);
		}
		DBApp.select("opc1");
		String selectTrace2 = DBApp.getLastTrace("opc1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("a4", cols3);
		String [][] records_a4 = new String[72][cols3.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a4", records_a4[i]);
		}
		DBApp.select("a4");
		String selectTrace3 = DBApp.getLastTrace("a4");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("r4pu1", cols0);
		String [][] records_r4pu1 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r4pu1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4pu1", records_r4pu1[i]);
		}
		DBApp.select("r4pu1");
		String selectTrace0 = DBApp.getLastTrace("r4pu1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+89));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("x069", cols1);
		String [][] records_x069 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x069[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x069", records_x069[i]);
		}
		DBApp.select("x069");
		String selectTrace1 = DBApp.getLastTrace("x069");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+88));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("uqm", cols2);
		String [][] records_uqm = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uqm[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uqm", records_uqm[i]);
		}
		DBApp.select("uqm");
		String selectTrace2 = DBApp.getLastTrace("uqm");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+83));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xww", cols0);
		String [][] records_xww = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xww[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xww", records_xww[i]);
		}
		DBApp.select("xww");
		String selectTrace0 = DBApp.getLastTrace("xww");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+49));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("df0", cols1);
		String [][] records_df0 = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_df0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("df0", records_df0[i]);
		}
		DBApp.select("df0");
		String selectTrace1 = DBApp.getLastTrace("df0");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d52", cols2);
		String [][] records_d52 = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d52[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d52", records_d52[i]);
		}
		DBApp.select("d52");
		String selectTrace2 = DBApp.getLastTrace("d52");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+61));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("kar", cols0);
		String [][] records_kar = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kar[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kar", records_kar[i]);
		}
		DBApp.select("kar");
		String selectTrace0 = DBApp.getLastTrace("kar");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+89));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r546", cols0);
		String [][] records_r546 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r546[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r546", records_r546[i]);
		}
		DBApp.select("r546");
		String selectTrace0 = DBApp.getLastTrace("r546");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(65.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+65));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("le33", cols1);
		String [][] records_le33 = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_le33[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("le33", records_le33[i]);
		}
		DBApp.select("le33");
		String selectTrace1 = DBApp.getLastTrace("le33");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+26));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("xw6v5", cols2);
		String [][] records_xw6v5 = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xw6v5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xw6v5", records_xw6v5[i]);
		}
		DBApp.select("xw6v5");
		String selectTrace2 = DBApp.getLastTrace("xw6v5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+72));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xr", cols3);
		String [][] records_xr = new String[95][cols3.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xr[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xr", records_xr[i]);
		}
		DBApp.select("xr");
		String selectTrace3 = DBApp.getLastTrace("xr");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(95.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+95));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ja", cols0);
		String [][] records_ja = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ja[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ja", records_ja[i]);
		}
		DBApp.select("ja");
		String selectTrace0 = DBApp.getLastTrace("ja");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+5));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("n85", cols1);
		String [][] records_n85 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n85[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n85", records_n85[i]);
		}
		DBApp.select("n85");
		String selectTrace1 = DBApp.getLastTrace("n85");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+23));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionSelectionTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j40lh", cols0);
		String [][] records_j40lh = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j40lh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j40lh", records_j40lh[i]);
		}
		DBApp.select("j40lh");
		String selectTrace0 = DBApp.getLastTrace("j40lh");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+37));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("kbf", cols1);
		String [][] records_kbf = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kbf[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kbf", records_kbf[i]);
		}
		DBApp.select("kbf");
		String selectTrace1 = DBApp.getLastTrace("kbf");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+12));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("a4n60", cols2);
		String [][] records_a4n60 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_a4n60[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a4n60", records_a4n60[i]);
		}
		DBApp.select("a4n60");
		String selectTrace2 = DBApp.getLastTrace("a4n60");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+32));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tx1", cols3);
		String [][] records_tx1 = new String[77][cols3.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_tx1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tx1", records_tx1[i]);
		}
		DBApp.select("tx1");
		String selectTrace3 = DBApp.getLastTrace("tx1");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+77));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("j3e", cols4);
		String [][] records_j3e = new String[85][cols4.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_j3e[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j3e", records_j3e[i]);
		}
		DBApp.select("j3e");
		String selectTrace4 = DBApp.getLastTrace("j3e");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace4.startsWith("Select all"));
		int pageCount4 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace4.contains("pages:"+pageCount4));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace4.contains("records:"+85));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace4.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k5", cols0);
		String [][] records_k5 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k5", records_k5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(251));
		for(int i=0;i<7;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("k5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_k5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xiq", cols1);
		String [][] records_xiq = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xiq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xiq", records_xiq[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(753));
		for(int i=0;i<51;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("xiq",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_xiq[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s27", cols2);
		String [][] records_s27 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s27[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s27", records_s27[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(299));
		for(int i=0;i<77;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("s27",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_s27[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o3", cols0);
		String [][] records_o3 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o3", records_o3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(482));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("dl7", cols1);
		String [][] records_dl7 = new String[74][cols1.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dl7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dl7", records_dl7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(713));
		for(int i=0;i<74;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("dl7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_dl7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("n1", cols2);
		String [][] records_n1 = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n1", records_n1[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(484));
		for(int i=0;i<100;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("n1",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_n1[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("y2we", cols0);
		String [][] records_y2we = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y2we[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y2we", records_y2we[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(332));
		for(int i=0;i<13;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("y2we",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_y2we[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("em", cols0);
		String [][] records_em = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_em[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("em", records_em[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(603));
		for(int i=0;i<71;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("em",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_em[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s60fy", cols1);
		String [][] records_s60fy = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s60fy[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s60fy", records_s60fy[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<4; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(623));
		for(int i=0;i<4;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("s60fy",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_s60fy[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("qh", cols2);
		String [][] records_qh = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qh[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qh", records_qh[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(791));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("qh",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_qh[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("lcvu", cols3);
		String [][] records_lcvu = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_lcvu[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lcvu", records_lcvu[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(408));
		for(int i=0;i<79;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("lcvu",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_lcvu[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xloo", cols4);
		String [][] records_xloo = new String[79][cols4.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xloo[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xloo", records_xloo[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(698));
		for(int i=0;i<79;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("xloo",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_xloo[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("nzb", cols0);
		String [][] records_nzb = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nzb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nzb", records_nzb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<99; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(708));
		for(int i=0;i<99;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("nzb",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_nzb[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("m5814", cols1);
		String [][] records_m5814 = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m5814[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m5814", records_m5814[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(34));
		for(int i=0;i<32;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("m5814",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_m5814[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("u62", cols0);
		String [][] records_u62 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u62[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u62", records_u62[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(370));
		for(int i=0;i<96;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("u62",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_u62[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ciq", cols1);
		String [][] records_ciq = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ciq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ciq", records_ciq[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<29; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(669));
		for(int i=0;i<29;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ciq",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ciq[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("dc4", cols2);
		String [][] records_dc4 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dc4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dc4", records_dc4[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(758));
		for(int i=0;i<40;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("dc4",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_dc4[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ipf3v", cols0);
		String [][] records_ipf3v = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ipf3v[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ipf3v", records_ipf3v[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(261));
		for(int i=0;i<75;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ipf3v",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ipf3v[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q9u", cols0);
		String [][] records_q9u = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q9u[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q9u", records_q9u[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<36; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(818));
		for(int i=0;i<36;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("q9u",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_q9u[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gkmw6", cols0);
		String [][] records_gkmw6 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gkmw6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gkmw6", records_gkmw6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(76));
		for(int i=0;i<22;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("gkmw6",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_gkmw6[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("qw1", cols0);
		String [][] records_qw1 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qw1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qw1", records_qw1[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(724));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("qw1",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_qw1[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("pfm", cols1);
		String [][] records_pfm = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pfm[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pfm", records_pfm[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(759));
		for(int i=0;i<24;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("pfm",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_pfm[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y27", cols0);
		String [][] records_y27 = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y27[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y27", records_y27[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<59; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(337));
		for(int i=0;i<59;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("y27",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_y27[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("rn", cols1);
		String [][] records_rn = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rn[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rn", records_rn[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(664));
		for(int i=0;i<90;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("rn",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_rn[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("eq2", cols2);
		String [][] records_eq2 = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_eq2[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eq2", records_eq2[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(947));
		for(int i=0;i<17;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("eq2",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_eq2[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("eh19z", cols0);
		String [][] records_eh19z = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_eh19z[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eh19z", records_eh19z[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(203));
		for(int i=0;i<35;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("eh19z",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_eh19z[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y8", cols1);
		String [][] records_y8 = new String[65][cols1.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y8", records_y8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(255));
		for(int i=0;i<65;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("y8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_y8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("c1o", cols2);
		String [][] records_c1o = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c1o[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c1o", records_c1o[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(881));
		for(int i=0;i<60;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("c1o",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_c1o[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e87", cols3);
		String [][] records_e87 = new String[40][cols3.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e87[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e87", records_e87[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(842));
		for(int i=0;i<40;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("e87",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_e87[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("t6", cols0);
		String [][] records_t6 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6", records_t6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(679));
		for(int i=0;i<27;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t6",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t6[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("gv7", cols1);
		String [][] records_gv7 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gv7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gv7", records_gv7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(408));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("gv7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_gv7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("vsw", cols2);
		String [][] records_vsw = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vsw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vsw", records_vsw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(995));
		for(int i=0;i<17;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("vsw",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_vsw[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("ro", cols0);
		String [][] records_ro = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ro[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ro", records_ro[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(269));
		for(int i=0;i<90;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ro",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ro[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("oz", cols1);
		String [][] records_oz = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oz[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oz", records_oz[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(611));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("oz",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_oz[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("mv6", cols2);
		String [][] records_mv6 = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mv6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mv6", records_mv6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(187));
		for(int i=0;i<25;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("mv6",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_mv6[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q9v", cols3);
		String [][] records_q9v = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_q9v[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q9v", records_q9v[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(218));
		for(int i=0;i<10;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("q9v",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_q9v[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zdd", cols0);
		String [][] records_zdd = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zdd[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zdd", records_zdd[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<65; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(316));
		for(int i=0;i<65;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("zdd",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_zdd[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("er9a", cols1);
		String [][] records_er9a = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_er9a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("er9a", records_er9a[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(439));
		for(int i=0;i<45;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("er9a",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_er9a[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k5b49", cols2);
		String [][] records_k5b49 = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k5b49[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k5b49", records_k5b49[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(484));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("k5b49",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_k5b49[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xd8", cols3);
		String [][] records_xd8 = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xd8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xd8", records_xd8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(874));
		for(int i=0;i<79;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("xd8",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_xd8[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("ku4o", cols0);
		String [][] records_ku4o = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ku4o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ku4o", records_ku4o[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<10; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(16));
		for(int i=0;i<10;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ku4o",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ku4o[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("vzd", cols1);
		String [][] records_vzd = new String[27][cols1.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vzd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vzd", records_vzd[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(738));
		for(int i=0;i<27;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("vzd",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_vzd[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("n3mrg", cols0);
		String [][] records_n3mrg = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n3mrg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n3mrg", records_n3mrg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<54; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(333));
		for(int i=0;i<54;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("n3mrg",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_n3mrg[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("i1v8", cols1);
		String [][] records_i1v8 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i1v8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i1v8", records_i1v8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(145));
		for(int i=0;i<79;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("i1v8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_i1v8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("r0u", cols2);
		String [][] records_r0u = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r0u[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r0u", records_r0u[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(198));
		for(int i=0;i<73;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("r0u",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_r0u[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("bw8s", cols3);
		String [][] records_bw8s = new String[41][cols3.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bw8s[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bw8s", records_bw8s[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<41; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(968));
		for(int i=0;i<41;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("bw8s",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_bw8s[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u2a", cols0);
		String [][] records_u2a = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u2a[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u2a", records_u2a[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(105));
		for(int i=0;i<11;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("u2a",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_u2a[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("o7j", cols0);
		String [][] records_o7j = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o7j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o7j", records_o7j[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(211));
		for(int i=0;i<83;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("o7j",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_o7j[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("a3t", cols0);
		String [][] records_a3t = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a3t[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a3t", records_a3t[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(440));
		for(int i=0;i<39;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("a3t",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_a3t[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("fh", cols1);
		String [][] records_fh = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fh[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fh", records_fh[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<47; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(282));
		for(int i=0;i<47;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("fh",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_fh[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("ixvt", cols2);
		String [][] records_ixvt = new String[44][cols2.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ixvt[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ixvt", records_ixvt[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(81));
		for(int i=0;i<44;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("ixvt",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ixvt[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("loe9m", cols0);
		String [][] records_loe9m = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_loe9m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("loe9m", records_loe9m[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<15; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(470));
		for(int i=0;i<15;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("loe9m",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_loe9m[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("gw9", cols1);
		String [][] records_gw9 = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gw9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gw9", records_gw9[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(592));
		for(int i=0;i<7;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("gw9",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_gw9[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("po423", cols2);
		String [][] records_po423 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_po423[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("po423", records_po423[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(629));
		for(int i=0;i<77;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("po423",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_po423[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("j17", cols0);
		String [][] records_j17 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j17[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j17", records_j17[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(981));
		for(int i=0;i<44;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("j17",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_j17[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("q58", cols1);
		String [][] records_q58 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q58[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q58", records_q58[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(557));
		for(int i=0;i<37;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("q58",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_q58[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o9mq6", cols2);
		String [][] records_o9mq6 = new String[81][cols2.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o9mq6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o9mq6", records_o9mq6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(794));
		for(int i=0;i<81;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("o9mq6",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_o9mq6[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i8s", cols3);
		String [][] records_i8s = new String[68][cols3.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i8s[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i8s", records_i8s[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(124));
		for(int i=0;i<68;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("i8s",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_i8s[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("p93", cols4);
		String [][] records_p93 = new String[62][cols4.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_p93[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p93", records_p93[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(239));
		for(int i=0;i<62;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("p93",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_p93[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("kkwx", cols0);
		String [][] records_kkwx = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kkwx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kkwx", records_kkwx[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(118));
		for(int i=0;i<28;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("kkwx",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_kkwx[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("dxl8", cols1);
		String [][] records_dxl8 = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dxl8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dxl8", records_dxl8[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(629));
		for(int i=0;i<46;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("dxl8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_dxl8[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("uee6", cols2);
		String [][] records_uee6 = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uee6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uee6", records_uee6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(976));
		for(int i=0;i<60;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("uee6",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_uee6[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h3yp", cols0);
		String [][] records_h3yp = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h3yp[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h3yp", records_h3yp[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(423));
		for(int i=0;i<77;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("h3yp",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_h3yp[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s2", cols1);
		String [][] records_s2 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s2", records_s2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<17; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(36));
		for(int i=0;i<17;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("s2",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_s2[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vey", cols0);
		String [][] records_vey = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vey[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vey", records_vey[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(402));
		for(int i=0;i<12;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("vey",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_vey[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("i12r", cols1);
		String [][] records_i12r = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i12r[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i12r", records_i12r[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<7; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(719));
		for(int i=0;i<7;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("i12r",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_i12r[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o321t", cols2);
		String [][] records_o321t = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o321t[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o321t", records_o321t[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(409));
		for(int i=0;i<50;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("o321t",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_o321t[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h484", cols3);
		String [][] records_h484 = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h484[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h484", records_h484[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(709));
		for(int i=0;i<64;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("h484",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_h484[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q6p", cols4);
		String [][] records_q6p = new String[32][cols4.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_q6p[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q6p", records_q6p[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(41));
		for(int i=0;i<32;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("q6p",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_q6p[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rkn43", cols0);
		String [][] records_rkn43 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rkn43[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rkn43", records_rkn43[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(41));
		for(int i=0;i<89;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("rkn43",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_rkn43[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v67", cols1);
		String [][] records_v67 = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v67[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v67", records_v67[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(235));
		for(int i=0;i<66;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("v67",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_v67[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j1", cols2);
		String [][] records_j1 = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j1", records_j1[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(41));
		for(int i=0;i<96;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("j1",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_j1[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("wos", cols3);
		String [][] records_wos = new String[60][cols3.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wos[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wos", records_wos[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<60; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(481));
		for(int i=0;i<60;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("wos",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_wos[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g5", cols0);
		String [][] records_g5 = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g5", records_g5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<37; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(25));
		for(int i=0;i<37;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("g5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_g5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("pj", cols1);
		String [][] records_pj = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pj", records_pj[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(719));
		for(int i=0;i<92;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("pj",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_pj[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("j7u", cols2);
		String [][] records_j7u = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j7u[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j7u", records_j7u[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<13; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(82));
		for(int i=0;i<13;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("j7u",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_j7u[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("a79b4", cols0);
		String [][] records_a79b4 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a79b4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a79b4", records_a79b4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(978));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("a79b4",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_a79b4[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("umt0", cols1);
		String [][] records_umt0 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_umt0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("umt0", records_umt0[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(321));
		for(int i=0;i<88;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("umt0",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_umt0[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("e5", cols2);
		String [][] records_e5 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e5", records_e5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(714));
		for(int i=0;i<32;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("e5",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_e5[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f1", cols3);
		String [][] records_f1 = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f1[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f1", records_f1[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(548));
		for(int i=0;i<85;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("f1",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_f1[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mu2", cols4);
		String [][] records_mu2 = new String[68][cols4.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_mu2[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mu2", records_mu2[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(246));
		for(int i=0;i<68;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("mu2",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_mu2[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("le", cols0);
		String [][] records_le = new String[100][cols0.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_le[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("le", records_le[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<100; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(269));
		for(int i=0;i<100;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("le",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_le[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tvv", cols1);
		String [][] records_tvv = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tvv[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tvv", records_tvv[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(553));
		for(int i=0;i<20;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("tvv",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_tvv[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u3", cols2);
		String [][] records_u3 = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u3", records_u3[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(710));
		for(int i=0;i<98;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("u3",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_u3[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("kb", cols3);
		String [][] records_kb = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_kb[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kb", records_kb[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<24; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(71));
		for(int i=0;i<24;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("kb",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_kb[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xviy9", cols0);
		String [][] records_xviy9 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xviy9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xviy9", records_xviy9[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<89; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(244));
		for(int i=0;i<89;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("xviy9",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_xviy9[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("t6k7", cols0);
		String [][] records_t6k7 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t6k7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6k7", records_t6k7[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(221));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t6k7",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t6k7[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b9wb4", cols0);
		String [][] records_b9wb4 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b9wb4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9wb4", records_b9wb4[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(823));
		for(int i=0;i<25;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("b9wb4",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_b9wb4[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g5422", cols1);
		String [][] records_g5422 = new String[55][cols1.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g5422[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g5422", records_g5422[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<55; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(434));
		for(int i=0;i<55;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("g5422",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_g5422[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q7", cols2);
		String [][] records_q7 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q7", records_q7[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(381));
		for(int i=0;i<51;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("q7",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_q7[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("g2cc6", cols3);
		String [][] records_g2cc6 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g2cc6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g2cc6", records_g2cc6[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(28));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("g2cc6",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_g2cc6[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("ap", cols4);
		String [][] records_ap = new String[2][cols4.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ap[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ap", records_ap[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<2; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(770));
		for(int i=0;i<2;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("ap",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_ap[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h8", cols0);
		String [][] records_h8 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h8", records_h8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(761));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("h8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_h8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j3", cols1);
		String [][] records_j3 = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j3", records_j3[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<41; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(158));
		for(int i=0;i<41;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("j3",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_j3[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x7l", cols0);
		String [][] records_x7l = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x7l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7l", records_x7l[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<19; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(957));
		for(int i=0;i<19;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("x7l",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_x7l[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("sp1", cols1);
		String [][] records_sp1 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sp1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sp1", records_sp1[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(587));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("sp1",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_sp1[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ud", cols0);
		String [][] records_ud = new String[94][cols0.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ud[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ud", records_ud[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<94; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(371));
		for(int i=0;i<94;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ud",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ud[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ow8cs", cols0);
		String [][] records_ow8cs = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ow8cs[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ow8cs", records_ow8cs[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(848));
		for(int i=0;i<46;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ow8cs",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ow8cs[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u87", cols1);
		String [][] records_u87 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u87[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u87", records_u87[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(977));
		for(int i=0;i<51;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("u87",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_u87[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w7", cols2);
		String [][] records_w7 = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w7", records_w7[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(294));
		for(int i=0;i<96;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("w7",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_w7[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k0e", cols3);
		String [][] records_k0e = new String[31][cols3.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_k0e[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k0e", records_k0e[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<31; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(366));
		for(int i=0;i<31;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("k0e",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_k0e[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("uw", cols0);
		String [][] records_uw = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_uw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uw", records_uw[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<39; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(617));
		for(int i=0;i<39;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("uw",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_uw[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h4", cols1);
		String [][] records_h4 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h4", records_h4[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(302));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("h4",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_h4[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x5", cols2);
		String [][] records_x5 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x5[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x5", records_x5[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<22; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(662));
		for(int i=0;i<22;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("x5",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_x5[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rn4", cols3);
		String [][] records_rn4 = new String[61][cols3.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_rn4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rn4", records_rn4[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(810));
		for(int i=0;i<61;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("rn4",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_rn4[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vtz1", cols4);
		String [][] records_vtz1 = new String[8][cols4.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vtz1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vtz1", records_vtz1[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(631));
		for(int i=0;i<8;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("vtz1",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_vtz1[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("bq1gk", cols0);
		String [][] records_bq1gk = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bq1gk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bq1gk", records_bq1gk[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<50; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(121));
		for(int i=0;i<50;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("bq1gk",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_bq1gk[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("lu6zx", cols1);
		String [][] records_lu6zx = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lu6zx[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lu6zx", records_lu6zx[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(348));
		for(int i=0;i<81;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("lu6zx",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_lu6zx[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r4", cols2);
		String [][] records_r4 = new String[92][cols2.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r4", records_r4[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<92; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(941));
		for(int i=0;i<92;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("r4",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_r4[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("c7z", cols3);
		String [][] records_c7z = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c7z[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c7z", records_c7z[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(993));
		for(int i=0;i<97;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("c7z",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_c7z[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("mw", cols4);
		String [][] records_mw = new String[33][cols4.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_mw[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mw", records_mw[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(951));
		for(int i=0;i<33;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("mw",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_mw[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r87", cols0);
		String [][] records_r87 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r87[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r87", records_r87[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(702));
		for(int i=0;i<80;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("r87",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_r87[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("o2", cols1);
		String [][] records_o2 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o2", records_o2[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<68; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(184));
		for(int i=0;i<68;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("o2",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_o2[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("km18", cols2);
		String [][] records_km18 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_km18[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("km18", records_km18[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(49));
		for(int i=0;i<27;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("km18",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_km18[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("h84", cols3);
		String [][] records_h84 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_h84[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h84", records_h84[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(780));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("h84",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_h84[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x7z1j", cols0);
		String [][] records_x7z1j = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x7z1j[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x7z1j", records_x7z1j[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<67; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(725));
		for(int i=0;i<67;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("x7z1j",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_x7z1j[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("nz", cols1);
		String [][] records_nz = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_nz[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nz", records_nz[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<41; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(912));
		for(int i=0;i<41;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("nz",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_nz[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("zjy", cols2);
		String [][] records_zjy = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zjy[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zjy", records_zjy[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(326));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("zjy",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_zjy[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("ge4o5", cols3);
		String [][] records_ge4o5 = new String[63][cols3.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ge4o5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ge4o5", records_ge4o5[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<63; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(953));
		for(int i=0;i<63;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ge4o5",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ge4o5[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("yl06", cols0);
		String [][] records_yl06 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yl06[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yl06", records_yl06[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<51; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(37));
		for(int i=0;i<51;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("yl06",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_yl06[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ymnf", cols1);
		String [][] records_ymnf = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ymnf[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ymnf", records_ymnf[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<8; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(937));
		for(int i=0;i<8;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ymnf",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ymnf[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("t0n8", cols0);
		String [][] records_t0n8 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t0n8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t0n8", records_t0n8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(828));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t0n8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t0n8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("ia90", cols1);
		String [][] records_ia90 = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ia90[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ia90", records_ia90[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(511));
		for(int i=0;i<26;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("ia90",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_ia90[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("t49", cols0);
		String [][] records_t49 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t49[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t49", records_t49[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(77));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t49",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t49[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k46v", cols1);
		String [][] records_k46v = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k46v[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k46v", records_k46v[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(75));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("k46v",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_k46v[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("hwb", cols0);
		String [][] records_hwb = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hwb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hwb", records_hwb[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(115));
		for(int i=0;i<1;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("hwb",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_hwb[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("oa5ve", cols1);
		String [][] records_oa5ve = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oa5ve[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oa5ve", records_oa5ve[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(833));
		for(int i=0;i<32;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("oa5ve",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_oa5ve[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jc77k", cols0);
		String [][] records_jc77k = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jc77k[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jc77k", records_jc77k[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<41; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(729));
		for(int i=0;i<41;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("jc77k",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_jc77k[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("oll0a", cols1);
		String [][] records_oll0a = new String[58][cols1.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_oll0a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oll0a", records_oll0a[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(868));
		for(int i=0;i<58;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("oll0a",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_oll0a[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lz51", cols0);
		String [][] records_lz51 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lz51[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lz51", records_lz51[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(860));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("lz51",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_lz51[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("jt5", cols1);
		String [][] records_jt5 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jt5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jt5", records_jt5[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(161));
		for(int i=0;i<6;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("jt5",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_jt5[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("lgxu", cols2);
		String [][] records_lgxu = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_lgxu[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lgxu", records_lgxu[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(248));
		for(int i=0;i<97;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("lgxu",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_lgxu[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("t8", cols0);
		String [][] records_t8 = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t8", records_t8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<61; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(174));
		for(int i=0;i<61;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("j14z6", cols1);
		String [][] records_j14z6 = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j14z6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j14z6", records_j14z6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(78));
		for(int i=0;i<32;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("j14z6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_j14z6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("i3l6", cols0);
		String [][] records_i3l6 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i3l6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i3l6", records_i3l6[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(594));
		for(int i=0;i<26;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i3l6",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i3l6[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n05p", cols0);
		String [][] records_n05p = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n05p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n05p", records_n05p[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(125));
		for(int i=0;i<84;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("n05p",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_n05p[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z3ta", cols1);
		String [][] records_z3ta = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z3ta[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z3ta", records_z3ta[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(334));
		for(int i=0;i<34;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("z3ta",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_z3ta[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("a23m", cols2);
		String [][] records_a23m = new String[57][cols2.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_a23m[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a23m", records_a23m[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<57; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(728));
		for(int i=0;i<57;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("a23m",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_a23m[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("jfe", cols3);
		String [][] records_jfe = new String[91][cols3.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jfe[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jfe", records_jfe[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(157));
		for(int i=0;i<91;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("jfe",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_jfe[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vz5", cols0);
		String [][] records_vz5 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vz5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vz5", records_vz5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(332));
		for(int i=0;i<45;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("vz5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_vz5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qh11", cols1);
		String [][] records_qh11 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qh11[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qh11", records_qh11[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(728));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("qh11",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_qh11[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("vs23", cols2);
		String [][] records_vs23 = new String[44][cols2.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vs23[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vs23", records_vs23[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<44; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(233));
		for(int i=0;i<44;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("vs23",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_vs23[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("w8", cols3);
		String [][] records_w8 = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w8[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w8", records_w8[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(914));
		for(int i=0;i<73;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("w8",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_w8[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("pdyg3", cols4);
		String [][] records_pdyg3 = new String[52][cols4.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_pdyg3[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pdyg3", records_pdyg3[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(865));
		for(int i=0;i<52;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("pdyg3",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_pdyg3[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ytzh", cols0);
		String [][] records_ytzh = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ytzh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ytzh", records_ytzh[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<82; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(916));
		for(int i=0;i<82;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ytzh",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ytzh[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("c7", cols1);
		String [][] records_c7 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c7", records_c7[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(86));
		for(int i=0;i<80;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("c7",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_c7[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kw", cols2);
		String [][] records_kw = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kw", records_kw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<6; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(536));
		for(int i=0;i<6;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("kw",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_kw[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qht78", cols0);
		String [][] records_qht78 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qht78[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qht78", records_qht78[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(496));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("qht78",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_qht78[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a6", cols1);
		String [][] records_a6 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a6", records_a6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<9; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(546));
		for(int i=0;i<9;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("a6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_a6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("t6l43", cols2);
		String [][] records_t6l43 = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_t6l43[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6l43", records_t6l43[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<90; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(390));
		for(int i=0;i<90;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("t6l43",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_t6l43[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("ycr2", cols3);
		String [][] records_ycr2 = new String[62][cols3.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ycr2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ycr2", records_ycr2[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(146));
		for(int i=0;i<62;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ycr2",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ycr2[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("t04", cols0);
		String [][] records_t04 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t04[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t04", records_t04[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<70; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(363));
		for(int i=0;i<70;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("t04",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_t04[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lh2lk", cols1);
		String [][] records_lh2lk = new String[52][cols1.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lh2lk[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lh2lk", records_lh2lk[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(632));
		for(int i=0;i<52;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("lh2lk",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_lh2lk[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("pd7", cols2);
		String [][] records_pd7 = new String[71][cols2.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pd7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pd7", records_pd7[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(132));
		for(int i=0;i<71;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("pd7",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_pd7[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("ab3", cols3);
		String [][] records_ab3 = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ab3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ab3", records_ab3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(614));
		for(int i=0;i<38;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ab3",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ab3[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h3", cols0);
		String [][] records_h3 = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h3", records_h3[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<71; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(696));
		for(int i=0;i<71;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("h3",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_h3[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("eppb", cols1);
		String [][] records_eppb = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eppb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("eppb", records_eppb[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(684));
		for(int i=0;i<75;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("eppb",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_eppb[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("yy28k", cols0);
		String [][] records_yy28k = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yy28k[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yy28k", records_yy28k[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(857));
		for(int i=0;i<26;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("yy28k",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_yy28k[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("w31", cols1);
		String [][] records_w31 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w31[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w31", records_w31[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(171));
		for(int i=0;i<88;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("w31",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_w31[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ib48", cols2);
		String [][] records_ib48 = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ib48[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ib48", records_ib48[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(85));
		for(int i=0;i<96;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("ib48",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ib48[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v4f", cols0);
		String [][] records_v4f = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v4f[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v4f", records_v4f[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(686));
		for(int i=0;i<79;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("v4f",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_v4f[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wir", cols1);
		String [][] records_wir = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wir[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wir", records_wir[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(860));
		for(int i=0;i<93;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("wir",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_wir[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hw1", cols2);
		String [][] records_hw1 = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hw1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hw1", records_hw1[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<1; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(342));
		for(int i=0;i<1;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("hw1",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_hw1[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q3l", cols0);
		String [][] records_q3l = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q3l[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q3l", records_q3l[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(842));
		for(int i=0;i<35;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("q3l",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_q3l[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("k8uq", cols1);
		String [][] records_k8uq = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k8uq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k8uq", records_k8uq[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(424));
		for(int i=0;i<25;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("k8uq",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_k8uq[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xx6", cols2);
		String [][] records_xx6 = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_xx6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("xx6", records_xx6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(48));
		for(int i=0;i<83;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("xx6",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_xx6[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("ws", cols3);
		String [][] records_ws = new String[40][cols3.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ws[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ws", records_ws[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(287));
		for(int i=0;i<40;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("ws",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_ws[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kn8q9", cols4);
		String [][] records_kn8q9 = new String[83][cols4.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_kn8q9[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kn8q9", records_kn8q9[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(281));
		for(int i=0;i<83;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("kn8q9",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_kn8q9[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("hg", cols0);
		String [][] records_hg = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hg[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hg", records_hg[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<45; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(725));
		for(int i=0;i<45;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("hg",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_hg[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("c8n", cols1);
		String [][] records_c8n = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c8n[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c8n", records_c8n[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<64; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(528));
		for(int i=0;i<64;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("c8n",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_c8n[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("p7r3s", cols2);
		String [][] records_p7r3s = new String[74][cols2.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p7r3s[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p7r3s", records_p7r3s[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(335));
		for(int i=0;i<74;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("p7r3s",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_p7r3s[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("abz4", cols3);
		String [][] records_abz4 = new String[91][cols3.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_abz4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("abz4", records_abz4[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(333));
		for(int i=0;i<91;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("abz4",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_abz4[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n639", cols4);
		String [][] records_n639 = new String[77][cols4.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_n639[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n639", records_n639[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<77; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(573));
		for(int i=0;i<77;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("n639",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_n639[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j2338", cols0);
		String [][] records_j2338 = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j2338[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j2338", records_j2338[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(541));
		for(int i=0;i<34;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("j2338",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_j2338[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("in8eo", cols1);
		String [][] records_in8eo = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_in8eo[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("in8eo", records_in8eo[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(382));
		for(int i=0;i<25;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("in8eo",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_in8eo[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y089", cols2);
		String [][] records_y089 = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y089[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y089", records_y089[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<34; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(51));
		for(int i=0;i<34;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("y089",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_y089[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("so", cols3);
		String [][] records_so = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_so[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("so", records_so[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<43; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(485));
		for(int i=0;i<43;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("so",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_so[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("os7", cols4);
		String [][] records_os7 = new String[72][cols4.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_os7[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("os7", records_os7[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<72; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(102));
		for(int i=0;i<72;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("os7",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_os7[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fik", cols0);
		String [][] records_fik = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fik[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fik", records_fik[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<42; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(783));
		for(int i=0;i<42;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("fik",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_fik[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("h6l", cols1);
		String [][] records_h6l = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h6l[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h6l", records_h6l[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<11; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(516));
		for(int i=0;i<11;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("h6l",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_h6l[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("hvg", cols2);
		String [][] records_hvg = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hvg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hvg", records_hvg[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<23; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(652));
		for(int i=0;i<23;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("hvg",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_hvg[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y20", cols3);
		String [][] records_y20 = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y20[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y20", records_y20[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<38; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(157));
		for(int i=0;i<38;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("y20",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_y20[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mii", cols0);
		String [][] records_mii = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mii[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mii", records_mii[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<93; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(59));
		for(int i=0;i<93;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("mii",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_mii[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u3x", cols1);
		String [][] records_u3x = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u3x[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u3x", records_u3x[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<48; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(252));
		for(int i=0;i<48;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("u3x",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_u3x[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bkw", cols2);
		String [][] records_bkw = new String[95][cols2.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bkw[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bkw", records_bkw[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(625));
		for(int i=0;i<95;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("bkw",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_bkw[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z163", cols0);
		String [][] records_z163 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z163[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z163", records_z163[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<52; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(498));
		for(int i=0;i<52;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("z163",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_z163[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u45", cols1);
		String [][] records_u45 = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u45[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u45", records_u45[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<30; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(591));
		for(int i=0;i<30;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("u45",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_u45[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("is", cols2);
		String [][] records_is = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_is[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("is", records_is[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<83; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(262));
		for(int i=0;i<83;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("is",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_is[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s5", cols3);
		String [][] records_s5 = new String[81][cols3.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s5", records_s5[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<81; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(94));
		for(int i=0;i<81;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("s5",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_s5[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c"};
		DBApp.createTable("sp", cols4);
		String [][] records_sp = new String[86][cols4.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_sp[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sp", records_sp[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<86; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(446));
		for(int i=0;i<86;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("sp",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_sp[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("rdt", cols0);
		String [][] records_rdt = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rdt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rdt", records_rdt[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<25; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(523));
		for(int i=0;i<25;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("rdt",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_rdt[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("gmb", cols1);
		String [][] records_gmb = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gmb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gmb", records_gmb[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<40; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(889));
		for(int i=0;i<40;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("gmb",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_gmb[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("q31t4", cols2);
		String [][] records_q31t4 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q31t4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q31t4", records_q31t4[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(24));
		for(int i=0;i<28;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("q31t4",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_q31t4[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ks", cols0);
		String [][] records_ks = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ks[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ks", records_ks[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<58; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(281));
		for(int i=0;i<58;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("ks",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_ks[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("x3x", cols0);
		String [][] records_x3x = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x3x[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x3x", records_x3x[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<27; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(936));
		for(int i=0;i<27;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("x3x",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_x3x[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("kwes", cols1);
		String [][] records_kwes = new String[33][cols1.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kwes[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kwes", records_kwes[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<33; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(322));
		for(int i=0;i<33;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("kwes",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_kwes[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ywl", cols2);
		String [][] records_ywl = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ywl[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ywl", records_ywl[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(364));
		for(int i=0;i<73;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("ywl",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ywl[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("at5j", cols3);
		String [][] records_at5j = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_at5j[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("at5j", records_at5j[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<28; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(284));
		for(int i=0;i<28;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("at5j",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_at5j[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("er", cols0);
		String [][] records_er = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_er[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("er", records_er[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<79; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(9));
		for(int i=0;i<79;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("er",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_er[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("oi", cols0);
		String [][] records_oi = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_oi[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oi", records_oi[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<18; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(853));
		for(int i=0;i<18;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("oi",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_oi[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("jhd", cols1);
		String [][] records_jhd = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jhd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jhd", records_jhd[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(184));
		for(int i=0;i<73;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("jhd",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_jhd[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("r42", cols2);
		String [][] records_r42 = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r42[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r42", records_r42[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<26; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(665));
		for(int i=0;i<26;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("r42",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_r42[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("woa", cols3);
		String [][] records_woa = new String[95][cols3.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_woa[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("woa", records_woa[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<95; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(654));
		for(int i=0;i<95;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("woa",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_woa[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("i0a", cols0);
		String [][] records_i0a = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i0a[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i0a", records_i0a[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<12; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(656));
		for(int i=0;i<12;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("i0a",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_i0a[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("jhl", cols1);
		String [][] records_jhl = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jhl[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jhl", records_jhl[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<84; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(936));
		for(int i=0;i<84;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("jhl",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_jhl[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yx4k", cols2);
		String [][] records_yx4k = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yx4k[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yx4k", records_yx4k[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<66; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(780));
		for(int i=0;i<66;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("yx4k",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_yx4k[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k254", cols0);
		String [][] records_k254 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k254[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k254", records_k254[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(660));
		for(int i=0;i<32;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("k254",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_k254[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("b5xg", cols1);
		String [][] records_b5xg = new String[74][cols1.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b5xg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b5xg", records_b5xg[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<74; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(878));
		for(int i=0;i<74;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("b5xg",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_b5xg[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yr6", cols2);
		String [][] records_yr6 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yr6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yr6", records_yr6[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(449));
		for(int i=0;i<85;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("yr6",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_yr6[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("x96", cols3);
		String [][] records_x96 = new String[88][cols3.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x96[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x96", records_x96[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<88; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(702));
		for(int i=0;i<88;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("x96",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_x96[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("svs", cols0);
		String [][] records_svs = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_svs[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("svs", records_svs[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<69; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(119));
		for(int i=0;i<69;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("svs",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_svs[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("dpj", cols0);
		String [][] records_dpj = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dpj[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dpj", records_dpj[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<75; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(286));
		for(int i=0;i<75;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("dpj",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_dpj[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("az6", cols1);
		String [][] records_az6 = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_az6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("az6", records_az6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(468));
		for(int i=0;i<73;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("az6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_az6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n66", cols0);
		String [][] records_n66 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n66[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n66", records_n66[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<53; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(983));
		for(int i=0;i<53;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("n66",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_n66[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("d91hc", cols1);
		String [][] records_d91hc = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d91hc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d91hc", records_d91hc[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<73; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(611));
		for(int i=0;i<73;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("d91hc",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_d91hc[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ecm", cols2);
		String [][] records_ecm = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ecm[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ecm", records_ecm[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<98; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(198));
		for(int i=0;i<98;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("ecm",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ecm[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("c3", cols3);
		String [][] records_c3 = new String[80][cols3.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c3[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c3", records_c3[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<80; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(913));
		for(int i=0;i<80;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("c3",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_c3[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("le4d", cols4);
		String [][] records_le4d = new String[35][cols4.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_le4d[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("le4d", records_le4d[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<35; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(40));
		for(int i=0;i<35;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("le4d",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_le4d[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("vs114", cols0);
		String [][] records_vs114 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vs114[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vs114", records_vs114[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<91; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(59));
		for(int i=0;i<91;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("vs114",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_vs114[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dtt", cols0);
		String [][] records_dtt = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dtt[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dtt", records_dtt[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<46; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(120));
		for(int i=0;i<46;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("dtt",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_dtt[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("tpyw8", cols0);
		String [][] records_tpyw8 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tpyw8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tpyw8", records_tpyw8[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<43; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(6));
		for(int i=0;i<43;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("tpyw8",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_tpyw8[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("lw6", cols1);
		String [][] records_lw6 = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lw6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lw6", records_lw6[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<96; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(916));
		for(int i=0;i<96;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("lw6",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_lw6[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("v7y", cols2);
		String [][] records_v7y = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v7y[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v7y", records_v7y[i]);
		}
		ArrayList<Integer> recordNumbers2 = new ArrayList<>();
		for (int i=0; i<85; i++){
			recordNumbers2.add(i);
		}
		Collections.shuffle(recordNumbers2, new Random(246));
		for(int i=0;i<85;i++)
		{
			int pageCount2 = recordNumbers2.get(i)/DBApp.dataPageSize;
			int recordCount2 = recordNumbers2.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect2 = DBApp.select("v7y",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_v7y[recordNumbers2.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("wje", cols3);
		String [][] records_wje = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wje[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wje", records_wje[i]);
		}
		ArrayList<Integer> recordNumbers3 = new ArrayList<>();
		for (int i=0; i<20; i++){
			recordNumbers3.add(i);
		}
		Collections.shuffle(recordNumbers3, new Random(598));
		for(int i=0;i<20;i++)
		{
			int pageCount3 = recordNumbers3.get(i)/DBApp.dataPageSize;
			int recordCount3 = recordNumbers3.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect3 = DBApp.select("wje",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_wje[recordNumbers3.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("ef2l", cols4);
		String [][] records_ef2l = new String[97][cols4.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ef2l[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ef2l", records_ef2l[i]);
		}
		ArrayList<Integer> recordNumbers4 = new ArrayList<>();
		for (int i=0; i<97; i++){
			recordNumbers4.add(i);
		}
		Collections.shuffle(recordNumbers4, new Random(240));
		for(int i=0;i<97;i++)
		{
			int pageCount4 = recordNumbers4.get(i)/DBApp.dataPageSize;
			int recordCount4 = recordNumbers4.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect4 = DBApp.select("ef2l",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_ef2l[recordNumbers4.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionPointerDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yrz5", cols0);
		String [][] records_yrz5 = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yrz5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yrz5", records_yrz5[i]);
		}
		ArrayList<Integer> recordNumbers0 = new ArrayList<>();
		for (int i=0; i<62; i++){
			recordNumbers0.add(i);
		}
		Collections.shuffle(recordNumbers0, new Random(354));
		for(int i=0;i<62;i++)
		{
			int pageCount0 = recordNumbers0.get(i)/DBApp.dataPageSize;
			int recordCount0 = recordNumbers0.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect0 = DBApp.select("yrz5",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_yrz5[recordNumbers0.get(i)])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("k880", cols1);
		String [][] records_k880 = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k880[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k880", records_k880[i]);
		}
		ArrayList<Integer> recordNumbers1 = new ArrayList<>();
		for (int i=0; i<32; i++){
			recordNumbers1.add(i);
		}
		Collections.shuffle(recordNumbers1, new Random(11));
		for(int i=0;i<32;i++)
		{
			int pageCount1 = recordNumbers1.get(i)/DBApp.dataPageSize;
			int recordCount1 = recordNumbers1.get(i)%DBApp.dataPageSize;
			ArrayList<String[]> tableSelect1 = DBApp.select("k880",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_k880[recordNumbers1.get(i)])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(85);
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