package DBMS;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.Test;
public class DBAppTests
{

	@Test(timeout = 10000)
	public void test0TableFolderCreationDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"g478b","v2ywj"};
		DBApp.createTable("sr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("sr"));
		String[] cols1 = {"ylv01","whw","ga","h89","w19","x26"};
		DBApp.createTable("tu2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("tu2"));
		String[] cols2 = {"bbxsd","sm9pt","cf1t","n27","m82c5"};
		DBApp.createTable("y348", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("y348"));
		String[] cols3 = {"kuw","e9c92","z8t","kb","bc"};
		DBApp.createTable("iuc", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("iuc"));
	
		FileManager.reset();
	}
	@Test(timeout = 10000)
	public void test0TableSerializationDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"v53"};
		DBApp.createTable("d3sh", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("d3sh.db"));
		String[] cols1 = {"fj5"};
		DBApp.createTable("kt", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("kt.db"));
		String[] cols2 = {"d40","n20","sm1","t1b0","dij","q16","e42c","pk9k"};
		DBApp.createTable("v905w", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("v905w.db"));
		String[] cols3 = {"xg188","q7l","cb","ou","x2","x8b0","dlsa5","x1","ov1pu","yu5"};
		DBApp.createTable("yj65", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("yj65.db"));
		String[] cols4 = {"b3t","zr7","h65p1","px"};
		DBApp.createTable("ae", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("ae.db"));
	
		FileManager.reset();
	}
	@Test(timeout = 10000)
	public void test0TableTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"inbp9","xn87","v6vg"};
		DBApp.createTable("h06h", cols0);
		String tableTrace0 = DBApp.getLastTrace("h06h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("h06h"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"r9pe","f2c","dtq0","j4","a249","y6k"};
		DBApp.createTable("li3", cols1);
		String tableTrace1 = DBApp.getLastTrace("li3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("li3"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionPageSerializationDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"o8","v6","f4k7z","l4z","g3897","dbm","m7s"};
		DBApp.createTable("n01", cols0);
		for(int i=0;i<67;i++)
		{
			String [] record_n01 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n01[j] = genRandString();
			}
			DBApp.insert("n01", record_n01);
		}
		int pagesCount_n01 = (int)Math.ceil(67.0/DBApp.dataPageSize);
			ArrayList<String> res_n01 = new ArrayList<>();
			for(int i=0;i<pagesCount_n01;i++)
			{
				res_n01.add(i+".db");
			}
			Collections.sort(res_n01);
			String pagesTraceString_n01 = "n01{ " + String.join(" ",res_n01);	String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are in inserted.("+pagesTraceString_n01+")", fileManagerTrace0.contains(pagesTraceString_n01));
		//--------------------------------------------------------------------------
		String[] cols1 = {"hb6yl","p935m","tl","nz5","t0","p2qaz","ef"};
		DBApp.createTable("a13", cols1);
		for(int i=0;i<17;i++)
		{
			String [] record_a13 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_a13[j] = genRandString();
			}
			DBApp.insert("a13", record_a13);
		}
		int pagesCount_a13 = (int)Math.ceil(17.0/DBApp.dataPageSize);
			ArrayList<String> res_a13 = new ArrayList<>();
			for(int i=0;i<pagesCount_a13;i++)
			{
				res_a13.add(i+".db");
			}
			Collections.sort(res_a13);
			String pagesTraceString_a13 = "a13{ " + String.join(" ",res_a13);	String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are in inserted.("+pagesTraceString_a13+")", fileManagerTrace1.contains(pagesTraceString_a13));
		//--------------------------------------------------------------------------
		String[] cols2 = {"jpa","gw"};
		DBApp.createTable("tg", cols2);
		for(int i=0;i<44;i++)
		{
			String [] record_tg = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_tg[j] = genRandString();
			}
			DBApp.insert("tg", record_tg);
		}
		int pagesCount_tg = (int)Math.ceil(44.0/DBApp.dataPageSize);
			ArrayList<String> res_tg = new ArrayList<>();
			for(int i=0;i<pagesCount_tg;i++)
			{
				res_tg.add(i+".db");
			}
			Collections.sort(res_tg);
			String pagesTraceString_tg = "tg{ " + String.join(" ",res_tg);	String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are in inserted.("+pagesTraceString_tg+")", fileManagerTrace2.contains(pagesTraceString_tg));
		//--------------------------------------------------------------------------
		String[] cols3 = {"p33","d4ce","qv8b","yin9c","r4l3g"};
		DBApp.createTable("q0x8", cols3);
		for(int i=0;i<60;i++)
		{
			String [] record_q0x8 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_q0x8[j] = genRandString();
			}
			DBApp.insert("q0x8", record_q0x8);
		}
		int pagesCount_q0x8 = (int)Math.ceil(60.0/DBApp.dataPageSize);
			ArrayList<String> res_q0x8 = new ArrayList<>();
			for(int i=0;i<pagesCount_q0x8;i++)
			{
				res_q0x8.add(i+".db");
			}
			Collections.sort(res_q0x8);
			String pagesTraceString_q0x8 = "q0x8{ " + String.join(" ",res_q0x8);	String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are in inserted.("+pagesTraceString_q0x8+")", fileManagerTrace3.contains(pagesTraceString_q0x8));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionPageTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"z40"};
		DBApp.createTable("q6x", cols0);
		String [][] records_q6x = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q6x[i][j] = genRandString();
			}
			DBApp.insert("q6x", records_q6x[i]);
			String tableTrace0 = DBApp.getLastTrace("q6x");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q6x[i])+")", tableTrace0.contains(Arrays.toString(records_q6x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"iq6","thq","mdd"};
		DBApp.createTable("wos1", cols1);
		String [][] records_wos1 = new String[61][cols1.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wos1[i][j] = genRandString();
			}
			DBApp.insert("wos1", records_wos1[i]);
			String tableTrace1 = DBApp.getLastTrace("wos1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wos1[i])+")", tableTrace1.contains(Arrays.toString(records_wos1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"vo","v0rr","b8b","d5","u8t4k","h7c","j75"};
		DBApp.createTable("d9yy", cols2);
		String [][] records_d9yy = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d9yy[i][j] = genRandString();
			}
			DBApp.insert("d9yy", records_d9yy[i]);
			String tableTrace2 = DBApp.getLastTrace("d9yy");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d9yy[i])+")", tableTrace2.contains(Arrays.toString(records_d9yy[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"qw","o8mn","cs"};
		DBApp.createTable("j2w", cols3);
		String [][] records_j2w = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j2w[i][j] = genRandString();
			}
			DBApp.insert("j2w", records_j2w[i]);
			String tableTrace3 = DBApp.getLastTrace("j2w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j2w[i])+")", tableTrace3.contains(Arrays.toString(records_j2w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"y1o96","v733n"};
		DBApp.createTable("r6p2", cols4);
		String [][] records_r6p2 = new String[7][cols4.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r6p2[i][j] = genRandString();
			}
			DBApp.insert("r6p2", records_r6p2[i]);
			String tableTrace4 = DBApp.getLastTrace("r6p2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r6p2[i])+")", tableTrace4.contains(Arrays.toString(records_r6p2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("z0p8", cols0);
		String [][] records_z0p8 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z0p8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z0p8", records_z0p8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("z0p8");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivelent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_z0p8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("ooj88", cols0);
		String [][] records_ooj88 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ooj88[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ooj88", records_ooj88[i]);
		}
		DBApp.select("ooj88");
		String selectTrace0 = DBApp.getLastTrace("ooj88");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace0.startsWith("Select all"));
		int pageCount0 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace0.contains("pages:"+pageCount0));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace0.contains("records:"+26));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("x42", cols1);
		String [][] records_x42 = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x42[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x42", records_x42[i]);
		}
		DBApp.select("x42");
		String selectTrace1 = DBApp.getLastTrace("x42");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace1.startsWith("Select all"));
		int pageCount1 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace1.contains("pages:"+pageCount1));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace1.contains("records:"+87));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace1.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("q82bn", cols2);
		String [][] records_q82bn = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q82bn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q82bn", records_q82bn[i]);
		}
		DBApp.select("q82bn");
		String selectTrace2 = DBApp.getLastTrace("q82bn");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace2.startsWith("Select all"));
		int pageCount2 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace2.contains("pages:"+pageCount2));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace2.contains("records:"+42));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace2.contains("execution time"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("d5", cols3);
		String [][] records_d5 = new String[54][cols3.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d5[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d5", records_d5[i]);
		}
		DBApp.select("d5");
		String selectTrace3 = DBApp.getLastTrace("d5");
		assertTrue("Conditionless selection trace should start with the keyword -Select all-.", selectTrace3.startsWith("Select all"));
		int pageCount3 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		assertTrue("Conditionless selection trace should report the number of pages correctly.", selectTrace3.contains("pages:"+pageCount3));
		assertTrue("Conditionless selection trace should report the number of records correctly.", selectTrace3.contains("records:"+54));
		assertTrue("Conditionless selection trace should report the execution time.", selectTrace3.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionPointerDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("v5g", cols0);
		String [][] records_v5g = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v5g[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v5g", records_v5g[i]);
		}
		int pageCount0 = 0;
		int recordCount0 = 0;
		for(int i=0;i<5;i++)
		{
			ArrayList<String[]> tableSelect0 = DBApp.select("v5g",pageCount0,recordCount0);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect0.get(0)).equals(Arrays.toString(records_v5g[i])));
			recordCount0++;
			if(recordCount0==DBApp.dataPageSize)
			{
				pageCount0++;
				recordCount0=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("r8", cols1);
		String [][] records_r8 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r8", records_r8[i]);
		}
		int pageCount1 = 0;
		int recordCount1 = 0;
		for(int i=0;i<35;i++)
		{
			ArrayList<String[]> tableSelect1 = DBApp.select("r8",pageCount1,recordCount1);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect1.get(0)).equals(Arrays.toString(records_r8[i])));
			recordCount1++;
			if(recordCount1==DBApp.dataPageSize)
			{
				pageCount1++;
				recordCount1=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ml", cols2);
		String [][] records_ml = new String[78][cols2.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ml[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ml", records_ml[i]);
		}
		int pageCount2 = 0;
		int recordCount2 = 0;
		for(int i=0;i<78;i++)
		{
			ArrayList<String[]> tableSelect2 = DBApp.select("ml",pageCount2,recordCount2);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect2.get(0)).equals(Arrays.toString(records_ml[i])));
			recordCount2++;
			if(recordCount2==DBApp.dataPageSize)
			{
				pageCount2++;
				recordCount2=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("gej9", cols3);
		String [][] records_gej9 = new String[68][cols3.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gej9[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gej9", records_gej9[i]);
		}
		int pageCount3 = 0;
		int recordCount3 = 0;
		for(int i=0;i<68;i++)
		{
			ArrayList<String[]> tableSelect3 = DBApp.select("gej9",pageCount3,recordCount3);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect3.get(0)).equals(Arrays.toString(records_gej9[i])));
			recordCount3++;
			if(recordCount3==DBApp.dataPageSize)
			{
				pageCount3++;
				recordCount3=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("m06", cols4);
		String [][] records_m06 = new String[52][cols4.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_m06[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m06", records_m06[i]);
		}
		int pageCount4 = 0;
		int recordCount4 = 0;
		for(int i=0;i<52;i++)
		{
			ArrayList<String[]> tableSelect4 = DBApp.select("m06",pageCount4,recordCount4);
			assertTrue("Inserted and selected by pointests records should be equivelent.", Arrays.toString(tableSelect4.get(0)).equals(Arrays.toString(records_m06[i])));
			recordCount4++;
			if(recordCount4==DBApp.dataPageSize)
			{
				pageCount4++;
				recordCount4=0;
			}
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionPointerTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("y4", cols0);
		String [][] records_y4 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y4", records_y4[i]);
		}
		int pageCount0 = 0;
		int recordCount0 = 0;
		for(int i=0;i<92;i++)
		{
			DBApp.select("y4",pageCount0,recordCount0);
			String selectTrace0 = DBApp.getLastTrace("y4");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace0.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace0.contains("page:"+pageCount0));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace0.contains("record:"+recordCount0));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace0.contains("execution time"));
			recordCount0++;
			if(recordCount0==DBApp.dataPageSize)
			{
				pageCount0++;
				recordCount0=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("obloo", cols1);
		String [][] records_obloo = new String[65][cols1.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_obloo[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("obloo", records_obloo[i]);
		}
		int pageCount1 = 0;
		int recordCount1 = 0;
		for(int i=0;i<65;i++)
		{
			DBApp.select("obloo",pageCount1,recordCount1);
			String selectTrace1 = DBApp.getLastTrace("obloo");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace1.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace1.contains("page:"+pageCount1));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace1.contains("record:"+recordCount1));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace1.contains("execution time"));
			recordCount1++;
			if(recordCount1==DBApp.dataPageSize)
			{
				pageCount1++;
				recordCount1=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r3v", cols2);
		String [][] records_r3v = new String[82][cols2.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r3v[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r3v", records_r3v[i]);
		}
		int pageCount2 = 0;
		int recordCount2 = 0;
		for(int i=0;i<82;i++)
		{
			DBApp.select("r3v",pageCount2,recordCount2);
			String selectTrace2 = DBApp.getLastTrace("r3v");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace2.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace2.contains("page:"+pageCount2));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace2.contains("record:"+recordCount2));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace2.contains("execution time"));
			recordCount2++;
			if(recordCount2==DBApp.dataPageSize)
			{
				pageCount2++;
				recordCount2=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("hw", cols3);
		String [][] records_hw = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_hw[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hw", records_hw[i]);
		}
		int pageCount3 = 0;
		int recordCount3 = 0;
		for(int i=0;i<10;i++)
		{
			DBApp.select("hw",pageCount3,recordCount3);
			String selectTrace3 = DBApp.getLastTrace("hw");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace3.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace3.contains("page:"+pageCount3));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace3.contains("record:"+recordCount3));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace3.contains("execution time"));
			recordCount3++;
			if(recordCount3==DBApp.dataPageSize)
			{
				pageCount3++;
				recordCount3=0;
			}
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("j7i2", cols4);
		String [][] records_j7i2 = new String[47][cols4.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_j7i2[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j7i2", records_j7i2[i]);
		}
		int pageCount4 = 0;
		int recordCount4 = 0;
		for(int i=0;i<47;i++)
		{
			DBApp.select("j7i2",pageCount4,recordCount4);
			String selectTrace4 = DBApp.getLastTrace("j7i2");
			assertTrue("Pointer selection trace should start with the keyword -Select pointer-.", selectTrace4.startsWith("Select pointer"));
			assertTrue("Pointer selection trace should report the number of the page correctly.", selectTrace4.contains("page:"+pageCount4));
			assertTrue("Pointer selection trace should report the number of the record correctly.", selectTrace4.contains("record:"+recordCount4));
			assertTrue("Pointer selection trace should report the execution time.", selectTrace4.contains("execution time"));
			recordCount4++;
			if(recordCount4==DBApp.dataPageSize)
			{
				pageCount4++;
				recordCount4=0;
			}
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionConditionDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("rh", cols0);
		String [][] records_rh = new String[161][cols0.length];
		for(int i=0;i<161;i++)
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
		//record156: [a156, b0, c0]
		//record157: [a157, b1, c1]
		//record158: [a158, b0, c2]
		//record159: [a159, b1, c0]
		//record160: [a160, b0, c1]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("rh", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 80, records:
		//record0: [a1, b1, c1]
		//record1: [a3, b1, c0]
		//record2: [a5, b1, c2]
		//record3: [a7, b1, c1]
		//record4: [a9, b1, c0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect0.size() == 80);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("xq", cols1);
		String [][] records_xq = new String[325][cols1.length];
		for(int i=0;i<325;i++)
		{
			records_xq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xq", records_xq[i]);
		}
		//first 5 records:
		//record0: [a0, b0]
		//record1: [a1, b1]
		//record2: [a2, b0]
		//record3: [a3, b1]
		//record4: [a4, b0]
		//last 5 records:
		//record320: [a320, b0]
		//record321: [a321, b1]
		//record322: [a322, b0]
		//record323: [a323, b1]
		//record324: [a324, b0]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.select("xq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 163, records:
		//record0: [a0, b0]
		//record1: [a2, b0]
		//record2: [a4, b0]
		//record3: [a6, b0]
		//record4: [a8, b0]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect1.size() == 163);
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
		DBApp.createTable("kp7h7", cols2);
		String [][] records_kp7h7 = new String[422][cols2.length];
		for(int i=0;i<422;i++)
		{
			records_kp7h7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kp7h7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kp7h7", records_kp7h7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record417: [a417, b1, c0, d1, e2, f3]
		//record418: [a418, b0, c1, d2, e3, f4]
		//record419: [a419, b1, c2, d3, e4, f5]
		//record420: [a420, b0, c0, d0, e0, f0]
		//record421: [a421, b1, c1, d1, e1, f1]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.select("kp7h7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 211, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a3, b1, c0, d3, e3, f3]
		//record2: [a5, b1, c2, d1, e0, f5]
		//record3: [a7, b1, c1, d3, e2, f1]
		//record4: [a9, b1, c0, d1, e4, f3]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect2.size() == 211);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("m6t1x", cols3);
		String [][] records_m6t1x = new String[256][cols3.length];
		for(int i=0;i<256;i++)
		{
			records_m6t1x[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m6t1x[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m6t1x", records_m6t1x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0]
		//record1: [a1, b1, c1, d1, e1, f1]
		//record2: [a2, b0, c2, d2, e2, f2]
		//record3: [a3, b1, c0, d3, e3, f3]
		//record4: [a4, b0, c1, d0, e4, f4]
		//last 5 records:
		//record251: [a251, b1, c2, d3, e1, f5]
		//record252: [a252, b0, c0, d0, e2, f0]
		//record253: [a253, b1, c1, d1, e3, f1]
		//record254: [a254, b0, c2, d2, e4, f2]
		//record255: [a255, b1, c0, d3, e0, f3]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.select("m6t1x", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 85, records:
		//record0: [a1, b1, c1, d1, e1, f1]
		//record1: [a4, b0, c1, d0, e4, f4]
		//record2: [a7, b1, c1, d3, e2, f1]
		//record3: [a10, b0, c1, d2, e0, f4]
		//record4: [a13, b1, c1, d1, e3, f1]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect3.size() == 85);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4"})));
		assertTrue("Conditional selection should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("mpm", cols4);
		String [][] records_mpm = new String[110][cols4.length];
		for(int i=0;i<110;i++)
		{
			records_mpm[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mpm[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mpm", records_mpm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0]
		//record1: [a1, b1, c1, d1, e1]
		//record2: [a2, b0, c2, d2, e2]
		//record3: [a3, b1, c0, d3, e3]
		//record4: [a4, b0, c1, d0, e4]
		//last 5 records:
		//record105: [a105, b1, c0, d1, e0]
		//record106: [a106, b0, c1, d2, e1]
		//record107: [a107, b1, c2, d3, e2]
		//record108: [a108, b0, c0, d0, e3]
		//record109: [a109, b1, c1, d1, e4]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		ArrayList<String[]> tableSelect4 = DBApp.select("mpm", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 55, records:
		//record0: [a1, b1, c1, d1, e1]
		//record1: [a3, b1, c0, d3, e3]
		//record2: [a5, b1, c2, d1, e0]
		//record3: [a7, b1, c1, d3, e2]
		//record4: [a9, b1, c0, d1, e4]
		assertTrue("Conditional selection should return the correct number of records.", tableSelect4.size() == 55);
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
	public void test0TableInsertionSelectionConditionTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ar", cols0);
		String [][] records_ar = new String[227][cols0.length];
		for(int i=0;i<227;i++)
		{
			records_ar[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ar[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ar", records_ar[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2, n13, o13]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3, n0, o14]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9, m4, n1, o0]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5, n2, o1]
		String[] ConditionColumns0 = {"m","c","l","k","b"};
		String[] ConditionColumnsValues0 = {"m3","c0","l9","k4","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.select("ar", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6]
		int pageCount0 = (int)Math.ceil(227.0/DBApp.dataPageSize);
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
		String selectTrace0 = DBApp.getLastTrace("ar");
		assertTrue("Conditional selection trace should start with the keyword -Select condition-.", selectTrace0.startsWith("Select condition"));
		assertTrue("Conditional selection trace should report the condition of selection correctly.", selectTrace0.contains(Arrays.toString(ConditionColumns0)+"->"+Arrays.toString(ConditionColumnsValues0)));
		assertTrue("Conditional selection trace should report the numbers of records per pages correctly.", selectTrace0.contains(pagesCounts0.toString()));
		assertTrue("Conditional selection trace should report the number of records correctly.", selectTrace0.contains("records:"+1));
		assertTrue("Conditional selection trace should report the execution time.", selectTrace0.contains("execution time"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableFullTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("rc5", cols0);
		String [][] records_rc5 = new String[190][cols0.length];
		for(int i=0;i<190;i++)
		{
			records_rc5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rc5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rc5", records_rc5[i]);
		}
	
		int pageCount0 = (int)Math.ceil(190.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0-1);
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((190%DBApp.dataPageSize)-1);
		DBApp.select("rc5", pageSelectPointer00,recordSelectPointer00);
	
		String selectTrace0 = DBApp.getFullTrace("rc5");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 193);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 190"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("c6", cols1);
		String [][] records_c6 = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			records_c6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c6", records_c6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(39.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("c6");
	
		int pageSelectPointer11 = genRandNum(pageCount1-1);
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1-1);
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("c6");
	
		DBApp.select("c6");
	
		DBApp.select("c6");
	
		String[] ConditionColumns16 = {"l"};
		String[] ConditionColumnsValues16 = {"l6"};
		DBApp.select("c6", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("c6");
	
		int pageSelectPointer18 = genRandNum(pageCount1-1);
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1-1);
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("c6");
	
		DBApp.select("c6");
	
		int pageSelectPointer112 = genRandNum(pageCount1-1);
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1-1);
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((39%DBApp.dataPageSize)-1);
		DBApp.select("c6", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("c6");
	
		String selectTrace1 = DBApp.getFullTrace("c6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 56);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 39"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("w0y", cols2);
		String [][] records_w0y = new String[382][cols2.length];
		for(int i=0;i<382;i++)
		{
			records_w0y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w0y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w0y", records_w0y[i]);
		}
	
		int pageCount2 = (int)Math.ceil(382.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2-1);
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"f"};
		String[] ConditionColumnsValues21 = {"f4"};
		DBApp.select("w0y", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2-1);
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"d"};
		String[] ConditionColumnsValues23 = {"d3"};
		DBApp.select("w0y", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2-1);
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("w0y");
	
		DBApp.select("w0y");
	
		DBApp.select("w0y");
	
		int pageSelectPointer28 = genRandNum(pageCount2-1);
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"b","e","d","g"};
		String[] ConditionColumnsValues29 = {"b1","e0","d3","g6"};
		DBApp.select("w0y", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2-1);
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2-1);
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2-1);
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("w0y");
	
		DBApp.select("w0y");
	
		DBApp.select("w0y");
	
		DBApp.select("w0y");
	
		int pageSelectPointer217 = genRandNum(pageCount2-1);
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((382%DBApp.dataPageSize)-1);
		DBApp.select("w0y", pageSelectPointer217,recordSelectPointer217);
	
		String selectTrace2 = DBApp.getFullTrace("w0y");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 402);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 382"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("n9", cols3);
		String [][] records_n9 = new String[117][cols3.length];
		for(int i=0;i<117;i++)
		{
			records_n9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n9", records_n9[i]);
		}
	
		int pageCount3 = (int)Math.ceil(117.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns30 = {"c"};
		String[] ConditionColumnsValues30 = {"c2"};
		DBApp.select("n9", ConditionColumns30, ConditionColumnsValues30);
	
		String[] ConditionColumns31 = {"c"};
		String[] ConditionColumnsValues31 = {"c1"};
		DBApp.select("n9", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("n9");
	
		int pageSelectPointer33 = genRandNum(pageCount3-1);
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((117%DBApp.dataPageSize)-1);
		DBApp.select("n9", pageSelectPointer33,recordSelectPointer33);
	
		DBApp.select("n9");
	
		String[] ConditionColumns35 = {"c"};
		String[] ConditionColumnsValues35 = {"c1"};
		DBApp.select("n9", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("n9");
	
		String[] ConditionColumns37 = {"c"};
		String[] ConditionColumnsValues37 = {"c2"};
		DBApp.select("n9", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3-1);
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize-1);
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((117%DBApp.dataPageSize)-1);
		DBApp.select("n9", pageSelectPointer38,recordSelectPointer38);
	
		String selectTrace3 = DBApp.getFullTrace("n9");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 128);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 117"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		return (int) (Math.random() * max) + 1;
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