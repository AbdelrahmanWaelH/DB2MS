package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests15
{

	@Test(timeout = 1000000)
	public void test71TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b7", cols0);
		String [][] records_b7 = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_b7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b7", records_b7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(140.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		String[] ConditionColumns00 = {"l"};
		String[] ConditionColumnsValues00 = {"l1"};
		DBApp.select("b7", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"c"};
		String[] ConditionColumnsValues02 = {"c1"};
		DBApp.select("b7", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("b7");
	
		DBApp.select("b7");
	
		String[] ConditionColumns05 = {"l"};
		String[] ConditionColumnsValues05 = {"l4"};
		DBApp.select("b7", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"k","l","i","h","b"};
		String[] ConditionColumnsValues06 = {"k3","l10","i4","h2","b0"};
		DBApp.select("b7", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("b7");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"i"};
		String[] ConditionColumnsValues013 = {"i7"};
		DBApp.select("b7", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("b7");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("b7", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("b7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 159);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 140"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m2o", cols1);
		String [][] records_m2o = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_m2o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m2o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m2o", records_m2o[i]);
		}
	
		int pageCount1 = (int)Math.ceil(5.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("m2o");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("m2o");
	
		String[] ConditionColumns14 = {"i"};
		String[] ConditionColumnsValues14 = {"i1"};
		DBApp.select("m2o", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"g"};
		String[] ConditionColumnsValues15 = {"g3"};
		DBApp.select("m2o", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"d"};
		String[] ConditionColumnsValues16 = {"d3"};
		DBApp.select("m2o", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("m2o");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"g"};
		String[] ConditionColumnsValues111 = {"g4"};
		DBApp.select("m2o", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"e"};
		String[] ConditionColumnsValues113 = {"e4"};
		DBApp.select("m2o", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer115,recordSelectPointer115);
	
		String[] ConditionColumns116 = {"b","h","c"};
		String[] ConditionColumnsValues116 = {"b0","h2","c2"};
		DBApp.select("m2o", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("m2o");
	
		DBApp.select("m2o");
	
		DBApp.select("m2o");
	
		DBApp.select("m2o");
	
		String[] ConditionColumns121 = {"e"};
		String[] ConditionColumnsValues121 = {"e3"};
		DBApp.select("m2o", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("m2o", pageSelectPointer122,recordSelectPointer122);
	
		String selectTrace1 = DBApp.getFullTrace("m2o");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 30);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 5"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qy0", cols0);
		String [][] records_qy0 = new String[100][cols0.length];
		for(int i=0;i<100;i++)
		{
			records_qy0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qy0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qy0", records_qy0[i]);
		}
	
		int pageCount0 = (int)Math.ceil(100.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("qy0");
	
		String[] ConditionColumns04 = {"i"};
		String[] ConditionColumnsValues04 = {"i2"};
		DBApp.select("qy0", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"i","a","f","m","j","n"};
		String[] ConditionColumnsValues05 = {"i7","a16","f4","m3","j6","n2"};
		DBApp.select("qy0", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"n","a","h","f","j"};
		String[] ConditionColumnsValues06 = {"n0","a84","h4","f0","j4"};
		DBApp.select("qy0", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("qy0");
	
		DBApp.select("qy0");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"i"};
		String[] ConditionColumnsValues016 = {"i7"};
		DBApp.select("qy0", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"h"};
		String[] ConditionColumnsValues017 = {"h1"};
		DBApp.select("qy0", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"c"};
		String[] ConditionColumnsValues018 = {"c1"};
		DBApp.select("qy0", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"j"};
		String[] ConditionColumnsValues019 = {"j9"};
		DBApp.select("qy0", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer020,recordSelectPointer020);
	
		String[] ConditionColumns021 = {"j"};
		String[] ConditionColumnsValues021 = {"j8"};
		DBApp.select("qy0", ConditionColumns021, ConditionColumnsValues021);
	
		String[] ConditionColumns022 = {"c"};
		String[] ConditionColumnsValues022 = {"c0"};
		DBApp.select("qy0", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("qy0");
	
		String[] ConditionColumns025 = {"j","h","l","g","d"};
		String[] ConditionColumnsValues025 = {"j4","h6","l2","g0","d2"};
		DBApp.select("qy0", ConditionColumns025, ConditionColumnsValues025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((100%DBApp.dataPageSize))-1;
		DBApp.select("qy0", pageSelectPointer027,recordSelectPointer027);
	
		String selectTrace0 = DBApp.getFullTrace("qy0");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 130);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 100"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f24", cols1);
		String [][] records_f24 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			records_f24[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f24[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f24", records_f24[i]);
		}
	
		int pageCount1 = (int)Math.ceil(92.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("f24");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"g","k","h","d"};
		String[] ConditionColumnsValues12 = {"g1","k3","h4","d0"};
		DBApp.select("f24", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"e","h"};
		String[] ConditionColumnsValues14 = {"e0","h6"};
		DBApp.select("f24", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("f24");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"e"};
		String[] ConditionColumnsValues17 = {"e2"};
		DBApp.select("f24", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"d"};
		String[] ConditionColumnsValues18 = {"d0"};
		DBApp.select("f24", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"k"};
		String[] ConditionColumnsValues110 = {"k9"};
		DBApp.select("f24", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("f24");
	
		String[] ConditionColumns112 = {"b"};
		String[] ConditionColumnsValues112 = {"b0"};
		DBApp.select("f24", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((92%DBApp.dataPageSize))-1;
		DBApp.select("f24", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"c"};
		String[] ConditionColumnsValues117 = {"c2"};
		DBApp.select("f24", ConditionColumns117, ConditionColumnsValues117);
	
		DBApp.select("f24");
	
		String selectTrace1 = DBApp.getFullTrace("f24");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 113);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 92"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ea", cols2);
		String [][] records_ea = new String[255][cols2.length];
		for(int i=0;i<255;i++)
		{
			records_ea[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ea[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ea", records_ea[i]);
		}
	
		int pageCount2 = (int)Math.ceil(255.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns20 = {"g","d"};
		String[] ConditionColumnsValues20 = {"g3","d0"};
		DBApp.select("ea", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("ea");
	
		String[] ConditionColumns23 = {"g","b","d"};
		String[] ConditionColumnsValues23 = {"g4","b1","d1"};
		DBApp.select("ea", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"c","b","f"};
		String[] ConditionColumnsValues24 = {"c1","b1","f1"};
		DBApp.select("ea", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("ea");
	
		String[] ConditionColumns26 = {"g"};
		String[] ConditionColumnsValues26 = {"g0"};
		DBApp.select("ea", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("ea");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"g"};
		String[] ConditionColumnsValues29 = {"g5"};
		DBApp.select("ea", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"g"};
		String[] ConditionColumnsValues210 = {"g1"};
		DBApp.select("ea", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("ea");
	
		String[] ConditionColumns212 = {"b"};
		String[] ConditionColumnsValues212 = {"b1"};
		DBApp.select("ea", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("ea");
	
		String[] ConditionColumns214 = {"e"};
		String[] ConditionColumnsValues214 = {"e3"};
		DBApp.select("ea", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"f"};
		String[] ConditionColumnsValues215 = {"f3"};
		DBApp.select("ea", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"f"};
		String[] ConditionColumnsValues216 = {"f3"};
		DBApp.select("ea", ConditionColumns216, ConditionColumnsValues216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer217,recordSelectPointer217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer218,recordSelectPointer218);
	
		String[] ConditionColumns219 = {"e","g"};
		String[] ConditionColumnsValues219 = {"e0","g4"};
		DBApp.select("ea", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("ea");
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer221,recordSelectPointer221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((255%DBApp.dataPageSize))-1;
		DBApp.select("ea", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("ea");
	
		String[] ConditionColumns224 = {"e"};
		String[] ConditionColumnsValues224 = {"e3"};
		DBApp.select("ea", ConditionColumns224, ConditionColumnsValues224);
	
		String selectTrace2 = DBApp.getFullTrace("ea");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 282);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 255"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dxr1c", cols0);
		String [][] records_dxr1c = new String[292][cols0.length];
		for(int i=0;i<292;i++)
		{
			records_dxr1c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dxr1c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dxr1c", records_dxr1c[i]);
		}
	
		int pageCount0 = (int)Math.ceil(292.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns00 = {"g"};
		String[] ConditionColumnsValues00 = {"g4"};
		DBApp.select("dxr1c", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"i","h","f","d"};
		String[] ConditionColumnsValues01 = {"i8","h2","f2","d2"};
		DBApp.select("dxr1c", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"c","h","j","b","d"};
		String[] ConditionColumnsValues03 = {"c1","h5","j1","b1","d1"};
		DBApp.select("dxr1c", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("dxr1c");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("dxr1c");
	
		DBApp.select("dxr1c");
	
		String[] ConditionColumns08 = {"g"};
		String[] ConditionColumnsValues08 = {"g0"};
		DBApp.select("dxr1c", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("dxr1c");
	
		String[] ConditionColumns010 = {"k"};
		String[] ConditionColumnsValues010 = {"k3"};
		DBApp.select("dxr1c", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"j","i","g","k"};
		String[] ConditionColumnsValues011 = {"j8","i4","g0","k7"};
		DBApp.select("dxr1c", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"k"};
		String[] ConditionColumnsValues012 = {"k8"};
		DBApp.select("dxr1c", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("dxr1c");
	
		String[] ConditionColumns015 = {"d","a","g"};
		String[] ConditionColumnsValues015 = {"d1","a53","g4"};
		DBApp.select("dxr1c", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"a"};
		String[] ConditionColumnsValues016 = {"a278"};
		DBApp.select("dxr1c", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("dxr1c");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((292%DBApp.dataPageSize))-1;
		DBApp.select("dxr1c", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("dxr1c");
	
		String[] ConditionColumns022 = {"k"};
		String[] ConditionColumnsValues022 = {"k2"};
		DBApp.select("dxr1c", ConditionColumns022, ConditionColumnsValues022);
	
		String[] ConditionColumns023 = {"a"};
		String[] ConditionColumnsValues023 = {"a173"};
		DBApp.select("dxr1c", ConditionColumns023, ConditionColumnsValues023);
	
		DBApp.select("dxr1c");
	
		String[] ConditionColumns025 = {"i","l","d"};
		String[] ConditionColumnsValues025 = {"i3","l3","d3"};
		DBApp.select("dxr1c", ConditionColumns025, ConditionColumnsValues025);
	
		DBApp.select("dxr1c");
	
		DBApp.select("dxr1c");
	
		String selectTrace0 = DBApp.getFullTrace("dxr1c");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 322);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 292"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("s8", cols1);
		String [][] records_s8 = new String[404][cols1.length];
		for(int i=0;i<404;i++)
		{
			records_s8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s8", records_s8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(404.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((404%DBApp.dataPageSize))-1;
		DBApp.select("s8", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("s8");
	
		DBApp.select("s8");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b1"};
		DBApp.select("s8", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("s8");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((404%DBApp.dataPageSize))-1;
		DBApp.select("s8", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"f"};
		String[] ConditionColumnsValues16 = {"f4"};
		DBApp.select("s8", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("s8");
	
		String[] ConditionColumns18 = {"f"};
		String[] ConditionColumnsValues18 = {"f0"};
		DBApp.select("s8", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("s8");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((404%DBApp.dataPageSize))-1;
		DBApp.select("s8", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b0"};
		DBApp.select("s8", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((404%DBApp.dataPageSize))-1;
		DBApp.select("s8", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((404%DBApp.dataPageSize))-1;
		DBApp.select("s8", pageSelectPointer113,recordSelectPointer113);
	
		String selectTrace1 = DBApp.getFullTrace("s8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 420);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 404"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("b9o", cols2);
		String [][] records_b9o = new String[395][cols2.length];
		for(int i=0;i<395;i++)
		{
			records_b9o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b9o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b9o", records_b9o[i]);
		}
	
		int pageCount2 = (int)Math.ceil(395.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns20 = {"b"};
		String[] ConditionColumnsValues20 = {"b1"};
		DBApp.select("b9o", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"c"};
		String[] ConditionColumnsValues21 = {"c0"};
		DBApp.select("b9o", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("b9o");
	
		DBApp.select("b9o");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"b"};
		String[] ConditionColumnsValues25 = {"b0"};
		DBApp.select("b9o", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("b9o");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"c"};
		String[] ConditionColumnsValues210 = {"c2"};
		DBApp.select("b9o", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((395%DBApp.dataPageSize))-1;
		DBApp.select("b9o", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("b9o");
	
		DBApp.select("b9o");
	
		DBApp.select("b9o");
	
		DBApp.select("b9o");
	
		DBApp.select("b9o");
	
		String[] ConditionColumns219 = {"b"};
		String[] ConditionColumnsValues219 = {"b0"};
		DBApp.select("b9o", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("b9o");
	
		String selectTrace2 = DBApp.getFullTrace("b9o");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 418);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 395"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("tp", cols0);
		String [][] records_tp = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_tp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tp", records_tp[i]);
		}
	
		int pageCount0 = (int)Math.ceil(22.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a15"};
		DBApp.select("tp", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("tp", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a16"};
		DBApp.select("tp", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("tp", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("tp");
	
		DBApp.select("tp");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("tp", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("tp", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a1"};
		DBApp.select("tp", ConditionColumns08, ConditionColumnsValues08);
	
		String selectTrace0 = DBApp.getFullTrace("tp");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 33);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 22"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b0s", cols1);
		String [][] records_b0s = new String[424][cols1.length];
		for(int i=0;i<424;i++)
		{
			records_b0s[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b0s[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b0s", records_b0s[i]);
		}
	
		int pageCount1 = (int)Math.ceil(424.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		String[] ConditionColumns10 = {"i"};
		String[] ConditionColumnsValues10 = {"i0"};
		DBApp.select("b0s", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("b0s");
	
		String[] ConditionColumns12 = {"n","g","i","k","j","l"};
		String[] ConditionColumnsValues12 = {"n2","g2","i3","k2","j6","l0"};
		DBApp.select("b0s", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("b0s");
	
		String[] ConditionColumns14 = {"n","m","h"};
		String[] ConditionColumnsValues14 = {"n11","m2","h3"};
		DBApp.select("b0s", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"e","n"};
		String[] ConditionColumnsValues15 = {"e4","n3"};
		DBApp.select("b0s", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("b0s", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"e","g","f","l"};
		String[] ConditionColumnsValues17 = {"e3","g5","f2","l2"};
		DBApp.select("b0s", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("b0s");
	
		DBApp.select("b0s");
	
		DBApp.select("b0s");
	
		String[] ConditionColumns111 = {"e"};
		String[] ConditionColumnsValues111 = {"e3"};
		DBApp.select("b0s", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("b0s");
	
		String[] ConditionColumns113 = {"i","d","b","a","f","m","h"};
		String[] ConditionColumnsValues113 = {"i1","d0","b0","a316","f4","m4","h4"};
		DBApp.select("b0s", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("b0s");
	
		DBApp.select("b0s");
	
		String selectTrace1 = DBApp.getFullTrace("b0s");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 442);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 424"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("q7", cols2);
		String [][] records_q7 = new String[175][cols2.length];
		for(int i=0;i<175;i++)
		{
			records_q7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q7", records_q7[i]);
		}
	
		int pageCount2 = (int)Math.ceil(175.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("q7");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((175%DBApp.dataPageSize))-1;
		DBApp.select("q7", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("q7");
	
		String[] ConditionColumns23 = {"d"};
		String[] ConditionColumnsValues23 = {"d3"};
		DBApp.select("q7", ConditionColumns23, ConditionColumnsValues23);
	
		DBApp.select("q7");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((175%DBApp.dataPageSize))-1;
		DBApp.select("q7", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("q7");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((175%DBApp.dataPageSize))-1;
		DBApp.select("q7", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("q7");
	
		String[] ConditionColumns29 = {"a","d","c"};
		String[] ConditionColumnsValues29 = {"a156","d0","c0"};
		DBApp.select("q7", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"c","b"};
		String[] ConditionColumnsValues210 = {"c0","b1"};
		DBApp.select("q7", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("q7");
	
		DBApp.select("q7");
	
		DBApp.select("q7");
	
		DBApp.select("q7");
	
		DBApp.select("q7");
	
		DBApp.select("q7");
	
		String[] ConditionColumns217 = {"f","b","c"};
		String[] ConditionColumnsValues217 = {"f2","b0","c2"};
		DBApp.select("q7", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"d"};
		String[] ConditionColumnsValues218 = {"d3"};
		DBApp.select("q7", ConditionColumns218, ConditionColumnsValues218);
	
		String selectTrace2 = DBApp.getFullTrace("q7");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 196);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 175"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("v0qt", cols3);
		String [][] records_v0qt = new String[313][cols3.length];
		for(int i=0;i<313;i++)
		{
			records_v0qt[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v0qt[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v0qt", records_v0qt[i]);
		}
	
		int pageCount3 = (int)Math.ceil(313.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		DBApp.select("v0qt");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"e","a","d"};
		String[] ConditionColumnsValues32 = {"e0","a180","d0"};
		DBApp.select("v0qt", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b0"};
		DBApp.select("v0qt", ConditionColumns33, ConditionColumnsValues33);
	
		DBApp.select("v0qt");
	
		DBApp.select("v0qt");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer36,recordSelectPointer36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer38,recordSelectPointer38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer39,recordSelectPointer39);
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer310,recordSelectPointer310);
	
		String[] ConditionColumns311 = {"a"};
		String[] ConditionColumnsValues311 = {"a4"};
		DBApp.select("v0qt", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"f"};
		String[] ConditionColumnsValues312 = {"f5"};
		DBApp.select("v0qt", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer313,recordSelectPointer313);
	
		String[] ConditionColumns314 = {"b","e"};
		String[] ConditionColumnsValues314 = {"b1","e1"};
		DBApp.select("v0qt", ConditionColumns314, ConditionColumnsValues314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer315,recordSelectPointer315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer316,recordSelectPointer316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("v0qt", pageSelectPointer317,recordSelectPointer317);
	
		DBApp.select("v0qt");
	
		DBApp.select("v0qt");
	
		DBApp.select("v0qt");
	
		String[] ConditionColumns321 = {"d"};
		String[] ConditionColumnsValues321 = {"d0"};
		DBApp.select("v0qt", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"b"};
		String[] ConditionColumnsValues322 = {"b0"};
		DBApp.select("v0qt", ConditionColumns322, ConditionColumnsValues322);
	
		DBApp.select("v0qt");
	
		String selectTrace3 = DBApp.getFullTrace("v0qt");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 339);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 313"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("phpoa", cols4);
		String [][] records_phpoa = new String[247][cols4.length];
		for(int i=0;i<247;i++)
		{
			records_phpoa[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_phpoa[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("phpoa", records_phpoa[i]);
		}
	
		int pageCount4 = (int)Math.ceil(247.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns40 = {"e","c"};
		String[] ConditionColumnsValues40 = {"e2","c0"};
		DBApp.select("phpoa", ConditionColumns40, ConditionColumnsValues40);
	
		DBApp.select("phpoa");
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer42,recordSelectPointer42);
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer43,recordSelectPointer43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer44,recordSelectPointer44);
	
		DBApp.select("phpoa");
	
		DBApp.select("phpoa");
	
		DBApp.select("phpoa");
	
		String[] ConditionColumns48 = {"e"};
		String[] ConditionColumnsValues48 = {"e0"};
		DBApp.select("phpoa", ConditionColumns48, ConditionColumnsValues48);
	
		DBApp.select("phpoa");
	
		DBApp.select("phpoa");
	
		DBApp.select("phpoa");
	
		DBApp.select("phpoa");
	
		String[] ConditionColumns413 = {"c"};
		String[] ConditionColumnsValues413 = {"c1"};
		DBApp.select("phpoa", ConditionColumns413, ConditionColumnsValues413);
	
		String[] ConditionColumns414 = {"c"};
		String[] ConditionColumnsValues414 = {"c1"};
		DBApp.select("phpoa", ConditionColumns414, ConditionColumnsValues414);
	
		String[] ConditionColumns415 = {"b","e"};
		String[] ConditionColumnsValues415 = {"b0","e2"};
		DBApp.select("phpoa", ConditionColumns415, ConditionColumnsValues415);
	
		DBApp.select("phpoa");
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer417,recordSelectPointer417);
	
		String[] ConditionColumns418 = {"b"};
		String[] ConditionColumnsValues418 = {"b1"};
		DBApp.select("phpoa", ConditionColumns418, ConditionColumnsValues418);
	
		String[] ConditionColumns419 = {"e"};
		String[] ConditionColumnsValues419 = {"e2"};
		DBApp.select("phpoa", ConditionColumns419, ConditionColumnsValues419);
	
		String[] ConditionColumns420 = {"d"};
		String[] ConditionColumnsValues420 = {"d3"};
		DBApp.select("phpoa", ConditionColumns420, ConditionColumnsValues420);
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer421,recordSelectPointer421);
	
		DBApp.select("phpoa");
	
		String[] ConditionColumns423 = {"b"};
		String[] ConditionColumnsValues423 = {"b0"};
		DBApp.select("phpoa", ConditionColumns423, ConditionColumnsValues423);
	
		int pageSelectPointer424 = genRandNum(pageCount4)-1;
		int recordSelectPointer424 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer424==pageCount4-1)
			recordSelectPointer424 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer424,recordSelectPointer424);
	
		String[] ConditionColumns425 = {"b"};
		String[] ConditionColumnsValues425 = {"b0"};
		DBApp.select("phpoa", ConditionColumns425, ConditionColumnsValues425);
	
		DBApp.select("phpoa");
	
		int pageSelectPointer427 = genRandNum(pageCount4)-1;
		int recordSelectPointer427 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer427==pageCount4-1)
			recordSelectPointer427 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("phpoa", pageSelectPointer427,recordSelectPointer427);
	
		String selectTrace4 = DBApp.getFullTrace("phpoa");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 277);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 247"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("sxl", cols0);
		String [][] records_sxl = new String[252][cols0.length];
		for(int i=0;i<252;i++)
		{
			records_sxl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sxl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sxl", records_sxl[i]);
		}
	
		int pageCount0 = (int)Math.ceil(252.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("sxl");
	
		String[] ConditionColumns01 = {"a","c"};
		String[] ConditionColumnsValues01 = {"a27","c0"};
		DBApp.select("sxl", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("sxl");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((252%DBApp.dataPageSize))-1;
		DBApp.select("sxl", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("sxl");
	
		String[] ConditionColumns05 = {"d"};
		String[] ConditionColumnsValues05 = {"d2"};
		DBApp.select("sxl", ConditionColumns05, ConditionColumnsValues05);
	
		String selectTrace0 = DBApp.getFullTrace("sxl");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 260);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 252"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("bxiw", cols1);
		String [][] records_bxiw = new String[267][cols1.length];
		for(int i=0;i<267;i++)
		{
			records_bxiw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bxiw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bxiw", records_bxiw[i]);
		}
	
		int pageCount1 = (int)Math.ceil(267.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((267%DBApp.dataPageSize))-1;
		DBApp.select("bxiw", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b1"};
		DBApp.select("bxiw", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("bxiw", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((267%DBApp.dataPageSize))-1;
		DBApp.select("bxiw", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b1"};
		DBApp.select("bxiw", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((267%DBApp.dataPageSize))-1;
		DBApp.select("bxiw", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		String[] ConditionColumns113 = {"b"};
		String[] ConditionColumnsValues113 = {"b1"};
		DBApp.select("bxiw", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("bxiw");
	
		String[] ConditionColumns115 = {"a"};
		String[] ConditionColumnsValues115 = {"a152"};
		DBApp.select("bxiw", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("bxiw");
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b0"};
		DBApp.select("bxiw", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"a"};
		String[] ConditionColumnsValues118 = {"a155"};
		DBApp.select("bxiw", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("bxiw");
	
		String[] ConditionColumns120 = {"a"};
		String[] ConditionColumnsValues120 = {"a217"};
		DBApp.select("bxiw", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("bxiw", ConditionColumns121, ConditionColumnsValues121);
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		DBApp.select("bxiw");
	
		String selectTrace1 = DBApp.getFullTrace("bxiw");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 294);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 267"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jfo20", cols2);
		String [][] records_jfo20 = new String[474][cols2.length];
		for(int i=0;i<474;i++)
		{
			records_jfo20[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jfo20[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jfo20", records_jfo20[i]);
		}
	
		int pageCount2 = (int)Math.ceil(474.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b1"};
		DBApp.select("jfo20", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"d"};
		String[] ConditionColumnsValues23 = {"d1"};
		DBApp.select("jfo20", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("jfo20");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("jfo20");
	
		DBApp.select("jfo20");
	
		String[] ConditionColumns216 = {"e"};
		String[] ConditionColumnsValues216 = {"e0"};
		DBApp.select("jfo20", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"k","b","g","j","i"};
		String[] ConditionColumnsValues217 = {"k9","b0","g2","j0","i6"};
		DBApp.select("jfo20", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer218,recordSelectPointer218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer219,recordSelectPointer219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((474%DBApp.dataPageSize))-1;
		DBApp.select("jfo20", pageSelectPointer220,recordSelectPointer220);
	
		String selectTrace2 = DBApp.getFullTrace("jfo20");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 497);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 474"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("or", cols0);
		String [][] records_or = new String[407][cols0.length];
		for(int i=0;i<407;i++)
		{
			records_or[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_or[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("or", records_or[i]);
		}
	
		int pageCount0 = (int)Math.ceil(407.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((407%DBApp.dataPageSize))-1;
		DBApp.select("or", pageSelectPointer00,recordSelectPointer00);
	
		String selectTrace0 = DBApp.getFullTrace("or");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 410);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 407"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("nmmd", cols1);
		String [][] records_nmmd = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_nmmd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nmmd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nmmd", records_nmmd[i]);
		}
	
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("nmmd");
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b1"};
		DBApp.select("nmmd", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("nmmd");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c0"};
		DBApp.select("nmmd", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b1"};
		DBApp.select("nmmd", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("nmmd");
	
		DBApp.select("nmmd");
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c1"};
		DBApp.select("nmmd", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b1"};
		DBApp.select("nmmd", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("nmmd");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b1"};
		DBApp.select("nmmd", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"a"};
		String[] ConditionColumnsValues118 = {"a8"};
		DBApp.select("nmmd", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("nmmd");
	
		String[] ConditionColumns120 = {"b"};
		String[] ConditionColumnsValues120 = {"b1"};
		DBApp.select("nmmd", ConditionColumns120, ConditionColumnsValues120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer121,recordSelectPointer121);
	
		DBApp.select("nmmd");
	
		DBApp.select("nmmd");
	
		String[] ConditionColumns124 = {"a"};
		String[] ConditionColumnsValues124 = {"a4"};
		DBApp.select("nmmd", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("nmmd");
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer126,recordSelectPointer126);
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("nmmd", pageSelectPointer127,recordSelectPointer127);
	
		String selectTrace1 = DBApp.getFullTrace("nmmd");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 39);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 9"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("tle", cols0);
		String [][] records_tle = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_tle[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tle[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tle", records_tle[i]);
		}
	
		int pageCount0 = (int)Math.ceil(46.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("tle", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("tle");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("tle", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("tle", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("tle", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c2"};
		DBApp.select("tle", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("tle", pageSelectPointer06,recordSelectPointer06);
	
		String selectTrace0 = DBApp.getFullTrace("tle");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 55);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 46"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("n8i82", cols1);
		String [][] records_n8i82 = new String[203][cols1.length];
		for(int i=0;i<203;i++)
		{
			records_n8i82[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n8i82[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n8i82", records_n8i82[i]);
		}
	
		int pageCount1 = (int)Math.ceil(203.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c2"};
		DBApp.select("n8i82", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b0"};
		DBApp.select("n8i82", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("n8i82");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"c"};
		String[] ConditionColumnsValues19 = {"c2"};
		DBApp.select("n8i82", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("n8i82");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("n8i82");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("n8i82");
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d0"};
		DBApp.select("n8i82", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b1"};
		DBApp.select("n8i82", ConditionColumns117, ConditionColumnsValues117);
	
		DBApp.select("n8i82");
	
		DBApp.select("n8i82");
	
		String[] ConditionColumns120 = {"d"};
		String[] ConditionColumnsValues120 = {"d3"};
		DBApp.select("n8i82", ConditionColumns120, ConditionColumnsValues120);
	
		DBApp.select("n8i82");
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer122,recordSelectPointer122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((203%DBApp.dataPageSize))-1;
		DBApp.select("n8i82", pageSelectPointer123,recordSelectPointer123);
	
		DBApp.select("n8i82");
	
		String selectTrace1 = DBApp.getFullTrace("n8i82");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 230);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 203"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x19r", cols2);
		String [][] records_x19r = new String[287][cols2.length];
		for(int i=0;i<287;i++)
		{
			records_x19r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x19r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x19r", records_x19r[i]);
		}
	
		int pageCount2 = (int)Math.ceil(287.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		String[] ConditionColumns20 = {"i","h","b","l"};
		String[] ConditionColumnsValues20 = {"i3","h5","b1","l9"};
		DBApp.select("x19r", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer214,recordSelectPointer214);
	
		DBApp.select("x19r");
	
		String[] ConditionColumns216 = {"b"};
		String[] ConditionColumnsValues216 = {"b1"};
		DBApp.select("x19r", ConditionColumns216, ConditionColumnsValues216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer217,recordSelectPointer217);
	
		String[] ConditionColumns218 = {"c"};
		String[] ConditionColumnsValues218 = {"c2"};
		DBApp.select("x19r", ConditionColumns218, ConditionColumnsValues218);
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		DBApp.select("x19r");
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("x19r", pageSelectPointer223,recordSelectPointer223);
	
		String selectTrace2 = DBApp.getFullTrace("x19r");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 313);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 287"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ehc", cols3);
		String [][] records_ehc = new String[171][cols3.length];
		for(int i=0;i<171;i++)
		{
			records_ehc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ehc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ehc", records_ehc[i]);
		}
	
		int pageCount3 = (int)Math.ceil(171.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("ehc");
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer33,recordSelectPointer33);
	
		String[] ConditionColumns34 = {"m"};
		String[] ConditionColumnsValues34 = {"m12"};
		DBApp.select("ehc", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer35,recordSelectPointer35);
	
		DBApp.select("ehc");
	
		DBApp.select("ehc");
	
		DBApp.select("ehc");
	
		DBApp.select("ehc");
	
		String[] ConditionColumns310 = {"l"};
		String[] ConditionColumnsValues310 = {"l3"};
		DBApp.select("ehc", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"f"};
		String[] ConditionColumnsValues311 = {"f3"};
		DBApp.select("ehc", ConditionColumns311, ConditionColumnsValues311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer312,recordSelectPointer312);
	
		DBApp.select("ehc");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("ehc");
	
		DBApp.select("ehc");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"i"};
		String[] ConditionColumnsValues318 = {"i6"};
		DBApp.select("ehc", ConditionColumns318, ConditionColumnsValues318);
	
		DBApp.select("ehc");
	
		String[] ConditionColumns320 = {"a","f","n","e","o"};
		String[] ConditionColumnsValues320 = {"a139","f1","n13","e4","o4"};
		DBApp.select("ehc", ConditionColumns320, ConditionColumnsValues320);
	
		int pageSelectPointer321 = genRandNum(pageCount3)-1;
		int recordSelectPointer321 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer321==pageCount3-1)
			recordSelectPointer321 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer321,recordSelectPointer321);
	
		DBApp.select("ehc");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer323,recordSelectPointer323);
	
		String[] ConditionColumns324 = {"b","d","f"};
		String[] ConditionColumnsValues324 = {"b1","d1","f1"};
		DBApp.select("ehc", ConditionColumns324, ConditionColumnsValues324);
	
		DBApp.select("ehc");
	
		int pageSelectPointer326 = genRandNum(pageCount3)-1;
		int recordSelectPointer326 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer326==pageCount3-1)
			recordSelectPointer326 = genRandNum((171%DBApp.dataPageSize))-1;
		DBApp.select("ehc", pageSelectPointer326,recordSelectPointer326);
	
		DBApp.select("ehc");
	
		DBApp.select("ehc");
	
		String selectTrace3 = DBApp.getFullTrace("ehc");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 202);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 171"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("tv", cols0);
		String [][] records_tv = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_tv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tv", records_tv[i]);
		}
	
		int pageCount0 = (int)Math.ceil(140.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("tv");
	
		DBApp.select("tv");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("tv", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"d","c"};
		String[] ConditionColumnsValues03 = {"d2","c0"};
		DBApp.select("tv", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("tv", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("tv", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("tv", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"d","c"};
		String[] ConditionColumnsValues07 = {"d2","c1"};
		DBApp.select("tv", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("tv");
	
		String selectTrace0 = DBApp.getFullTrace("tv");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 151);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 140"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("v47", cols1);
		String [][] records_v47 = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			records_v47[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v47[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v47", records_v47[i]);
		}
	
		int pageCount1 = (int)Math.ceil(83.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("v47");
	
		DBApp.select("v47");
	
		DBApp.select("v47");
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a46"};
		DBApp.select("v47", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"a"};
		String[] ConditionColumnsValues14 = {"a20"};
		DBApp.select("v47", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("v47");
	
		DBApp.select("v47");
	
		String[] ConditionColumns17 = {"c"};
		String[] ConditionColumnsValues17 = {"c1"};
		DBApp.select("v47", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((83%DBApp.dataPageSize))-1;
		DBApp.select("v47", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((83%DBApp.dataPageSize))-1;
		DBApp.select("v47", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b1"};
		DBApp.select("v47", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("v47");
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a2"};
		DBApp.select("v47", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"a"};
		String[] ConditionColumnsValues113 = {"a8"};
		DBApp.select("v47", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((83%DBApp.dataPageSize))-1;
		DBApp.select("v47", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((83%DBApp.dataPageSize))-1;
		DBApp.select("v47", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((83%DBApp.dataPageSize))-1;
		DBApp.select("v47", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"c"};
		String[] ConditionColumnsValues117 = {"c1"};
		DBApp.select("v47", ConditionColumns117, ConditionColumnsValues117);
	
		String selectTrace1 = DBApp.getFullTrace("v47");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 103);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 83"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("jm5", cols2);
		String [][] records_jm5 = new String[496][cols2.length];
		for(int i=0;i<496;i++)
		{
			records_jm5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jm5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jm5", records_jm5[i]);
		}
	
		int pageCount2 = (int)Math.ceil(496.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("jm5");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"b"};
		String[] ConditionColumnsValues24 = {"b1"};
		DBApp.select("jm5", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a456"};
		DBApp.select("jm5", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"b"};
		String[] ConditionColumnsValues28 = {"b0"};
		DBApp.select("jm5", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("jm5");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("jm5");
	
		DBApp.select("jm5");
	
		String[] ConditionColumns213 = {"b"};
		String[] ConditionColumnsValues213 = {"b1"};
		DBApp.select("jm5", ConditionColumns213, ConditionColumnsValues213);
	
		String[] ConditionColumns214 = {"b"};
		String[] ConditionColumnsValues214 = {"b1"};
		DBApp.select("jm5", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a66"};
		DBApp.select("jm5", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer216,recordSelectPointer216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("jm5", pageSelectPointer217,recordSelectPointer217);
	
		String selectTrace2 = DBApp.getFullTrace("jm5");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 516);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 496"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("e7t2q", cols0);
		String [][] records_e7t2q = new String[272][cols0.length];
		for(int i=0;i<272;i++)
		{
			records_e7t2q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e7t2q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e7t2q", records_e7t2q[i]);
		}
	
		int pageCount0 = (int)Math.ceil(272.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("e7t2q");
	
		DBApp.select("e7t2q");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("e7t2q", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("e7t2q");
	
		DBApp.select("e7t2q");
	
		String[] ConditionColumns05 = {"d"};
		String[] ConditionColumnsValues05 = {"d1"};
		DBApp.select("e7t2q", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("e7t2q");
	
		DBApp.select("e7t2q");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("e7t2q", pageSelectPointer08,recordSelectPointer08);
	
		String selectTrace0 = DBApp.getFullTrace("e7t2q");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 283);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 272"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("bj3", cols1);
		String [][] records_bj3 = new String[208][cols1.length];
		for(int i=0;i<208;i++)
		{
			records_bj3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bj3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bj3", records_bj3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(208.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c0"};
		DBApp.select("bj3", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"c"};
		String[] ConditionColumnsValues11 = {"c2"};
		DBApp.select("bj3", ConditionColumns11, ConditionColumnsValues11);
	
		String selectTrace1 = DBApp.getFullTrace("bj3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 212);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 208"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("who8", cols0);
		String [][] records_who8 = new String[128][cols0.length];
		for(int i=0;i<128;i++)
		{
			records_who8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_who8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("who8", records_who8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(128.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("who8");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b0"};
		DBApp.select("who8", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("who8");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("who8");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((128%DBApp.dataPageSize))-1;
		DBApp.select("who8", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("who8");
	
		DBApp.select("who8");
	
		String[] ConditionColumns014 = {"b"};
		String[] ConditionColumnsValues014 = {"b1"};
		DBApp.select("who8", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("who8");
	
		DBApp.select("who8");
	
		String selectTrace0 = DBApp.getFullTrace("who8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 147);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 128"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("e3", cols0);
		String [][] records_e3 = new String[342][cols0.length];
		for(int i=0;i<342;i++)
		{
			records_e3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e3", records_e3[i]);
		}
	
		int pageCount0 = (int)Math.ceil(342.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"b"};
		String[] ConditionColumnsValues02 = {"b0"};
		DBApp.select("e3", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("e3");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"b"};
		String[] ConditionColumnsValues09 = {"b1"};
		DBApp.select("e3", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("e3");
	
		DBApp.select("e3");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("e3");
	
		String[] ConditionColumns014 = {"c"};
		String[] ConditionColumnsValues014 = {"c2"};
		DBApp.select("e3", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"b"};
		String[] ConditionColumnsValues015 = {"b0"};
		DBApp.select("e3", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("e3");
	
		String[] ConditionColumns018 = {"b"};
		String[] ConditionColumnsValues018 = {"b1"};
		DBApp.select("e3", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"c"};
		String[] ConditionColumnsValues019 = {"c0"};
		DBApp.select("e3", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("e3");
	
		DBApp.select("e3");
	
		DBApp.select("e3");
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((342%DBApp.dataPageSize))-1;
		DBApp.select("e3", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("e3");
	
		String[] ConditionColumns026 = {"c"};
		String[] ConditionColumnsValues026 = {"c1"};
		DBApp.select("e3", ConditionColumns026, ConditionColumnsValues026);
	
		DBApp.select("e3");
	
		String[] ConditionColumns028 = {"b"};
		String[] ConditionColumnsValues028 = {"b1"};
		DBApp.select("e3", ConditionColumns028, ConditionColumnsValues028);
	
		DBApp.select("e3");
	
		String selectTrace0 = DBApp.getFullTrace("e3");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 374);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 342"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vckfx", cols1);
		String [][] records_vckfx = new String[290][cols1.length];
		for(int i=0;i<290;i++)
		{
			records_vckfx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vckfx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vckfx", records_vckfx[i]);
		}
	
		int pageCount1 = (int)Math.ceil(290.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("vckfx");
	
		String[] ConditionColumns11 = {"f"};
		String[] ConditionColumnsValues11 = {"f1"};
		DBApp.select("vckfx", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((290%DBApp.dataPageSize))-1;
		DBApp.select("vckfx", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("vckfx");
	
		String[] ConditionColumns14 = {"h"};
		String[] ConditionColumnsValues14 = {"h4"};
		DBApp.select("vckfx", ConditionColumns14, ConditionColumnsValues14);
	
		String selectTrace1 = DBApp.getFullTrace("vckfx");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 297);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 290"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("v4s9f", cols2);
		String [][] records_v4s9f = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			records_v4s9f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v4s9f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v4s9f", records_v4s9f[i]);
		}
	
		int pageCount2 = (int)Math.ceil(58.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a52"};
		DBApp.select("v4s9f", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"a"};
		String[] ConditionColumnsValues21 = {"a31"};
		DBApp.select("v4s9f", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((58%DBApp.dataPageSize))-1;
		DBApp.select("v4s9f", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("v4s9f");
	
		String[] ConditionColumns24 = {"a"};
		String[] ConditionColumnsValues24 = {"a8"};
		DBApp.select("v4s9f", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("v4s9f");
	
		DBApp.select("v4s9f");
	
		DBApp.select("v4s9f");
	
		DBApp.select("v4s9f");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((58%DBApp.dataPageSize))-1;
		DBApp.select("v4s9f", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((58%DBApp.dataPageSize))-1;
		DBApp.select("v4s9f", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a56"};
		DBApp.select("v4s9f", ConditionColumns211, ConditionColumnsValues211);
	
		String selectTrace2 = DBApp.getFullTrace("v4s9f");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 72);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 58"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i741", cols0);
		String [][] records_i741 = new String[180][cols0.length];
		for(int i=0;i<180;i++)
		{
			records_i741[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i741[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i741", records_i741[i]);
		}
	
		int pageCount0 = (int)Math.ceil(180.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		DBApp.select("i741");
	
		DBApp.select("i741");
	
		DBApp.select("i741");
	
		String[] ConditionColumns03 = {"f","c"};
		String[] ConditionColumnsValues03 = {"f0","c0"};
		DBApp.select("i741", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d1"};
		DBApp.select("i741", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"b"};
		String[] ConditionColumnsValues05 = {"b0"};
		DBApp.select("i741", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"e","d","f"};
		String[] ConditionColumnsValues06 = {"e4","d2","f4"};
		DBApp.select("i741", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"b"};
		String[] ConditionColumnsValues08 = {"b0"};
		DBApp.select("i741", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"g"};
		String[] ConditionColumnsValues09 = {"g4"};
		DBApp.select("i741", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"g","f"};
		String[] ConditionColumnsValues010 = {"g4","f2"};
		DBApp.select("i741", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("i741");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("i741");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"d"};
		String[] ConditionColumnsValues019 = {"d0"};
		DBApp.select("i741", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("i741");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("i741", pageSelectPointer021,recordSelectPointer021);
	
		String[] ConditionColumns022 = {"f"};
		String[] ConditionColumnsValues022 = {"f4"};
		DBApp.select("i741", ConditionColumns022, ConditionColumnsValues022);
	
		DBApp.select("i741");
	
		String selectTrace0 = DBApp.getFullTrace("i741");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 206);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 180"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n6tq", cols0);
		String [][] records_n6tq = new String[359][cols0.length];
		for(int i=0;i<359;i++)
		{
			records_n6tq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n6tq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n6tq", records_n6tq[i]);
		}
	
		int pageCount0 = (int)Math.ceil(359.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("n6tq", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("n6tq", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("n6tq");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("n6tq", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"f"};
		String[] ConditionColumnsValues04 = {"f1"};
		DBApp.select("n6tq", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("n6tq");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 366);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 359"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("pona", cols0);
		String [][] records_pona = new String[330][cols0.length];
		for(int i=0;i<330;i++)
		{
			records_pona[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pona[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pona", records_pona[i]);
		}
	
		int pageCount0 = (int)Math.ceil(330.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("pona");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a186"};
		DBApp.select("pona", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a286"};
		DBApp.select("pona", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a122"};
		DBApp.select("pona", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("pona");
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a250"};
		DBApp.select("pona", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a161"};
		DBApp.select("pona", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a84"};
		DBApp.select("pona", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("pona");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"a"};
		String[] ConditionColumnsValues013 = {"a154"};
		DBApp.select("pona", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a107"};
		DBApp.select("pona", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"a"};
		String[] ConditionColumnsValues017 = {"a77"};
		DBApp.select("pona", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((330%DBApp.dataPageSize))-1;
		DBApp.select("pona", pageSelectPointer018,recordSelectPointer018);
	
		String selectTrace0 = DBApp.getFullTrace("pona");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 351);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 330"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f6", cols0);
		String [][] records_f6 = new String[235][cols0.length];
		for(int i=0;i<235;i++)
		{
			records_f6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f6", records_f6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(235.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"i"};
		String[] ConditionColumnsValues03 = {"i8"};
		DBApp.select("f6", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("f6");
	
		DBApp.select("f6");
	
		DBApp.select("f6");
	
		DBApp.select("f6");
	
		String[] ConditionColumns011 = {"f"};
		String[] ConditionColumnsValues011 = {"f4"};
		DBApp.select("f6", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("f6");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"k","j","i"};
		String[] ConditionColumnsValues014 = {"k9","j4","i1"};
		DBApp.select("f6", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("f6");
	
		DBApp.select("f6");
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"b","d","c"};
		String[] ConditionColumnsValues020 = {"b0","d2","c0"};
		DBApp.select("f6", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("f6");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("f6");
	
		String[] ConditionColumns026 = {"k"};
		String[] ConditionColumnsValues026 = {"k3"};
		DBApp.select("f6", ConditionColumns026, ConditionColumnsValues026);
	
		String[] ConditionColumns027 = {"c","m","g","f"};
		String[] ConditionColumnsValues027 = {"c2","m12","g1","f5"};
		DBApp.select("f6", ConditionColumns027, ConditionColumnsValues027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("f6", pageSelectPointer028,recordSelectPointer028);
	
		DBApp.select("f6");
	
		String selectTrace0 = DBApp.getFullTrace("f6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 267);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 235"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("p68", cols1);
		String [][] records_p68 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			records_p68[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p68[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p68", records_p68[i]);
		}
	
		int pageCount1 = (int)Math.ceil(68.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b0"};
		DBApp.select("p68", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b0"};
		DBApp.select("p68", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a65"};
		DBApp.select("p68", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b0"};
		DBApp.select("p68", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("p68");
	
		String[] ConditionColumns113 = {"b"};
		String[] ConditionColumnsValues113 = {"b1"};
		DBApp.select("p68", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer114,recordSelectPointer114);
	
		String[] ConditionColumns115 = {"a"};
		String[] ConditionColumnsValues115 = {"a64"};
		DBApp.select("p68", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"b"};
		String[] ConditionColumnsValues116 = {"b1"};
		DBApp.select("p68", ConditionColumns116, ConditionColumnsValues116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer117,recordSelectPointer117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		DBApp.select("p68");
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer125,recordSelectPointer125);
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("p68", pageSelectPointer126,recordSelectPointer126);
	
		DBApp.select("p68");
	
		String selectTrace1 = DBApp.getFullTrace("p68");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 98);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 68"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n2e", cols2);
		String [][] records_n2e = new String[180][cols2.length];
		for(int i=0;i<180;i++)
		{
			records_n2e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n2e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n2e", records_n2e[i]);
		}
	
		int pageCount2 = (int)Math.ceil(180.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("n2e");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("n2e", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"i"};
		String[] ConditionColumnsValues22 = {"i8"};
		DBApp.select("n2e", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("n2e");
	
		DBApp.select("n2e");
	
		String[] ConditionColumns25 = {"f"};
		String[] ConditionColumnsValues25 = {"f5"};
		DBApp.select("n2e", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("n2e");
	
		String selectTrace2 = DBApp.getFullTrace("n2e");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 189);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 180"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("k9ne", cols3);
		String [][] records_k9ne = new String[202][cols3.length];
		for(int i=0;i<202;i++)
		{
			records_k9ne[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k9ne[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k9ne", records_k9ne[i]);
		}
	
		int pageCount3 = (int)Math.ceil(202.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("k9ne");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("k9ne", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"a"};
		String[] ConditionColumnsValues32 = {"a94"};
		DBApp.select("k9ne", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("k9ne");
	
		DBApp.select("k9ne");
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("k9ne", pageSelectPointer35,recordSelectPointer35);
	
		String selectTrace3 = DBApp.getFullTrace("k9ne");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 210);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 202"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("xtx", cols0);
		String [][] records_xtx = new String[258][cols0.length];
		for(int i=0;i<258;i++)
		{
			records_xtx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xtx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xtx", records_xtx[i]);
		}
	
		int pageCount0 = (int)Math.ceil(258.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("xtx");
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c2"};
		DBApp.select("xtx", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("xtx");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("xtx", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"b"};
		String[] ConditionColumnsValues04 = {"b0"};
		DBApp.select("xtx", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("xtx");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 265);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 258"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("co0", cols1);
		String [][] records_co0 = new String[106][cols1.length];
		for(int i=0;i<106;i++)
		{
			records_co0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_co0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("co0", records_co0[i]);
		}
	
		int pageCount1 = (int)Math.ceil(106.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("co0");
	
		String[] ConditionColumns11 = {"a"};
		String[] ConditionColumnsValues11 = {"a41"};
		DBApp.select("co0", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("co0");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b0"};
		DBApp.select("co0", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((106%DBApp.dataPageSize))-1;
		DBApp.select("co0", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((106%DBApp.dataPageSize))-1;
		DBApp.select("co0", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((106%DBApp.dataPageSize))-1;
		DBApp.select("co0", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((106%DBApp.dataPageSize))-1;
		DBApp.select("co0", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a43"};
		DBApp.select("co0", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("co0");
	
		DBApp.select("co0");
	
		DBApp.select("co0");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((106%DBApp.dataPageSize))-1;
		DBApp.select("co0", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"b"};
		String[] ConditionColumnsValues113 = {"b0"};
		DBApp.select("co0", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("co0");
	
		DBApp.select("co0");
	
		String[] ConditionColumns116 = {"b"};
		String[] ConditionColumnsValues116 = {"b0"};
		DBApp.select("co0", ConditionColumns116, ConditionColumnsValues116);
	
		String selectTrace1 = DBApp.getFullTrace("co0");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 125);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 106"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qep8", cols2);
		String [][] records_qep8 = new String[308][cols2.length];
		for(int i=0;i<308;i++)
		{
			records_qep8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qep8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qep8", records_qep8[i]);
		}
	
		int pageCount2 = (int)Math.ceil(308.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		DBApp.select("qep8");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("qep8", pageSelectPointer21,recordSelectPointer21);
	
		String selectTrace2 = DBApp.getFullTrace("qep8");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 312);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 308"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("flz00", cols3);
		String [][] records_flz00 = new String[399][cols3.length];
		for(int i=0;i<399;i++)
		{
			records_flz00[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_flz00[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("flz00", records_flz00[i]);
		}
	
		int pageCount3 = (int)Math.ceil(399.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		String[] ConditionColumns30 = {"i"};
		String[] ConditionColumnsValues30 = {"i1"};
		DBApp.select("flz00", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("flz00");
	
		DBApp.select("flz00");
	
		DBApp.select("flz00");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer35,recordSelectPointer35);
	
		DBApp.select("flz00");
	
		String[] ConditionColumns37 = {"b"};
		String[] ConditionColumnsValues37 = {"b1"};
		DBApp.select("flz00", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("flz00");
	
		String[] ConditionColumns310 = {"d"};
		String[] ConditionColumnsValues310 = {"d2"};
		DBApp.select("flz00", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"a"};
		String[] ConditionColumnsValues311 = {"a170"};
		DBApp.select("flz00", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"f"};
		String[] ConditionColumnsValues312 = {"f5"};
		DBApp.select("flz00", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"e","h","j"};
		String[] ConditionColumnsValues313 = {"e4","h6","j4"};
		DBApp.select("flz00", ConditionColumns313, ConditionColumnsValues313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer314,recordSelectPointer314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer315,recordSelectPointer315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer316,recordSelectPointer316);
	
		String[] ConditionColumns317 = {"h"};
		String[] ConditionColumnsValues317 = {"h4"};
		DBApp.select("flz00", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("flz00");
	
		String[] ConditionColumns319 = {"e"};
		String[] ConditionColumnsValues319 = {"e3"};
		DBApp.select("flz00", ConditionColumns319, ConditionColumnsValues319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer320,recordSelectPointer320);
	
		DBApp.select("flz00");
	
		DBApp.select("flz00");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((399%DBApp.dataPageSize))-1;
		DBApp.select("flz00", pageSelectPointer323,recordSelectPointer323);
	
		DBApp.select("flz00");
	
		DBApp.select("flz00");
	
		DBApp.select("flz00");
	
		String selectTrace3 = DBApp.getFullTrace("flz00");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 428);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 399"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("m3e", cols4);
		String [][] records_m3e = new String[310][cols4.length];
		for(int i=0;i<310;i++)
		{
			records_m3e[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_m3e[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("m3e", records_m3e[i]);
		}
	
		int pageCount4 = (int)Math.ceil(310.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("m3e");
	
		String[] ConditionColumns41 = {"b"};
		String[] ConditionColumnsValues41 = {"b0"};
		DBApp.select("m3e", ConditionColumns41, ConditionColumnsValues41);
	
		String[] ConditionColumns42 = {"b"};
		String[] ConditionColumnsValues42 = {"b1"};
		DBApp.select("m3e", ConditionColumns42, ConditionColumnsValues42);
	
		DBApp.select("m3e");
	
		String[] ConditionColumns44 = {"b"};
		String[] ConditionColumnsValues44 = {"b0"};
		DBApp.select("m3e", ConditionColumns44, ConditionColumnsValues44);
	
		DBApp.select("m3e");
	
		int pageSelectPointer46 = genRandNum(pageCount4)-1;
		int recordSelectPointer46 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer46==pageCount4-1)
			recordSelectPointer46 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer46,recordSelectPointer46);
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer47,recordSelectPointer47);
	
		String[] ConditionColumns48 = {"b"};
		String[] ConditionColumnsValues48 = {"b1"};
		DBApp.select("m3e", ConditionColumns48, ConditionColumnsValues48);
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer49,recordSelectPointer49);
	
		DBApp.select("m3e");
	
		String[] ConditionColumns411 = {"b"};
		String[] ConditionColumnsValues411 = {"b0"};
		DBApp.select("m3e", ConditionColumns411, ConditionColumnsValues411);
	
		DBApp.select("m3e");
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer413,recordSelectPointer413);
	
		int pageSelectPointer414 = genRandNum(pageCount4)-1;
		int recordSelectPointer414 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer414==pageCount4-1)
			recordSelectPointer414 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer414,recordSelectPointer414);
	
		DBApp.select("m3e");
	
		int pageSelectPointer416 = genRandNum(pageCount4)-1;
		int recordSelectPointer416 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer416==pageCount4-1)
			recordSelectPointer416 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer416,recordSelectPointer416);
	
		String[] ConditionColumns417 = {"b"};
		String[] ConditionColumnsValues417 = {"b1"};
		DBApp.select("m3e", ConditionColumns417, ConditionColumnsValues417);
	
		String[] ConditionColumns418 = {"b"};
		String[] ConditionColumnsValues418 = {"b1"};
		DBApp.select("m3e", ConditionColumns418, ConditionColumnsValues418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer419,recordSelectPointer419);
	
		String[] ConditionColumns420 = {"a"};
		String[] ConditionColumnsValues420 = {"a235"};
		DBApp.select("m3e", ConditionColumns420, ConditionColumnsValues420);
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer421,recordSelectPointer421);
	
		String[] ConditionColumns422 = {"a"};
		String[] ConditionColumnsValues422 = {"a99"};
		DBApp.select("m3e", ConditionColumns422, ConditionColumnsValues422);
	
		DBApp.select("m3e");
	
		DBApp.select("m3e");
	
		int pageSelectPointer425 = genRandNum(pageCount4)-1;
		int recordSelectPointer425 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer425==pageCount4-1)
			recordSelectPointer425 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer425,recordSelectPointer425);
	
		int pageSelectPointer426 = genRandNum(pageCount4)-1;
		int recordSelectPointer426 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer426==pageCount4-1)
			recordSelectPointer426 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer426,recordSelectPointer426);
	
		int pageSelectPointer427 = genRandNum(pageCount4)-1;
		int recordSelectPointer427 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer427==pageCount4-1)
			recordSelectPointer427 = genRandNum((310%DBApp.dataPageSize))-1;
		DBApp.select("m3e", pageSelectPointer427,recordSelectPointer427);
	
		DBApp.select("m3e");
	
		String[] ConditionColumns429 = {"a"};
		String[] ConditionColumnsValues429 = {"a85"};
		DBApp.select("m3e", ConditionColumns429, ConditionColumnsValues429);
	
		String selectTrace4 = DBApp.getFullTrace("m3e");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 342);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 310"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("d4i", cols0);
		String [][] records_d4i = new String[369][cols0.length];
		for(int i=0;i<369;i++)
		{
			records_d4i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d4i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d4i", records_d4i[i]);
		}
	
		int pageCount0 = (int)Math.ceil(369.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		String[] ConditionColumns00 = {"k"};
		String[] ConditionColumnsValues00 = {"k10"};
		DBApp.select("d4i", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("d4i");
	
		DBApp.select("d4i");
	
		DBApp.select("d4i");
	
		String[] ConditionColumns04 = {"h"};
		String[] ConditionColumnsValues04 = {"h5"};
		DBApp.select("d4i", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("d4i");
	
		String[] ConditionColumns06 = {"m","c","b"};
		String[] ConditionColumnsValues06 = {"m7","c2","b1"};
		DBApp.select("d4i", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("d4i");
	
		DBApp.select("d4i");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("d4i", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("d4i", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("d4i");
	
		DBApp.select("d4i");
	
		String[] ConditionColumns013 = {"b"};
		String[] ConditionColumnsValues013 = {"b0"};
		DBApp.select("d4i", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"h","a","n","j","f","g"};
		String[] ConditionColumnsValues014 = {"h2","a146","n6","j6","f2","g6"};
		DBApp.select("d4i", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"h","m","b","l","k"};
		String[] ConditionColumnsValues015 = {"h6","m11","b0","l10","k2"};
		DBApp.select("d4i", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"n"};
		String[] ConditionColumnsValues016 = {"n10"};
		DBApp.select("d4i", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"h","i","k","f","c","n"};
		String[] ConditionColumnsValues017 = {"h4","i3","k8","f0","c0","n4"};
		DBApp.select("d4i", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("d4i", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"n"};
		String[] ConditionColumnsValues019 = {"n6"};
		DBApp.select("d4i", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"d"};
		String[] ConditionColumnsValues020 = {"d0"};
		DBApp.select("d4i", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("d4i");
	
		String[] ConditionColumns022 = {"d","e","l"};
		String[] ConditionColumnsValues022 = {"d0","e0","l4"};
		DBApp.select("d4i", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((369%DBApp.dataPageSize))-1;
		DBApp.select("d4i", pageSelectPointer023,recordSelectPointer023);
	
		String[] ConditionColumns024 = {"m","k","e","j","l","i"};
		String[] ConditionColumnsValues024 = {"m5","k2","e2","j7","l9","i3"};
		DBApp.select("d4i", ConditionColumns024, ConditionColumnsValues024);
	
		String[] ConditionColumns025 = {"h","m","i","g","l","e","n"};
		String[] ConditionColumnsValues025 = {"h4","m3","i1","g4","l4","e2","n4"};
		DBApp.select("d4i", ConditionColumns025, ConditionColumnsValues025);
	
		String selectTrace0 = DBApp.getFullTrace("d4i");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 397);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 369"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("x53", cols1);
		String [][] records_x53 = new String[229][cols1.length];
		for(int i=0;i<229;i++)
		{
			records_x53[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x53[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x53", records_x53[i]);
		}
	
		int pageCount1 = (int)Math.ceil(229.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns10 = {"b"};
		String[] ConditionColumnsValues10 = {"b1"};
		DBApp.select("x53", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"e","d"};
		String[] ConditionColumnsValues11 = {"e3","d1"};
		DBApp.select("x53", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"a"};
		String[] ConditionColumnsValues12 = {"a210"};
		DBApp.select("x53", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((229%DBApp.dataPageSize))-1;
		DBApp.select("x53", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((229%DBApp.dataPageSize))-1;
		DBApp.select("x53", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"c","d"};
		String[] ConditionColumnsValues15 = {"c1","d0"};
		DBApp.select("x53", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"d"};
		String[] ConditionColumnsValues16 = {"d0"};
		DBApp.select("x53", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("x53");
	
		DBApp.select("x53");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((229%DBApp.dataPageSize))-1;
		DBApp.select("x53", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b","c"};
		String[] ConditionColumnsValues110 = {"b1","c2"};
		DBApp.select("x53", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((229%DBApp.dataPageSize))-1;
		DBApp.select("x53", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("x53");
	
		String[] ConditionColumns113 = {"c"};
		String[] ConditionColumnsValues113 = {"c2"};
		DBApp.select("x53", ConditionColumns113, ConditionColumnsValues113);
	
		String selectTrace1 = DBApp.getFullTrace("x53");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 245);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 229"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d17", cols2);
		String [][] records_d17 = new String[392][cols2.length];
		for(int i=0;i<392;i++)
		{
			records_d17[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d17[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d17", records_d17[i]);
		}
	
		int pageCount2 = (int)Math.ceil(392.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((392%DBApp.dataPageSize))-1;
		DBApp.select("d17", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("d17");
	
		String selectTrace2 = DBApp.getFullTrace("d17");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 396);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 392"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("sc72f", cols3);
		String [][] records_sc72f = new String[303][cols3.length];
		for(int i=0;i<303;i++)
		{
			records_sc72f[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_sc72f[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("sc72f", records_sc72f[i]);
		}
	
		int pageCount3 = (int)Math.ceil(303.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("sc72f", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("sc72f", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"a"};
		String[] ConditionColumnsValues32 = {"a241"};
		DBApp.select("sc72f", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("sc72f");
	
		DBApp.select("sc72f");
	
		DBApp.select("sc72f");
	
		String selectTrace3 = DBApp.getFullTrace("sc72f");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 311);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 303"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z6zm", cols0);
		String [][] records_z6zm = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_z6zm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z6zm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z6zm", records_z6zm[i]);
		}
	
		int pageCount0 = (int)Math.ceil(212.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((212%DBApp.dataPageSize))-1;
		DBApp.select("z6zm", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"j"};
		String[] ConditionColumnsValues01 = {"j6"};
		DBApp.select("z6zm", ConditionColumns01, ConditionColumnsValues01);
	
		String selectTrace0 = DBApp.getFullTrace("z6zm");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 216);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 212"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("v2gd", cols1);
		String [][] records_v2gd = new String[433][cols1.length];
		for(int i=0;i<433;i++)
		{
			records_v2gd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v2gd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v2gd", records_v2gd[i]);
		}
	
		int pageCount1 = (int)Math.ceil(433.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		DBApp.select("v2gd");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((433%DBApp.dataPageSize))-1;
		DBApp.select("v2gd", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b0"};
		DBApp.select("v2gd", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((433%DBApp.dataPageSize))-1;
		DBApp.select("v2gd", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("v2gd");
	
		DBApp.select("v2gd");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((433%DBApp.dataPageSize))-1;
		DBApp.select("v2gd", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b0"};
		DBApp.select("v2gd", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"c"};
		String[] ConditionColumnsValues18 = {"c1"};
		DBApp.select("v2gd", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b0"};
		DBApp.select("v2gd", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b0"};
		DBApp.select("v2gd", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("v2gd");
	
		String selectTrace1 = DBApp.getFullTrace("v2gd");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 447);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 433"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("cj3", cols2);
		String [][] records_cj3 = new String[170][cols2.length];
		for(int i=0;i<170;i++)
		{
			records_cj3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cj3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cj3", records_cj3[i]);
		}
	
		int pageCount2 = (int)Math.ceil(170.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("cj3");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"c"};
		String[] ConditionColumnsValues24 = {"c1"};
		DBApp.select("cj3", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"e"};
		String[] ConditionColumnsValues27 = {"e1"};
		DBApp.select("cj3", ConditionColumns27, ConditionColumnsValues27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("cj3");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer212,recordSelectPointer212);
	
		String[] ConditionColumns213 = {"b"};
		String[] ConditionColumnsValues213 = {"b1"};
		DBApp.select("cj3", ConditionColumns213, ConditionColumnsValues213);
	
		String[] ConditionColumns214 = {"b","c"};
		String[] ConditionColumnsValues214 = {"b0","c2"};
		DBApp.select("cj3", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("cj3");
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((170%DBApp.dataPageSize))-1;
		DBApp.select("cj3", pageSelectPointer217,recordSelectPointer217);
	
		String selectTrace2 = DBApp.getFullTrace("cj3");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 190);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 170"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w66", cols3);
		String [][] records_w66 = new String[313][cols3.length];
		for(int i=0;i<313;i++)
		{
			records_w66[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w66[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w66", records_w66[i]);
		}
	
		int pageCount3 = (int)Math.ceil(313.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer30,recordSelectPointer30);
	
		DBApp.select("w66");
	
		String[] ConditionColumns32 = {"f"};
		String[] ConditionColumnsValues32 = {"f0"};
		DBApp.select("w66", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("w66");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer35,recordSelectPointer35);
	
		String[] ConditionColumns36 = {"n","k","e","c","j","a","l"};
		String[] ConditionColumnsValues36 = {"n2","k8","e4","c1","j4","a184","l4"};
		DBApp.select("w66", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("w66");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("w66");
	
		String[] ConditionColumns310 = {"e"};
		String[] ConditionColumnsValues310 = {"e1"};
		DBApp.select("w66", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer311,recordSelectPointer311);
	
		DBApp.select("w66");
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer313,recordSelectPointer313);
	
		String[] ConditionColumns314 = {"j"};
		String[] ConditionColumnsValues314 = {"j6"};
		DBApp.select("w66", ConditionColumns314, ConditionColumnsValues314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((313%DBApp.dataPageSize))-1;
		DBApp.select("w66", pageSelectPointer315,recordSelectPointer315);
	
		DBApp.select("w66");
	
		String[] ConditionColumns317 = {"g"};
		String[] ConditionColumnsValues317 = {"g1"};
		DBApp.select("w66", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("w66");
	
		DBApp.select("w66");
	
		String[] ConditionColumns320 = {"l"};
		String[] ConditionColumnsValues320 = {"l7"};
		DBApp.select("w66", ConditionColumns320, ConditionColumnsValues320);
	
		DBApp.select("w66");
	
		String selectTrace3 = DBApp.getFullTrace("w66");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 337);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 313"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q17", cols4);
		String [][] records_q17 = new String[393][cols4.length];
		for(int i=0;i<393;i++)
		{
			records_q17[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q17[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q17", records_q17[i]);
		}
	
		int pageCount4 = (int)Math.ceil(393.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns40 = {"a"};
		String[] ConditionColumnsValues40 = {"a67"};
		DBApp.select("q17", ConditionColumns40, ConditionColumnsValues40);
	
		String[] ConditionColumns41 = {"c"};
		String[] ConditionColumnsValues41 = {"c2"};
		DBApp.select("q17", ConditionColumns41, ConditionColumnsValues41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"a"};
		String[] ConditionColumnsValues43 = {"a51"};
		DBApp.select("q17", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"e","d"};
		String[] ConditionColumnsValues44 = {"e2","d3"};
		DBApp.select("q17", ConditionColumns44, ConditionColumnsValues44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("q17");
	
		String[] ConditionColumns47 = {"d"};
		String[] ConditionColumnsValues47 = {"d1"};
		DBApp.select("q17", ConditionColumns47, ConditionColumnsValues47);
	
		String[] ConditionColumns48 = {"e","b"};
		String[] ConditionColumnsValues48 = {"e2","b1"};
		DBApp.select("q17", ConditionColumns48, ConditionColumnsValues48);
	
		DBApp.select("q17");
	
		int pageSelectPointer410 = genRandNum(pageCount4)-1;
		int recordSelectPointer410 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer410==pageCount4-1)
			recordSelectPointer410 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer410,recordSelectPointer410);
	
		String[] ConditionColumns411 = {"g","f"};
		String[] ConditionColumnsValues411 = {"g5","f3"};
		DBApp.select("q17", ConditionColumns411, ConditionColumnsValues411);
	
		String[] ConditionColumns412 = {"e","b"};
		String[] ConditionColumnsValues412 = {"e0","b0"};
		DBApp.select("q17", ConditionColumns412, ConditionColumnsValues412);
	
		DBApp.select("q17");
	
		String[] ConditionColumns414 = {"f","c","d"};
		String[] ConditionColumnsValues414 = {"f5","c2","d3"};
		DBApp.select("q17", ConditionColumns414, ConditionColumnsValues414);
	
		int pageSelectPointer415 = genRandNum(pageCount4)-1;
		int recordSelectPointer415 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer415==pageCount4-1)
			recordSelectPointer415 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer415,recordSelectPointer415);
	
		DBApp.select("q17");
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer417,recordSelectPointer417);
	
		int pageSelectPointer418 = genRandNum(pageCount4)-1;
		int recordSelectPointer418 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer418==pageCount4-1)
			recordSelectPointer418 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer418,recordSelectPointer418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("q17", pageSelectPointer419,recordSelectPointer419);
	
		String[] ConditionColumns420 = {"f"};
		String[] ConditionColumnsValues420 = {"f2"};
		DBApp.select("q17", ConditionColumns420, ConditionColumnsValues420);
	
		String selectTrace4 = DBApp.getFullTrace("q17");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 416);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 393"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("nhe6", cols0);
		String [][] records_nhe6 = new String[268][cols0.length];
		for(int i=0;i<268;i++)
		{
			records_nhe6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nhe6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nhe6", records_nhe6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(268.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		String[] ConditionColumns00 = {"a","e"};
		String[] ConditionColumnsValues00 = {"a147","e2"};
		DBApp.select("nhe6", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"b","c"};
		String[] ConditionColumnsValues01 = {"b1","c2"};
		DBApp.select("nhe6", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("nhe6");
	
		String[] ConditionColumns04 = {"c"};
		String[] ConditionColumnsValues04 = {"c1"};
		DBApp.select("nhe6", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"d"};
		String[] ConditionColumnsValues05 = {"d2"};
		DBApp.select("nhe6", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("nhe6");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("nhe6");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("nhe6");
	
		String[] ConditionColumns011 = {"a"};
		String[] ConditionColumnsValues011 = {"a260"};
		DBApp.select("nhe6", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"d"};
		String[] ConditionColumnsValues012 = {"d1"};
		DBApp.select("nhe6", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d0"};
		DBApp.select("nhe6", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("nhe6");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"d","e"};
		String[] ConditionColumnsValues016 = {"d1","e0"};
		DBApp.select("nhe6", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"d"};
		String[] ConditionColumnsValues017 = {"d1"};
		DBApp.select("nhe6", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("nhe6");
	
		DBApp.select("nhe6");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((268%DBApp.dataPageSize))-1;
		DBApp.select("nhe6", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("nhe6");
	
		DBApp.select("nhe6");
	
		String selectTrace0 = DBApp.getFullTrace("nhe6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 294);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 268"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("j3", cols1);
		String [][] records_j3 = new String[240][cols1.length];
		for(int i=0;i<240;i++)
		{
			records_j3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j3", records_j3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(240.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a217"};
		DBApp.select("j3", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("j3");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("j3");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("j3");
	
		String[] ConditionColumns116 = {"b"};
		String[] ConditionColumnsValues116 = {"b1"};
		DBApp.select("j3", ConditionColumns116, ConditionColumnsValues116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("j3");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer119,recordSelectPointer119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer120,recordSelectPointer120);
	
		DBApp.select("j3");
	
		String[] ConditionColumns122 = {"b"};
		String[] ConditionColumnsValues122 = {"b1"};
		DBApp.select("j3", ConditionColumns122, ConditionColumnsValues122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer123,recordSelectPointer123);
	
		String[] ConditionColumns124 = {"b"};
		String[] ConditionColumnsValues124 = {"b1"};
		DBApp.select("j3", ConditionColumns124, ConditionColumnsValues124);
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer125,recordSelectPointer125);
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		int pageSelectPointer128 = genRandNum(pageCount1)-1;
		int recordSelectPointer128 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer128==pageCount1-1)
			recordSelectPointer128 = genRandNum((240%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer128,recordSelectPointer128);
	
		String selectTrace1 = DBApp.getFullTrace("j3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 271);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 240"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y71", cols2);
		String [][] records_y71 = new String[436][cols2.length];
		for(int i=0;i<436;i++)
		{
			records_y71[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y71[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y71", records_y71[i]);
		}
	
		int pageCount2 = (int)Math.ceil(436.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("y71");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"f","e","g"};
		String[] ConditionColumnsValues23 = {"f4","e2","g4"};
		DBApp.select("y71", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"e"};
		String[] ConditionColumnsValues25 = {"e0"};
		DBApp.select("y71", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"e"};
		String[] ConditionColumnsValues27 = {"e3"};
		DBApp.select("y71", ConditionColumns27, ConditionColumnsValues27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("y71");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("y71");
	
		DBApp.select("y71");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer215,recordSelectPointer215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer216,recordSelectPointer216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer217,recordSelectPointer217);
	
		String[] ConditionColumns218 = {"b"};
		String[] ConditionColumnsValues218 = {"b1"};
		DBApp.select("y71", ConditionColumns218, ConditionColumnsValues218);
	
		String[] ConditionColumns219 = {"b"};
		String[] ConditionColumnsValues219 = {"b1"};
		DBApp.select("y71", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"e"};
		String[] ConditionColumnsValues220 = {"e0"};
		DBApp.select("y71", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"f","g"};
		String[] ConditionColumnsValues221 = {"f1","g0"};
		DBApp.select("y71", ConditionColumns221, ConditionColumnsValues221);
	
		String[] ConditionColumns222 = {"e"};
		String[] ConditionColumnsValues222 = {"e3"};
		DBApp.select("y71", ConditionColumns222, ConditionColumnsValues222);
	
		DBApp.select("y71");
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((436%DBApp.dataPageSize))-1;
		DBApp.select("y71", pageSelectPointer224,recordSelectPointer224);
	
		String[] ConditionColumns225 = {"e"};
		String[] ConditionColumnsValues225 = {"e2"};
		DBApp.select("y71", ConditionColumns225, ConditionColumnsValues225);
	
		DBApp.select("y71");
	
		String[] ConditionColumns227 = {"g"};
		String[] ConditionColumnsValues227 = {"g6"};
		DBApp.select("y71", ConditionColumns227, ConditionColumnsValues227);
	
		String[] ConditionColumns228 = {"f"};
		String[] ConditionColumnsValues228 = {"f3"};
		DBApp.select("y71", ConditionColumns228, ConditionColumnsValues228);
	
		String[] ConditionColumns229 = {"c"};
		String[] ConditionColumnsValues229 = {"c2"};
		DBApp.select("y71", ConditionColumns229, ConditionColumnsValues229);
	
		String selectTrace2 = DBApp.getFullTrace("y71");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 468);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 436"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("rx", cols3);
		String [][] records_rx = new String[354][cols3.length];
		for(int i=0;i<354;i++)
		{
			records_rx[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_rx[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("rx", records_rx[i]);
		}
	
		int pageCount3 = (int)Math.ceil(354.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer32,recordSelectPointer32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer36,recordSelectPointer36);
	
		String[] ConditionColumns37 = {"f"};
		String[] ConditionColumnsValues37 = {"f5"};
		DBApp.select("rx", ConditionColumns37, ConditionColumnsValues37);
	
		DBApp.select("rx");
	
		String[] ConditionColumns39 = {"b","f"};
		String[] ConditionColumnsValues39 = {"b0","f0"};
		DBApp.select("rx", ConditionColumns39, ConditionColumnsValues39);
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("rx");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((354%DBApp.dataPageSize))-1;
		DBApp.select("rx", pageSelectPointer312,recordSelectPointer312);
	
		DBApp.select("rx");
	
		String[] ConditionColumns314 = {"j","b","h"};
		String[] ConditionColumnsValues314 = {"j6","b0","h4"};
		DBApp.select("rx", ConditionColumns314, ConditionColumnsValues314);
	
		String[] ConditionColumns315 = {"k"};
		String[] ConditionColumnsValues315 = {"k8"};
		DBApp.select("rx", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("rx");
	
		String[] ConditionColumns317 = {"c","k","g"};
		String[] ConditionColumnsValues317 = {"c2","k2","g6"};
		DBApp.select("rx", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("rx");
	
		String selectTrace3 = DBApp.getFullTrace("rx");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 375);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 354"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wjsgg", cols0);
		String [][] records_wjsgg = new String[484][cols0.length];
		for(int i=0;i<484;i++)
		{
			records_wjsgg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wjsgg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wjsgg", records_wjsgg[i]);
		}
	
		int pageCount0 = (int)Math.ceil(484.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns00 = {"g"};
		String[] ConditionColumnsValues00 = {"g0"};
		DBApp.select("wjsgg", ConditionColumns00, ConditionColumnsValues00);
	
		String selectTrace0 = DBApp.getFullTrace("wjsgg");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 487);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 484"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("md", cols1);
		String [][] records_md = new String[428][cols1.length];
		for(int i=0;i<428;i++)
		{
			records_md[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_md[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("md", records_md[i]);
		}
	
		int pageCount1 = (int)Math.ceil(428.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("md");
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("md", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("md");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("md");
	
		String[] ConditionColumns18 = {"b"};
		String[] ConditionColumnsValues18 = {"b0"};
		DBApp.select("md", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b0"};
		DBApp.select("md", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("md");
	
		DBApp.select("md");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("md", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("md");
	
		DBApp.select("md");
	
		String selectTrace1 = DBApp.getFullTrace("md");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 446);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 428"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("m1", cols2);
		String [][] records_m1 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			records_m1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m1", records_m1[i]);
		}
	
		int pageCount2 = (int)Math.ceil(89.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("m1");
	
		DBApp.select("m1");
	
		String[] ConditionColumns22 = {"a"};
		String[] ConditionColumnsValues22 = {"a53"};
		DBApp.select("m1", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("m1");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"c","e"};
		String[] ConditionColumnsValues25 = {"c1","e0"};
		DBApp.select("m1", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"f"};
		String[] ConditionColumnsValues26 = {"f1"};
		DBApp.select("m1", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("m1");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("m1");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"e"};
		String[] ConditionColumnsValues212 = {"e1"};
		DBApp.select("m1", ConditionColumns212, ConditionColumnsValues212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"e"};
		String[] ConditionColumnsValues214 = {"e1"};
		DBApp.select("m1", ConditionColumns214, ConditionColumnsValues214);
	
		DBApp.select("m1");
	
		DBApp.select("m1");
	
		String[] ConditionColumns217 = {"b"};
		String[] ConditionColumnsValues217 = {"b1"};
		DBApp.select("m1", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"b"};
		String[] ConditionColumnsValues218 = {"b0"};
		DBApp.select("m1", ConditionColumns218, ConditionColumnsValues218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((89%DBApp.dataPageSize))-1;
		DBApp.select("m1", pageSelectPointer219,recordSelectPointer219);
	
		DBApp.select("m1");
	
		DBApp.select("m1");
	
		String selectTrace2 = DBApp.getFullTrace("m1");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 113);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 89"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("vn6vk", cols3);
		String [][] records_vn6vk = new String[485][cols3.length];
		for(int i=0;i<485;i++)
		{
			records_vn6vk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vn6vk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vn6vk", records_vn6vk[i]);
		}
	
		int pageCount3 = (int)Math.ceil(485.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("vn6vk", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"b"};
		String[] ConditionColumnsValues31 = {"b0"};
		DBApp.select("vn6vk", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"d"};
		String[] ConditionColumnsValues32 = {"d2"};
		DBApp.select("vn6vk", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b1"};
		DBApp.select("vn6vk", ConditionColumns33, ConditionColumnsValues33);
	
		String[] ConditionColumns34 = {"c"};
		String[] ConditionColumnsValues34 = {"c1"};
		DBApp.select("vn6vk", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("vn6vk", pageSelectPointer35,recordSelectPointer35);
	
		DBApp.select("vn6vk");
	
		String[] ConditionColumns37 = {"b"};
		String[] ConditionColumnsValues37 = {"b1"};
		DBApp.select("vn6vk", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("vn6vk", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("vn6vk");
	
		DBApp.select("vn6vk");
	
		String[] ConditionColumns311 = {"b"};
		String[] ConditionColumnsValues311 = {"b1"};
		DBApp.select("vn6vk", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"d"};
		String[] ConditionColumnsValues312 = {"d1"};
		DBApp.select("vn6vk", ConditionColumns312, ConditionColumnsValues312);
	
		DBApp.select("vn6vk");
	
		String selectTrace3 = DBApp.getFullTrace("vn6vk");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 501);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 485"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("da0", cols4);
		String [][] records_da0 = new String[475][cols4.length];
		for(int i=0;i<475;i++)
		{
			records_da0[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_da0[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("da0", records_da0[i]);
		}
	
		int pageCount4 = (int)Math.ceil(475.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("da0");
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer41,recordSelectPointer41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"i"};
		String[] ConditionColumnsValues43 = {"i7"};
		DBApp.select("da0", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"d","i"};
		String[] ConditionColumnsValues44 = {"d3","i2"};
		DBApp.select("da0", ConditionColumns44, ConditionColumnsValues44);
	
		String[] ConditionColumns45 = {"e"};
		String[] ConditionColumnsValues45 = {"e1"};
		DBApp.select("da0", ConditionColumns45, ConditionColumnsValues45);
	
		String[] ConditionColumns46 = {"d","g"};
		String[] ConditionColumnsValues46 = {"d1","g5"};
		DBApp.select("da0", ConditionColumns46, ConditionColumnsValues46);
	
		String[] ConditionColumns47 = {"c"};
		String[] ConditionColumnsValues47 = {"c1"};
		DBApp.select("da0", ConditionColumns47, ConditionColumnsValues47);
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer48,recordSelectPointer48);
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer49,recordSelectPointer49);
	
		DBApp.select("da0");
	
		int pageSelectPointer411 = genRandNum(pageCount4)-1;
		int recordSelectPointer411 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer411==pageCount4-1)
			recordSelectPointer411 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer411,recordSelectPointer411);
	
		String[] ConditionColumns412 = {"c"};
		String[] ConditionColumnsValues412 = {"c1"};
		DBApp.select("da0", ConditionColumns412, ConditionColumnsValues412);
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer413,recordSelectPointer413);
	
		int pageSelectPointer414 = genRandNum(pageCount4)-1;
		int recordSelectPointer414 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer414==pageCount4-1)
			recordSelectPointer414 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer414,recordSelectPointer414);
	
		int pageSelectPointer415 = genRandNum(pageCount4)-1;
		int recordSelectPointer415 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer415==pageCount4-1)
			recordSelectPointer415 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer415,recordSelectPointer415);
	
		DBApp.select("da0");
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer417,recordSelectPointer417);
	
		int pageSelectPointer418 = genRandNum(pageCount4)-1;
		int recordSelectPointer418 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer418==pageCount4-1)
			recordSelectPointer418 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer418,recordSelectPointer418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer419,recordSelectPointer419);
	
		String[] ConditionColumns420 = {"c"};
		String[] ConditionColumnsValues420 = {"c0"};
		DBApp.select("da0", ConditionColumns420, ConditionColumnsValues420);
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((475%DBApp.dataPageSize))-1;
		DBApp.select("da0", pageSelectPointer421,recordSelectPointer421);
	
		DBApp.select("da0");
	
		DBApp.select("da0");
	
		DBApp.select("da0");
	
		String selectTrace4 = DBApp.getFullTrace("da0");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 502);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 475"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("uv2", cols0);
		String [][] records_uv2 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			records_uv2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uv2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uv2", records_uv2[i]);
		}
	
		int pageCount0 = (int)Math.ceil(68.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("uv2", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("uv2");
	
		String selectTrace0 = DBApp.getFullTrace("uv2");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 72);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 68"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("jc", cols1);
		String [][] records_jc = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_jc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jc", records_jc[i]);
		}
	
		int pageCount1 = (int)Math.ceil(5.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"d","e","f"};
		String[] ConditionColumnsValues12 = {"d0","e4","f4"};
		DBApp.select("jc", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("jc");
	
		DBApp.select("jc");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("jc");
	
		DBApp.select("jc");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("jc");
	
		String[] ConditionColumns113 = {"f","b"};
		String[] ConditionColumnsValues113 = {"f1","b1"};
		DBApp.select("jc", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"f"};
		String[] ConditionColumnsValues114 = {"f2"};
		DBApp.select("jc", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((5%DBApp.dataPageSize))-1;
		DBApp.select("jc", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("jc");
	
		String[] ConditionColumns119 = {"f","b","d"};
		String[] ConditionColumnsValues119 = {"f1","b1","d1"};
		DBApp.select("jc", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("jc");
	
		DBApp.select("jc");
	
		String[] ConditionColumns122 = {"f"};
		String[] ConditionColumnsValues122 = {"f1"};
		DBApp.select("jc", ConditionColumns122, ConditionColumnsValues122);
	
		String selectTrace1 = DBApp.getFullTrace("jc");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 30);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 5"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("gt9", cols2);
		String [][] records_gt9 = new String[497][cols2.length];
		for(int i=0;i<497;i++)
		{
			records_gt9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gt9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gt9", records_gt9[i]);
		}
	
		int pageCount2 = (int)Math.ceil(497.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		String[] ConditionColumns20 = {"g"};
		String[] ConditionColumnsValues20 = {"g1"};
		DBApp.select("gt9", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"i","d"};
		String[] ConditionColumnsValues21 = {"i1","d3"};
		DBApp.select("gt9", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"d","f","b","c"};
		String[] ConditionColumnsValues22 = {"d0","f2","b0","c2"};
		DBApp.select("gt9", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("gt9");
	
		DBApp.select("gt9");
	
		DBApp.select("gt9");
	
		String[] ConditionColumns26 = {"j","f"};
		String[] ConditionColumnsValues26 = {"j8","f4"};
		DBApp.select("gt9", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("gt9");
	
		String[] ConditionColumns28 = {"d","a","b","g"};
		String[] ConditionColumnsValues28 = {"d2","a410","b0","g4"};
		DBApp.select("gt9", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"b"};
		String[] ConditionColumnsValues29 = {"b1"};
		DBApp.select("gt9", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"j"};
		String[] ConditionColumnsValues214 = {"j1"};
		DBApp.select("gt9", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"j"};
		String[] ConditionColumnsValues215 = {"j7"};
		DBApp.select("gt9", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer216,recordSelectPointer216);
	
		String[] ConditionColumns217 = {"i"};
		String[] ConditionColumnsValues217 = {"i2"};
		DBApp.select("gt9", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"i"};
		String[] ConditionColumnsValues218 = {"i1"};
		DBApp.select("gt9", ConditionColumns218, ConditionColumnsValues218);
	
		String[] ConditionColumns219 = {"c","k"};
		String[] ConditionColumnsValues219 = {"c1","k3"};
		DBApp.select("gt9", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"a","i","b","d","h"};
		String[] ConditionColumnsValues220 = {"a175","i4","b1","d3","h7"};
		DBApp.select("gt9", ConditionColumns220, ConditionColumnsValues220);
	
		DBApp.select("gt9");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("gt9", pageSelectPointer222,recordSelectPointer222);
	
		String[] ConditionColumns223 = {"h"};
		String[] ConditionColumnsValues223 = {"h3"};
		DBApp.select("gt9", ConditionColumns223, ConditionColumnsValues223);
	
		String selectTrace2 = DBApp.getFullTrace("gt9");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 523);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 497"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("w19o9", cols3);
		String [][] records_w19o9 = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			records_w19o9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w19o9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w19o9", records_w19o9[i]);
		}
	
		int pageCount3 = (int)Math.ceil(15.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((15%DBApp.dataPageSize))-1;
		DBApp.select("w19o9", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"c"};
		String[] ConditionColumnsValues31 = {"c2"};
		DBApp.select("w19o9", ConditionColumns31, ConditionColumnsValues31);
	
		String selectTrace3 = DBApp.getFullTrace("w19o9");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 19);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 15"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d3lzt", cols0);
		String [][] records_d3lzt = new String[146][cols0.length];
		for(int i=0;i<146;i++)
		{
			records_d3lzt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d3lzt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d3lzt", records_d3lzt[i]);
		}
	
		int pageCount0 = (int)Math.ceil(146.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((146%DBApp.dataPageSize))-1;
		DBApp.select("d3lzt", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"h"};
		String[] ConditionColumnsValues01 = {"h4"};
		DBApp.select("d3lzt", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"e"};
		String[] ConditionColumnsValues02 = {"e0"};
		DBApp.select("d3lzt", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a98"};
		DBApp.select("d3lzt", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("d3lzt");
	
		DBApp.select("d3lzt");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((146%DBApp.dataPageSize))-1;
		DBApp.select("d3lzt", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"c","e"};
		String[] ConditionColumnsValues07 = {"c2","e2"};
		DBApp.select("d3lzt", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"f","g"};
		String[] ConditionColumnsValues08 = {"f1","g1"};
		DBApp.select("d3lzt", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("d3lzt");
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((146%DBApp.dataPageSize))-1;
		DBApp.select("d3lzt", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"h"};
		String[] ConditionColumnsValues011 = {"h7"};
		DBApp.select("d3lzt", ConditionColumns011, ConditionColumnsValues011);
	
		String selectTrace0 = DBApp.getFullTrace("d3lzt");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 160);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 146"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w15", cols1);
		String [][] records_w15 = new String[476][cols1.length];
		for(int i=0;i<476;i++)
		{
			records_w15[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w15[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w15", records_w15[i]);
		}
	
		int pageCount1 = (int)Math.ceil(476.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		DBApp.select("w15");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("w15", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("w15", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("w15");
	
		DBApp.select("w15");
	
		DBApp.select("w15");
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a162"};
		DBApp.select("w15", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("w15", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("w15", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("w15", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b1"};
		DBApp.select("w15", ConditionColumns110, ConditionColumnsValues110);
	
		String selectTrace1 = DBApp.getFullTrace("w15");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 489);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 476"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("e5565", cols0);
		String [][] records_e5565 = new String[494][cols0.length];
		for(int i=0;i<494;i++)
		{
			records_e5565[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e5565[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e5565", records_e5565[i]);
		}
	
		int pageCount0 = (int)Math.ceil(494.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns00 = {"h"};
		String[] ConditionColumnsValues00 = {"h6"};
		DBApp.select("e5565", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((494%DBApp.dataPageSize))-1;
		DBApp.select("e5565", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("e5565");
	
		DBApp.select("e5565");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((494%DBApp.dataPageSize))-1;
		DBApp.select("e5565", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c0"};
		DBApp.select("e5565", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"f","h","c"};
		String[] ConditionColumnsValues06 = {"f5","h3","c2"};
		DBApp.select("e5565", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((494%DBApp.dataPageSize))-1;
		DBApp.select("e5565", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("e5565");
	
		String[] ConditionColumns09 = {"g"};
		String[] ConditionColumnsValues09 = {"g5"};
		DBApp.select("e5565", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("e5565");
	
		DBApp.select("e5565");
	
		String[] ConditionColumns012 = {"c"};
		String[] ConditionColumnsValues012 = {"c0"};
		DBApp.select("e5565", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("e5565");
	
		String selectTrace0 = DBApp.getFullTrace("e5565");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 510);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 494"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("c909u", cols1);
		String [][] records_c909u = new String[142][cols1.length];
		for(int i=0;i<142;i++)
		{
			records_c909u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c909u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c909u", records_c909u[i]);
		}
	
		int pageCount1 = (int)Math.ceil(142.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("c909u");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b0"};
		DBApp.select("c909u", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("c909u");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("c909u");
	
		String[] ConditionColumns17 = {"c"};
		String[] ConditionColumnsValues17 = {"c0"};
		DBApp.select("c909u", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("c909u");
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b1"};
		DBApp.select("c909u", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("c909u");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("c909u", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"d","f","a"};
		String[] ConditionColumnsValues114 = {"d3","f3","a75"};
		DBApp.select("c909u", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"f"};
		String[] ConditionColumnsValues115 = {"f3"};
		DBApp.select("c909u", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("c909u");
	
		String selectTrace1 = DBApp.getFullTrace("c909u");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 161);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 142"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("pjc9", cols2);
		String [][] records_pjc9 = new String[209][cols2.length];
		for(int i=0;i<209;i++)
		{
			records_pjc9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pjc9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pjc9", records_pjc9[i]);
		}
	
		int pageCount2 = (int)Math.ceil(209.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("pjc9", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"d"};
		String[] ConditionColumnsValues21 = {"d3"};
		DBApp.select("pjc9", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"f"};
		String[] ConditionColumnsValues22 = {"f1"};
		DBApp.select("pjc9", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("pjc9", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"b","j","f","c"};
		String[] ConditionColumnsValues24 = {"b0","j4","f4","c1"};
		DBApp.select("pjc9", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"b"};
		String[] ConditionColumnsValues25 = {"b1"};
		DBApp.select("pjc9", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"e"};
		String[] ConditionColumnsValues26 = {"e0"};
		DBApp.select("pjc9", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("pjc9");
	
		String[] ConditionColumns28 = {"e"};
		String[] ConditionColumnsValues28 = {"e1"};
		DBApp.select("pjc9", ConditionColumns28, ConditionColumnsValues28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("pjc9", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("pjc9", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"b","i","d"};
		String[] ConditionColumnsValues211 = {"b0","i3","d0"};
		DBApp.select("pjc9", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"h","f","g","i"};
		String[] ConditionColumnsValues212 = {"h6","f2","g2","i5"};
		DBApp.select("pjc9", ConditionColumns212, ConditionColumnsValues212);
	
		String[] ConditionColumns213 = {"j"};
		String[] ConditionColumnsValues213 = {"j7"};
		DBApp.select("pjc9", ConditionColumns213, ConditionColumnsValues213);
	
		String selectTrace2 = DBApp.getFullTrace("pjc9");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 225);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 209"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("u8b", cols3);
		String [][] records_u8b = new String[140][cols3.length];
		for(int i=0;i<140;i++)
		{
			records_u8b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u8b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u8b", records_u8b[i]);
		}
	
		int pageCount3 = (int)Math.ceil(140.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("u8b");
	
		DBApp.select("u8b");
	
		String[] ConditionColumns32 = {"c"};
		String[] ConditionColumnsValues32 = {"c1"};
		DBApp.select("u8b", ConditionColumns32, ConditionColumnsValues32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("u8b", pageSelectPointer33,recordSelectPointer33);
	
		DBApp.select("u8b");
	
		DBApp.select("u8b");
	
		DBApp.select("u8b");
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("u8b", pageSelectPointer37,recordSelectPointer37);
	
		DBApp.select("u8b");
	
		String[] ConditionColumns39 = {"b"};
		String[] ConditionColumnsValues39 = {"b1"};
		DBApp.select("u8b", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"a"};
		String[] ConditionColumnsValues310 = {"a15"};
		DBApp.select("u8b", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"b"};
		String[] ConditionColumnsValues311 = {"b0"};
		DBApp.select("u8b", ConditionColumns311, ConditionColumnsValues311);
	
		DBApp.select("u8b");
	
		String[] ConditionColumns313 = {"a"};
		String[] ConditionColumnsValues313 = {"a90"};
		DBApp.select("u8b", ConditionColumns313, ConditionColumnsValues313);
	
		String[] ConditionColumns314 = {"a"};
		String[] ConditionColumnsValues314 = {"a35"};
		DBApp.select("u8b", ConditionColumns314, ConditionColumnsValues314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("u8b", pageSelectPointer315,recordSelectPointer315);
	
		DBApp.select("u8b");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("u8b", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"c"};
		String[] ConditionColumnsValues318 = {"c1"};
		DBApp.select("u8b", ConditionColumns318, ConditionColumnsValues318);
	
		String selectTrace3 = DBApp.getFullTrace("u8b");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 161);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 140"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("md9", cols0);
		String [][] records_md9 = new String[387][cols0.length];
		for(int i=0;i<387;i++)
		{
			records_md9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_md9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("md9", records_md9[i]);
		}
	
		int pageCount0 = (int)Math.ceil(387.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("md9");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"a","d","c"};
		String[] ConditionColumnsValues03 = {"a385","d1","c1"};
		DBApp.select("md9", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("md9");
	
		DBApp.select("md9");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("md9");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("md9");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"e"};
		String[] ConditionColumnsValues012 = {"e4"};
		DBApp.select("md9", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"b","c"};
		String[] ConditionColumnsValues013 = {"b1","c0"};
		DBApp.select("md9", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("md9");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("md9");
	
		DBApp.select("md9");
	
		String[] ConditionColumns018 = {"f"};
		String[] ConditionColumnsValues018 = {"f3"};
		DBApp.select("md9", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"f"};
		String[] ConditionColumnsValues020 = {"f1"};
		DBApp.select("md9", ConditionColumns020, ConditionColumnsValues020);
	
		String[] ConditionColumns021 = {"e"};
		String[] ConditionColumnsValues021 = {"e0"};
		DBApp.select("md9", ConditionColumns021, ConditionColumnsValues021);
	
		String[] ConditionColumns022 = {"e"};
		String[] ConditionColumnsValues022 = {"e0"};
		DBApp.select("md9", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((387%DBApp.dataPageSize))-1;
		DBApp.select("md9", pageSelectPointer023,recordSelectPointer023);
	
		String[] ConditionColumns024 = {"b"};
		String[] ConditionColumnsValues024 = {"b0"};
		DBApp.select("md9", ConditionColumns024, ConditionColumnsValues024);
	
		String selectTrace0 = DBApp.getFullTrace("md9");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 414);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 387"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("z0b1v", cols1);
		String [][] records_z0b1v = new String[321][cols1.length];
		for(int i=0;i<321;i++)
		{
			records_z0b1v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z0b1v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z0b1v", records_z0b1v[i]);
		}
	
		int pageCount1 = (int)Math.ceil(321.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"e","c"};
		String[] ConditionColumnsValues11 = {"e1","c1"};
		DBApp.select("z0b1v", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"d"};
		String[] ConditionColumnsValues13 = {"d2"};
		DBApp.select("z0b1v", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("z0b1v");
	
		DBApp.select("z0b1v");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("z0b1v");
	
		DBApp.select("z0b1v");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("z0b1v");
	
		String[] ConditionColumns111 = {"e"};
		String[] ConditionColumnsValues111 = {"e2"};
		DBApp.select("z0b1v", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"d"};
		String[] ConditionColumnsValues113 = {"d2"};
		DBApp.select("z0b1v", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("z0b1v");
	
		DBApp.select("z0b1v");
	
		DBApp.select("z0b1v");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("z0b1v");
	
		DBApp.select("z0b1v");
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((321%DBApp.dataPageSize))-1;
		DBApp.select("z0b1v", pageSelectPointer120,recordSelectPointer120);
	
		DBApp.select("z0b1v");
	
		String selectTrace1 = DBApp.getFullTrace("z0b1v");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 345);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 321"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dt44", cols2);
		String [][] records_dt44 = new String[78][cols2.length];
		for(int i=0;i<78;i++)
		{
			records_dt44[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dt44[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dt44", records_dt44[i]);
		}
	
		int pageCount2 = (int)Math.ceil(78.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dt44", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("dt44");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dt44", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dt44", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dt44", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"g"};
		String[] ConditionColumnsValues25 = {"g2"};
		DBApp.select("dt44", ConditionColumns25, ConditionColumnsValues25);
	
		String selectTrace2 = DBApp.getFullTrace("dt44");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 86);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 78"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("p3k", cols3);
		String [][] records_p3k = new String[417][cols3.length];
		for(int i=0;i<417;i++)
		{
			records_p3k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p3k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p3k", records_p3k[i]);
		}
	
		int pageCount3 = (int)Math.ceil(417.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("p3k");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer32,recordSelectPointer32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("p3k");
	
		DBApp.select("p3k");
	
		String[] ConditionColumns37 = {"b"};
		String[] ConditionColumnsValues37 = {"b1"};
		DBApp.select("p3k", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"b"};
		String[] ConditionColumnsValues38 = {"b0"};
		DBApp.select("p3k", ConditionColumns38, ConditionColumnsValues38);
	
		DBApp.select("p3k");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer310,recordSelectPointer310);
	
		String[] ConditionColumns311 = {"a"};
		String[] ConditionColumnsValues311 = {"a49"};
		DBApp.select("p3k", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"b"};
		String[] ConditionColumnsValues312 = {"b0"};
		DBApp.select("p3k", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer313,recordSelectPointer313);
	
		String[] ConditionColumns314 = {"b"};
		String[] ConditionColumnsValues314 = {"b0"};
		DBApp.select("p3k", ConditionColumns314, ConditionColumnsValues314);
	
		String[] ConditionColumns315 = {"b"};
		String[] ConditionColumnsValues315 = {"b0"};
		DBApp.select("p3k", ConditionColumns315, ConditionColumnsValues315);
	
		String[] ConditionColumns316 = {"b"};
		String[] ConditionColumnsValues316 = {"b0"};
		DBApp.select("p3k", ConditionColumns316, ConditionColumnsValues316);
	
		String[] ConditionColumns317 = {"a"};
		String[] ConditionColumnsValues317 = {"a221"};
		DBApp.select("p3k", ConditionColumns317, ConditionColumnsValues317);
	
		String[] ConditionColumns318 = {"b"};
		String[] ConditionColumnsValues318 = {"b1"};
		DBApp.select("p3k", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"b"};
		String[] ConditionColumnsValues319 = {"b1"};
		DBApp.select("p3k", ConditionColumns319, ConditionColumnsValues319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((417%DBApp.dataPageSize))-1;
		DBApp.select("p3k", pageSelectPointer320,recordSelectPointer320);
	
		DBApp.select("p3k");
	
		String selectTrace3 = DBApp.getFullTrace("p3k");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 441);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 417"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("h0a", cols4);
		String [][] records_h0a = new String[246][cols4.length];
		for(int i=0;i<246;i++)
		{
			records_h0a[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_h0a[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("h0a", records_h0a[i]);
		}
	
		int pageCount4 = (int)Math.ceil(246.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer41,recordSelectPointer41);
	
		String[] ConditionColumns42 = {"c"};
		String[] ConditionColumnsValues42 = {"c1"};
		DBApp.select("h0a", ConditionColumns42, ConditionColumnsValues42);
	
		DBApp.select("h0a");
	
		DBApp.select("h0a");
	
		DBApp.select("h0a");
	
		String[] ConditionColumns46 = {"c","f","e","a"};
		String[] ConditionColumnsValues46 = {"c2","f2","e3","a38"};
		DBApp.select("h0a", ConditionColumns46, ConditionColumnsValues46);
	
		String[] ConditionColumns47 = {"c"};
		String[] ConditionColumnsValues47 = {"c1"};
		DBApp.select("h0a", ConditionColumns47, ConditionColumnsValues47);
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer48,recordSelectPointer48);
	
		String[] ConditionColumns49 = {"d"};
		String[] ConditionColumnsValues49 = {"d3"};
		DBApp.select("h0a", ConditionColumns49, ConditionColumnsValues49);
	
		int pageSelectPointer410 = genRandNum(pageCount4)-1;
		int recordSelectPointer410 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer410==pageCount4-1)
			recordSelectPointer410 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer410,recordSelectPointer410);
	
		int pageSelectPointer411 = genRandNum(pageCount4)-1;
		int recordSelectPointer411 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer411==pageCount4-1)
			recordSelectPointer411 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer411,recordSelectPointer411);
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer412,recordSelectPointer412);
	
		String[] ConditionColumns413 = {"d"};
		String[] ConditionColumnsValues413 = {"d3"};
		DBApp.select("h0a", ConditionColumns413, ConditionColumnsValues413);
	
		int pageSelectPointer414 = genRandNum(pageCount4)-1;
		int recordSelectPointer414 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer414==pageCount4-1)
			recordSelectPointer414 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer414,recordSelectPointer414);
	
		DBApp.select("h0a");
	
		DBApp.select("h0a");
	
		String[] ConditionColumns417 = {"e","c"};
		String[] ConditionColumnsValues417 = {"e0","c1"};
		DBApp.select("h0a", ConditionColumns417, ConditionColumnsValues417);
	
		int pageSelectPointer418 = genRandNum(pageCount4)-1;
		int recordSelectPointer418 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer418==pageCount4-1)
			recordSelectPointer418 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer418,recordSelectPointer418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer419,recordSelectPointer419);
	
		int pageSelectPointer420 = genRandNum(pageCount4)-1;
		int recordSelectPointer420 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer420==pageCount4-1)
			recordSelectPointer420 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer420,recordSelectPointer420);
	
		String[] ConditionColumns421 = {"b"};
		String[] ConditionColumnsValues421 = {"b0"};
		DBApp.select("h0a", ConditionColumns421, ConditionColumnsValues421);
	
		int pageSelectPointer422 = genRandNum(pageCount4)-1;
		int recordSelectPointer422 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer422==pageCount4-1)
			recordSelectPointer422 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer422,recordSelectPointer422);
	
		DBApp.select("h0a");
	
		String[] ConditionColumns424 = {"d"};
		String[] ConditionColumnsValues424 = {"d1"};
		DBApp.select("h0a", ConditionColumns424, ConditionColumnsValues424);
	
		int pageSelectPointer425 = genRandNum(pageCount4)-1;
		int recordSelectPointer425 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer425==pageCount4-1)
			recordSelectPointer425 = genRandNum((246%DBApp.dataPageSize))-1;
		DBApp.select("h0a", pageSelectPointer425,recordSelectPointer425);
	
		DBApp.select("h0a");
	
		String selectTrace4 = DBApp.getFullTrace("h0a");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 275);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 246"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x6", cols0);
		String [][] records_x6 = new String[186][cols0.length];
		for(int i=0;i<186;i++)
		{
			records_x6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x6", records_x6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(186.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("x6");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"g"};
		String[] ConditionColumnsValues05 = {"g1"};
		DBApp.select("x6", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c2"};
		DBApp.select("x6", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"c"};
		String[] ConditionColumnsValues010 = {"c0"};
		DBApp.select("x6", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("x6");
	
		DBApp.select("x6");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("x6");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("x6");
	
		String[] ConditionColumns017 = {"a"};
		String[] ConditionColumnsValues017 = {"a55"};
		DBApp.select("x6", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"g"};
		String[] ConditionColumnsValues018 = {"g2"};
		DBApp.select("x6", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"d","b"};
		String[] ConditionColumnsValues019 = {"d0","b0"};
		DBApp.select("x6", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("x6");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("x6");
	
		DBApp.select("x6");
	
		String[] ConditionColumns026 = {"c","f","e"};
		String[] ConditionColumnsValues026 = {"c0","f3","e3"};
		DBApp.select("x6", ConditionColumns026, ConditionColumnsValues026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer027,recordSelectPointer027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer028,recordSelectPointer028);
	
		int pageSelectPointer029 = genRandNum(pageCount0)-1;
		int recordSelectPointer029 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer029==pageCount0-1)
			recordSelectPointer029 = genRandNum((186%DBApp.dataPageSize))-1;
		DBApp.select("x6", pageSelectPointer029,recordSelectPointer029);
	
		String selectTrace0 = DBApp.getFullTrace("x6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 218);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 186"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j0", cols0);
		String [][] records_j0 = new String[481][cols0.length];
		for(int i=0;i<481;i++)
		{
			records_j0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j0", records_j0[i]);
		}
	
		int pageCount0 = (int)Math.ceil(481.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		String[] ConditionColumns00 = {"g"};
		String[] ConditionColumnsValues00 = {"g4"};
		DBApp.select("j0", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c0"};
		DBApp.select("j0", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((481%DBApp.dataPageSize))-1;
		DBApp.select("j0", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"g","a","f"};
		String[] ConditionColumnsValues03 = {"g0","a315","f3"};
		DBApp.select("j0", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"b","d","f","c"};
		String[] ConditionColumnsValues04 = {"b0","d2","f0","c0"};
		DBApp.select("j0", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("j0");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 488);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 481"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("k5", cols1);
		String [][] records_k5 = new String[478][cols1.length];
		for(int i=0;i<478;i++)
		{
			records_k5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k5", records_k5[i]);
		}
	
		int pageCount1 = (int)Math.ceil(478.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c0"};
		DBApp.select("k5", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b0"};
		DBApp.select("k5", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"c"};
		String[] ConditionColumnsValues13 = {"c2"};
		DBApp.select("k5", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"a"};
		String[] ConditionColumnsValues14 = {"a341"};
		DBApp.select("k5", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("k5");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("k5");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("k5");
	
		DBApp.select("k5");
	
		String[] ConditionColumns116 = {"a"};
		String[] ConditionColumnsValues116 = {"a126"};
		DBApp.select("k5", ConditionColumns116, ConditionColumnsValues116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((478%DBApp.dataPageSize))-1;
		DBApp.select("k5", pageSelectPointer117,recordSelectPointer117);
	
		String[] ConditionColumns118 = {"a"};
		String[] ConditionColumnsValues118 = {"a283"};
		DBApp.select("k5", ConditionColumns118, ConditionColumnsValues118);
	
		String selectTrace1 = DBApp.getFullTrace("k5");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 499);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 478"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mws", cols2);
		String [][] records_mws = new String[237][cols2.length];
		for(int i=0;i<237;i++)
		{
			records_mws[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mws[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mws", records_mws[i]);
		}
	
		int pageCount2 = (int)Math.ceil(237.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("mws");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"i","k","m"};
		String[] ConditionColumnsValues22 = {"i3","k8","m12"};
		DBApp.select("mws", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("mws");
	
		String[] ConditionColumns24 = {"h","c","j","l","d","b"};
		String[] ConditionColumnsValues24 = {"h7","c2","j5","l11","d3","b1"};
		DBApp.select("mws", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("mws");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"h"};
		String[] ConditionColumnsValues28 = {"h6"};
		DBApp.select("mws", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"e","g","d","j","h","f"};
		String[] ConditionColumnsValues29 = {"e3","g1","d3","j3","h7","f3"};
		DBApp.select("mws", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("mws");
	
		DBApp.select("mws");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("mws");
	
		DBApp.select("mws");
	
		String[] ConditionColumns216 = {"c"};
		String[] ConditionColumnsValues216 = {"c2"};
		DBApp.select("mws", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"j","k","b","i","d"};
		String[] ConditionColumnsValues217 = {"j8","k1","b0","i8","d0"};
		DBApp.select("mws", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer218,recordSelectPointer218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer219,recordSelectPointer219);
	
		String[] ConditionColumns220 = {"a","b","j","k","i","e"};
		String[] ConditionColumnsValues220 = {"a67","b1","j7","k1","i4","e2"};
		DBApp.select("mws", ConditionColumns220, ConditionColumnsValues220);
	
		DBApp.select("mws");
	
		String[] ConditionColumns222 = {"l"};
		String[] ConditionColumnsValues222 = {"l11"};
		DBApp.select("mws", ConditionColumns222, ConditionColumnsValues222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("mws", pageSelectPointer223,recordSelectPointer223);
	
		DBApp.select("mws");
	
		String selectTrace2 = DBApp.getFullTrace("mws");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 264);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 237"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z73c", cols3);
		String [][] records_z73c = new String[237][cols3.length];
		for(int i=0;i<237;i++)
		{
			records_z73c[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z73c[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z73c", records_z73c[i]);
		}
	
		int pageCount3 = (int)Math.ceil(237.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		DBApp.select("z73c");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("z73c", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("z73c");
	
		DBApp.select("z73c");
	
		DBApp.select("z73c");
	
		DBApp.select("z73c");
	
		String[] ConditionColumns36 = {"m"};
		String[] ConditionColumnsValues36 = {"m9"};
		DBApp.select("z73c", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("z73c");
	
		DBApp.select("z73c");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("z73c", pageSelectPointer39,recordSelectPointer39);
	
		String[] ConditionColumns310 = {"b"};
		String[] ConditionColumnsValues310 = {"b1"};
		DBApp.select("z73c", ConditionColumns310, ConditionColumnsValues310);
	
		String selectTrace3 = DBApp.getFullTrace("z73c");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 250);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 237"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ca", cols0);
		String [][] records_ca = new String[377][cols0.length];
		for(int i=0;i<377;i++)
		{
			records_ca[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ca[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ca", records_ca[i]);
		}
	
		int pageCount0 = (int)Math.ceil(377.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"i","h"};
		String[] ConditionColumnsValues07 = {"i2","h7"};
		DBApp.select("ca", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("ca");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"g"};
		String[] ConditionColumnsValues011 = {"g4"};
		DBApp.select("ca", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("ca");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"h"};
		String[] ConditionColumnsValues018 = {"h6"};
		DBApp.select("ca", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer019,recordSelectPointer019);
	
		DBApp.select("ca");
	
		DBApp.select("ca");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("ca");
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer025,recordSelectPointer025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer027,recordSelectPointer027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((377%DBApp.dataPageSize))-1;
		DBApp.select("ca", pageSelectPointer028,recordSelectPointer028);
	
		String[] ConditionColumns029 = {"e"};
		String[] ConditionColumnsValues029 = {"e3"};
		DBApp.select("ca", ConditionColumns029, ConditionColumnsValues029);
	
		String selectTrace0 = DBApp.getFullTrace("ca");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 409);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 377"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("mgt6", cols0);
		String [][] records_mgt6 = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			records_mgt6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mgt6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mgt6", records_mgt6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(87.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		String[] ConditionColumns00 = {"d","a"};
		String[] ConditionColumnsValues00 = {"d1","a61"};
		DBApp.select("mgt6", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((87%DBApp.dataPageSize))-1;
		DBApp.select("mgt6", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((87%DBApp.dataPageSize))-1;
		DBApp.select("mgt6", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b1"};
		DBApp.select("mgt6", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"b","c"};
		String[] ConditionColumnsValues04 = {"b0","c1"};
		DBApp.select("mgt6", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c2"};
		DBApp.select("mgt6", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("mgt6");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((87%DBApp.dataPageSize))-1;
		DBApp.select("mgt6", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("mgt6");
	
		String[] ConditionColumns09 = {"d"};
		String[] ConditionColumnsValues09 = {"d2"};
		DBApp.select("mgt6", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((87%DBApp.dataPageSize))-1;
		DBApp.select("mgt6", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"d"};
		String[] ConditionColumnsValues011 = {"d0"};
		DBApp.select("mgt6", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("mgt6");
	
		String[] ConditionColumns013 = {"b"};
		String[] ConditionColumnsValues013 = {"b0"};
		DBApp.select("mgt6", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("mgt6");
	
		String[] ConditionColumns015 = {"b"};
		String[] ConditionColumnsValues015 = {"b1"};
		DBApp.select("mgt6", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((87%DBApp.dataPageSize))-1;
		DBApp.select("mgt6", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("mgt6");
	
		DBApp.select("mgt6");
	
		String selectTrace0 = DBApp.getFullTrace("mgt6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 108);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 87"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("a9", cols1);
		String [][] records_a9 = new String[249][cols1.length];
		for(int i=0;i<249;i++)
		{
			records_a9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a9", records_a9[i]);
		}
	
		int pageCount1 = (int)Math.ceil(249.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a219"};
		DBApp.select("a9", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"c"};
		String[] ConditionColumnsValues13 = {"c2"};
		DBApp.select("a9", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("a9");
	
		String[] ConditionColumns15 = {"d"};
		String[] ConditionColumnsValues15 = {"d1"};
		DBApp.select("a9", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("a9");
	
		String[] ConditionColumns19 = {"d"};
		String[] ConditionColumnsValues19 = {"d2"};
		DBApp.select("a9", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("a9");
	
		DBApp.select("a9");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"c"};
		String[] ConditionColumnsValues114 = {"c1"};
		DBApp.select("a9", ConditionColumns114, ConditionColumnsValues114);
	
		DBApp.select("a9");
	
		String[] ConditionColumns116 = {"d"};
		String[] ConditionColumnsValues116 = {"d2"};
		DBApp.select("a9", ConditionColumns116, ConditionColumnsValues116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("a9");
	
		String[] ConditionColumns119 = {"b"};
		String[] ConditionColumnsValues119 = {"b1"};
		DBApp.select("a9", ConditionColumns119, ConditionColumnsValues119);
	
		String[] ConditionColumns120 = {"b"};
		String[] ConditionColumnsValues120 = {"b1"};
		DBApp.select("a9", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b0"};
		DBApp.select("a9", ConditionColumns121, ConditionColumnsValues121);
	
		String[] ConditionColumns122 = {"c","b"};
		String[] ConditionColumnsValues122 = {"c0","b1"};
		DBApp.select("a9", ConditionColumns122, ConditionColumnsValues122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("a9", pageSelectPointer123,recordSelectPointer123);
	
		String selectTrace1 = DBApp.getFullTrace("a9");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 275);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 249"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("wm681", cols2);
		String [][] records_wm681 = new String[298][cols2.length];
		for(int i=0;i<298;i++)
		{
			records_wm681[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wm681[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wm681", records_wm681[i]);
		}
	
		int pageCount2 = (int)Math.ceil(298.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns20 = {"j"};
		String[] ConditionColumnsValues20 = {"j7"};
		DBApp.select("wm681", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((298%DBApp.dataPageSize))-1;
		DBApp.select("wm681", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((298%DBApp.dataPageSize))-1;
		DBApp.select("wm681", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"j"};
		String[] ConditionColumnsValues23 = {"j8"};
		DBApp.select("wm681", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"b","d","a"};
		String[] ConditionColumnsValues24 = {"b0","d0","a116"};
		DBApp.select("wm681", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"h","g","d","c"};
		String[] ConditionColumnsValues25 = {"h4","g5","d0","c1"};
		DBApp.select("wm681", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("wm681");
	
		DBApp.select("wm681");
	
		DBApp.select("wm681");
	
		DBApp.select("wm681");
	
		String[] ConditionColumns210 = {"b"};
		String[] ConditionColumnsValues210 = {"b1"};
		DBApp.select("wm681", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a297"};
		DBApp.select("wm681", ConditionColumns211, ConditionColumnsValues211);
	
		DBApp.select("wm681");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((298%DBApp.dataPageSize))-1;
		DBApp.select("wm681", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"a"};
		String[] ConditionColumnsValues214 = {"a130"};
		DBApp.select("wm681", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"g"};
		String[] ConditionColumnsValues215 = {"g2"};
		DBApp.select("wm681", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("wm681");
	
		String[] ConditionColumns217 = {"g","b","i"};
		String[] ConditionColumnsValues217 = {"g5","b1","i7"};
		DBApp.select("wm681", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("wm681");
	
		String[] ConditionColumns219 = {"f","d","b"};
		String[] ConditionColumnsValues219 = {"f1","d3","b1"};
		DBApp.select("wm681", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"d","b","a","c","f"};
		String[] ConditionColumnsValues220 = {"d1","b1","a85","c1","f1"};
		DBApp.select("wm681", ConditionColumns220, ConditionColumnsValues220);
	
		DBApp.select("wm681");
	
		DBApp.select("wm681");
	
		DBApp.select("wm681");
	
		String[] ConditionColumns224 = {"g","e"};
		String[] ConditionColumnsValues224 = {"g4","e3"};
		DBApp.select("wm681", ConditionColumns224, ConditionColumnsValues224);
	
		String selectTrace2 = DBApp.getFullTrace("wm681");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 325);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 298"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ruz", cols3);
		String [][] records_ruz = new String[86][cols3.length];
		for(int i=0;i<86;i++)
		{
			records_ruz[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ruz[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ruz", records_ruz[i]);
		}
	
		int pageCount3 = (int)Math.ceil(86.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns30 = {"d","i","f","b","m","e","k"};
		String[] ConditionColumnsValues30 = {"d2","i3","f0","b0","m4","e0","k8"};
		DBApp.select("ruz", ConditionColumns30, ConditionColumnsValues30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("ruz");
	
		String[] ConditionColumns33 = {"f"};
		String[] ConditionColumnsValues33 = {"f1"};
		DBApp.select("ruz", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("ruz");
	
		DBApp.select("ruz");
	
		DBApp.select("ruz");
	
		String[] ConditionColumns38 = {"b"};
		String[] ConditionColumnsValues38 = {"b1"};
		DBApp.select("ruz", ConditionColumns38, ConditionColumnsValues38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("ruz");
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer311,recordSelectPointer311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer312,recordSelectPointer312);
	
		DBApp.select("ruz");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("ruz");
	
		String[] ConditionColumns316 = {"j"};
		String[] ConditionColumnsValues316 = {"j9"};
		DBApp.select("ruz", ConditionColumns316, ConditionColumnsValues316);
	
		String[] ConditionColumns317 = {"c"};
		String[] ConditionColumnsValues317 = {"c0"};
		DBApp.select("ruz", ConditionColumns317, ConditionColumnsValues317);
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((86%DBApp.dataPageSize))-1;
		DBApp.select("ruz", pageSelectPointer318,recordSelectPointer318);
	
		DBApp.select("ruz");
	
		DBApp.select("ruz");
	
		String selectTrace3 = DBApp.getFullTrace("ruz");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 109);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 86"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("muw1", cols0);
		String [][] records_muw1 = new String[427][cols0.length];
		for(int i=0;i<427;i++)
		{
			records_muw1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_muw1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("muw1", records_muw1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(427.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("muw1");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("muw1", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("muw1", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("muw1");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("muw1", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("muw1");
	
		DBApp.select("muw1");
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c2"};
		DBApp.select("muw1", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("muw1");
	
		String[] ConditionColumns09 = {"c"};
		String[] ConditionColumnsValues09 = {"c0"};
		DBApp.select("muw1", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("muw1");
	
		String[] ConditionColumns011 = {"c"};
		String[] ConditionColumnsValues011 = {"c2"};
		DBApp.select("muw1", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("muw1", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("muw1", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"b"};
		String[] ConditionColumnsValues014 = {"b0"};
		DBApp.select("muw1", ConditionColumns014, ConditionColumnsValues014);
	
		String selectTrace0 = DBApp.getFullTrace("muw1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 444);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 427"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("h7b37", cols1);
		String [][] records_h7b37 = new String[105][cols1.length];
		for(int i=0;i<105;i++)
		{
			records_h7b37[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h7b37[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h7b37", records_h7b37[i]);
		}
	
		int pageCount1 = (int)Math.ceil(105.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("h7b37");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d2"};
		DBApp.select("h7b37", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"d"};
		String[] ConditionColumnsValues13 = {"d0"};
		DBApp.select("h7b37", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("h7b37");
	
		String[] ConditionColumns15 = {"d"};
		String[] ConditionColumnsValues15 = {"d1"};
		DBApp.select("h7b37", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("h7b37");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("h7b37");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("h7b37");
	
		String[] ConditionColumns113 = {"d"};
		String[] ConditionColumnsValues113 = {"d1"};
		DBApp.select("h7b37", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("h7b37");
	
		String[] ConditionColumns116 = {"c","d"};
		String[] ConditionColumnsValues116 = {"c0","d3"};
		DBApp.select("h7b37", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b1"};
		DBApp.select("h7b37", ConditionColumns117, ConditionColumnsValues117);
	
		DBApp.select("h7b37");
	
		DBApp.select("h7b37");
	
		String[] ConditionColumns120 = {"b"};
		String[] ConditionColumnsValues120 = {"b0"};
		DBApp.select("h7b37", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"d"};
		String[] ConditionColumnsValues121 = {"d3"};
		DBApp.select("h7b37", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer122,recordSelectPointer122);
	
		String[] ConditionColumns123 = {"e"};
		String[] ConditionColumnsValues123 = {"e3"};
		DBApp.select("h7b37", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"c"};
		String[] ConditionColumnsValues124 = {"c1"};
		DBApp.select("h7b37", ConditionColumns124, ConditionColumnsValues124);
	
		String[] ConditionColumns125 = {"e"};
		String[] ConditionColumnsValues125 = {"e0"};
		DBApp.select("h7b37", ConditionColumns125, ConditionColumnsValues125);
	
		String[] ConditionColumns126 = {"e"};
		String[] ConditionColumnsValues126 = {"e2"};
		DBApp.select("h7b37", ConditionColumns126, ConditionColumnsValues126);
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("h7b37", pageSelectPointer127,recordSelectPointer127);
	
		DBApp.select("h7b37");
	
		String[] ConditionColumns129 = {"b","e"};
		String[] ConditionColumnsValues129 = {"b0","e4"};
		DBApp.select("h7b37", ConditionColumns129, ConditionColumnsValues129);
	
		String selectTrace1 = DBApp.getFullTrace("h7b37");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 137);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 105"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("fk", cols2);
		String [][] records_fk = new String[286][cols2.length];
		for(int i=0;i<286;i++)
		{
			records_fk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fk", records_fk[i]);
		}
	
		int pageCount2 = (int)Math.ceil(286.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("fk");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"j"};
		String[] ConditionColumnsValues22 = {"j8"};
		DBApp.select("fk", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"j","h","a"};
		String[] ConditionColumnsValues23 = {"j1","h7","a191"};
		DBApp.select("fk", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("fk");
	
		String[] ConditionColumns28 = {"f"};
		String[] ConditionColumnsValues28 = {"f1"};
		DBApp.select("fk", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"g","b","k","f","i"};
		String[] ConditionColumnsValues29 = {"g4","b0","k4","f2","i5"};
		DBApp.select("fk", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("fk");
	
		DBApp.select("fk");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("fk", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"c","i","e","b","h"};
		String[] ConditionColumnsValues215 = {"c1","i4","e1","b1","h7"};
		DBApp.select("fk", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("fk");
	
		String[] ConditionColumns217 = {"f"};
		String[] ConditionColumnsValues217 = {"f3"};
		DBApp.select("fk", ConditionColumns217, ConditionColumnsValues217);
	
		String selectTrace2 = DBApp.getFullTrace("fk");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 306);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 286"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("km3g8", cols3);
		String [][] records_km3g8 = new String[426][cols3.length];
		for(int i=0;i<426;i++)
		{
			records_km3g8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_km3g8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("km3g8", records_km3g8[i]);
		}
	
		int pageCount3 = (int)Math.ceil(426.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("km3g8");
	
		String[] ConditionColumns31 = {"k","e","h"};
		String[] ConditionColumnsValues31 = {"k7","e0","h1"};
		DBApp.select("km3g8", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"i"};
		String[] ConditionColumnsValues32 = {"i2"};
		DBApp.select("km3g8", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("km3g8");
	
		DBApp.select("km3g8");
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("km3g8");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"m"};
		String[] ConditionColumnsValues39 = {"m3"};
		DBApp.select("km3g8", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"c"};
		String[] ConditionColumnsValues310 = {"c2"};
		DBApp.select("km3g8", ConditionColumns310, ConditionColumnsValues310);
	
		DBApp.select("km3g8");
	
		DBApp.select("km3g8");
	
		DBApp.select("km3g8");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"h","i","b"};
		String[] ConditionColumnsValues315 = {"h4","i6","b0"};
		DBApp.select("km3g8", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("km3g8");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer317,recordSelectPointer317);
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer318,recordSelectPointer318);
	
		String[] ConditionColumns319 = {"b"};
		String[] ConditionColumnsValues319 = {"b1"};
		DBApp.select("km3g8", ConditionColumns319, ConditionColumnsValues319);
	
		String[] ConditionColumns320 = {"i"};
		String[] ConditionColumnsValues320 = {"i7"};
		DBApp.select("km3g8", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"f"};
		String[] ConditionColumnsValues321 = {"f0"};
		DBApp.select("km3g8", ConditionColumns321, ConditionColumnsValues321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((426%DBApp.dataPageSize))-1;
		DBApp.select("km3g8", pageSelectPointer322,recordSelectPointer322);
	
		String selectTrace3 = DBApp.getFullTrace("km3g8");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 451);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 426"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("l03", cols0);
		String [][] records_l03 = new String[214][cols0.length];
		for(int i=0;i<214;i++)
		{
			records_l03[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l03[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l03", records_l03[i]);
		}
	
		int pageCount0 = (int)Math.ceil(214.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		String[] ConditionColumns00 = {"d","e"};
		String[] ConditionColumnsValues00 = {"d2","e2"};
		DBApp.select("l03", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"e","d"};
		String[] ConditionColumnsValues01 = {"e0","d3"};
		DBApp.select("l03", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("l03");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("l03");
	
		DBApp.select("l03");
	
		DBApp.select("l03");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"f"};
		String[] ConditionColumnsValues08 = {"f3"};
		DBApp.select("l03", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("l03");
	
		DBApp.select("l03");
	
		String[] ConditionColumns011 = {"d"};
		String[] ConditionColumnsValues011 = {"d3"};
		DBApp.select("l03", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("l03");
	
		DBApp.select("l03");
	
		String[] ConditionColumns014 = {"f"};
		String[] ConditionColumnsValues014 = {"f3"};
		DBApp.select("l03", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"f"};
		String[] ConditionColumnsValues016 = {"f0"};
		DBApp.select("l03", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"b"};
		String[] ConditionColumnsValues020 = {"b0"};
		DBApp.select("l03", ConditionColumns020, ConditionColumnsValues020);
	
		String[] ConditionColumns021 = {"d"};
		String[] ConditionColumnsValues021 = {"d3"};
		DBApp.select("l03", ConditionColumns021, ConditionColumnsValues021);
	
		String[] ConditionColumns022 = {"f","d"};
		String[] ConditionColumnsValues022 = {"f1","d3"};
		DBApp.select("l03", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer023,recordSelectPointer023);
	
		String[] ConditionColumns024 = {"d","f"};
		String[] ConditionColumnsValues024 = {"d2","f0"};
		DBApp.select("l03", ConditionColumns024, ConditionColumnsValues024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer025,recordSelectPointer025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((214%DBApp.dataPageSize))-1;
		DBApp.select("l03", pageSelectPointer026,recordSelectPointer026);
	
		DBApp.select("l03");
	
		String[] ConditionColumns028 = {"b"};
		String[] ConditionColumnsValues028 = {"b1"};
		DBApp.select("l03", ConditionColumns028, ConditionColumnsValues028);
	
		DBApp.select("l03");
	
		String selectTrace0 = DBApp.getFullTrace("l03");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 246);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 214"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cc", cols1);
		String [][] records_cc = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_cc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cc", records_cc[i]);
		}
	
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		String[] ConditionColumns10 = {"e"};
		String[] ConditionColumnsValues10 = {"e3"};
		DBApp.select("cc", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("cc");
	
		DBApp.select("cc");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("cc");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("cc");
	
		String[] ConditionColumns18 = {"i"};
		String[] ConditionColumnsValues18 = {"i1"};
		DBApp.select("cc", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"d","h","g"};
		String[] ConditionColumnsValues110 = {"d0","h0","g1"};
		DBApp.select("cc", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("cc");
	
		String[] ConditionColumns112 = {"d","c"};
		String[] ConditionColumnsValues112 = {"d3","c1"};
		DBApp.select("cc", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("cc");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer117,recordSelectPointer117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("cc", pageSelectPointer119,recordSelectPointer119);
	
		String selectTrace1 = DBApp.getFullTrace("cc");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 31);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 9"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vl", cols2);
		String [][] records_vl = new String[423][cols2.length];
		for(int i=0;i<423;i++)
		{
			records_vl[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vl[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vl", records_vl[i]);
		}
	
		int pageCount2 = (int)Math.ceil(423.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("vl");
	
		DBApp.select("vl");
	
		DBApp.select("vl");
	
		DBApp.select("vl");
	
		DBApp.select("vl");
	
		String[] ConditionColumns25 = {"c"};
		String[] ConditionColumnsValues25 = {"c2"};
		DBApp.select("vl", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"g"};
		String[] ConditionColumnsValues26 = {"g1"};
		DBApp.select("vl", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("vl");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"f"};
		String[] ConditionColumnsValues210 = {"f0"};
		DBApp.select("vl", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"b"};
		String[] ConditionColumnsValues212 = {"b1"};
		DBApp.select("vl", ConditionColumns212, ConditionColumnsValues212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("vl");
	
		String[] ConditionColumns215 = {"f"};
		String[] ConditionColumnsValues215 = {"f3"};
		DBApp.select("vl", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer216,recordSelectPointer216);
	
		DBApp.select("vl");
	
		String[] ConditionColumns218 = {"e"};
		String[] ConditionColumnsValues218 = {"e2"};
		DBApp.select("vl", ConditionColumns218, ConditionColumnsValues218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer219,recordSelectPointer219);
	
		DBApp.select("vl");
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((423%DBApp.dataPageSize))-1;
		DBApp.select("vl", pageSelectPointer221,recordSelectPointer221);
	
		String selectTrace2 = DBApp.getFullTrace("vl");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 447);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 423"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("dzsig", cols0);
		String [][] records_dzsig = new String[190][cols0.length];
		for(int i=0;i<190;i++)
		{
			records_dzsig[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dzsig[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dzsig", records_dzsig[i]);
		}
	
		int pageCount0 = (int)Math.ceil(190.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("dzsig", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("dzsig");
	
		DBApp.select("dzsig");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("dzsig");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("dzsig");
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("dzsig", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"b"};
		String[] ConditionColumnsValues011 = {"b0"};
		DBApp.select("dzsig", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a152"};
		DBApp.select("dzsig", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((190%DBApp.dataPageSize))-1;
		DBApp.select("dzsig", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("dzsig");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 209);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 190"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("upj", cols1);
		String [][] records_upj = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_upj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_upj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("upj", records_upj[i]);
		}
	
		int pageCount1 = (int)Math.ceil(9.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("upj", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("upj", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("upj", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a7"};
		DBApp.select("upj", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"a"};
		String[] ConditionColumnsValues14 = {"a1"};
		DBApp.select("upj", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"a"};
		String[] ConditionColumnsValues15 = {"a3"};
		DBApp.select("upj", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a8"};
		DBApp.select("upj", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("upj");
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a5"};
		DBApp.select("upj", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("upj");
	
		String[] ConditionColumns110 = {"a"};
		String[] ConditionColumnsValues110 = {"a3"};
		DBApp.select("upj", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"a"};
		String[] ConditionColumnsValues111 = {"a8"};
		DBApp.select("upj", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a4"};
		DBApp.select("upj", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("upj");
	
		DBApp.select("upj");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((9%DBApp.dataPageSize))-1;
		DBApp.select("upj", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("upj");
	
		String[] ConditionColumns117 = {"a"};
		String[] ConditionColumnsValues117 = {"a1"};
		DBApp.select("upj", ConditionColumns117, ConditionColumnsValues117);
	
		String selectTrace1 = DBApp.getFullTrace("upj");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 29);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 9"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ez", cols2);
		String [][] records_ez = new String[227][cols2.length];
		for(int i=0;i<227;i++)
		{
			records_ez[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ez[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ez", records_ez[i]);
		}
	
		int pageCount2 = (int)Math.ceil(227.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"e"};
		String[] ConditionColumnsValues22 = {"e0"};
		DBApp.select("ez", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("ez");
	
		DBApp.select("ez");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a107"};
		DBApp.select("ez", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("ez");
	
		String[] ConditionColumns29 = {"h","a","g","f"};
		String[] ConditionColumnsValues29 = {"h5","a77","g0","f5"};
		DBApp.select("ez", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("ez");
	
		String[] ConditionColumns211 = {"b","f"};
		String[] ConditionColumnsValues211 = {"b1","f1"};
		DBApp.select("ez", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"h","e","c"};
		String[] ConditionColumnsValues212 = {"h7","e1","c2"};
		DBApp.select("ez", ConditionColumns212, ConditionColumnsValues212);
	
		String[] ConditionColumns213 = {"c"};
		String[] ConditionColumnsValues213 = {"c2"};
		DBApp.select("ez", ConditionColumns213, ConditionColumnsValues213);
	
		DBApp.select("ez");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"f","e"};
		String[] ConditionColumnsValues216 = {"f3","e2"};
		DBApp.select("ez", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"h","b"};
		String[] ConditionColumnsValues217 = {"h6","b0"};
		DBApp.select("ez", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer218,recordSelectPointer218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ez", pageSelectPointer219,recordSelectPointer219);
	
		String selectTrace2 = DBApp.getFullTrace("ez");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 249);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 227"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("s0e", cols0);
		String [][] records_s0e = new String[168][cols0.length];
		for(int i=0;i<168;i++)
		{
			records_s0e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s0e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s0e", records_s0e[i]);
		}
	
		int pageCount0 = (int)Math.ceil(168.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("s0e");
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e2"};
		DBApp.select("s0e", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"d"};
		String[] ConditionColumnsValues02 = {"d1"};
		DBApp.select("s0e", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("s0e");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("s0e", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"a","d"};
		String[] ConditionColumnsValues05 = {"a81","d1"};
		DBApp.select("s0e", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((168%DBApp.dataPageSize))-1;
		DBApp.select("s0e", pageSelectPointer06,recordSelectPointer06);
	
		String selectTrace0 = DBApp.getFullTrace("s0e");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 177);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 168"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("jvxf", cols1);
		String [][] records_jvxf = new String[263][cols1.length];
		for(int i=0;i<263;i++)
		{
			records_jvxf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jvxf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jvxf", records_jvxf[i]);
		}
	
		int pageCount1 = (int)Math.ceil(263.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("jvxf");
	
		String[] ConditionColumns11 = {"f"};
		String[] ConditionColumnsValues11 = {"f5"};
		DBApp.select("jvxf", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("jvxf");
	
		DBApp.select("jvxf");
	
		String[] ConditionColumns14 = {"g","c","i","b"};
		String[] ConditionColumnsValues14 = {"g4","c1","i4","b0"};
		DBApp.select("jvxf", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("jvxf");
	
		DBApp.select("jvxf");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"f"};
		String[] ConditionColumnsValues18 = {"f1"};
		DBApp.select("jvxf", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"e"};
		String[] ConditionColumnsValues19 = {"e2"};
		DBApp.select("jvxf", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"g"};
		String[] ConditionColumnsValues110 = {"g1"};
		DBApp.select("jvxf", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("jvxf");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("jvxf");
	
		String[] ConditionColumns114 = {"c","i","h","g"};
		String[] ConditionColumnsValues114 = {"c1","i7","h6","g0"};
		DBApp.select("jvxf", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"i"};
		String[] ConditionColumnsValues115 = {"i1"};
		DBApp.select("jvxf", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"f","d","e"};
		String[] ConditionColumnsValues117 = {"f0","d2","e3"};
		DBApp.select("jvxf", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer119,recordSelectPointer119);
	
		DBApp.select("jvxf");
	
		DBApp.select("jvxf");
	
		DBApp.select("jvxf");
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer123,recordSelectPointer123);
	
		DBApp.select("jvxf");
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer125,recordSelectPointer125);
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("jvxf", pageSelectPointer126,recordSelectPointer126);
	
		String selectTrace1 = DBApp.getFullTrace("jvxf");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 292);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 263"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wjfif", cols2);
		String [][] records_wjfif = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_wjfif[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wjfif[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wjfif", records_wjfif[i]);
		}
	
		int pageCount2 = (int)Math.ceil(46.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"f","e","c"};
		String[] ConditionColumnsValues22 = {"f0","e4","c0"};
		DBApp.select("wjfif", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("wjfif");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("wjfif");
	
		DBApp.select("wjfif");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"g"};
		String[] ConditionColumnsValues29 = {"g3"};
		DBApp.select("wjfif", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("wjfif");
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a5"};
		DBApp.select("wjfif", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("wjfif", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("wjfif");
	
		String[] ConditionColumns214 = {"h","g","a"};
		String[] ConditionColumnsValues214 = {"h4","g4","a4"};
		DBApp.select("wjfif", ConditionColumns214, ConditionColumnsValues214);
	
		DBApp.select("wjfif");
	
		String selectTrace2 = DBApp.getFullTrace("wjfif");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 64);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 46"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gbb", cols3);
		String [][] records_gbb = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			records_gbb[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gbb[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gbb", records_gbb[i]);
		}
	
		int pageCount3 = (int)Math.ceil(24.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("gbb");
	
		DBApp.select("gbb");
	
		DBApp.select("gbb");
	
		String[] ConditionColumns33 = {"e","h","g","a"};
		String[] ConditionColumnsValues33 = {"e3","h3","g3","a3"};
		DBApp.select("gbb", ConditionColumns33, ConditionColumnsValues33);
	
		DBApp.select("gbb");
	
		DBApp.select("gbb");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((24%DBApp.dataPageSize))-1;
		DBApp.select("gbb", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("gbb");
	
		DBApp.select("gbb");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((24%DBApp.dataPageSize))-1;
		DBApp.select("gbb", pageSelectPointer39,recordSelectPointer39);
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((24%DBApp.dataPageSize))-1;
		DBApp.select("gbb", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("gbb");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((24%DBApp.dataPageSize))-1;
		DBApp.select("gbb", pageSelectPointer312,recordSelectPointer312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((24%DBApp.dataPageSize))-1;
		DBApp.select("gbb", pageSelectPointer313,recordSelectPointer313);
	
		DBApp.select("gbb");
	
		String selectTrace3 = DBApp.getFullTrace("gbb");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 41);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 24"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dtw19", cols4);
		String [][] records_dtw19 = new String[241][cols4.length];
		for(int i=0;i<241;i++)
		{
			records_dtw19[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dtw19[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dtw19", records_dtw19[i]);
		}
	
		int pageCount4 = (int)Math.ceil(241.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		String[] ConditionColumns40 = {"c"};
		String[] ConditionColumnsValues40 = {"c0"};
		DBApp.select("dtw19", ConditionColumns40, ConditionColumnsValues40);
	
		String[] ConditionColumns41 = {"f","e"};
		String[] ConditionColumnsValues41 = {"f2","e1"};
		DBApp.select("dtw19", ConditionColumns41, ConditionColumnsValues41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((241%DBApp.dataPageSize))-1;
		DBApp.select("dtw19", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"h"};
		String[] ConditionColumnsValues43 = {"h3"};
		DBApp.select("dtw19", ConditionColumns43, ConditionColumnsValues43);
	
		String[] ConditionColumns44 = {"d"};
		String[] ConditionColumnsValues44 = {"d0"};
		DBApp.select("dtw19", ConditionColumns44, ConditionColumnsValues44);
	
		DBApp.select("dtw19");
	
		String[] ConditionColumns46 = {"b","g"};
		String[] ConditionColumnsValues46 = {"b1","g0"};
		DBApp.select("dtw19", ConditionColumns46, ConditionColumnsValues46);
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((241%DBApp.dataPageSize))-1;
		DBApp.select("dtw19", pageSelectPointer47,recordSelectPointer47);
	
		String selectTrace4 = DBApp.getFullTrace("dtw19");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 251);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 241"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s1y9", cols0);
		String [][] records_s1y9 = new String[483][cols0.length];
		for(int i=0;i<483;i++)
		{
			records_s1y9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s1y9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s1y9", records_s1y9[i]);
		}
	
		int pageCount0 = (int)Math.ceil(483.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("s1y9");
	
		DBApp.select("s1y9");
	
		DBApp.select("s1y9");
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b1"};
		DBApp.select("s1y9", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("s1y9");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("s1y9");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("s1y9");
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d1"};
		DBApp.select("s1y9", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"h"};
		String[] ConditionColumnsValues014 = {"h7"};
		DBApp.select("s1y9", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("s1y9");
	
		String[] ConditionColumns016 = {"g","f","d"};
		String[] ConditionColumnsValues016 = {"g5","f5","d1"};
		DBApp.select("s1y9", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer017,recordSelectPointer017);
	
		DBApp.select("s1y9");
	
		String[] ConditionColumns019 = {"g"};
		String[] ConditionColumnsValues019 = {"g0"};
		DBApp.select("s1y9", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("s1y9");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("s1y9");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("s1y9");
	
		DBApp.select("s1y9");
	
		String[] ConditionColumns027 = {"g","c","f"};
		String[] ConditionColumnsValues027 = {"g6","c0","f0"};
		DBApp.select("s1y9", ConditionColumns027, ConditionColumnsValues027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("s1y9", pageSelectPointer028,recordSelectPointer028);
	
		String selectTrace0 = DBApp.getFullTrace("s1y9");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 514);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 483"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("hs", cols1);
		String [][] records_hs = new String[159][cols1.length];
		for(int i=0;i<159;i++)
		{
			records_hs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hs", records_hs[i]);
		}
	
		int pageCount1 = (int)Math.ceil(159.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c0"};
		DBApp.select("hs", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("hs");
	
		DBApp.select("hs");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b1"};
		DBApp.select("hs", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((159%DBApp.dataPageSize))-1;
		DBApp.select("hs", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("hs");
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("hs", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("hs");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((159%DBApp.dataPageSize))-1;
		DBApp.select("hs", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((159%DBApp.dataPageSize))-1;
		DBApp.select("hs", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("hs");
	
		String[] ConditionColumns111 = {"c"};
		String[] ConditionColumnsValues111 = {"c0"};
		DBApp.select("hs", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a152"};
		DBApp.select("hs", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("hs");
	
		String selectTrace1 = DBApp.getFullTrace("hs");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 175);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 159"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("l4", cols2);
		String [][] records_l4 = new String[233][cols2.length];
		for(int i=0;i<233;i++)
		{
			records_l4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l4", records_l4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(233.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		DBApp.select("l4");
	
		DBApp.select("l4");
	
		String[] ConditionColumns22 = {"f"};
		String[] ConditionColumnsValues22 = {"f3"};
		DBApp.select("l4", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"j","b"};
		String[] ConditionColumnsValues23 = {"j3","b1"};
		DBApp.select("l4", ConditionColumns23, ConditionColumnsValues23);
	
		String selectTrace2 = DBApp.getFullTrace("l4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 239);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 233"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("j4e", cols3);
		String [][] records_j4e = new String[285][cols3.length];
		for(int i=0;i<285;i++)
		{
			records_j4e[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j4e[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j4e", records_j4e[i]);
		}
	
		int pageCount3 = (int)Math.ceil(285.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("j4e");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("j4e");
	
		DBApp.select("j4e");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("j4e");
	
		DBApp.select("j4e");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer39,recordSelectPointer39);
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("j4e");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("j4e", pageSelectPointer312,recordSelectPointer312);
	
		String[] ConditionColumns313 = {"b"};
		String[] ConditionColumnsValues313 = {"b1"};
		DBApp.select("j4e", ConditionColumns313, ConditionColumnsValues313);
	
		DBApp.select("j4e");
	
		String[] ConditionColumns315 = {"b"};
		String[] ConditionColumnsValues315 = {"b0"};
		DBApp.select("j4e", ConditionColumns315, ConditionColumnsValues315);
	
		String selectTrace3 = DBApp.getFullTrace("j4e");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 303);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 285"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("p70", cols0);
		String [][] records_p70 = new String[492][cols0.length];
		for(int i=0;i<492;i++)
		{
			records_p70[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p70[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p70", records_p70[i]);
		}
	
		int pageCount0 = (int)Math.ceil(492.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		String[] ConditionColumns00 = {"e"};
		String[] ConditionColumnsValues00 = {"e1"};
		DBApp.select("p70", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e2"};
		DBApp.select("p70", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("p70");
	
		DBApp.select("p70");
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d0"};
		DBApp.select("p70", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c2"};
		DBApp.select("p70", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"c"};
		String[] ConditionColumnsValues06 = {"c1"};
		DBApp.select("p70", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("p70");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("p70");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((492%DBApp.dataPageSize))-1;
		DBApp.select("p70", pageSelectPointer014,recordSelectPointer014);
	
		String selectTrace0 = DBApp.getFullTrace("p70");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 509);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 492"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("m2gr", cols1);
		String [][] records_m2gr = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			records_m2gr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m2gr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m2gr", records_m2gr[i]);
		}
	
		int pageCount1 = (int)Math.ceil(46.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("m2gr");
	
		String[] ConditionColumns11 = {"a"};
		String[] ConditionColumnsValues11 = {"a34"};
		DBApp.select("m2gr", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("m2gr");
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a42"};
		DBApp.select("m2gr", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a45"};
		DBApp.select("m2gr", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("m2gr");
	
		String[] ConditionColumns19 = {"a"};
		String[] ConditionColumnsValues19 = {"a45"};
		DBApp.select("m2gr", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("m2gr");
	
		DBApp.select("m2gr");
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a34"};
		DBApp.select("m2gr", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"a"};
		String[] ConditionColumnsValues113 = {"a25"};
		DBApp.select("m2gr", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a32"};
		DBApp.select("m2gr", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("m2gr");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"a"};
		String[] ConditionColumnsValues120 = {"a44"};
		DBApp.select("m2gr", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"a"};
		String[] ConditionColumnsValues121 = {"a13"};
		DBApp.select("m2gr", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((46%DBApp.dataPageSize))-1;
		DBApp.select("m2gr", pageSelectPointer122,recordSelectPointer122);
	
		String selectTrace1 = DBApp.getFullTrace("m2gr");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 71);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 46"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("ipf3p", cols2);
		String [][] records_ipf3p = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			records_ipf3p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ipf3p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ipf3p", records_ipf3p[i]);
		}
	
		int pageCount2 = (int)Math.ceil(56.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		DBApp.select("ipf3p");
	
		DBApp.select("ipf3p");
	
		String[] ConditionColumns22 = {"a"};
		String[] ConditionColumnsValues22 = {"a14"};
		DBApp.select("ipf3p", ConditionColumns22, ConditionColumnsValues22);
	
		String selectTrace2 = DBApp.getFullTrace("ipf3p");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 61);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 56"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("yo9mr", cols0);
		String [][] records_yo9mr = new String[209][cols0.length];
		for(int i=0;i<209;i++)
		{
			records_yo9mr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yo9mr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yo9mr", records_yo9mr[i]);
		}
	
		int pageCount0 = (int)Math.ceil(209.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("yo9mr");
	
		String[] ConditionColumns02 = {"a","d","h","m","b","l"};
		String[] ConditionColumnsValues02 = {"a85","d1","h5","m7","b1","l1"};
		DBApp.select("yo9mr", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("yo9mr");
	
		String[] ConditionColumns05 = {"i"};
		String[] ConditionColumnsValues05 = {"i1"};
		DBApp.select("yo9mr", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b1"};
		DBApp.select("yo9mr", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("yo9mr");
	
		DBApp.select("yo9mr");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("yo9mr");
	
		String[] ConditionColumns012 = {"a","i","g","j"};
		String[] ConditionColumnsValues012 = {"a144","i0","g4","j4"};
		DBApp.select("yo9mr", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("yo9mr");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("yo9mr");
	
		DBApp.select("yo9mr");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("yo9mr", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"k","f","j","i"};
		String[] ConditionColumnsValues020 = {"k4","f2","j8","i5"};
		DBApp.select("yo9mr", ConditionColumns020, ConditionColumnsValues020);
	
		String selectTrace0 = DBApp.getFullTrace("yo9mr");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 232);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 209"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tl", cols1);
		String [][] records_tl = new String[284][cols1.length];
		for(int i=0;i<284;i++)
		{
			records_tl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tl", records_tl[i]);
		}
	
		int pageCount1 = (int)Math.ceil(284.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns10 = {"j","g","k"};
		String[] ConditionColumnsValues10 = {"j1","g1","k5"};
		DBApp.select("tl", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		String[] ConditionColumns13 = {"f"};
		String[] ConditionColumnsValues13 = {"f0"};
		DBApp.select("tl", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("tl", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("tl");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("tl", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("tl", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		DBApp.select("tl");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("tl", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"j","e","a"};
		String[] ConditionColumnsValues120 = {"j3","e3","a243"};
		DBApp.select("tl", ConditionColumns120, ConditionColumnsValues120);
	
		String selectTrace1 = DBApp.getFullTrace("tl");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 307);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 284"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("en06s", cols0);
		String [][] records_en06s = new String[415][cols0.length];
		for(int i=0;i<415;i++)
		{
			records_en06s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_en06s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("en06s", records_en06s[i]);
		}
	
		int pageCount0 = (int)Math.ceil(415.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("en06s");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("en06s");
	
		String[] ConditionColumns03 = {"e"};
		String[] ConditionColumnsValues03 = {"e2"};
		DBApp.select("en06s", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("en06s");
	
		String[] ConditionColumns05 = {"i","h","e"};
		String[] ConditionColumnsValues05 = {"i0","h0","e0"};
		DBApp.select("en06s", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("en06s");
	
		String[] ConditionColumns08 = {"c"};
		String[] ConditionColumnsValues08 = {"c0"};
		DBApp.select("en06s", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("en06s");
	
		DBApp.select("en06s");
	
		DBApp.select("en06s");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer014,recordSelectPointer014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"i","f","c"};
		String[] ConditionColumnsValues016 = {"i5","f5","c2"};
		DBApp.select("en06s", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"e"};
		String[] ConditionColumnsValues018 = {"e0"};
		DBApp.select("en06s", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("en06s");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("en06s", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("en06s");
	
		String[] ConditionColumns022 = {"b"};
		String[] ConditionColumnsValues022 = {"b0"};
		DBApp.select("en06s", ConditionColumns022, ConditionColumnsValues022);
	
		String selectTrace0 = DBApp.getFullTrace("en06s");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 440);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 415"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zx5", cols1);
		String [][] records_zx5 = new String[258][cols1.length];
		for(int i=0;i<258;i++)
		{
			records_zx5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zx5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zx5", records_zx5[i]);
		}
	
		int pageCount1 = (int)Math.ceil(258.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		DBApp.select("zx5");
	
		String[] ConditionColumns11 = {"d","n","m","c"};
		String[] ConditionColumnsValues11 = {"d0","n6","m0","c2"};
		DBApp.select("zx5", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"k"};
		String[] ConditionColumnsValues12 = {"k7"};
		DBApp.select("zx5", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx5", pageSelectPointer13,recordSelectPointer13);
	
		String selectTrace1 = DBApp.getFullTrace("zx5");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 264);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 258"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("dt", cols2);
		String [][] records_dt = new String[233][cols2.length];
		for(int i=0;i<233;i++)
		{
			records_dt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dt", records_dt[i]);
		}
	
		int pageCount2 = (int)Math.ceil(233.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("dt");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((233%DBApp.dataPageSize))-1;
		DBApp.select("dt", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"d"};
		String[] ConditionColumnsValues22 = {"d3"};
		DBApp.select("dt", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((233%DBApp.dataPageSize))-1;
		DBApp.select("dt", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("dt");
	
		String selectTrace2 = DBApp.getFullTrace("dt");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 240);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 233"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("mgy", cols0);
		String [][] records_mgy = new String[295][cols0.length];
		for(int i=0;i<295;i++)
		{
			records_mgy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mgy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mgy", records_mgy[i]);
		}
	
		int pageCount0 = (int)Math.ceil(295.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("mgy", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("mgy");
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a12"};
		DBApp.select("mgy", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("mgy");
	
		String selectTrace0 = DBApp.getFullTrace("mgy");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 301);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 295"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("bt", cols1);
		String [][] records_bt = new String[109][cols1.length];
		for(int i=0;i<109;i++)
		{
			records_bt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bt", records_bt[i]);
		}
	
		int pageCount1 = (int)Math.ceil(109.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c0"};
		DBApp.select("bt", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("bt", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("bt", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("bt");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("bt", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"b"};
		String[] ConditionColumnsValues15 = {"b1"};
		DBApp.select("bt", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("bt");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("bt", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("bt", pageSelectPointer18,recordSelectPointer18);
	
		String selectTrace1 = DBApp.getFullTrace("bt");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 120);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 109"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mdnh", cols0);
		String [][] records_mdnh = new String[254][cols0.length];
		for(int i=0;i<254;i++)
		{
			records_mdnh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mdnh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mdnh", records_mdnh[i]);
		}
	
		int pageCount0 = (int)Math.ceil(254.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e3"};
		DBApp.select("mdnh", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"b","i","g","k"};
		String[] ConditionColumnsValues02 = {"b1","i6","g2","k7"};
		DBApp.select("mdnh", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("mdnh");
	
		String[] ConditionColumns05 = {"e"};
		String[] ConditionColumnsValues05 = {"e1"};
		DBApp.select("mdnh", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("mdnh");
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a192"};
		DBApp.select("mdnh", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"b","e","g"};
		String[] ConditionColumnsValues08 = {"b0","e4","g5"};
		DBApp.select("mdnh", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"h"};
		String[] ConditionColumnsValues09 = {"h6"};
		DBApp.select("mdnh", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"l"};
		String[] ConditionColumnsValues010 = {"l3"};
		DBApp.select("mdnh", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("mdnh");
	
		DBApp.select("mdnh");
	
		String[] ConditionColumns014 = {"k"};
		String[] ConditionColumnsValues014 = {"k2"};
		DBApp.select("mdnh", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("mdnh");
	
		DBApp.select("mdnh");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer019,recordSelectPointer019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("mdnh");
	
		DBApp.select("mdnh");
	
		DBApp.select("mdnh");
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer024,recordSelectPointer024);
	
		String[] ConditionColumns025 = {"e"};
		String[] ConditionColumnsValues025 = {"e3"};
		DBApp.select("mdnh", ConditionColumns025, ConditionColumnsValues025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("mdnh", pageSelectPointer027,recordSelectPointer027);
	
		String[] ConditionColumns028 = {"g"};
		String[] ConditionColumnsValues028 = {"g2"};
		DBApp.select("mdnh", ConditionColumns028, ConditionColumnsValues028);
	
		String[] ConditionColumns029 = {"d"};
		String[] ConditionColumnsValues029 = {"d2"};
		DBApp.select("mdnh", ConditionColumns029, ConditionColumnsValues029);
	
		String selectTrace0 = DBApp.getFullTrace("mdnh");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 286);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 254"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("b20q", cols1);
		String [][] records_b20q = new String[350][cols1.length];
		for(int i=0;i<350;i++)
		{
			records_b20q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b20q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b20q", records_b20q[i]);
		}
	
		int pageCount1 = (int)Math.ceil(350.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("b20q");
	
		String[] ConditionColumns11 = {"a"};
		String[] ConditionColumnsValues11 = {"a57"};
		DBApp.select("b20q", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("b20q");
	
		DBApp.select("b20q");
	
		String[] ConditionColumns15 = {"a"};
		String[] ConditionColumnsValues15 = {"a272"};
		DBApp.select("b20q", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("b20q");
	
		String[] ConditionColumns17 = {"a"};
		String[] ConditionColumnsValues17 = {"a154"};
		DBApp.select("b20q", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("b20q");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("b20q");
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a34"};
		DBApp.select("b20q", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"a"};
		String[] ConditionColumnsValues115 = {"a21"};
		DBApp.select("b20q", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"a"};
		String[] ConditionColumnsValues116 = {"a246"};
		DBApp.select("b20q", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"a"};
		String[] ConditionColumnsValues117 = {"a165"};
		DBApp.select("b20q", ConditionColumns117, ConditionColumnsValues117);
	
		DBApp.select("b20q");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((350%DBApp.dataPageSize))-1;
		DBApp.select("b20q", pageSelectPointer119,recordSelectPointer119);
	
		String selectTrace1 = DBApp.getFullTrace("b20q");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 372);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 350"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w44l5", cols2);
		String [][] records_w44l5 = new String[270][cols2.length];
		for(int i=0;i<270;i++)
		{
			records_w44l5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w44l5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w44l5", records_w44l5[i]);
		}
	
		int pageCount2 = (int)Math.ceil(270.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns20 = {"d"};
		String[] ConditionColumnsValues20 = {"d3"};
		DBApp.select("w44l5", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("w44l5");
	
		String[] ConditionColumns22 = {"o"};
		String[] ConditionColumnsValues22 = {"o12"};
		DBApp.select("w44l5", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"f","k"};
		String[] ConditionColumnsValues25 = {"f0","k6"};
		DBApp.select("w44l5", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("w44l5");
	
		DBApp.select("w44l5");
	
		DBApp.select("w44l5");
	
		String[] ConditionColumns212 = {"c","o","k","a"};
		String[] ConditionColumnsValues212 = {"c2","o5","k7","a95"};
		DBApp.select("w44l5", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("w44l5");
	
		String[] ConditionColumns214 = {"j"};
		String[] ConditionColumnsValues214 = {"j4"};
		DBApp.select("w44l5", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("w44l5", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("w44l5");
	
		String[] ConditionColumns217 = {"c"};
		String[] ConditionColumnsValues217 = {"c2"};
		DBApp.select("w44l5", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"c"};
		String[] ConditionColumnsValues218 = {"c0"};
		DBApp.select("w44l5", ConditionColumns218, ConditionColumnsValues218);
	
		DBApp.select("w44l5");
	
		DBApp.select("w44l5");
	
		String selectTrace2 = DBApp.getFullTrace("w44l5");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 293);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 270"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ie", cols0);
		String [][] records_ie = new String[227][cols0.length];
		for(int i=0;i<227;i++)
		{
			records_ie[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ie[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ie", records_ie[i]);
		}
	
		int pageCount0 = (int)Math.ceil(227.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ie", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((227%DBApp.dataPageSize))-1;
		DBApp.select("ie", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("ie");
	
		DBApp.select("ie");
	
		String selectTrace0 = DBApp.getFullTrace("ie");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 233);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 227"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ia", cols1);
		String [][] records_ia = new String[235][cols1.length];
		for(int i=0;i<235;i++)
		{
			records_ia[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ia[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ia", records_ia[i]);
		}
	
		int pageCount1 = (int)Math.ceil(235.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("ia");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("ia");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"h"};
		String[] ConditionColumnsValues17 = {"h6"};
		DBApp.select("ia", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer111,recordSelectPointer111);
	
		String[] ConditionColumns112 = {"f"};
		String[] ConditionColumnsValues112 = {"f5"};
		DBApp.select("ia", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((235%DBApp.dataPageSize))-1;
		DBApp.select("ia", pageSelectPointer117,recordSelectPointer117);
	
		String[] ConditionColumns118 = {"g"};
		String[] ConditionColumnsValues118 = {"g0"};
		DBApp.select("ia", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		DBApp.select("ia");
	
		String selectTrace1 = DBApp.getFullTrace("ia");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 264);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 235"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qj", cols2);
		String [][] records_qj = new String[220][cols2.length];
		for(int i=0;i<220;i++)
		{
			records_qj[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qj[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qj", records_qj[i]);
		}
	
		int pageCount2 = (int)Math.ceil(220.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("qj");
	
		String[] ConditionColumns21 = {"f"};
		String[] ConditionColumnsValues21 = {"f5"};
		DBApp.select("qj", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"h"};
		String[] ConditionColumnsValues22 = {"h0"};
		DBApp.select("qj", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("qj");
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"h","c","e"};
		String[] ConditionColumnsValues27 = {"h0","c2","e0"};
		DBApp.select("qj", ConditionColumns27, ConditionColumnsValues27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("qj");
	
		String[] ConditionColumns210 = {"h"};
		String[] ConditionColumnsValues210 = {"h4"};
		DBApp.select("qj", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"d","i","g"};
		String[] ConditionColumnsValues211 = {"d2","i1","g0"};
		DBApp.select("qj", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"h"};
		String[] ConditionColumnsValues212 = {"h4"};
		DBApp.select("qj", ConditionColumns212, ConditionColumnsValues212);
	
		String[] ConditionColumns213 = {"c"};
		String[] ConditionColumnsValues213 = {"c0"};
		DBApp.select("qj", ConditionColumns213, ConditionColumnsValues213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((220%DBApp.dataPageSize))-1;
		DBApp.select("qj", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"i"};
		String[] ConditionColumnsValues216 = {"i6"};
		DBApp.select("qj", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"i"};
		String[] ConditionColumnsValues217 = {"i3"};
		DBApp.select("qj", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("qj");
	
		DBApp.select("qj");
	
		String[] ConditionColumns220 = {"c"};
		String[] ConditionColumnsValues220 = {"c1"};
		DBApp.select("qj", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"f"};
		String[] ConditionColumnsValues221 = {"f2"};
		DBApp.select("qj", ConditionColumns221, ConditionColumnsValues221);
	
		DBApp.select("qj");
	
		DBApp.select("qj");
	
		DBApp.select("qj");
	
		String selectTrace2 = DBApp.getFullTrace("qj");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 247);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 220"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ljt", cols0);
		String [][] records_ljt = new String[101][cols0.length];
		for(int i=0;i<101;i++)
		{
			records_ljt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ljt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ljt", records_ljt[i]);
		}
	
		int pageCount0 = (int)Math.ceil(101.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("ljt");
	
		DBApp.select("ljt");
	
		String[] ConditionColumns08 = {"g"};
		String[] ConditionColumnsValues08 = {"g6"};
		DBApp.select("ljt", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"e"};
		String[] ConditionColumnsValues011 = {"e4"};
		DBApp.select("ljt", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("ljt");
	
		String[] ConditionColumns013 = {"e","f","c"};
		String[] ConditionColumnsValues013 = {"e0","f0","c0"};
		DBApp.select("ljt", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"b","g"};
		String[] ConditionColumnsValues015 = {"b1","g5"};
		DBApp.select("ljt", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("ljt");
	
		String[] ConditionColumns017 = {"a","f","c"};
		String[] ConditionColumnsValues017 = {"a99","f3","c0"};
		DBApp.select("ljt", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"g"};
		String[] ConditionColumnsValues018 = {"g4"};
		DBApp.select("ljt", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("ljt");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((101%DBApp.dataPageSize))-1;
		DBApp.select("ljt", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("ljt");
	
		DBApp.select("ljt");
	
		String selectTrace0 = DBApp.getFullTrace("ljt");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 126);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 101"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sou", cols1);
		String [][] records_sou = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			records_sou[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sou[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sou", records_sou[i]);
		}
	
		int pageCount1 = (int)Math.ceil(6.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"n"};
		String[] ConditionColumnsValues11 = {"n3"};
		DBApp.select("sou", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"g"};
		String[] ConditionColumnsValues12 = {"g4"};
		DBApp.select("sou", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"j","k","c","i"};
		String[] ConditionColumnsValues18 = {"j5","k5","c2","i5"};
		DBApp.select("sou", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("sou");
	
		DBApp.select("sou");
	
		String[] ConditionColumns112 = {"n"};
		String[] ConditionColumnsValues112 = {"n5"};
		DBApp.select("sou", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("sou");
	
		String[] ConditionColumns114 = {"e","i","n","c","k"};
		String[] ConditionColumnsValues114 = {"e3","i3","n3","c0","k3"};
		DBApp.select("sou", ConditionColumns114, ConditionColumnsValues114);
	
		DBApp.select("sou");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("sou");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((6%DBApp.dataPageSize))-1;
		DBApp.select("sou", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("sou");
	
		DBApp.select("sou");
	
		String selectTrace1 = DBApp.getFullTrace("sou");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 29);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 6"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("ylw", cols2);
		String [][] records_ylw = new String[150][cols2.length];
		for(int i=0;i<150;i++)
		{
			records_ylw[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ylw[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ylw", records_ylw[i]);
		}
	
		int pageCount2 = (int)Math.ceil(150.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a64"};
		DBApp.select("ylw", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("ylw");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"a"};
		String[] ConditionColumnsValues23 = {"a78"};
		DBApp.select("ylw", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"a"};
		String[] ConditionColumnsValues25 = {"a10"};
		DBApp.select("ylw", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("ylw");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a69"};
		DBApp.select("ylw", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("ylw");
	
		DBApp.select("ylw");
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer216,recordSelectPointer216);
	
		DBApp.select("ylw");
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer218,recordSelectPointer218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer219,recordSelectPointer219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer220,recordSelectPointer220);
	
		DBApp.select("ylw");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer222,recordSelectPointer222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer223,recordSelectPointer223);
	
		String[] ConditionColumns224 = {"a"};
		String[] ConditionColumnsValues224 = {"a43"};
		DBApp.select("ylw", ConditionColumns224, ConditionColumnsValues224);
	
		DBApp.select("ylw");
	
		String[] ConditionColumns226 = {"a"};
		String[] ConditionColumnsValues226 = {"a79"};
		DBApp.select("ylw", ConditionColumns226, ConditionColumnsValues226);
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("ylw", pageSelectPointer227,recordSelectPointer227);
	
		String selectTrace2 = DBApp.getFullTrace("ylw");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 180);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 150"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g9c", cols3);
		String [][] records_g9c = new String[412][cols3.length];
		for(int i=0;i<412;i++)
		{
			records_g9c[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g9c[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g9c", records_g9c[i]);
		}
	
		int pageCount3 = (int)Math.ceil(412.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("g9c");
	
		DBApp.select("g9c");
	
		String[] ConditionColumns32 = {"g"};
		String[] ConditionColumnsValues32 = {"g4"};
		DBApp.select("g9c", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("g9c");
	
		DBApp.select("g9c");
	
		DBApp.select("g9c");
	
		String[] ConditionColumns36 = {"f"};
		String[] ConditionColumnsValues36 = {"f2"};
		DBApp.select("g9c", ConditionColumns36, ConditionColumnsValues36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("g9c", pageSelectPointer37,recordSelectPointer37);
	
		String[] ConditionColumns38 = {"g","d","f"};
		String[] ConditionColumnsValues38 = {"g0","d3","f1"};
		DBApp.select("g9c", ConditionColumns38, ConditionColumnsValues38);
	
		DBApp.select("g9c");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("g9c", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("g9c");
	
		String[] ConditionColumns312 = {"c","b"};
		String[] ConditionColumnsValues312 = {"c1","b1"};
		DBApp.select("g9c", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("g9c", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("g9c", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"c"};
		String[] ConditionColumnsValues315 = {"c0"};
		DBApp.select("g9c", ConditionColumns315, ConditionColumnsValues315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("g9c", pageSelectPointer316,recordSelectPointer316);
	
		String[] ConditionColumns317 = {"g"};
		String[] ConditionColumnsValues317 = {"g5"};
		DBApp.select("g9c", ConditionColumns317, ConditionColumnsValues317);
	
		String selectTrace3 = DBApp.getFullTrace("g9c");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 432);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 412"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("d000", cols0);
		String [][] records_d000 = new String[483][cols0.length];
		for(int i=0;i<483;i++)
		{
			records_d000[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d000[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d000", records_d000[i]);
		}
	
		int pageCount0 = (int)Math.ceil(483.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("d000", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("d000");
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a117"};
		DBApp.select("d000", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a434"};
		DBApp.select("d000", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("d000", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a464"};
		DBApp.select("d000", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("d000");
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a18"};
		DBApp.select("d000", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("d000", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((483%DBApp.dataPageSize))-1;
		DBApp.select("d000", pageSelectPointer09,recordSelectPointer09);
	
		String selectTrace0 = DBApp.getFullTrace("d000");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 495);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 483"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("z1an1", cols0);
		String [][] records_z1an1 = new String[239][cols0.length];
		for(int i=0;i<239;i++)
		{
			records_z1an1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z1an1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z1an1", records_z1an1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(239.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		String[] ConditionColumns00 = {"e","i","d"};
		String[] ConditionColumnsValues00 = {"e2","i1","d2"};
		DBApp.select("z1an1", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("z1an1");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((239%DBApp.dataPageSize))-1;
		DBApp.select("z1an1", pageSelectPointer02,recordSelectPointer02);
	
		String selectTrace0 = DBApp.getFullTrace("z1an1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 244);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 239"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("vx9", cols1);
		String [][] records_vx9 = new String[465][cols1.length];
		for(int i=0;i<465;i++)
		{
			records_vx9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vx9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vx9", records_vx9[i]);
		}
	
		int pageCount1 = (int)Math.ceil(465.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("vx9");
	
		DBApp.select("vx9");
	
		DBApp.select("vx9");
	
		String[] ConditionColumns13 = {"c"};
		String[] ConditionColumnsValues13 = {"c1"};
		DBApp.select("vx9", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("vx9");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((465%DBApp.dataPageSize))-1;
		DBApp.select("vx9", pageSelectPointer15,recordSelectPointer15);
	
		String selectTrace1 = DBApp.getFullTrace("vx9");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 473);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 465"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("i57", cols2);
		String [][] records_i57 = new String[91][cols2.length];
		for(int i=0;i<91;i++)
		{
			records_i57[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i57[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i57", records_i57[i]);
		}
	
		int pageCount2 = (int)Math.ceil(91.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a25"};
		DBApp.select("i57", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("i57");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("i57");
	
		DBApp.select("i57");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a85"};
		DBApp.select("i57", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"a"};
		String[] ConditionColumnsValues28 = {"a25"};
		DBApp.select("i57", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("i57");
	
		DBApp.select("i57");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"a"};
		String[] ConditionColumnsValues214 = {"a28"};
		DBApp.select("i57", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"a"};
		String[] ConditionColumnsValues216 = {"a38"};
		DBApp.select("i57", ConditionColumns216, ConditionColumnsValues216);
	
		DBApp.select("i57");
	
		String[] ConditionColumns218 = {"a"};
		String[] ConditionColumnsValues218 = {"a11"};
		DBApp.select("i57", ConditionColumns218, ConditionColumnsValues218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((91%DBApp.dataPageSize))-1;
		DBApp.select("i57", pageSelectPointer219,recordSelectPointer219);
	
		String[] ConditionColumns220 = {"a"};
		String[] ConditionColumnsValues220 = {"a5"};
		DBApp.select("i57", ConditionColumns220, ConditionColumnsValues220);
	
		String selectTrace2 = DBApp.getFullTrace("i57");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 114);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 91"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s2", cols3);
		String [][] records_s2 = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			records_s2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s2", records_s2[i]);
		}
	
		int pageCount3 = (int)Math.ceil(2.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("s2");
	
		DBApp.select("s2");
	
		DBApp.select("s2");
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer33,recordSelectPointer33);
	
		String[] ConditionColumns34 = {"k"};
		String[] ConditionColumnsValues34 = {"k1"};
		DBApp.select("s2", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("s2");
	
		String[] ConditionColumns38 = {"h","g","m","i","f"};
		String[] ConditionColumnsValues38 = {"h1","g1","m1","i1","f1"};
		DBApp.select("s2", ConditionColumns38, ConditionColumnsValues38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("s2");
	
		DBApp.select("s2");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer312,recordSelectPointer312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((2%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"b","m","a"};
		String[] ConditionColumnsValues315 = {"b1","m1","a1"};
		DBApp.select("s2", ConditionColumns315, ConditionColumnsValues315);
	
		String selectTrace3 = DBApp.getFullTrace("s2");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 20);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 2"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e"};
		DBApp.createTable("pt552", cols4);
		String [][] records_pt552 = new String[425][cols4.length];
		for(int i=0;i<425;i++)
		{
			records_pt552[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_pt552[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("pt552", records_pt552[i]);
		}
	
		int pageCount4 = (int)Math.ceil(425.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		DBApp.select("pt552");
	
		DBApp.select("pt552");
	
		String[] ConditionColumns42 = {"e"};
		String[] ConditionColumnsValues42 = {"e2"};
		DBApp.select("pt552", ConditionColumns42, ConditionColumnsValues42);
	
		String[] ConditionColumns43 = {"b"};
		String[] ConditionColumnsValues43 = {"b1"};
		DBApp.select("pt552", ConditionColumns43, ConditionColumnsValues43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("pt552", pageSelectPointer44,recordSelectPointer44);
	
		DBApp.select("pt552");
	
		String[] ConditionColumns46 = {"b"};
		String[] ConditionColumnsValues46 = {"b0"};
		DBApp.select("pt552", ConditionColumns46, ConditionColumnsValues46);
	
		DBApp.select("pt552");
	
		DBApp.select("pt552");
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("pt552", pageSelectPointer49,recordSelectPointer49);
	
		int pageSelectPointer410 = genRandNum(pageCount4)-1;
		int recordSelectPointer410 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer410==pageCount4-1)
			recordSelectPointer410 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("pt552", pageSelectPointer410,recordSelectPointer410);
	
		String[] ConditionColumns411 = {"e","d"};
		String[] ConditionColumnsValues411 = {"e1","d1"};
		DBApp.select("pt552", ConditionColumns411, ConditionColumnsValues411);
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("pt552", pageSelectPointer412,recordSelectPointer412);
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("pt552", pageSelectPointer413,recordSelectPointer413);
	
		String selectTrace4 = DBApp.getFullTrace("pt552");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 441);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 425"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("l232", cols0);
		String [][] records_l232 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			records_l232[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l232[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l232", records_l232[i]);
		}
	
		int pageCount0 = (int)Math.ceil(7.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("l232");
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c1"};
		DBApp.select("l232", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"c","d"};
		String[] ConditionColumnsValues09 = {"c2","d2"};
		DBApp.select("l232", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("l232");
	
		DBApp.select("l232");
	
		String[] ConditionColumns012 = {"e"};
		String[] ConditionColumnsValues012 = {"e1"};
		DBApp.select("l232", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("l232");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("l232");
	
		String[] ConditionColumns016 = {"e"};
		String[] ConditionColumnsValues016 = {"e1"};
		DBApp.select("l232", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"d"};
		String[] ConditionColumnsValues017 = {"d0"};
		DBApp.select("l232", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"d"};
		String[] ConditionColumnsValues018 = {"d2"};
		DBApp.select("l232", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("l232");
	
		String[] ConditionColumns020 = {"c"};
		String[] ConditionColumnsValues020 = {"c0"};
		DBApp.select("l232", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("l232");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer023,recordSelectPointer023);
	
		String[] ConditionColumns024 = {"b","e"};
		String[] ConditionColumnsValues024 = {"b1","e0"};
		DBApp.select("l232", ConditionColumns024, ConditionColumnsValues024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer025,recordSelectPointer025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((7%DBApp.dataPageSize))-1;
		DBApp.select("l232", pageSelectPointer027,recordSelectPointer027);
	
		String selectTrace0 = DBApp.getFullTrace("l232");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 37);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 7"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("f25b", cols1);
		String [][] records_f25b = new String[244][cols1.length];
		for(int i=0;i<244;i++)
		{
			records_f25b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f25b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f25b", records_f25b[i]);
		}
	
		int pageCount1 = (int)Math.ceil(244.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns10 = {"i","g","j"};
		String[] ConditionColumnsValues10 = {"i8","g3","j3"};
		DBApp.select("f25b", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((244%DBApp.dataPageSize))-1;
		DBApp.select("f25b", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("f25b");
	
		String[] ConditionColumns13 = {"e"};
		String[] ConditionColumnsValues13 = {"e1"};
		DBApp.select("f25b", ConditionColumns13, ConditionColumnsValues13);
	
		String selectTrace1 = DBApp.getFullTrace("f25b");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 250);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 244"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1119);
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